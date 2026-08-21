import asyncio
import hashlib
import ipaddress
import logging
import re
import secrets
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urlsplit, urlunsplit

import aiosqlite
import httpx
from starlette.requests import Request

from .config import GA4_API_SECRET, GA4_MEASUREMENT_ID
from .db import connection_transaction

logger = logging.getLogger(__name__)

# The Measurement Protocol rejects these names outright, so server-side events
# have to be spelled differently even where they mirror an automatic GA4 event.
RESERVED_EVENT_NAMES = frozenset(
    {
        "app_remove",
        "first_open",
        "first_visit",
        "in_app_purchase",
        "session_start",
        "user_engagement",
    }
)

USER_ID_COOKIE = "_uid"
USER_ID_COOKIE_MAX_AGE = 60 * 60 * 24 * 365 * 2
SESSION_TIMEOUT = timedelta(minutes=30)
_CAMPAIGN_QUERY_PARAMETERS = {
    "utm_id": "campaign_id",
    "utm_campaign": "campaign",
    "utm_source": "source",
    "utm_medium": "medium",
    "utm_term": "term",
    "utm_content": "content",
}

_GA4_ENDPOINT = "https://www.google-analytics.com/mp/collect"
_GA4_CLIENT_ID_RE = re.compile(r"[1-9]\d*\.[1-9]\d*")
_LEGACY_USER_ID_RE = re.compile(r"[A-Za-z0-9_-]{22}")


class _Ga4HttpxLogFilter(logging.Filter):
    """Keep GA4 query credentials out of successful HTTPX request logs."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != "httpx" or not isinstance(record.args, tuple):
            return True
        record.args = tuple(
            _GA4_ENDPOINT if str(value).startswith(f"{_GA4_ENDPOINT}?") else value
            for value in record.args
        )
        return True


logging.getLogger("httpx").addFilter(_Ga4HttpxLogFilter())


@dataclass(frozen=True)
class VisitState:
    is_new_user: bool
    is_new_session: bool
    session_id: int
    session_number: int


@dataclass(frozen=True)
class CurrentSession:
    session_id: int
    session_number: int


@dataclass(frozen=True)
class PageContext:
    page_location: str
    page_referrer: str | None
    campaign: dict[str, str]


def page_context(request: Request) -> PageContext:
    """Build GA4 page attribution without forwarding arbitrary query data."""
    campaign: dict[str, str] = {}
    retained_query: list[tuple[str, str]] = []
    for query_name, value in request.query_params.multi_items():
        event_name = _CAMPAIGN_QUERY_PARAMETERS.get(query_name)
        cleaned = value.strip()[:100]
        if event_name is None or not cleaned or event_name in campaign:
            continue
        campaign[event_name] = cleaned
        retained_query.append((query_name, cleaned))

    page_location = str(
        request.url.replace(
            path=_analytics_path(request.url.path),
            query=urlencode(retained_query),
        )
    )[:1000]
    page_referrer = _safe_referrer(
        request.headers.get("referer"),
        current_hostname=request.url.hostname,
    )
    return PageContext(page_location, page_referrer, campaign)


def page_location_for_path(request: Request, path: str) -> str | None:
    """Turn a same-site browser pathname into a full GA4 page URL."""
    parsed = urlsplit(path)
    if parsed.scheme or parsed.netloc or not parsed.path.startswith("/"):
        return None
    return str(request.url.replace(path=_analytics_path(parsed.path), query=""))[:1000]


def _analytics_path(path: str) -> str:
    return re.sub(r"^/session/(?!join(?:/|$))[^/]+(?=/|$)", "/session/_", path)


def _safe_referrer(referrer: str | None, *, current_hostname: str | None) -> str | None:
    if not referrer:
        return None
    parsed = urlsplit(referrer)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    try:
        port = parsed.port
    except ValueError:
        return None
    hostname = parsed.hostname
    authority = f"[{hostname}]" if ":" in hostname else hostname
    if port and not (
        (parsed.scheme == "http" and port == 80) or (parsed.scheme == "https" and port == 443)
    ):
        authority = f"{authority}:{port}"
    path = (
        _analytics_path(parsed.path)
        if hostname.casefold() == (current_hostname or "").casefold()
        else "/"
    )
    return urlunsplit((parsed.scheme, authority, path or "/", "", ""))[:420]


def get_client_ip(request: Request) -> str | None:
    return request.client.host if request.client else None


def new_user_id() -> str:
    random_component = secrets.randbelow(2**31 - 1) + 1
    timestamp_component = max(1, int(datetime.now(timezone.utc).timestamp()))
    return f"{random_component}.{timestamp_component}"


def is_valid_user_id(user_id: str | None) -> bool:
    return bool(
        user_id and (_GA4_CLIENT_ID_RE.fullmatch(user_id) or _LEGACY_USER_ID_RE.fullmatch(user_id))
    )


def client_id_for_user_id(user_id: str) -> str:
    """Return a documented GA4 web client ID, preserving legacy cookie stability."""
    if _GA4_CLIENT_ID_RE.fullmatch(user_id):
        return user_id
    if not _LEGACY_USER_ID_RE.fullmatch(user_id):
        raise ValueError("A valid analytics user id is required")

    digest = hashlib.sha256(f"meetsomewhere-ga4-client-id-v1\0{user_id}".encode()).digest()
    modulus = 2**31 - 1
    first = int.from_bytes(digest[:8], "big") % modulus + 1
    second = int.from_bytes(digest[8:16], "big") % modulus + 1
    return f"{first}.{second}"


def schedule_analytics_task(application, awaitable: Awaitable[object]) -> asyncio.Task:
    """Retain analytics work so application shutdown can drain it safely."""
    tasks = getattr(application.state, "analytics_tasks", None)
    if tasks is None:
        tasks = set()
        application.state.analytics_tasks = tasks
    task = asyncio.create_task(awaitable)
    tasks.add(task)
    task.add_done_callback(tasks.discard)
    return task


async def drain_analytics_tasks(application) -> None:
    tasks = getattr(application.state, "analytics_tasks", set())
    while tasks:
        await asyncio.gather(*tuple(tasks), return_exceptions=True)


async def record_visit(
    db: aiosqlite.Connection,
    user_id: str,
    *,
    now: datetime | None = None,
) -> VisitState:
    """Upsert a visitor's tracking row.

    A new analytics session begins after 30 minutes without a tracked event.
    """
    now = now or datetime.now(timezone.utc)
    async with _visit_lock(db):
        async with connection_transaction(db):
            return await _record_visit_locked(db, user_id, now)


async def _record_visit_locked(
    db: aiosqlite.Connection,
    user_id: str,
    now: datetime,
) -> VisitState:
    today = now.date().isoformat()
    now_iso = now.isoformat()
    proposed_session_id = max(1, int(now.timestamp()))

    insert = await db.execute(
        "INSERT OR IGNORE INTO analytics_users "
        "(user_id, first_seen_at, last_seen_at, last_seen_date, visit_count, "
        "current_session_id) VALUES (?, ?, ?, ?, 1, ?)",
        (user_id, now_iso, now_iso, today, proposed_session_id),
    )
    if insert.rowcount == 1:
        await db.commit()
        return VisitState(True, True, proposed_session_id, 1)

    async with db.execute(
        "SELECT last_seen_at, visit_count, current_session_id "
        "FROM analytics_users WHERE user_id = ?",
        (user_id,),
    ) as cursor:
        row = await cursor.fetchone()
    if row is None:
        raise RuntimeError("Analytics visitor disappeared during session update")

    last_seen_at, visit_count, current_session_id = row
    last_seen = datetime.fromisoformat(last_seen_at)
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=timezone.utc)
    is_new_session = current_session_id is None or now - last_seen >= SESSION_TIMEOUT
    if is_new_session:
        visit_count += 1
        session_id = max(proposed_session_id, (current_session_id or 0) + 1)
    else:
        session_id = current_session_id
    await db.execute(
        "UPDATE analytics_users SET last_seen_at = ?, last_seen_date = ?, visit_count = ?, "
        "current_session_id = ? "
        "WHERE user_id = ?",
        (now_iso, today, visit_count, session_id, user_id),
    )
    await db.commit()
    return VisitState(False, is_new_session, session_id, visit_count)


async def get_current_session(
    db: aiosqlite.Connection,
    user_id: str,
) -> CurrentSession | None:
    async with db.execute(
        "SELECT current_session_id, visit_count FROM analytics_users WHERE user_id = ?",
        (user_id,),
    ) as cursor:
        row = await cursor.fetchone()
    if row is None or row[0] is None:
        return None
    return CurrentSession(session_id=row[0], session_number=row[1])


async def touch_current_session(
    db: aiosqlite.Connection,
    user_id: str,
    *,
    now: datetime | None = None,
) -> CurrentSession | None:
    """Record browser engagement without starting a new analytics session."""
    now = now or datetime.now(timezone.utc)
    async with _visit_lock(db):
        async with connection_transaction(db):
            session = await get_current_session(db, user_id)
            if session is None:
                return None
            now_iso = now.isoformat()
            await db.execute(
                "UPDATE analytics_users SET "
                "last_seen_at = CASE WHEN last_seen_at < ? THEN ? ELSE last_seen_at END, "
                "last_seen_date = CASE WHEN last_seen_at < ? THEN ? ELSE last_seen_date END "
                "WHERE user_id = ?",
                (now_iso, now_iso, now_iso, now.date().isoformat(), user_id),
            )
            await db.commit()
            return session


def _visit_lock(db: aiosqlite.Connection) -> asyncio.Lock:
    lock = getattr(db, "_analytics_visit_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        db._analytics_visit_lock = lock
    return lock


async def send_events(
    user_id: str,
    events: list[dict],
    *,
    country: str | None = None,
    ip_address: str | None = None,
) -> None:
    """Forward events to GA4 via the Measurement Protocol. Never raises."""
    if not GA4_MEASUREMENT_ID or not GA4_API_SECRET:
        return
    try:
        payload = measurement_payload(user_id, events, country=country, ip_address=ip_address)
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(
                _GA4_ENDPOINT,
                params={"measurement_id": GA4_MEASUREMENT_ID, "api_secret": GA4_API_SECRET},
                json=payload,
            )
            resp.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to send GA4 event (%s)", type(exc).__name__)


def measurement_payload(
    client_id: str,
    events: list[dict],
    *,
    country: str | None = None,
    ip_address: str | None = None,
) -> dict:
    payload = {"client_id": client_id_for_user_id(client_id), "events": events}
    if country and len(country) == 2 and country.isalpha():
        payload["user_location"] = {"country_id": country.upper()}
    if ip_address:
        try:
            address = ipaddress.ip_address(ip_address)
        except ValueError:
            pass
        else:
            if address.is_global:
                payload["ip_override"] = str(address)
    return payload


def page_view_events(
    *,
    page_location: str,
    page_title: str,
    session_id: int,
    session_number: int,
    is_new_user: bool,
    page_referrer: str | None = None,
    campaign: dict[str, str] | None = None,
) -> list[dict]:
    session_params = _session_params(session_id, session_number)
    events = []
    if campaign:
        events.append(
            {
                "name": "campaign_details",
                "params": {**campaign, **session_params},
            }
        )
    page_params = {
        "page_location": page_location,
        "page_title": page_title,
        "engagement_time_msec": 1,
        "visitor_type": "new" if is_new_user else "returning",
        **session_params,
    }
    if page_referrer:
        page_params["page_referrer"] = page_referrer
    events.append(
        {
            "name": "page_view",
            "params": page_params,
        }
    )
    return events


def tool_used_event(
    *,
    tool_name: str,
    session_id: int,
    session_number: int,
    **extra_params: str,
) -> list[dict]:
    session_params = _session_params(session_id, session_number)
    return [
        {
            "name": "tool_used",
            "params": {
                "tool_name": tool_name,
                "engagement_time_msec": 1,
                **session_params,
                **extra_params,
            },
        }
    ]


def engagement_event(
    *,
    page_location: str,
    engagement_time_msec: int,
    session_id: int,
    session_number: int,
) -> list[dict]:
    session_params = _session_params(session_id, session_number)
    return [
        {
            "name": "srv_engagement",
            "params": {
                "page_location": page_location,
                "engagement_time_msec": engagement_time_msec,
                **session_params,
            },
        }
    ]


def _session_params(session_id: int, session_number: int) -> dict[str, int]:
    if isinstance(session_id, bool) or not isinstance(session_id, int) or session_id <= 0:
        raise ValueError("GA4 session_id must be a positive integer")
    if (
        isinstance(session_number, bool)
        or not isinstance(session_number, int)
        or session_number <= 0
    ):
        raise ValueError("GA4 session_number must be a positive integer")
    return {"session_id": session_id, "session_number": session_number}
