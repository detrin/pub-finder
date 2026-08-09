import hashlib
import logging
import secrets
from datetime import datetime, timezone

import aiosqlite
import httpx

from .config import GA4_API_SECRET, GA4_MEASUREMENT_ID

logger = logging.getLogger(__name__)

USER_ID_COOKIE = "_uid"
USER_ID_COOKIE_MAX_AGE = 60 * 60 * 24 * 365 * 2

_GA4_ENDPOINT = "https://www.google-analytics.com/mp/collect"


def new_user_id() -> str:
    return secrets.token_urlsafe(16)


def _session_id(user_id: str, day: str) -> str:
    digest = hashlib.sha256(f"{user_id}\0{day}".encode()).hexdigest()
    return digest[:16]


async def record_visit(db: aiosqlite.Connection, user_id: str) -> tuple[bool, bool, int]:
    """Upsert a visitor's tracking row.

    Returns (is_new_user, is_new_session, session_number). A new session is
    counted once per UTC calendar day, since we track a single long-lived
    cookie rather than a separate session cookie.
    """
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    now_iso = now.isoformat()

    async with db.execute(
        "SELECT last_seen_date, visit_count FROM analytics_users WHERE user_id = ?",
        (user_id,),
    ) as cursor:
        row = await cursor.fetchone()

    if row is None:
        await db.execute(
            "INSERT INTO analytics_users (user_id, first_seen_at, last_seen_at, "
            "last_seen_date, visit_count) VALUES (?, ?, ?, ?, 1)",
            (user_id, now_iso, now_iso, today),
        )
        await db.commit()
        return True, True, 1

    last_seen_date, visit_count = row
    is_new_session = last_seen_date != today
    visit_count = visit_count + 1 if is_new_session else visit_count
    await db.execute(
        "UPDATE analytics_users SET last_seen_at = ?, last_seen_date = ?, visit_count = ? "
        "WHERE user_id = ?",
        (now_iso, today, visit_count, user_id),
    )
    await db.commit()
    return False, is_new_session, visit_count


async def send_events(
    user_id: str,
    events: list[dict],
) -> None:
    """Forward events to GA4 via the Measurement Protocol. Never raises."""
    if not GA4_MEASUREMENT_ID or not GA4_API_SECRET:
        return
    payload = {"client_id": user_id, "events": events}
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(
                _GA4_ENDPOINT,
                params={"measurement_id": GA4_MEASUREMENT_ID, "api_secret": GA4_API_SECRET},
                json=payload,
            )
            resp.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to send GA4 event: %s", exc)


def page_view_events(
    *,
    page_path: str,
    page_title: str,
    session_id: str,
    session_number: int,
    is_new_user: bool,
    is_new_session: bool,
) -> list[dict]:
    session_params = {"ga_session_id": session_id, "ga_session_number": session_number}
    events = []
    if is_new_user:
        events.append({"name": "first_visit", "params": dict(session_params)})
    if is_new_session:
        events.append({"name": "session_start", "params": dict(session_params)})
    events.append(
        {
            "name": "page_view",
            "params": {
                "page_location": page_path,
                "page_title": page_title,
                "engagement_time_msec": 1,
                **session_params,
            },
        }
    )
    return events


def tool_used_event(
    *,
    tool_name: str,
    session_id: str,
    session_number: int,
    **extra_params: str,
) -> list[dict]:
    return [
        {
            "name": "tool_used",
            "params": {
                "tool_name": tool_name,
                "engagement_time_msec": 1,
                "ga_session_id": session_id,
                "ga_session_number": session_number,
                **extra_params,
            },
        }
    ]


def engagement_event(
    *,
    page_path: str,
    engagement_time_msec: int,
    session_id: str,
    session_number: int,
) -> list[dict]:
    return [
        {
            "name": "user_engagement",
            "params": {
                "page_location": page_path,
                "engagement_time_msec": engagement_time_msec,
                "ga_session_id": session_id,
                "ga_session_number": session_number,
            },
        }
    ]


def session_id_for_today(user_id: str) -> str:
    return _session_id(user_id, datetime.now(timezone.utc).date().isoformat())
