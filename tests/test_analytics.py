"""Event shapes sent to GA4 via the Measurement Protocol.

The Measurement Protocol silently drops reserved event names (first_visit,
session_start, user_engagement), so every event we emit must use a name of our
own, and session/engagement metadata must use the parameter names GA4 actually
reads.
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from types import SimpleNamespace

import aiosqlite
import httpx
import pytest
from starlette.requests import Request

import backend.analytics as analytics
from backend.analytics import (
    RESERVED_EVENT_NAMES,
    engagement_event,
    page_view_events,
    record_visit,
    tool_used_event,
)
from backend.db import init_db


def _names(events: list[dict]) -> list[str]:
    return [event["name"] for event in events]


def test_new_user_id_uses_the_documented_ga4_web_client_id_shape():
    user_id = analytics.new_user_id()

    assert re.fullmatch(r"[1-9]\d*\.[1-9]\d*", user_id)
    assert analytics.is_valid_user_id(user_id)


def test_legacy_user_id_maps_stably_to_a_documented_ga4_client_id():
    legacy_user_id = "AbCdEfGhIjKlMnOpQrStUv"

    first = analytics.client_id_for_user_id(legacy_user_id)
    second = analytics.client_id_for_user_id(legacy_user_id)

    assert first == second
    assert re.fullmatch(r"[1-9]\d*\.[1-9]\d*", first)
    assert analytics.is_valid_user_id(legacy_user_id)


def test_invalid_user_id_cannot_become_a_ga4_client_id():
    with pytest.raises(ValueError, match="valid analytics user id"):
        analytics.client_id_for_user_id("attacker-chosen")


@pytest.mark.asyncio
async def test_transport_never_raises_for_an_invalid_user_id(monkeypatch):
    monkeypatch.setattr(analytics, "GA4_MEASUREMENT_ID", "G-TEST")
    monkeypatch.setattr(analytics, "GA4_API_SECRET", "test-secret")

    await analytics.send_events(
        "attacker-chosen",
        [{"name": "page_view", "params": {}}],
    )


@pytest.mark.asyncio
async def test_managed_analytics_tasks_are_retained_and_drained():
    application = SimpleNamespace(state=SimpleNamespace())
    started = asyncio.Event()
    release = asyncio.Event()

    async def worker():
        started.set()
        await release.wait()

    task = analytics.schedule_analytics_task(application, worker())
    await started.wait()

    assert task in application.state.analytics_tasks

    release.set()
    await analytics.drain_analytics_tasks(application)

    assert task.done()
    assert application.state.analytics_tasks == set()


def _request(
    url: str,
    *,
    referrer: str | None = None,
    client_host: str = "127.0.0.1",
    forwarded_for: str | None = None,
) -> Request:
    scheme, remainder = url.split("://", 1)
    authority, path_and_query = remainder.split("/", 1)
    path, separator, query = path_and_query.partition("?")
    headers = [(b"host", authority.encode())]
    if referrer:
        headers.append((b"referer", referrer.encode()))
    if forwarded_for:
        headers.append((b"x-forwarded-for", forwarded_for.encode()))
    return Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": scheme,
            "server": (authority, 443 if scheme == "https" else 80),
            "path": f"/{path}",
            "query_string": query.encode() if separator else b"",
            "headers": headers,
            "client": (client_host, 12345),
        }
    )


@pytest.mark.asyncio
async def test_first_recorded_visit_has_a_positive_numeric_session_id():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)

    try:
        visit = await record_visit(
            db,
            "stable-client-id",
            now=datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),
        )
    finally:
        await db.close()

    assert visit.is_new_user is True
    assert visit.is_new_session is True
    assert visit.session_number == 1
    assert isinstance(visit.session_id, int)
    assert visit.session_id > 0


@pytest.mark.asyncio
async def test_visit_with_less_than_thirty_minutes_idle_reuses_the_session():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)
    first_time = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)

    try:
        first = await record_visit(db, "stable-client-id", now=first_time)
        repeated = await record_visit(
            db,
            "stable-client-id",
            now=datetime(2026, 8, 21, 12, 29, 59, tzinfo=timezone.utc),
        )
    finally:
        await db.close()

    assert repeated.is_new_user is False
    assert repeated.is_new_session is False
    assert repeated.session_id == first.session_id
    assert repeated.session_number == 1


@pytest.mark.asyncio
async def test_visit_after_thirty_minutes_idle_starts_the_next_session():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)

    try:
        first = await record_visit(
            db,
            "stable-client-id",
            now=datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),
        )
        repeated = await record_visit(
            db,
            "stable-client-id",
            now=datetime(2026, 8, 21, 12, 30, tzinfo=timezone.utc),
        )
    finally:
        await db.close()

    assert repeated.is_new_user is False
    assert repeated.is_new_session is True
    assert repeated.session_id != first.session_id
    assert repeated.session_number == 2


@pytest.mark.asyncio
async def test_current_session_lookup_reuses_the_recorded_session_for_engagement():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)

    try:
        visit = await record_visit(
            db,
            "stable-client-id",
            now=datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),
        )
        current = await analytics.get_current_session(db, "stable-client-id")
    finally:
        await db.close()

    assert current is not None
    assert current.session_id == visit.session_id
    assert current.session_number == visit.session_number


@pytest.mark.asyncio
async def test_engagement_touch_extends_the_current_session_activity():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)

    try:
        first = await record_visit(
            db,
            "stable-client-id",
            now=datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),
        )
        touched = await analytics.touch_current_session(
            db,
            "stable-client-id",
            now=datetime(2026, 8, 21, 12, 45, tzinfo=timezone.utc),
        )
        repeated = await record_visit(
            db,
            "stable-client-id",
            now=datetime(2026, 8, 21, 13, 14, 59, tzinfo=timezone.utc),
        )
    finally:
        await db.close()

    assert touched is not None
    assert touched.session_id == first.session_id
    assert repeated.session_id == first.session_id
    assert repeated.is_new_session is False


@pytest.mark.asyncio
async def test_concurrent_first_events_share_one_visitor_session():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)
    now = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)

    try:
        visits = await asyncio.gather(
            record_visit(db, "stable-client-id", now=now),
            record_visit(db, "stable-client-id", now=now),
        )
    finally:
        await db.close()

    assert sum(visit.is_new_user for visit in visits) == 1
    assert {visit.session_id for visit in visits} == {1_787_313_600}
    assert {visit.session_number for visit in visits} == {1}


@pytest.mark.asyncio
async def test_concurrent_events_after_idle_share_one_new_session():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)
    await record_visit(
        db,
        "stable-client-id",
        now=datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),
    )

    try:
        visits = await asyncio.gather(
            record_visit(
                db,
                "stable-client-id",
                now=datetime(2026, 8, 21, 12, 30, tzinfo=timezone.utc),
            ),
            record_visit(
                db,
                "stable-client-id",
                now=datetime(2026, 8, 21, 12, 30, 1, tzinfo=timezone.utc),
            ),
        )
    finally:
        await db.close()

    assert {visit.session_id for visit in visits} == {1_787_315_400}
    assert {visit.session_number for visit in visits} == {2}


def test_page_view_of_a_brand_new_visitor_avoids_reserved_event_names():
    events = page_view_events(
        page_location="https://meetsomewhere.eu/",
        page_title="/",
        session_id=1_727_000_000,
        session_number=1,
        is_new_user=True,
    )

    assert not RESERVED_EVENT_NAMES.intersection(_names(events))


def test_page_view_marks_a_first_time_visitor_as_new():
    events = page_view_events(
        page_location="https://meetsomewhere.eu/",
        page_title="/",
        session_id=1_727_000_000,
        session_number=1,
        is_new_user=True,
    )

    page_view = next(event for event in events if event["name"] == "page_view")
    assert page_view["params"]["visitor_type"] == "new"


def test_page_view_marks_a_returning_visitor_as_returning():
    events = page_view_events(
        page_location="https://meetsomewhere.eu/",
        page_title="/",
        session_id=1_727_000_000,
        session_number=4,
        is_new_user=False,
    )

    page_view = next(event for event in events if event["name"] == "page_view")
    assert page_view["params"]["visitor_type"] == "returning"


def test_page_view_uses_the_session_parameter_names_ga4_reads():
    events = page_view_events(
        page_location="https://meetsomewhere.eu/",
        page_title="/",
        session_id=1_727_000_000,
        session_number=2,
        is_new_user=False,
    )

    params = events[0]["params"]
    assert params["session_id"] == 1_727_000_000
    assert params["session_number"] == 2


def test_tool_used_uses_the_session_parameter_names_ga4_reads():
    events = tool_used_event(tool_name="pub_search", session_id=1_727_000_000, session_number=2)

    params = events[0]["params"]
    assert params["tool_name"] == "pub_search"
    assert params["session_id"] == 1_727_000_000
    assert params["session_number"] == 2


def test_engagement_event_avoids_reserved_names_and_keeps_the_measured_time():
    events = engagement_event(
        page_location="https://meetsomewhere.eu/how-it-works",
        engagement_time_msec=42_000,
        session_id=1_727_000_000,
        session_number=1,
    )

    assert not RESERVED_EVENT_NAMES.intersection(_names(events))
    params = events[0]["params"]
    assert params["engagement_time_msec"] == 42_000
    assert params["session_id"] == 1_727_000_000


def test_event_builders_reject_a_non_numeric_session_id():
    with pytest.raises(ValueError, match="positive integer"):
        tool_used_event(tool_name="pub_search", session_id="abc123", session_number=1)


def test_page_context_forwards_attribution_without_leaking_other_query_values():
    request = _request(
        "https://meetsomewhere.eu/session/join?code=secret&name=Petra"
        "&utm_source=newsletter&utm_medium=email&utm_campaign=summer",
        referrer="https://example.org/articles/meet?subscriber=42",
    )

    context = analytics.page_context(request)

    assert context.page_location == (
        "https://meetsomewhere.eu/session/join"
        "?utm_source=newsletter&utm_medium=email&utm_campaign=summer"
    )
    assert context.page_referrer == "https://example.org/"
    assert context.campaign == {
        "source": "newsletter",
        "medium": "email",
        "campaign": "summer",
    }


def test_page_context_redacts_private_session_codes_from_the_page_path():
    request = _request(
        "https://meetsomewhere.eu/session/private-code/results",
        referrer="https://meetsomewhere.eu/session/other-private-code",
    )

    context = analytics.page_context(request)

    assert context.page_location == "https://meetsomewhere.eu/session/_/results"
    assert context.page_referrer == "https://meetsomewhere.eu/session/_"


def test_client_ip_uses_the_proxy_validated_request_client_not_a_raw_header():
    request = _request(
        "https://meetsomewhere.eu/",
        client_host="8.8.8.8",
        forwarded_for="1.1.1.1",
    )

    assert analytics.get_client_ip(request) == "8.8.8.8"


def test_page_view_sends_campaign_before_the_attributed_page_view():
    events = page_view_events(
        page_location="https://meetsomewhere.eu/?utm_source=newsletter",
        page_title="Meet Somewhere",
        page_referrer="https://example.org/",
        campaign={"source": "newsletter", "medium": "email"},
        session_id=1_727_000_000,
        session_number=1,
        is_new_user=True,
    )

    assert events == [
        {
            "name": "campaign_details",
            "params": {
                "source": "newsletter",
                "medium": "email",
                "session_id": 1_727_000_000,
                "session_number": 1,
            },
        },
        {
            "name": "page_view",
            "params": {
                "page_location": "https://meetsomewhere.eu/?utm_source=newsletter",
                "page_title": "Meet Somewhere",
                "page_referrer": "https://example.org/",
                "engagement_time_msec": 1,
                "visitor_type": "new",
                "session_id": 1_727_000_000,
                "session_number": 1,
            },
        },
    ]


def test_measurement_payload_uses_ga4_user_location_without_mutating_events():
    events = [{"name": "page_view", "params": {"page_location": "https://example.test/"}}]

    payload = analytics.measurement_payload("123456789.1787313600", events, country="CZ")

    assert payload == {
        "client_id": "123456789.1787313600",
        "events": events,
        "user_location": {"country_id": "CZ"},
    }
    assert events == [{"name": "page_view", "params": {"page_location": "https://example.test/"}}]


def test_measurement_payload_only_uses_a_public_ip_override():
    events = [{"name": "page_view", "params": {}}]

    public_payload = analytics.measurement_payload(
        "123456789.1787313600",
        events,
        ip_address="8.8.8.8",
    )
    private_payload = analytics.measurement_payload(
        "123456789.1787313600",
        events,
        ip_address="127.0.0.1",
    )

    assert public_payload["ip_override"] == "8.8.8.8"
    assert "ip_override" not in private_payload


@pytest.mark.asyncio
async def test_ga4_transport_errors_do_not_log_secrets(monkeypatch, caplog):
    class FailingClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def post(self, *args, **kwargs):
            raise RuntimeError("request failed with api_secret=top-secret")

    monkeypatch.setattr(analytics, "GA4_MEASUREMENT_ID", "G-TEST")
    monkeypatch.setattr(analytics, "GA4_API_SECRET", "top-secret")
    monkeypatch.setattr(analytics.httpx, "AsyncClient", lambda **kwargs: FailingClient())

    with caplog.at_level(logging.WARNING):
        await analytics.send_events(
            "123456789.1787313600",
            [{"name": "page_view", "params": {}}],
        )

    assert "top-secret" not in caplog.text
    assert "api_secret=" not in caplog.text
    assert "RuntimeError" in caplog.text


@pytest.mark.asyncio
async def test_successful_ga4_transport_redacts_the_query_credential_from_httpx_logs(
    monkeypatch,
    caplog,
):
    real_client = httpx.AsyncClient

    def ga4_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204, request=request)

    monkeypatch.setattr(analytics, "GA4_MEASUREMENT_ID", "G-TEST")
    monkeypatch.setattr(analytics, "GA4_API_SECRET", "success-secret")
    monkeypatch.setattr(
        analytics.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(
            transport=httpx.MockTransport(ga4_response),
            **kwargs,
        ),
    )

    with caplog.at_level(logging.INFO, logger="httpx"):
        await analytics.send_events(
            "123456789.1787313600",
            [{"name": "page_view", "params": {}}],
        )

    assert "HTTP Request: POST https://www.google-analytics.com/mp/collect" in caplog.text
    assert "success-secret" not in caplog.text
    assert "measurement_id=" not in caplog.text
    assert "api_secret=" not in caplog.text
