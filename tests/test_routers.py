import asyncio
import re
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import aiosqlite
import pytest
import pytest_asyncio
from bs4 import BeautifulSoup
from httpx import ASGITransport, AsyncClient

import backend.app as app_module
import routers.track as track_router
from backend.analytics import record_visit
from backend.app import app
from backend.db import create_session, get_participants, init_db, join_session


@pytest_asyncio.fixture(autouse=True)
async def setup_test_db():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)
    app.state.db = db
    app.state.all_stops = []
    app.state.analytics_tasks = set()
    yield
    await app_module.drain_analytics_tasks(app)
    await db.close()


@pytest.mark.asyncio
async def test_home_page():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/")
    assert response.status_code == 200
    assert "Meet Somewhere" in response.text
    assert "Find a place that works for everyone." in response.text
    assert "Let’s meet" in response.text
    assert "Somewhere" in response.text
    assert "Náměstí Míru" not in response.text


@pytest.mark.asyncio
async def test_analytics_cookie_is_secure_and_reused_across_pages():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="https://test") as client:
        first = await client.get("/")
        first_user_id = client.cookies.get("_uid")
        second = await client.get("/how-it-works")

    set_cookie = first.headers["set-cookie"]
    assert first_user_id
    assert "HttpOnly" in set_cookie
    assert "Secure" in set_cookie
    assert "SameSite=lax" in set_cookie
    assert "_uid=" not in second.headers.get("set-cookie", "")


@pytest.mark.asyncio
async def test_invalid_analytics_cookie_is_replaced_with_a_server_identifier():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="https://test") as client:
        client.cookies.set("_uid", "attacker-chosen", domain="test.local", path="/")
        response = await client.get("/")

    replacement = client.cookies.get("_uid", domain="test.local", path="/")
    assert replacement != "attacker-chosen"
    assert re.fullmatch(r"[1-9]\d*\.[1-9]\d*", replacement)
    assert "_uid=" in response.headers["set-cookie"]


@pytest.mark.asyncio
async def test_engagement_beacon_reuses_the_numeric_session_and_full_page_url(monkeypatch):
    user_id = "123456789.1787313600"
    visit = await record_visit(
        app.state.db,
        user_id,
        now=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    send_events = AsyncMock()
    monkeypatch.setattr(track_router, "send_events", send_events)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("8.8.8.8", 12345)),
        base_url="https://test",
        cookies={"_uid": user_id},
    ) as client:
        response = await client.post(
            "/e",
            data={"path": "/how-it-works", "engagement_time_msec": "42000"},
        )
        await asyncio.sleep(0)

    assert response.status_code == 204
    send_events.assert_awaited_once()
    client_id, events = send_events.await_args.args
    assert client_id == user_id
    assert send_events.await_args.kwargs == {"ip_address": "8.8.8.8"}
    assert events[0]["params"] == {
        "page_location": "https://test/how-it-works",
        "engagement_time_msec": 42_000,
        "session_id": visit.session_id,
        "session_number": visit.session_number,
    }
    async with app.state.db.execute(
        "SELECT last_seen_at FROM analytics_users WHERE user_id = ?",
        (user_id,),
    ) as cursor:
        last_seen_at = (await cursor.fetchone())[0]
    assert datetime.fromisoformat(last_seen_at) > datetime(2020, 1, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_engagement_beacon_rejects_an_external_page_location(monkeypatch):
    user_id = "123456789.1787313600"
    await record_visit(app.state.db, user_id)
    send_events = AsyncMock()
    monkeypatch.setattr(track_router, "send_events", send_events)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="https://test",
        cookies={"_uid": user_id},
    ) as client:
        response = await client.post(
            "/e",
            data={"path": "https://attacker.example/private", "engagement_time_msec": "1000"},
        )
        await asyncio.sleep(0)

    assert response.status_code == 204
    send_events.assert_not_awaited()


@pytest.mark.asyncio
async def test_engagement_beacon_rejects_an_invalid_analytics_cookie(monkeypatch):
    await record_visit(app.state.db, "attacker-chosen")
    send_events = AsyncMock()
    monkeypatch.setattr(track_router, "send_events", send_events)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="https://test",
        cookies={"_uid": "attacker-chosen"},
    ) as client:
        response = await client.post(
            "/e",
            data={"path": "/how-it-works", "engagement_time_msec": "1000"},
        )
        await asyncio.sleep(0)

    assert response.status_code == 204
    send_events.assert_not_awaited()


@pytest.mark.asyncio
async def test_page_view_forwards_only_safe_campaign_attribution(monkeypatch):
    captured = {}
    sent = asyncio.Event()

    async def capture_events(client_id, events, *, ip_address=None):
        captured.update(client_id=client_id, events=events, ip_address=ip_address)
        sent.set()

    monkeypatch.setattr(app_module, "send_events", capture_events)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("8.8.8.8", 12345)),
        base_url="https://test",
    ) as client:
        response = await client.get(
            "/?code=secret&utm_source=newsletter&utm_medium=email&utm_campaign=summer",
            headers={
                "referer": "https://example.org/article?subscriber=42",
            },
        )
        await asyncio.wait_for(sent.wait(), timeout=1)

    assert response.status_code == 200
    assert captured["client_id"] == client.cookies.get("_uid")
    assert [event["name"] for event in captured["events"]] == [
        "campaign_details",
        "page_view",
    ]
    page_view = captured["events"][1]["params"]
    assert page_view["page_location"] == (
        "https://test/?utm_source=newsletter&utm_medium=email&utm_campaign=summer"
    )
    assert page_view["page_referrer"] == "https://example.org/"
    assert captured["ip_address"] == "8.8.8.8"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "data"),
    [
        ("/anything/search", {}),
        ("/session/create", {}),
    ],
)
async def test_tool_tracking_requires_a_successful_known_route(monkeypatch, path, data):
    send_events = AsyncMock()
    monkeypatch.setattr(app_module, "send_events", send_events)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="https://test") as client:
        await client.post(path, data=data)
        await app_module.drain_analytics_tasks(app)

    send_events.assert_not_awaited()


@pytest.mark.asyncio
async def test_language_switch_defaults_to_english_and_can_set_czech():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        english = await client.get("/")
        switched = await client.get("/language/cs?next=/", follow_redirects=False)
        czech = await client.get("/")

    assert 'class="language-switch"' in english.text
    assert "Find a place that works for everyone." in english.text
    assert switched.status_code == 303
    assert "language=cs" in switched.headers["set-cookie"]
    assert "Najděte místo, které vyhovuje všem." in czech.text


@pytest.mark.asyncio
async def test_czech_session_translates_dynamic_planning_controls():
    session = await create_session(app.state.db, "Test Session", "Daniel")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        client.cookies.set("language", "cs")
        response = await client.get(f"/session/{session['code']}")

    assert "Hospody" in response.text
    assert "Celá cesta" in response.text
    assert "Everyone is ready." not in response.text
    assert "Přidejte ještě jednoho účastníka." in response.text
    assert ">saved<" not in response.text


@pytest.mark.asyncio
async def test_csp_allows_only_self_hosted_fonts_and_existing_map_assets():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/")

    csp = response.headers["content-security-policy"]
    assert "font-src 'self'" in csp
    assert "fonts.googleapis.com" not in csp
    assert "https://unpkg.com" in csp


@pytest.mark.asyncio
async def test_create_session():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/session/create",
            data={"session_name": "Test Session", "creator_name": "Daniel"},
            follow_redirects=False,
        )
    assert response.status_code == 303
    assert "/session/" in response.headers["location"]


@pytest.mark.asyncio
async def test_join_session():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post(
            "/session/create",
            data={"session_name": "Test Session", "creator_name": "Daniel"},
            follow_redirects=False,
        )
        location = create_resp.headers["location"]
        code = location.split("/session/")[1]
        join_resp = await client.get(
            f"/session/join?code={code}&name=Petra",
            follow_redirects=False,
        )
    assert join_resp.status_code == 303


@pytest.mark.asyncio
async def test_session_page():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_resp = await client.post(
            "/session/create",
            data={"session_name": "Test Session", "creator_name": "Daniel"},
            follow_redirects=False,
        )
        location = create_resp.headers["location"]
        page = await client.get(location, follow_redirects=True)
    assert page.status_code == 200
    assert "Daniel" in page.text


@pytest.mark.asyncio
async def test_participants_refresh_keeps_the_current_participant_card_ui():
    session = await create_session(app.state.db, "Test Session", "Daniel")
    await join_session(app.state.db, session["code"], "Petra")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/session/{session['code']}/participants")

    assert 'class="participants-panel"' in response.text
    assert 'class="participant-row"' in response.text
    assert "Stops set" not in response.text


@pytest.mark.asyncio
async def test_session_page_autosaves_valid_stop_selections():
    session = await create_session(app.state.db, "Test Session", "Daniel")
    await join_session(app.state.db, session["code"], "Petra")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        page = await client.get(f"/session/{session['code']}")

    soup = BeautifulSoup(page.text, "html.parser")
    stop_forms = soup.select("form.stop-form")
    assert len(stop_forms) == 2
    for form in stop_forms:
        assert form["hx-trigger"] == (
            "change target:[data-stop-input], change target:[data-same-start-end]"
        )
        assert form["hx-sync"] == "this:replace"
        participant_id = form.select_one('input[name="participant_id"]')["value"]
        indicator_id = f"stop-save-status-{participant_id}"
        assert form["hx-indicator"] == f"#{indicator_id}"
        status = soup.select_one(f"#{indicator_id}")
        assert status is not None
        assert status.get_text(strip=True) == "saving"
        assert status["role"] == "status"
        assert status["aria-live"] == "polite"
    assert ">Save<" not in page.text


@pytest.mark.asyncio
async def test_update_stops_persists_disabled_return_when_stops_are_equal():
    session = await create_session(app.state.db, "Test Session", "Daniel")
    participant = (await get_participants(app.state.db, session["code"]))[0]
    app.state.all_stops = ["Anděl"]

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            f"/session/{session['code']}/stops",
            data={
                "participant_id": participant["id"],
                "start_stop": "Anděl",
                "end_stop": "Anděl",
            },
        )

    assert response.status_code == 200
    saved = (await get_participants(app.state.db, session["code"]))[0]
    assert saved["same_start_end"] is False


@pytest.mark.asyncio
async def test_update_stops_cannot_modify_participant_from_another_session():
    first = await create_session(app.state.db, "First", "Alice")
    second = await create_session(app.state.db, "Second", "Bob")
    alice = (await get_participants(app.state.db, first["code"]))[0]
    app.state.all_stops = ["Anděl", "Florenc"]

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            f"/session/{second['code']}/stops",
            data={
                "participant_id": alice["id"],
                "start_stop": "Anděl",
                "end_stop": "Florenc",
            },
        )

    assert response.status_code == 200
    assert "Participant not found in this session" in response.text
    alice_after = (await get_participants(app.state.db, first["code"]))[0]
    assert alice_after["start_stop"] == ""
    assert alice_after["end_stop"] == ""


@pytest.mark.asyncio
async def test_feedback_csp_allows_google_form_and_blocks_inline_handlers():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/feedback")

    policy = response.headers["content-security-policy"]
    assert "frame-src https://docs.google.com" in policy
    assert "script-src-attr 'none'" in policy


@pytest.mark.asyncio
async def test_invalid_session_redirect_renders_specific_home_message():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test", follow_redirects=True
    ) as client:
        response = await client.get("/session/not-valid/results")

    assert "This invite link is not valid." in response.text
    assert "Start a new plan" in response.text
    assert 'data-system-message="error"' in response.text


@pytest.mark.asyncio
async def test_session_page_does_not_emit_inline_event_handlers():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        create_response = await client.post(
            "/session/create",
            data={"session_name": "Test Session", "creator_name": "Daniel"},
            follow_redirects=False,
        )
        response = await client.get(
            create_response.headers["location"],
            follow_redirects=True,
        )

    assert " onclick=" not in response.text
    assert " onchange=" not in response.text
    assert "<script>" not in response.text
