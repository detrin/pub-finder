import aiosqlite
import pytest
import pytest_asyncio
from bs4 import BeautifulSoup
from httpx import ASGITransport, AsyncClient

from backend.app import app
from backend.db import create_session, get_participants, init_db, join_session


@pytest_asyncio.fixture(autouse=True)
async def setup_test_db():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)
    app.state.db = db
    app.state.all_stops = []
    yield
    await db.close()


@pytest.mark.asyncio
async def test_home_page():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/")
    assert response.status_code == 200
    assert "Meet Somewhere" in response.text
    assert "Create a Session" in response.text


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
        assert not form.has_attr("hx-indicator")
        status = form.select_one(".stop-save-status.htmx-indicator")
        assert status is not None
        assert status.get_text(strip=True) == "Saving…"
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
