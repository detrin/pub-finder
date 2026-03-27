import pytest
import pytest_asyncio
import aiosqlite
from httpx import ASGITransport, AsyncClient

from db import init_db
from main import app, app_state


@pytest_asyncio.fixture(autouse=True)
async def setup_test_db():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)
    app.state.db = db
    app_state["db"] = db
    app_state.setdefault("all_stops", [])
    yield
    await db.close()


@pytest.mark.asyncio
async def test_home_page():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/")
    assert response.status_code == 200
    assert "Pub Finder" in response.text
    assert "Create a Session" in response.text


@pytest.mark.asyncio
async def test_create_session():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/session/create",
            data={"creator_name": "Daniel"},
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
            data={"creator_name": "Daniel"},
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
            data={"creator_name": "Daniel"},
            follow_redirects=False,
        )
        location = create_resp.headers["location"]
        page = await client.get(location)
    assert page.status_code == 200
    assert "Daniel" in page.text
