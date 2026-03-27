import pytest
import pytest_asyncio
import aiosqlite
from db import init_db, create_session, join_session, get_session, add_participant_stops, get_participants


@pytest_asyncio.fixture
async def db():
    conn = await aiosqlite.connect(":memory:")
    await init_db(conn)
    yield conn
    await conn.close()


@pytest.mark.asyncio
async def test_create_session(db):
    session = await create_session(db, "Test Session", "Daniel")
    assert session["code"]
    assert len(session["code"]) == 32
    assert session["creator_name"] == "Daniel"


@pytest.mark.asyncio
async def test_join_session(db):
    session = await create_session(db, "Test Session", "Daniel")
    participant = await join_session(db, session["code"], "Petra")
    assert participant["name"] == "Petra"
    assert participant["session_code"] == session["code"]


@pytest.mark.asyncio
async def test_join_nonexistent_session(db):
    result = await join_session(db, "nonexistent", "Petra")
    assert result is None


@pytest.mark.asyncio
async def test_get_session(db):
    session = await create_session(db, "Test Session", "Daniel")
    fetched = await get_session(db, session["code"])
    assert fetched["code"] == session["code"]
    assert fetched["creator_name"] == "Daniel"


@pytest.mark.asyncio
async def test_add_stops(db):
    session = await create_session(db, "Test Session", "Daniel")
    participant = await join_session(db, session["code"], "Petra")
    await add_participant_stops(db, participant["id"], start_stop="Anděl", end_stop="Florenc")
    participants = await get_participants(db, session["code"])
    petra = [p for p in participants if p["name"] == "Petra"][0]
    assert petra["start_stop"] == "Anděl"
    assert petra["end_stop"] == "Florenc"


@pytest.mark.asyncio
async def test_creator_is_participant(db):
    session = await create_session(db, "Test Session", "Daniel")
    participants = await get_participants(db, session["code"])
    assert len(participants) == 1
    assert participants[0]["name"] == "Daniel"
