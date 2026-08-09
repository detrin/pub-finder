import aiosqlite
import pytest
import pytest_asyncio

from backend.db import (
    add_participant_stops,
    create_session,
    get_participants,
    get_session,
    init_db,
    join_session,
    reserve_places_requests,
)


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
async def test_places_request_reservations_enforce_a_daily_global_budget(db):
    assert await reserve_places_requests(db, 2, daily_limit=3) is True
    assert await reserve_places_requests(db, 1, daily_limit=3) is True
    assert await reserve_places_requests(db, 1, daily_limit=3) is False


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
    await add_participant_stops(
        db,
        session["code"],
        participant["id"],
        start_stop="Anděl",
        end_stop="Florenc",
    )
    participants = await get_participants(db, session["code"])
    petra = [p for p in participants if p["name"] == "Petra"][0]
    assert petra["start_stop"] == "Anděl"
    assert petra["end_stop"] == "Florenc"


@pytest.mark.asyncio
async def test_add_stops_persists_return_choice_when_stops_are_equal(db):
    session = await create_session(db, "Test Session", "Daniel")
    participant = (await get_participants(db, session["code"]))[0]

    await add_participant_stops(
        db,
        session["code"],
        participant["id"],
        start_stop="Anděl",
        end_stop="Anděl",
        same_start_end=False,
    )

    saved = (await get_participants(db, session["code"]))[0]
    assert saved["start_stop"] == "Anděl"
    assert saved["end_stop"] == "Anděl"
    assert saved["same_start_end"] is False


@pytest.mark.asyncio
async def test_add_stops_cannot_update_participant_from_another_session(db):
    first = await create_session(db, "First", "Alice")
    second = await create_session(db, "Second", "Bob")
    alice = (await get_participants(db, first["code"]))[0]

    updated = await add_participant_stops(
        db,
        second["code"],
        alice["id"],
        start_stop="Anděl",
        end_stop="Florenc",
    )

    assert updated is False
    alice_after = (await get_participants(db, first["code"]))[0]
    assert alice_after["start_stop"] == ""
    assert alice_after["end_stop"] == ""


@pytest.mark.asyncio
async def test_creator_is_participant(db):
    session = await create_session(db, "Test Session", "Daniel")
    participants = await get_participants(db, session["code"])
    assert len(participants) == 1
    assert participants[0]["name"] == "Daniel"
