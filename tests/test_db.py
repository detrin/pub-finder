import asyncio
from unittest.mock import AsyncMock

import aiosqlite
import pytest
import pytest_asyncio

from backend.db import (
    add_participant,
    add_participant_stops,
    create_session,
    get_participants,
    get_session,
    init_db,
    join_session,
    remove_participant,
    reserve_places_requests,
    update_participant_name,
)


@pytest_asyncio.fixture
async def db():
    conn = await aiosqlite.connect(":memory:")
    await init_db(conn)
    yield conn
    await conn.close()


@pytest.mark.asyncio
async def test_init_db_adds_numeric_session_id_to_existing_analytics_table():
    legacy = await aiosqlite.connect(":memory:")
    await legacy.execute(
        """
        CREATE TABLE analytics_users (
            user_id TEXT PRIMARY KEY,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            visit_count INTEGER NOT NULL DEFAULT 1,
            country TEXT
        )
        """
    )
    await legacy.execute(
        "INSERT INTO analytics_users "
        "(user_id, first_seen_at, last_seen_at, last_seen_date, visit_count) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            "123456789.1787313600",
            "2026-08-20T12:00:00+00:00",
            "2026-08-21T12:00:00+00:00",
            "2026-08-21",
            4,
        ),
    )

    try:
        await init_db(legacy)
        async with legacy.execute("PRAGMA table_info(analytics_users)") as cursor:
            columns = {row[1] for row in await cursor.fetchall()}
        async with legacy.execute(
            "SELECT visit_count, current_session_id FROM analytics_users"
        ) as cursor:
            migrated_user = await cursor.fetchone()
    finally:
        await legacy.close()

    assert "current_session_id" in columns
    assert migrated_user == (4, 1_787_313_600)


@pytest.mark.asyncio
async def test_create_session(db):
    session = await create_session(db, "Test Session", "Daniel")
    assert session["code"]
    assert len(session["code"]) == 32
    assert session["creator_name"] == "Daniel"


@pytest.mark.asyncio
async def test_create_session_rolls_back_session_and_participants_when_slot_insert_fails(
    db, monkeypatch
):
    monkeypatch.setattr(
        db,
        "executemany",
        AsyncMock(side_effect=RuntimeError("participant insert failed")),
    )

    with pytest.raises(RuntimeError, match="participant insert failed"):
        await create_session(db, "Friday crew", initial_stops=("A", "B"))

    async with db.execute("SELECT COUNT(*) FROM sessions") as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with db.execute("SELECT COUNT(*) FROM participants") as cursor:
        assert (await cursor.fetchone())[0] == 0


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
async def test_concurrent_joins_cannot_claim_two_slots_with_the_same_name(db):
    session = await create_session(db, "Test Session")

    await asyncio.gather(
        join_session(db, session["code"], "Alice"),
        join_session(db, session["code"], "Alice"),
    )

    participants = await get_participants(db, session["code"])
    assert [participant["name"] for participant in participants] == ["Alice", ""]


@pytest.mark.asyncio
async def test_concurrent_name_edits_cannot_create_duplicate_names(db):
    session = await create_session(db, "Test Session")
    participants = await get_participants(db, session["code"])

    results = await asyncio.gather(
        update_participant_name(db, session["code"], participants[0]["id"], "Alice"),
        update_participant_name(db, session["code"], participants[1]["id"], "Alice"),
    )

    saved = await get_participants(db, session["code"])
    assert results.count(True) == 1
    assert [participant["name"] for participant in saved].count("Alice") == 1


@pytest.mark.asyncio
async def test_concurrent_removals_preserve_two_participant_slots(db):
    session = await create_session(db, "Test Session", "Alice")
    bob = await add_participant(db, session["code"], "Bob")
    carol = await add_participant(db, session["code"], "Carol")

    results = await asyncio.gather(
        remove_participant(db, bob["id"], session["code"]),
        remove_participant(db, carol["id"], session["code"]),
    )

    assert results.count(True) == 1
    assert len(await get_participants(db, session["code"])) == 2


@pytest.mark.asyncio
async def test_concurrent_additions_cannot_exceed_twenty_participants(db):
    session = await create_session(db, "Test Session", "P1")
    for index in range(2, 20):
        await add_participant(db, session["code"], f"P{index}")

    await asyncio.gather(
        add_participant(db, session["code"], "P20"),
        add_participant(db, session["code"], "P21"),
    )

    assert len(await get_participants(db, session["code"])) == 20


@pytest.mark.asyncio
async def test_invite_join_cannot_exceed_twenty_participants(db):
    session = await create_session(db, "Test Session", "P1")
    for index in range(2, 21):
        await add_participant(db, session["code"], f"P{index}")

    participant = await join_session(db, session["code"], "Overflow")

    assert participant is None
    assert len(await get_participants(db, session["code"])) == 20


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
