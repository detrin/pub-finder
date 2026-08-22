import asyncio

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
from backend.places import cache_pubs_for_type


@pytest_asyncio.fixture
async def db():
    conn = await aiosqlite.connect(":memory:")
    await init_db(conn)
    yield conn
    await conn.close()


async def install_failing_session_participant_trigger(db):
    await db.executescript(
        """
        CREATE TRIGGER fail_named_session_participant_insert
        BEFORE INSERT ON participants
        WHEN EXISTS (
            SELECT 1 FROM sessions
            WHERE code = NEW.session_code AND name = 'Failing'
        )
        BEGIN
            SELECT RAISE(ABORT, 'participant insert failed');
        END;
        """
    )
    await db.commit()


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
    db,
):
    await install_failing_session_participant_trigger(db)

    with pytest.raises(aiosqlite.IntegrityError, match="participant insert failed"):
        await create_session(db, "Failing", initial_stops=("A", "B"))

    async with db.execute("SELECT COUNT(*) FROM sessions") as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with db.execute("SELECT COUNT(*) FROM participants") as cursor:
        assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_concurrent_session_creates_both_commit_atomically(db, monkeypatch):
    original_execute = db.execute
    original_commit = db.commit
    first_savepoint_open = asyncio.Event()
    second_savepoint_open = asyncio.Event()
    first_committed = asyncio.Event()

    async def execute(sql, parameters=None):
        task_name = asyncio.current_task().get_name()
        if sql.startswith("SAVEPOINT") and task_name == "first-create":
            cursor = await original_execute(sql, parameters)
            first_savepoint_open.set()
            await second_savepoint_open.wait()
            return cursor
        if sql.startswith("SAVEPOINT") and task_name == "second-create":
            await first_savepoint_open.wait()
            cursor = await original_execute(sql, parameters)
            second_savepoint_open.set()
            await first_committed.wait()
            return cursor
        return await original_execute(sql, parameters)

    async def commit():
        await original_commit()
        if asyncio.current_task().get_name() == "first-create":
            first_committed.set()

    monkeypatch.setattr(db, "execute", execute)
    monkeypatch.setattr(db, "commit", commit)
    first = asyncio.create_task(create_session(db, "First"), name="first-create")
    second = asyncio.create_task(create_session(db, "Second"), name="second-create")

    results = await asyncio.wait_for(
        asyncio.gather(first, second, return_exceptions=True),
        timeout=1,
    )

    monkeypatch.setattr(db, "execute", original_execute)
    monkeypatch.setattr(db, "commit", original_commit)
    assert all(isinstance(result, dict) for result in results)
    assert [(await get_session(db, result["code"]))["name"] for result in results] == [
        "First",
        "Second",
    ]


@pytest.mark.asyncio
async def test_failed_concurrent_session_create_cannot_undo_success(db, monkeypatch):
    await install_failing_session_participant_trigger(db)
    original_execute = db.execute
    original_commit = db.commit
    failing_savepoint_open = asyncio.Event()
    successful_savepoint_released = asyncio.Event()
    failure_rolled_back = asyncio.Event()

    async def execute(sql, parameters=None):
        task_name = asyncio.current_task().get_name()
        if sql.startswith("SAVEPOINT") and task_name == "failing-create":
            cursor = await original_execute(sql, parameters)
            failing_savepoint_open.set()
            await successful_savepoint_released.wait()
            return cursor
        if sql.startswith("SAVEPOINT") and task_name == "successful-create":
            await failing_savepoint_open.wait()
        cursor = await original_execute(sql, parameters)
        if sql.startswith("RELEASE") and task_name == "successful-create":
            successful_savepoint_released.set()
        if sql.startswith("RELEASE") and task_name == "failing-create":
            failure_rolled_back.set()
        return cursor

    async def commit():
        if asyncio.current_task().get_name() == "successful-create":
            await failure_rolled_back.wait()
        await original_commit()

    monkeypatch.setattr(db, "execute", execute)
    monkeypatch.setattr(db, "commit", commit)
    failing = asyncio.create_task(create_session(db, "Failing"), name="failing-create")
    successful = asyncio.create_task(create_session(db, "Successful"), name="successful-create")

    failed_result, successful_result = await asyncio.wait_for(
        asyncio.gather(failing, successful, return_exceptions=True),
        timeout=1,
    )

    monkeypatch.setattr(db, "execute", original_execute)
    monkeypatch.setattr(db, "commit", original_commit)
    assert isinstance(failed_result, aiosqlite.IntegrityError)
    assert isinstance(successful_result, dict)
    saved = await get_session(db, successful_result["code"])
    assert saved is not None
    assert saved["name"] == "Successful"


@pytest.mark.asyncio
async def test_session_create_is_atomic_against_an_unrelated_writer_commit(db, monkeypatch):
    original_execute = db.execute
    create_started = asyncio.Event()
    writer_committed = asyncio.Event()

    async def execute(sql, parameters=None):
        cursor = await original_execute(sql, parameters)
        if sql.startswith("SAVEPOINT") and asyncio.current_task().get_name() == "session-create":
            await writer_committed.wait()
        return cursor

    async def create():
        create_started.set()
        return await create_session(db, "Friday crew")

    async def write_usage():
        await create_started.wait()
        result = await reserve_places_requests(db, 1, daily_limit=3)
        writer_committed.set()
        return result

    monkeypatch.setattr(db, "execute", execute)
    created, reserved = await asyncio.wait_for(
        asyncio.gather(
            asyncio.create_task(create(), name="session-create"),
            asyncio.create_task(write_usage(), name="usage-writer"),
            return_exceptions=True,
        ),
        timeout=1,
    )

    monkeypatch.setattr(db, "execute", original_execute)
    assert isinstance(created, dict)
    assert reserved is True
    assert await get_session(db, created["code"]) is not None


@pytest.mark.asyncio
async def test_session_create_waits_for_an_open_writer_transaction_to_roll_back(db, monkeypatch):
    await db.executescript(
        """
        CREATE TRIGGER fail_cache_query_insert
        BEFORE INSERT ON pub_cache_queries
        BEGIN
            SELECT RAISE(ABORT, 'cache query insert failed');
        END;
        """
    )
    await db.commit()
    original_execute = db.execute
    partial_cache_write = asyncio.Event()
    create_started = asyncio.Event()

    async def execute(sql, parameters=None):
        cursor = await original_execute(sql, parameters)
        if sql.startswith("INSERT INTO pub_cache "):
            partial_cache_write.set()
            await create_started.wait()
        return cursor

    async def create_after_partial_write():
        await partial_cache_write.wait()
        create_started.set()
        return await create_session(db, "Friday crew")

    pub = {
        "place_id": "place-1",
        "name": "Test pub",
        "lat": 50.0,
        "lon": 14.0,
        "rating": 4.0,
        "rating_count": 10,
        "price_level": 1,
        "google_maps_url": "https://maps.google.com/",
        "opening_hours": None,
        "primary_type": "pub",
    }
    monkeypatch.setattr(db, "execute", execute)
    cache_result, create_result = await asyncio.wait_for(
        asyncio.gather(
            asyncio.create_task(
                cache_pubs_for_type(db, "Muzeum", "pub", 500, [pub]),
                name="cache-writer",
            ),
            asyncio.create_task(create_after_partial_write(), name="session-create"),
            return_exceptions=True,
        ),
        timeout=1,
    )

    monkeypatch.setattr(db, "execute", original_execute)
    assert isinstance(cache_result, aiosqlite.IntegrityError)
    assert isinstance(create_result, dict)
    async with db.execute("SELECT COUNT(*) FROM pub_cache") as cursor:
        assert (await cursor.fetchone())[0] == 0
    saved = await get_session(db, create_result["code"])
    assert saved is not None
    assert saved["name"] == "Friday crew"


@pytest.mark.asyncio
async def test_cancelled_writer_rolls_back_before_releasing_transaction_ownership(db, monkeypatch):
    original_execute = db.execute
    partial_cache_write = asyncio.Event()
    hold_writer = asyncio.Event()

    async def execute(sql, parameters=None):
        cursor = await original_execute(sql, parameters)
        if sql.startswith("INSERT INTO pub_cache "):
            partial_cache_write.set()
            await hold_writer.wait()
        return cursor

    pub = {
        "place_id": "place-1",
        "name": "Test pub",
        "lat": 50.0,
        "lon": 14.0,
        "rating": 4.0,
        "rating_count": 10,
        "price_level": 1,
        "google_maps_url": "https://maps.google.com/",
        "opening_hours": None,
        "primary_type": "pub",
    }
    monkeypatch.setattr(db, "execute", execute)
    writer = asyncio.create_task(
        cache_pubs_for_type(db, "Muzeum", "pub", 500, [pub]),
        name="cancelled-cache-writer",
    )
    await asyncio.wait_for(partial_cache_write.wait(), timeout=1)
    writer.cancel()

    with pytest.raises(asyncio.CancelledError):
        await writer

    monkeypatch.setattr(db, "execute", original_execute)
    assert db.in_transaction is False
    async with db.execute("SELECT COUNT(*) FROM pub_cache") as cursor:
        assert (await cursor.fetchone())[0] == 0
    session = await asyncio.wait_for(create_session(db, "Friday crew"), timeout=1)
    assert await get_session(db, session["code"]) is not None


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
async def test_add_stops_mirrors_the_end_when_same_start_end_is_enabled(db):
    session = await create_session(db, "Test Session", "Daniel")
    participant = (await get_participants(db, session["code"]))[0]

    await add_participant_stops(
        db,
        session["code"],
        participant["id"],
        start_stop="Anděl",
        end_stop="Florenc",
        same_start_end=True,
    )

    saved = (await get_participants(db, session["code"]))[0]
    assert saved["start_stop"] == "Anděl"
    assert saved["end_stop"] == "Anděl"
    assert saved["same_start_end"] is True


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
