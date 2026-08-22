import asyncio
import logging
import secrets
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

logger = logging.getLogger(__name__)


@asynccontextmanager
async def connection_transaction(db: aiosqlite.Connection) -> AsyncIterator[None]:
    lock = getattr(db, "_transaction_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        db._transaction_lock = lock
    async with lock:
        try:
            yield
        except BaseException:
            await asyncio.shield(db.rollback())
            raise


async def init_db(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            creator_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            active_search_id TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS participants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_code TEXT NOT NULL REFERENCES sessions(code),
            name TEXT NOT NULL,
            start_stop TEXT DEFAULT '',
            end_stop TEXT DEFAULT '',
            same_start_end INTEGER DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pub_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stop_name TEXT NOT NULL,
            place_id TEXT NOT NULL,
            name TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            rating REAL,
            rating_count INTEGER,
            price_level INTEGER,
            google_maps_url TEXT,
            opening_hours TEXT,
            primary_type TEXT DEFAULT '',
            cached_at TEXT NOT NULL,
            UNIQUE(stop_name, place_id)
        );

        CREATE TABLE IF NOT EXISTS pub_cache_queries (
            stop_name TEXT NOT NULL,
            place_type TEXT NOT NULL,
            radius INTEGER NOT NULL,
            cached_at TEXT NOT NULL,
            PRIMARY KEY (stop_name, place_type, radius)
        );

        CREATE TABLE IF NOT EXISTS pub_cache_matches (
            stop_name TEXT NOT NULL,
            place_type TEXT NOT NULL,
            radius INTEGER NOT NULL,
            place_id TEXT NOT NULL,
            PRIMARY KEY (stop_name, place_type, radius, place_id)
        );

        CREATE TABLE IF NOT EXISTS places_daily_usage (
            usage_date TEXT PRIMARY KEY,
            request_count INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS search_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_code TEXT NOT NULL REFERENCES sessions(code),
            results_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(session_code)
        );

        CREATE TABLE IF NOT EXISTS analytics_users (
            user_id TEXT PRIMARY KEY,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            visit_count INTEGER NOT NULL DEFAULT 1,
            country TEXT,
            current_session_id INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_participants_session ON participants(session_code);
        CREATE INDEX IF NOT EXISTS idx_pub_cache_stop ON pub_cache(stop_name, cached_at);
        CREATE INDEX IF NOT EXISTS idx_pub_cache_queries_fresh
            ON pub_cache_queries(stop_name, place_type, radius, cached_at);
        CREATE INDEX IF NOT EXISTS idx_sessions_created ON sessions(created_at);

        CREATE TRIGGER IF NOT EXISTS participants_unique_nonempty_name_insert
        BEFORE INSERT ON participants
        WHEN NEW.name <> '' AND EXISTS (
            SELECT 1 FROM participants
            WHERE session_code = NEW.session_code AND name = NEW.name
        )
        BEGIN
            SELECT RAISE(IGNORE);
        END;

        CREATE TRIGGER IF NOT EXISTS participants_max_twenty_insert
        BEFORE INSERT ON participants
        WHEN (
            SELECT COUNT(*) FROM participants WHERE session_code = NEW.session_code
        ) >= 20
        BEGIN
            SELECT RAISE(IGNORE);
        END;

        CREATE TRIGGER IF NOT EXISTS participants_unique_nonempty_name_update
        BEFORE UPDATE OF name ON participants
        WHEN NEW.name <> '' AND EXISTS (
            SELECT 1 FROM participants
            WHERE session_code = NEW.session_code AND name = NEW.name AND id <> OLD.id
        )
        BEGIN
            SELECT RAISE(IGNORE);
        END;
    """)

    # Migration: add opening_hours column if missing (for pre-existing databases)
    try:
        await db.execute("SELECT opening_hours FROM pub_cache LIMIT 1")
    except Exception:
        await db.execute("ALTER TABLE pub_cache ADD COLUMN opening_hours TEXT")
        await db.commit()

    # Migration: add primary_type column if missing
    try:
        await db.execute("SELECT primary_type FROM pub_cache LIMIT 1")
    except Exception:
        await db.execute("ALTER TABLE pub_cache ADD COLUMN primary_type TEXT DEFAULT ''")
        await db.commit()

    # Migration: search supersession token for pre-existing session databases.
    try:
        await db.execute("SELECT active_search_id FROM sessions LIMIT 1")
    except Exception:
        await db.execute(
            "ALTER TABLE sessions ADD COLUMN active_search_id TEXT NOT NULL DEFAULT ''"
        )
        await db.commit()

    # Migration: visitor country for pre-existing analytics databases.
    try:
        await db.execute("SELECT country FROM analytics_users LIMIT 1")
    except Exception:
        await db.execute("ALTER TABLE analytics_users ADD COLUMN country TEXT")
        await db.commit()

    # Migration: numeric GA4 session identifier for existing analytics databases.
    try:
        await db.execute("SELECT current_session_id FROM analytics_users LIMIT 1")
    except Exception:
        await db.execute("ALTER TABLE analytics_users ADD COLUMN current_session_id INTEGER")
        await db.commit()
    await db.execute(
        "UPDATE analytics_users SET current_session_id = "
        "COALESCE(CAST(strftime('%s', last_seen_at) AS INTEGER), 1) "
        "WHERE current_session_id IS NULL"
    )
    await db.commit()

    # Google Places content is not a durable application cache. Clear records created
    # by earlier versions; place data now only lives in the active response/session.
    await db.execute("DELETE FROM pub_cache_matches")
    await db.execute("DELETE FROM pub_cache_queries")
    await db.execute("DELETE FROM pub_cache")
    await db.commit()


async def create_session(
    db: aiosqlite.Connection,
    session_name: str,
    creator_name: str = "",
    initial_stops: Sequence[str] = (),
) -> dict:
    code = secrets.token_hex(16)
    now = datetime.now(timezone.utc).isoformat()
    stops = tuple(initial_stops)
    if creator_name:
        rows = [(code, creator_name, "", "", 1, now)]
    else:
        slot_count = max(2, len(stops))
        rows = [
            (
                code,
                "",
                stops[index] if index < len(stops) else "",
                stops[index] if index < len(stops) else "",
                1,
                now,
            )
            for index in range(slot_count)
        ]
    # token_hex keeps this interpolated SQLite identifier within a safe ASCII allowlist.
    savepoint = f"create_session_{secrets.token_hex(16)}"

    def write_session():
        connection = db._conn
        connection.execute(f"SAVEPOINT {savepoint}")
        try:
            connection.execute(
                "INSERT INTO sessions (code, name, creator_name, created_at) VALUES (?, ?, ?, ?)",
                (code, session_name, creator_name, now),
            )
            connection.executemany(
                "INSERT INTO participants "
                "(session_code, name, start_stop, end_stop, same_start_end, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows,
            )
        except Exception:
            connection.execute(f"ROLLBACK TO {savepoint}")
            connection.execute(f"RELEASE {savepoint}")
            raise
        connection.execute(f"RELEASE {savepoint}")
        connection.commit()

    # The lock excludes other write units; one queue item also excludes standalone reads.
    async with connection_transaction(db):
        await db._execute(write_session)
    return {"code": code, "name": session_name, "creator_name": creator_name, "created_at": now}


async def reserve_places_requests(
    db: aiosqlite.Connection, request_count: int, *, daily_limit: int
) -> bool:
    """Atomically reserve Google Places calls without exceeding the daily application cap."""
    if request_count <= 0:
        return True
    async with connection_transaction(db):
        cursor = await db.execute(
            "INSERT INTO places_daily_usage (usage_date, request_count) "
            "VALUES (date('now'), ?) "
            "ON CONFLICT(usage_date) DO UPDATE SET "
            "request_count = request_count + excluded.request_count "
            "WHERE request_count + excluded.request_count <= ?",
            (request_count, daily_limit),
        )
        await db.commit()
    return cursor.rowcount == 1


async def get_session(db: aiosqlite.Connection, code: str) -> Optional[dict]:
    db.row_factory = aiosqlite.Row
    async with db.execute(
        "SELECT id, code, name, creator_name, created_at FROM sessions WHERE code = ?",
        (code,),
    ) as cursor:
        row = await cursor.fetchone()
    if row is None:
        return None
    return {
        "id": row["id"],
        "code": row["code"],
        "name": row["name"],
        "creator_name": row["creator_name"],
        "created_at": row["created_at"],
    }


async def join_session(db: aiosqlite.Connection, code: str, name: str) -> Optional[dict]:
    session = await get_session(db, code)
    if session is None:
        return None
    async with connection_transaction(db):
        async with db.execute(
            "SELECT id FROM participants WHERE session_code = ? AND name = ?",
            (code, name),
        ) as cursor:
            existing = await cursor.fetchone()
        if existing:
            return {"id": existing[0], "name": name, "session_code": code, "created_at": ""}
        now = datetime.now(timezone.utc).isoformat()
        async with db.execute(
            "UPDATE participants SET name = ? WHERE id = ("
            "SELECT id FROM participants WHERE session_code = ? AND name = '' ORDER BY id LIMIT 1"
            ") AND session_code = ? RETURNING id",
            (name, code, code),
        ) as cursor:
            claimed = await cursor.fetchone()
        if claimed:
            await db.commit()
            return {"id": claimed[0], "name": name, "session_code": code, "created_at": now}
        async with db.execute(
            "INSERT INTO participants (session_code, name, created_at) "
            "VALUES (?, ?, ?) RETURNING id",
            (code, name, now),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            async with db.execute(
                "SELECT id FROM participants WHERE session_code = ? AND name = ?",
                (code, name),
            ) as cursor:
                row = await cursor.fetchone()
        if row is None:
            await db.commit()
            return None
        await db.commit()
        return {"id": row[0], "name": name, "session_code": code, "created_at": now}


async def get_participants(db: aiosqlite.Connection, code: str) -> list[dict]:
    db.row_factory = aiosqlite.Row
    async with db.execute(
        "SELECT id, name, start_stop, end_stop, same_start_end FROM participants "
        "WHERE session_code = ? ORDER BY id",
        (code,),
    ) as cursor:
        rows = await cursor.fetchall()
    return [
        {
            "id": row["id"],
            "name": row["name"],
            "start_stop": row["start_stop"],
            "end_stop": row["end_stop"],
            "same_start_end": bool(row["same_start_end"]),
        }
        for row in rows
    ]


async def add_participant(db: aiosqlite.Connection, session_code: str, name: str) -> Optional[dict]:
    """Add a participant to a session. Returns None if name is duplicate."""
    async with connection_transaction(db):
        async with db.execute(
            "SELECT id FROM participants WHERE session_code = ? AND name = ?",
            (session_code, name),
        ) as cursor:
            existing = await cursor.fetchone()
        if existing:
            return None
        now = datetime.now(timezone.utc).isoformat()
        async with db.execute(
            "UPDATE participants SET name = ? WHERE id = ("
            "SELECT id FROM participants WHERE session_code = ? AND name = '' ORDER BY id LIMIT 1"
            ") AND session_code = ? RETURNING id",
            (name, session_code, session_code),
        ) as cursor:
            claimed = await cursor.fetchone()
        if claimed:
            await db.commit()
            return {"id": claimed[0], "name": name, "session_code": session_code}
        async with db.execute(
            "INSERT INTO participants (session_code, name, created_at) "
            "VALUES (?, ?, ?) RETURNING id",
            (session_code, name, now),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            await db.commit()
            return None
        await db.commit()
        return {"id": row[0], "name": name, "session_code": session_code}


async def update_participant_name(
    db: aiosqlite.Connection,
    session_code: str,
    participant_id: int,
    name: str,
) -> bool:
    async with connection_transaction(db):
        result = await db.execute(
            "UPDATE participants SET name = ? WHERE id = ? AND session_code = ?",
            (name, participant_id, session_code),
        )
        await db.commit()
    return result.rowcount > 0


async def remove_participant(
    db: aiosqlite.Connection, participant_id: int, session_code: str
) -> bool:
    """Remove a participant. Returns True if deleted."""
    async with connection_transaction(db):
        result = await db.execute(
            "DELETE FROM participants WHERE id = ? AND session_code = ? AND ("
            "SELECT COUNT(*) FROM participants WHERE session_code = ?"
            ") > 2",
            (participant_id, session_code, session_code),
        )
        await db.commit()
    return result.rowcount > 0


async def add_participant_stops(
    db: aiosqlite.Connection,
    session_code: str,
    participant_id: int,
    start_stop: str,
    end_stop: str,
    same_start_end: bool | None = None,
) -> bool:
    same = start_stop == end_stop if same_start_end is None else same_start_end
    if same:
        end_stop = start_stop
    async with connection_transaction(db):
        result = await db.execute(
            "UPDATE participants SET start_stop = ?, end_stop = ?, same_start_end = ? "
            "WHERE id = ? AND session_code = ?",
            (start_stop, end_stop, int(same), participant_id, session_code),
        )
        await db.commit()
    return result.rowcount > 0


async def save_search_results(db: aiosqlite.Connection, session_code: str, results_data: dict):
    """Save search results for sharing."""
    import json

    now = datetime.now(timezone.utc).isoformat()
    async with connection_transaction(db):
        await db.execute(
            "INSERT OR REPLACE INTO search_results "
            "(session_code, results_json, created_at) VALUES (?, ?, ?)",
            (session_code, json.dumps(results_data, default=str), now),
        )
        await db.commit()


async def begin_search(db: aiosqlite.Connection, session_code: str, search_id: str) -> bool:
    async with connection_transaction(db):
        result = await db.execute(
            "UPDATE sessions SET active_search_id = ? WHERE code = ?", (search_id, session_code)
        )
        await db.commit()
    return result.rowcount > 0


async def save_search_results_if_active(
    db: aiosqlite.Connection, session_code: str, search_id: str, results_data: dict
) -> bool:
    """Persist only if this search is still the session's active generation."""
    import json

    now = datetime.now(timezone.utc).isoformat()
    async with connection_transaction(db):
        result = await db.execute(
            "INSERT OR REPLACE INTO search_results (session_code, results_json, created_at) "
            "SELECT ?, ?, ? WHERE EXISTS ("
            "SELECT 1 FROM sessions WHERE code = ? AND active_search_id = ?) ",
            (session_code, json.dumps(results_data, default=str), now, session_code, search_id),
        )
        await db.commit()
    return result.rowcount > 0


async def update_search_results(db: aiosqlite.Connection, session_code: str, results_data: dict):
    """Update persisted search data without changing when the search was run."""
    import json

    async with connection_transaction(db):
        await db.execute(
            "UPDATE search_results SET results_json = ? WHERE session_code = ?",
            (json.dumps(results_data, default=str), session_code),
        )
        await db.commit()


async def update_search_results_if_current(
    db: aiosqlite.Connection,
    session_code: str,
    results_data: dict,
    *,
    search_id: str,
    created_at: str,
) -> bool:
    """Update an expansion only if it still belongs to the search that started it."""
    import json

    async with connection_transaction(db):
        result = await db.execute(
            "UPDATE search_results SET results_json = ? "
            "WHERE session_code = ? AND created_at = ? "
            "AND COALESCE(json_extract(results_json, '$.search_id'), '') = ? "
            "AND EXISTS (SELECT 1 FROM sessions WHERE code = ? AND active_search_id = ?)",
            (
                json.dumps(results_data, default=str),
                session_code,
                created_at,
                search_id,
                session_code,
                search_id,
            ),
        )
        await db.commit()
    return result.rowcount > 0


async def get_search_results(db: aiosqlite.Connection, session_code: str) -> Optional[dict]:
    """Get saved search results."""
    import json

    async with db.execute(
        "SELECT results_json, created_at FROM search_results WHERE session_code = ?",
        (session_code,),
    ) as cursor:
        row = await cursor.fetchone()
    if row is None:
        return None
    return {"data": json.loads(row[0]), "created_at": row[1]}


async def get_visitor_country(db: aiosqlite.Connection, user_id: str) -> Optional[str]:
    async with db.execute(
        "SELECT country FROM analytics_users WHERE user_id = ?", (user_id,)
    ) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else None


async def set_visitor_country(db: aiosqlite.Connection, user_id: str, country: str) -> None:
    async with connection_transaction(db):
        await db.execute(
            "UPDATE analytics_users SET country = ? WHERE user_id = ?", (country, user_id)
        )
        await db.commit()


async def cleanup_old_sessions(db: aiosqlite.Connection, max_age_days: int = 30):
    """Delete sessions and their participants older than max_age_days."""
    async with connection_transaction(db):
        cursor = await db.execute(
            "SELECT code FROM sessions WHERE created_at < datetime('now', ?)",
            (f"-{max_age_days} days",),
        )
        old_sessions = await cursor.fetchall()
        if not old_sessions:
            return 0
        codes = [row[0] for row in old_sessions]
        placeholders = ",".join("?" for _ in codes)
        await db.execute(f"DELETE FROM participants WHERE session_code IN ({placeholders})", codes)
        await db.execute(
            f"DELETE FROM search_results WHERE session_code IN ({placeholders})", codes
        )
        await db.execute(f"DELETE FROM sessions WHERE code IN ({placeholders})", codes)
        await db.commit()
    logger.info("Cleaned up %d sessions older than %d days", len(codes), max_age_days)
    return len(codes)
