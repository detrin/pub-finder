import logging
import secrets
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

logger = logging.getLogger(__name__)


async def init_db(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            creator_name TEXT NOT NULL,
            created_at TEXT NOT NULL
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
            cached_at TEXT NOT NULL,
            UNIQUE(stop_name, place_id)
        );

        CREATE TABLE IF NOT EXISTS search_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_code TEXT NOT NULL REFERENCES sessions(code),
            results_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(session_code)
        );

        CREATE INDEX IF NOT EXISTS idx_participants_session ON participants(session_code);
        CREATE INDEX IF NOT EXISTS idx_pub_cache_stop ON pub_cache(stop_name, cached_at);
        CREATE INDEX IF NOT EXISTS idx_sessions_created ON sessions(created_at);
    """)


async def create_session(db: aiosqlite.Connection, session_name: str, creator_name: str) -> dict:
    code = secrets.token_hex(16)
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT INTO sessions (code, name, creator_name, created_at) VALUES (?, ?, ?, ?)",
        (code, session_name, creator_name, now),
    )
    await db.execute(
        "INSERT INTO participants (session_code, name, created_at) VALUES (?, ?, ?)",
        (code, creator_name, now),
    )
    await db.commit()
    return {"code": code, "name": session_name, "creator_name": creator_name, "created_at": now}


async def get_session(db: aiosqlite.Connection, code: str) -> Optional[dict]:
    db.row_factory = aiosqlite.Row
    async with db.execute(
        "SELECT id, code, name, creator_name, created_at FROM sessions WHERE code = ?",
        (code,),
    ) as cursor:
        row = await cursor.fetchone()
    if row is None:
        return None
    return {"id": row["id"], "code": row["code"], "name": row["name"], "creator_name": row["creator_name"], "created_at": row["created_at"]}


async def join_session(db: aiosqlite.Connection, code: str, name: str) -> Optional[dict]:
    session = await get_session(db, code)
    if session is None:
        return None
    # Check if participant already exists in this session
    async with db.execute(
        "SELECT id FROM participants WHERE session_code = ? AND name = ?",
        (code, name),
    ) as cursor:
        existing = await cursor.fetchone()
    if existing:
        return {"id": existing[0], "name": name, "session_code": code, "created_at": ""}
    now = datetime.now(timezone.utc).isoformat()
    async with db.execute(
        "INSERT INTO participants (session_code, name, created_at) VALUES (?, ?, ?) RETURNING id",
        (code, name, now),
    ) as cursor:
        row = await cursor.fetchone()
    await db.commit()
    return {"id": row[0], "name": name, "session_code": code, "created_at": now}


async def get_participants(db: aiosqlite.Connection, code: str) -> list[dict]:
    db.row_factory = aiosqlite.Row
    async with db.execute(
        "SELECT id, name, start_stop, end_stop, same_start_end FROM participants WHERE session_code = ?",
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


async def add_participant_stops(db: aiosqlite.Connection, participant_id: int, start_stop: str, end_stop: str):
    same = 1 if start_stop == end_stop else 0
    await db.execute(
        "UPDATE participants SET start_stop = ?, end_stop = ?, same_start_end = ? WHERE id = ?",
        (start_stop, end_stop, same, participant_id),
    )
    await db.commit()


async def save_search_results(db: aiosqlite.Connection, session_code: str, results_data: dict):
    """Save search results for sharing."""
    import json
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT OR REPLACE INTO search_results (session_code, results_json, created_at) VALUES (?, ?, ?)",
        (session_code, json.dumps(results_data), now),
    )
    await db.commit()


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


async def cleanup_old_sessions(db: aiosqlite.Connection, max_age_days: int = 30):
    """Delete sessions and their participants older than max_age_days."""
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
    await db.execute(f"DELETE FROM search_results WHERE session_code IN ({placeholders})", codes)
    await db.execute(f"DELETE FROM sessions WHERE code IN ({placeholders})", codes)
    await db.commit()
    logger.info("Cleaned up %d sessions older than %d days", len(codes), max_age_days)
    return len(codes)
