import secrets
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiosqlite


async def init_db(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
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
            place_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            rating REAL,
            rating_count INTEGER,
            price_level INTEGER,
            google_maps_url TEXT,
            cached_at TEXT NOT NULL
        );
    """)


async def create_session(db: aiosqlite.Connection, creator_name: str) -> dict:
    code = secrets.token_hex(4)
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT INTO sessions (code, creator_name, created_at) VALUES (?, ?, ?)",
        (code, creator_name, now),
    )
    await db.execute(
        "INSERT INTO participants (session_code, name, created_at) VALUES (?, ?, ?)",
        (code, creator_name, now),
    )
    await db.commit()
    return {"code": code, "creator_name": creator_name, "created_at": now}


async def get_session(db: aiosqlite.Connection, code: str) -> Optional[dict]:
    db.row_factory = aiosqlite.Row
    async with db.execute(
        "SELECT id, code, creator_name, created_at FROM sessions WHERE code = ?",
        (code,),
    ) as cursor:
        row = await cursor.fetchone()
    if row is None:
        return None
    return {"id": row["id"], "code": row["code"], "creator_name": row["creator_name"], "created_at": row["created_at"]}


async def join_session(db: aiosqlite.Connection, code: str, name: str) -> Optional[dict]:
    session = await get_session(db, code)
    if session is None:
        return None
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
