from __future__ import annotations

import hashlib
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from backend.db import get_search_results, get_session
from backend.reachability import VALID_DIRECTIONS, build_reachability_payload

router = APIRouter()

_CACHE_CONTROL = "private, max-age=300"
_SNAPSHOT_FIELDS = ("id", "name", "color", "start_stop", "end_stop")


def _validate_snapshot(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise HTTPException(status_code=422, detail="Saved participant snapshot is invalid")

    participants = []
    for item in value:
        if not isinstance(item, dict) or any(field not in item for field in _SNAPSHOT_FIELDS):
            raise HTTPException(status_code=422, detail="Saved participant snapshot is invalid")
        if isinstance(item["id"], bool) or not isinstance(item["id"], int):
            raise HTTPException(status_code=422, detail="Saved participant snapshot is invalid")
        if any(not isinstance(item[field], str) for field in _SNAPSHOT_FIELDS[1:]):
            raise HTTPException(status_code=422, detail="Saved participant snapshot is invalid")
        participants.append({field: item[field] for field in _SNAPSHOT_FIELDS})
    return participants


def _etag(session_code: str, created_at: str, direction: str) -> str:
    identity = f"{session_code}\0{created_at}\0{direction}".encode()
    return f'"{hashlib.sha256(identity).hexdigest()}"'


def _if_none_match_matches(header: str | None, etag: str) -> bool:
    if header is None:
        return False
    for raw_candidate in header.split(","):
        candidate = raw_candidate.strip()
        if candidate == "*" or candidate.removeprefix("W/") == etag:
            return True
    return False


@router.get("/session/{code}/reachability")
async def reachability(request: Request, code: str) -> Response:
    db = request.app.state.db
    if await get_session(db, code) is None:
        raise HTTPException(status_code=404, detail="Session not found")

    saved = await get_search_results(db, code)
    if saved is None:
        raise HTTPException(status_code=404, detail="Search results not found")
    data = saved.get("data")
    if not isinstance(data, dict):
        raise HTTPException(status_code=422, detail="Saved search results are invalid")

    direction = data.get("search_direction")
    if not isinstance(direction, str) or direction not in VALID_DIRECTIONS:
        raise HTTPException(status_code=422, detail="Saved search direction is invalid")
    participants = _validate_snapshot(data.get("participant_snapshot"))

    etag = _etag(code, saved["created_at"], direction)
    headers = {"Cache-Control": _CACHE_CONTROL, "ETag": etag}
    if _if_none_match_matches(request.headers.get("if-none-match"), etag):
        return Response(status_code=304, headers=headers)

    payload = build_reachability_payload(
        request.app.state.distance_table,
        request.app.state.stop_geo,
        participants,
        direction,
    )
    return JSONResponse(payload, headers=headers)
