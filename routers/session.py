import asyncio
import hashlib
import json
import logging
import time as _time
from collections import defaultdict
from datetime import datetime, timedelta

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import StreamingResponse

from backend.db import create_session, join_session, get_session, get_participants, add_participant_stops, add_participant, remove_participant

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/session")
templates = Jinja2Templates(directory="templates")

MAX_NAME_LENGTH = 50
MAX_PARTICIPANTS = 20

# Rate limiting: max actions per IP per window
_rate_timestamps: dict[str, list[float]] = defaultdict(list)
_RATE_LIMIT = 30  # actions per window
_RATE_WINDOW = 60  # seconds

# SSE connection tracking per session
_sse_connections: dict[str, int] = defaultdict(int)
_MAX_SSE_PER_SESSION = 10


def _get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _is_rate_limited(request: Request) -> bool:
    ip = _get_client_ip(request)
    now = _time.monotonic()
    _rate_timestamps[ip] = [t for t in _rate_timestamps[ip] if now - t < _RATE_WINDOW]
    if len(_rate_timestamps[ip]) >= _RATE_LIMIT:
        return True
    _rate_timestamps[ip].append(now)
    return False


@router.post("/create")
async def create(request: Request, session_name: str = Form(..., max_length=100), creator_name: str = Form(..., max_length=MAX_NAME_LENGTH)):
    if _is_rate_limited(request):
        return RedirectResponse(url="/?error=rate_limited", status_code=303)
    db = request.app.state.db
    session = await create_session(db, session_name.strip()[:100], creator_name.strip()[:MAX_NAME_LENGTH])
    return RedirectResponse(url=f"/session/join?code={session['code']}&name={creator_name.strip()[:MAX_NAME_LENGTH]}", status_code=303)


@router.get("/join")
async def join(request: Request, code: str, name: str = ""):
    if not name.strip():
        return templates.TemplateResponse(request, "join.html", {"code": code})
    if _is_rate_limited(request):
        return RedirectResponse(url="/?error=rate_limited", status_code=303)
    db = request.app.state.db
    result = await join_session(db, code, name.strip()[:MAX_NAME_LENGTH])
    if result is None:
        return RedirectResponse(url="/?error=session_not_found", status_code=303)
    return RedirectResponse(url=f"/session/{code}", status_code=303)


@router.get("/{code}", response_class=HTMLResponse, name="session_page")
async def session_page(request: Request, code: str):
    db = request.app.state.db
    session = await get_session(db, code)
    if session is None:
        return RedirectResponse(url="/?error=session_not_found", status_code=303)
    participants = await get_participants(db, code)

    all_stops = getattr(request.app.state, "all_stops", [])

    from backend.utils import get_next_meetup_time
    now = datetime.now()
    departure_dt = get_next_meetup_time(4, 20)
    return_dt = departure_dt + timedelta(hours=3)

    return templates.TemplateResponse(request, "session.html", {
        "session": session,
        "participants": participants,
        "all_stops": all_stops,
        "default_date": departure_dt.strftime("%Y-%m-%d"),
        "default_departure": departure_dt.strftime("%H:%M"),
        "default_return_date": return_dt.strftime("%Y-%m-%d"),
        "default_return": return_dt.strftime("%H:%M"),
        "today": now.strftime("%Y-%m-%d"),
        "max_date": (now + timedelta(days=31)).strftime("%Y-%m-%d"),
    })


@router.get("/{code}/participants", response_class=HTMLResponse)
async def participants_partial(request: Request, code: str):
    db = request.app.state.db
    participants = await get_participants(db, code)
    return templates.TemplateResponse(request, "partials/participant_list_inner.html", {
        "session": {"code": code},
        "participants": participants,
    })


@router.post("/{code}/stops", response_class=HTMLResponse)
async def update_stops(
    request: Request,
    code: str,
    participant_id: int = Form(...),
    start_stop: str = Form("", max_length=200),
    end_stop: str = Form("", max_length=200),
    same_start_end: bool = Form(False),
):
    db = request.app.state.db
    if same_start_end:
        end_stop = start_stop

    all_stops = getattr(request.app.state, "all_stops", [])
    errors = []
    if start_stop and start_stop not in all_stops:
        errors.append(f"Unknown stop: '{start_stop}'")
    if end_stop and end_stop not in all_stops and end_stop != start_stop:
        errors.append(f"Unknown stop: '{end_stop}'")

    if errors:
        participants = await get_participants(db, code)
        return templates.TemplateResponse(request, "partials/session_participants_inner.html", {
            "session": {"code": code},
            "participants": participants,
            "stop_error": "; ".join(errors),
        })

    await add_participant_stops(db, participant_id, start_stop, end_stop)
    participants = await get_participants(db, code)
    return templates.TemplateResponse(request, "partials/session_participants_inner.html", {
        "session": {"code": code},
        "participants": participants,
    })


@router.post("/{code}/add-participant", response_class=HTMLResponse)
async def add_participant_route(
    request: Request,
    code: str,
    participant_name: str = Form(..., max_length=MAX_NAME_LENGTH),
):
    db = request.app.state.db
    participants = await get_participants(db, code)
    name = participant_name.strip()[:MAX_NAME_LENGTH]
    error = None
    if _is_rate_limited(request):
        error = "Too many requests. Please wait."
    elif not name:
        error = "Name cannot be empty."
    elif len(participants) >= MAX_PARTICIPANTS:
        error = f"Maximum {MAX_PARTICIPANTS} participants reached."
    else:
        result = await add_participant(db, code, name)
        if result is None:
            error = f"'{name}' is already in this session."
        else:
            participants = await get_participants(db, code)

    return templates.TemplateResponse(request, "partials/session_participants_inner.html", {
        "session": {"code": code},
        "participants": participants,
        "participant_error": error,
    })


@router.post("/{code}/remove-participant", response_class=HTMLResponse)
async def remove_participant_route(
    request: Request,
    code: str,
    participant_id: int = Form(...),
):
    db = request.app.state.db
    participants = await get_participants(db, code)
    error = None
    if _is_rate_limited(request):
        error = "Too many requests. Please wait."
    elif len(participants) <= 1:
        error = "Cannot remove the last participant."
    else:
        await remove_participant(db, participant_id, code)
        participants = await get_participants(db, code)
    return templates.TemplateResponse(request, "partials/session_participants_inner.html", {
        "session": {"code": code},
        "participants": participants,
        "participant_error": error,
    })


@router.get("/{code}/events")
async def participant_events(request: Request, code: str):
    """SSE endpoint that pushes participant list HTML when data changes."""
    db = request.app.state.db

    # Validate session exists
    session = await get_session(db, code)
    if session is None:
        return StreamingResponse(iter([]), media_type="text/event-stream", status_code=404)

    # Limit concurrent SSE connections per session
    if _sse_connections[code] >= _MAX_SSE_PER_SESSION:
        return StreamingResponse(iter([]), media_type="text/event-stream", status_code=429)

    def _hash_participants(participants):
        raw = json.dumps(participants, sort_keys=True)
        return hashlib.md5(raw.encode()).hexdigest()

    async def event_stream():
        _sse_connections[code] += 1
        try:
            last_hash = None
            while True:
                if await request.is_disconnected():
                    break
                participants = await get_participants(db, code)
                current_hash = _hash_participants(participants)
                if current_hash != last_hash:
                    last_hash = current_hash
                    html = templates.get_template("partials/session_participants_inner.html").render({
                        "session": {"code": code},
                        "participants": participants,
                    })
                    escaped = html.replace("\n", "\ndata: ")
                    yield f"event: participants\ndata: {escaped}\n\n"
                await asyncio.sleep(3)
        finally:
            _sse_connections[code] = max(0, _sse_connections[code] - 1)

    return StreamingResponse(event_stream(), media_type="text/event-stream")
