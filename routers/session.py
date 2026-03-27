from datetime import datetime, timedelta

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from db import create_session, join_session, get_session, get_participants, add_participant_stops

router = APIRouter(prefix="/session")
templates = Jinja2Templates(directory="templates")


@router.post("/create")
async def create(request: Request, creator_name: str = Form(...)):
    db = request.app.state.db
    session = await create_session(db, creator_name)
    return RedirectResponse(url=f"/session/{session['code']}", status_code=303)


@router.get("/join")
async def join(request: Request, code: str, name: str):
    db = request.app.state.db
    result = await join_session(db, code, name)
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

    from main import app_state
    all_stops = app_state.get("all_stops", [])

    now = datetime.now()
    default_date = (now + timedelta(days=(4 - now.weekday()) % 7 or 7)).strftime("%Y-%m-%d")
    default_time = "18:00"

    return templates.TemplateResponse(request, "session.html", {
        "session": session,
        "participants": participants,
        "all_stops": all_stops,
        "default_date": default_date,
        "default_time": default_time,
    })


@router.get("/{code}/participants", response_class=HTMLResponse)
async def participants_partial(request: Request, code: str):
    db = request.app.state.db
    participants = await get_participants(db, code)
    return templates.TemplateResponse(request, "partials/participant_list.html", {
        "session": {"code": code},
        "participants": participants,
    })


@router.post("/{code}/stops", response_class=HTMLResponse)
async def update_stops(
    request: Request,
    code: str,
    participant_id: int = Form(...),
    start_stop: str = Form(""),
    end_stop: str = Form(""),
    same_start_end: bool = Form(False),
):
    db = request.app.state.db
    if same_start_end:
        end_stop = start_stop
    await add_participant_stops(db, participant_id, start_stop, end_stop)
    participants = await get_participants(db, code)
    return templates.TemplateResponse(request, "partials/participant_list.html", {
        "session": {"code": code},
        "participants": participants,
    })
