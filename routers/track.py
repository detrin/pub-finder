import asyncio

from fastapi import APIRouter, Form, Request
from starlette.responses import Response

from backend.analytics import (
    USER_ID_COOKIE,
    engagement_event,
    send_events,
    session_id_for_today,
)
from backend.db import get_visit_count

router = APIRouter()


@router.post("/e")
async def track_engagement(
    request: Request,
    path: str = Form(..., max_length=500),
    engagement_time_msec: int = Form(...),
):
    user_id = request.cookies.get(USER_ID_COOKIE)
    if not user_id or engagement_time_msec <= 0:
        return Response(status_code=204)

    db = request.app.state.db
    visit_count = await get_visit_count(db, user_id)
    session_id = session_id_for_today(user_id)
    events = engagement_event(
        page_path=path,
        engagement_time_msec=min(engagement_time_msec, 30 * 60 * 1000),
        session_id=session_id,
        session_number=visit_count,
    )
    asyncio.create_task(send_events(user_id, events))
    return Response(status_code=204)
