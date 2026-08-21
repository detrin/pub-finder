from fastapi import APIRouter, Form, Request
from starlette.responses import Response

from backend.analytics import (
    USER_ID_COOKIE,
    engagement_event,
    get_client_ip,
    is_valid_user_id,
    page_location_for_path,
    schedule_analytics_task,
    send_events,
    touch_current_session,
)

router = APIRouter()


@router.post("/e")
async def track_engagement(
    request: Request,
    path: str = Form(..., max_length=500),
    engagement_time_msec: int = Form(...),
):
    user_id = request.cookies.get(USER_ID_COOKIE)
    if not is_valid_user_id(user_id) or engagement_time_msec <= 0:
        return Response(status_code=204)

    db = request.app.state.db
    session = await touch_current_session(db, user_id)
    page_location = page_location_for_path(request, path)
    if session is None or page_location is None:
        return Response(status_code=204)
    events = engagement_event(
        page_location=page_location,
        engagement_time_msec=min(engagement_time_msec, 30 * 60 * 1000),
        session_id=session.session_id,
        session_number=session.session_number,
    )
    schedule_analytics_task(
        request.app,
        send_events(user_id, events, ip_address=get_client_ip(request)),
    )
    return Response(status_code=204)
