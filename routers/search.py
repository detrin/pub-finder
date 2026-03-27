from datetime import datetime

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from db import get_participants
from backend.optimization import get_optimal_stop_pairs, get_actual_time_optimal_stop_pairs
from backend.utils import validate_date_time, get_total_minutes_with_retries

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.post("/session/{code}/search", response_class=HTMLResponse)
async def search(
    request: Request,
    code: str,
    date: str = Form(...),
    time: str = Form(...),
    method: str = Form("minimize-worst-case"),
):
    db = request.app.state.db
    participants = await get_participants(db, code)
    stop_pairs = [
        (p["start_stop"], p["end_stop"] or p["start_stop"])
        for p in participants
        if p["start_stop"]
    ]

    if len(stop_pairs) < 2:
        return templates.TemplateResponse(
            "partials/results_table.html",
            {
                "request": request,
                "error": "At least 2 participants must have selected their stops.",
                "results": None,
            },
        )

    is_valid, error_msg = validate_date_time(date, time)
    if not is_valid:
        return templates.TemplateResponse(
            "partials/results_table.html",
            {"request": request, "error": error_msg, "results": None},
        )

    event_datetime = datetime.strptime(f"{date} {time}", "%d/%m/%Y %H:%M")
    from main import app_state

    distance_table = app_state["distance_table"]

    target_stops = get_optimal_stop_pairs(distance_table, method, stop_pairs)
    df_results = get_actual_time_optimal_stop_pairs(
        method, stop_pairs, target_stops, event_datetime, get_total_minutes_with_retries
    )

    return templates.TemplateResponse(
        "partials/results_table.html",
        {"request": request, "error": None, "results": df_results},
    )
