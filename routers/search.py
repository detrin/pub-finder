import asyncio
import json
import logging
import secrets
import time as _time
from collections import defaultdict
from datetime import datetime
from threading import Lock

import polars as pl
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import StreamingResponse

from backend.db import get_participants, get_session, save_search_results, get_search_results
from backend.optimization import get_optimal_stop_pairs, get_actual_time_optimal_stop_pairs
from backend.places import search_pubs_near_stop, get_cached_pubs, cache_pubs
from backend.utils import validate_date_time, get_total_minutes_with_retries

logger = logging.getLogger(__name__)

# Simple per-session rate limiter: max 3 searches per 60 seconds
_search_timestamps: dict[str, list[float]] = defaultdict(list)
SEARCH_RATE_LIMIT = 3
SEARCH_RATE_WINDOW = 60  # seconds

# In-flight search progress tracking
_search_progress: dict[str, dict] = {}
_search_progress_lock = Lock()


def _is_rate_limited(session_code: str) -> bool:
    now = _time.monotonic()
    timestamps = _search_timestamps[session_code]
    # Prune old entries
    _search_timestamps[session_code] = [t for t in timestamps if now - t < SEARCH_RATE_WINDOW]
    if len(_search_timestamps[session_code]) >= SEARCH_RATE_LIMIT:
        return True
    _search_timestamps[session_code].append(now)
    return False

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.post("/session/{code}/search", response_class=HTMLResponse)
async def search(
    request: Request,
    code: str,
    departure_date: str = Form(...),
    departure_time: str = Form(...),
    return_date: str = Form(...),
    return_time: str = Form(...),
    method: str = Form("minimize-worst-case"),
):
    if _is_rate_limited(code):
        return templates.TemplateResponse(
            "partials/results_table.html",
            {
                "request": request,
                "error": "Too many searches. Please wait a minute before trying again.",
                "results": None,
            },
        )

    db = request.app.state.db
    participants = await get_participants(db, code)
    active_participants = [p for p in participants if p["start_stop"]]
    stop_pairs = [
        (p["start_stop"], p["end_stop"] or p["start_stop"])
        for p in active_participants
    ]
    participant_names = [p["name"] for p in active_participants]

    if len(stop_pairs) < 2:
        return templates.TemplateResponse(
            "partials/results_table.html",
            {
                "request": request,
                "error": "At least 2 participants must have selected their stops.",
                "results": None,
            },
        )

    is_valid, error_msg = validate_date_time(departure_date, departure_time)
    if not is_valid:
        return templates.TemplateResponse(
            "partials/results_table.html",
            {"request": request, "error": f"Departure: {error_msg}", "results": None},
        )

    is_valid, error_msg = validate_date_time(return_date, return_time)
    if not is_valid:
        return templates.TemplateResponse(
            "partials/results_table.html",
            {"request": request, "error": f"Return: {error_msg}", "results": None},
        )

    # Create a search task ID and start search in background
    search_id = secrets.token_hex(8)
    with _search_progress_lock:
        _search_progress[search_id] = {"stage": "starting", "current": 0, "total": 0, "done": False, "result_html": None}

    asyncio.create_task(_run_search(
        request, code, search_id,
        departure_date, departure_time, return_date, return_time,
        method, stop_pairs, participant_names, active_participants,
    ))

    # Return a progress bar that connects to SSE
    return f"""<div id="search-progress" hx-ext="sse" sse-connect="/session/{code}/search-progress/{search_id}" sse-swap="progress" hx-swap="innerHTML">
    <div class="progress-bar-container">
        <div class="progress-bar-track">
            <div class="progress-bar-fill" style="width: 0%"></div>
        </div>
        <p class="progress-bar-label">Preparing search...</p>
    </div>
</div>"""


async def _run_search(
    request, code, search_id,
    departure_date, departure_time, return_date, return_time,
    method, stop_pairs, participant_names, active_participants,
):
    """Run the search in the background, updating progress along the way."""
    try:
        def progress_callback(stage, current, total):
            with _search_progress_lock:
                if search_id in _search_progress:
                    _search_progress[search_id].update({"stage": stage, "current": current, "total": total})

        departure_datetime = datetime.strptime(f"{departure_date} {departure_time}", "%Y-%m-%d %H:%M")
        return_datetime = datetime.strptime(f"{return_date} {return_time}", "%Y-%m-%d %H:%M")
        distance_table = request.app.state.distance_table

        with _search_progress_lock:
            _search_progress[search_id]["stage"] = "candidates"

        target_stops = await asyncio.to_thread(get_optimal_stop_pairs, distance_table, method, stop_pairs)

        with _search_progress_lock:
            _search_progress[search_id].update({"stage": "scraping", "current": 0, "total": len(target_stops)})

        df_results = await asyncio.to_thread(
            get_actual_time_optimal_stop_pairs,
            method, stop_pairs, target_stops, departure_datetime, get_total_minutes_with_retries,
            participant_names=participant_names,
            return_datetime=return_datetime,
            progress_callback=progress_callback,
        )

        db = request.app.state.db
        stop_geo = request.app.state.stop_geo
        top_stops = df_results["Target Stop"].to_list()

        with _search_progress_lock:
            _search_progress[search_id].update({"stage": "pubs", "current": 0, "total": len(top_stops)})

        pubs_by_stop_raw = {}
        places_api_error = False
        for i, stop_name in enumerate(top_stops):
            cached = await get_cached_pubs(db, stop_name)
            if cached:
                pubs_by_stop_raw[stop_name] = cached
            elif places_api_error:
                pubs_by_stop_raw[stop_name] = []
            else:
                geo_row = stop_geo.filter(pl.col("name") == stop_name)
                if len(geo_row) == 0:
                    continue
                lat = geo_row["lat"][0]
                lon = geo_row["lon"][0]
                try:
                    pubs = await search_pubs_near_stop(lat, lon)
                    await cache_pubs(db, stop_name, pubs)
                    pubs_by_stop_raw[stop_name] = pubs
                except Exception as e:
                    logger.warning("Places API error for %s: %s", stop_name, e)
                    pubs_by_stop_raw[stop_name] = []
                    places_api_error = True
            with _search_progress_lock:
                _search_progress[search_id]["current"] = i + 1

        # Deduplicate
        seen_place_ids: set[str] = set()
        pubs_by_stop = {}
        for stop_name in top_stops:
            unique_pubs = []
            for pub in pubs_by_stop_raw.get(stop_name, []):
                if pub["place_id"] not in seen_place_ids:
                    seen_place_ids.add(pub["place_id"])
                    unique_pubs.append(pub)
            pubs_by_stop[stop_name] = unique_pubs

        stop_geo_data = []
        for stop_name in top_stops:
            geo_row = stop_geo.filter(pl.col("name") == stop_name)
            if len(geo_row) > 0:
                stop_geo_data.append({
                    "name": stop_name, "lat": float(geo_row["lat"][0]), "lon": float(geo_row["lon"][0]),
                })

        pubs_flat = []
        for stop_name, pubs in pubs_by_stop.items():
            for pub in pubs:
                pubs_flat.append({
                    "stop": stop_name, "name": pub["name"], "lat": pub["lat"], "lon": pub["lon"],
                    "rating": pub["rating"], "rating_count": pub["rating_count"], "url": pub["google_maps_url"],
                })

        participants_geo = []
        for p, (start, end) in zip(active_participants, stop_pairs):
            for stop_name, label in [(start, "from"), (end, "to")]:
                geo_row = stop_geo.filter(pl.col("name") == stop_name)
                if len(geo_row) > 0:
                    participants_geo.append({
                        "name": p["name"], "stop": stop_name, "type": label,
                        "lat": float(geo_row["lat"][0]), "lon": float(geo_row["lon"][0]),
                    })

        warning = None
        if places_api_error:
            warning = "Google Places API limit reached — pub data may be incomplete for some stops."

        # Save results
        results_rows = df_results.rows(named=True)
        results_columns = df_results.columns
        await save_search_results(db, code, {
            "rows": results_rows,
            "columns": results_columns,
            "pubs_by_stop": {k: v for k, v in pubs_by_stop.items()},
            "stops_geo": stop_geo_data,
            "pubs_flat": pubs_flat,
            "participants_geo": participants_geo,
            "warning": warning,
        })

        result_html = templates.get_template("partials/results_table.html").render(
            request=request, error=None, results=df_results,
            pubs_by_stop=pubs_by_stop, stops_json=json.dumps(stop_geo_data),
            pubs_json=json.dumps(pubs_flat), participants_json=json.dumps(participants_geo),
            warning=warning,
        )

        with _search_progress_lock:
            _search_progress[search_id].update({"done": True, "result_html": result_html})

    except Exception as e:
        logger.error("Search failed: %s", e, exc_info=True)
        error_html = templates.get_template("partials/results_table.html").render(
            request=request, error=f"Search failed: {e}", results=None,
        )
        with _search_progress_lock:
            _search_progress[search_id].update({"done": True, "result_html": error_html})


@router.get("/session/{code}/search-progress/{search_id}")
async def search_progress_stream(request: Request, code: str, search_id: str):
    async def event_stream():
        while True:
            if await request.is_disconnected():
                break

            with _search_progress_lock:
                progress = _search_progress.get(search_id)

            if progress is None:
                yield 'event: progress\ndata: <p>Search not found.</p>\n\n'
                break

            if progress["done"]:
                html = progress["result_html"]
                escaped = html.replace("\n", "\ndata: ")
                yield f'event: progress\ndata: {escaped}\n\n'
                # Clean up
                with _search_progress_lock:
                    _search_progress.pop(search_id, None)
                break

            stage = progress["stage"]
            current = progress["current"]
            total = progress["total"]

            if stage == "starting" or stage == "candidates":
                pct = 5
                label = "Finding candidate stops..."
            elif stage == "scraping":
                pct = 10 + int((current / max(total, 1)) * 70)
                label = f"Querying live transit times... {current}/{total} stops"
            elif stage == "pubs":
                pct = 80 + int((current / max(total, 1)) * 18)
                label = f"Finding nearby pubs... {current}/{total} stops"
            else:
                pct = 0
                label = "Working..."

            yield f"""event: progress
data: <div class="progress-bar-container">
data:     <div class="progress-bar-track">
data:         <div class="progress-bar-fill" style="width: {pct}%"></div>
data:     </div>
data:     <p class="progress-bar-label">{label}</p>
data: </div>
\n"""
            await asyncio.sleep(0.5)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.get("/session/{code}/results", response_class=HTMLResponse)
async def results_page(request: Request, code: str):
    """Shareable results page — shows the last search results for a session."""
    db = request.app.state.db
    session = await get_session(db, code)
    if session is None:
        return RedirectResponse(url="/?error=session_not_found", status_code=303)

    saved = await get_search_results(db, code)
    if saved is None:
        return templates.TemplateResponse("results.html", {
            "request": request,
            "session": session,
            "has_results": False,
        })

    data = saved["data"]
    df_results = pl.DataFrame(data["rows"])

    return templates.TemplateResponse("results.html", {
        "request": request,
        "session": session,
        "has_results": True,
        "results": df_results,
        "pubs_by_stop": data["pubs_by_stop"],
        "stops_json": json.dumps(data["stops_geo"]),
        "pubs_json": json.dumps(data["pubs_flat"]),
        "participants_json": json.dumps(data["participants_geo"]),
        "warning": data.get("warning"),
        "created_at": saved["created_at"],
    })
