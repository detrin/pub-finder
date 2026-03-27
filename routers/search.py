import json
import logging
import time as _time
from collections import defaultdict
from datetime import datetime

import polars as pl
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from backend.db import get_participants, get_session, save_search_results, get_search_results
from backend.optimization import get_optimal_stop_pairs, get_actual_time_optimal_stop_pairs
from backend.places import search_pubs_near_stop, get_cached_pubs, cache_pubs
from backend.utils import validate_date_time, get_total_minutes_with_retries

logger = logging.getLogger(__name__)

# Simple per-session rate limiter: max 3 searches per 60 seconds
_search_timestamps: dict[str, list[float]] = defaultdict(list)
SEARCH_RATE_LIMIT = 3
SEARCH_RATE_WINDOW = 60  # seconds


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

    departure_datetime = datetime.strptime(f"{departure_date} {departure_time}", "%Y-%m-%d %H:%M")
    return_datetime = datetime.strptime(f"{return_date} {return_time}", "%Y-%m-%d %H:%M")
    distance_table = request.app.state.distance_table

    target_stops = get_optimal_stop_pairs(distance_table, method, stop_pairs)
    df_results = get_actual_time_optimal_stop_pairs(
        method, stop_pairs, target_stops, departure_datetime, get_total_minutes_with_retries,
        participant_names=participant_names,
        return_datetime=return_datetime,
    )

    stop_geo = request.app.state.stop_geo
    top_stops = df_results["Target Stop"].to_list()

    pubs_by_stop_raw = {}
    places_api_error = False
    for stop_name in top_stops:
        cached = await get_cached_pubs(db, stop_name)
        if cached:
            pubs_by_stop_raw[stop_name] = cached
            continue
        if places_api_error:
            # Skip further API calls after a failure
            pubs_by_stop_raw[stop_name] = []
            continue
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

    # Deduplicate: each pub appears only under the first stop it's found at
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

    # Save results for sharing
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

    return templates.TemplateResponse("partials/results_table.html", {
        "request": request, "error": None, "results": df_results,
        "pubs_by_stop": pubs_by_stop, "stops_json": json.dumps(stop_geo_data),
        "pubs_json": json.dumps(pubs_flat), "participants_json": json.dumps(participants_geo),
        "warning": warning,
    })


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
    # Reconstruct a minimal DataFrame-like object for the template
    import polars as pl
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
