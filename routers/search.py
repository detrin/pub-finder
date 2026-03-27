import json
from datetime import datetime

import polars as pl
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from db import get_participants
from backend.optimization import get_optimal_stop_pairs, get_actual_time_optimal_stop_pairs
from backend.places import search_pubs_near_stop, get_cached_pubs, cache_pubs
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

    stop_geo = pl.read_parquet("Prague_stops_geo.parquet")
    top_stops = df_results["Target Stop"].to_list()[:5]

    pubs_by_stop = {}
    for stop_name in top_stops:
        cached = await get_cached_pubs(db, stop_name)
        if cached:
            pubs_by_stop[stop_name] = cached
            continue
        geo_row = stop_geo.filter(pl.col("name") == stop_name)
        if len(geo_row) == 0:
            continue
        lat = geo_row["lat"][0]
        lon = geo_row["lon"][0]
        try:
            pubs = await search_pubs_near_stop(lat, lon)
            await cache_pubs(db, stop_name, pubs)
            pubs_by_stop[stop_name] = pubs
        except Exception as e:
            print(f"Places API error for {stop_name}: {e}")
            pubs_by_stop[stop_name] = []

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

    return templates.TemplateResponse("partials/results_table.html", {
        "request": request, "error": None, "results": df_results,
        "pubs_by_stop": pubs_by_stop, "stops_json": json.dumps(stop_geo_data), "pubs_json": json.dumps(pubs_flat),
    })
