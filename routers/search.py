import asyncio
import json
import logging
import secrets
import time as _time
from collections import defaultdict
from datetime import datetime

import polars as pl
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import StreamingResponse

from backend.db import get_participants, get_search_results, get_session, save_search_results
from backend.optimization import get_actual_time_optimal_stop_pairs, get_optimal_stop_pairs
from backend.places import (
    cache_pubs_for_type,
    get_cached_pubs_for_type,
    is_open_during,
    order_pubs_for_stop,
    search_pubs_near_stop,
)
from backend.utils import get_total_minutes_with_retries, validate_date_time

logger = logging.getLogger(__name__)

# Simple per-session rate limiter: max 3 searches per 60 seconds
_search_timestamps: dict[str, list[float]] = defaultdict(list)
SEARCH_RATE_LIMIT = 3
SEARCH_RATE_WINDOW = 60  # seconds
PUB_DISCOVERY_STOP_LIMIT = 5
PLACES_CONCURRENCY_LIMIT = 4
PLACES_SEARCH_RADIUS_METERS = 500


def _is_rate_limited(session_code: str) -> bool:
    now = _time.monotonic()
    timestamps = _search_timestamps[session_code]
    # Prune old entries
    _search_timestamps[session_code] = [t for t in timestamps if now - t < SEARCH_RATE_WINDOW]
    if len(_search_timestamps[session_code]) >= SEARCH_RATE_LIMIT:
        return True
    _search_timestamps[session_code].append(now)
    return False


def _render_progress_html(pct: int, label: str) -> str:
    return f"""<div class="progress-box">
<div class="progress-info">
<span class="progress-info-label">{label}</span>
<span class="progress-info-pct">{pct}%</span>
</div>
<div class="progress-track">
<div class="progress-fill" style="width:{pct}%"></div>
</div>
</div>"""


router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.post("/session/{code}/search", response_class=HTMLResponse)
async def search(
    request: Request,
    code: str,
    departure_date: str = Form(..., max_length=10),
    departure_time: str = Form(..., max_length=5),
    return_date: str = Form(..., max_length=10),
    return_time: str = Form(..., max_length=5),
    method: str = Form("minimize-worst-case", max_length=30),
    direction: str = Form("round-trip", max_length=20),
    place_types: list[str] = Form(default=["pub", "bar", "cafe"]),
):
    # Validate enum inputs
    valid_methods = {"minimize-worst-case", "minimize-total"}
    valid_directions = {"round-trip", "there-only", "back-only"}
    valid_place_types = {"pub", "bar", "cafe", "restaurant"}
    if method not in valid_methods:
        method = "minimize-worst-case"
    if direction not in valid_directions:
        direction = "round-trip"
    place_types = [pt for pt in place_types if pt in valid_place_types] or ["pub", "bar", "cafe"]

    if _is_rate_limited(code):
        return templates.TemplateResponse(
            request,
            "partials/results_table.html",
            {
                "error": "Too many searches. Please wait a minute before trying again.",
                "results": None,
            },
        )

    db = request.app.state.db
    participants = await get_participants(db, code)
    active_participants = [p for p in participants if p["start_stop"]]
    stop_pairs = [(p["start_stop"], p["end_stop"] or p["start_stop"]) for p in active_participants]
    participant_names = [p["name"] for p in active_participants]

    if len(stop_pairs) < 2:
        return templates.TemplateResponse(
            request,
            "partials/results_table.html",
            {
                "error": "At least 2 participants must have selected their stops.",
                "results": None,
            },
        )

    is_valid, error_msg = validate_date_time(departure_date, departure_time)
    if not is_valid:
        return templates.TemplateResponse(
            request,
            "partials/results_table.html",
            {"error": f"Departure: {error_msg}", "results": None},
        )

    is_valid, error_msg = validate_date_time(return_date, return_time)
    if not is_valid:
        return templates.TemplateResponse(
            request,
            "partials/results_table.html",
            {"error": f"Return: {error_msg}", "results": None},
        )

    # Create a search task ID and start search in background
    search_id = secrets.token_hex(8)
    registry = request.app.state.search_registry
    registry.prune()
    registry.create(search_id, code)
    registry.start(
        search_id,
        _run_search(
            request,
            code,
            search_id,
            departure_date,
            departure_time,
            return_date,
            return_time,
            method,
            direction,
            stop_pairs,
            participant_names,
            active_participants,
            place_types,
        ),
    )

    # Return a progress bar that connects to SSE
    return f"""<div id="search-progress" hx-ext="sse" sse-connect="/session/{code}/search-progress/{search_id}" sse-swap="progress" sse-close="complete" hx-swap="innerHTML">
    {_render_progress_html(0, "Preparing search...")}
</div>"""


async def _run_search(
    request,
    code,
    search_id,
    departure_date,
    departure_time,
    return_date,
    return_time,
    method,
    direction,
    stop_pairs,
    participant_names,
    active_participants,
    place_types,
):
    """Run the search in the background, updating progress along the way."""
    registry = request.app.state.search_registry
    try:

        def progress_callback(stage, current, total):
            registry.update(search_id, stage=stage, current=current, total=total)

        departure_datetime = datetime.strptime(
            f"{departure_date} {departure_time}", "%Y-%m-%d %H:%M"
        )
        return_datetime = datetime.strptime(f"{return_date} {return_time}", "%Y-%m-%d %H:%M")
        distance_table = request.app.state.distance_table

        registry.update(search_id, stage="candidates")

        target_stops = await registry.run_blocking(
            get_optimal_stop_pairs, distance_table, method, stop_pairs, direction=direction
        )

        registry.update(
            search_id,
            stage="scraping",
            current=0,
            total=len(target_stops),
        )

        df_results = await registry.run_blocking(
            get_actual_time_optimal_stop_pairs,
            method,
            stop_pairs,
            target_stops,
            departure_datetime,
            get_total_minutes_with_retries,
            participant_names=participant_names,
            return_datetime=return_datetime,
            progress_callback=progress_callback,
            direction=direction,
        )

        db = request.app.state.db
        stop_geo = request.app.state.stop_geo
        top_stops = df_results["Target Stop"].to_list()
        pub_search_stop_names = top_stops[:PUB_DISCOVERY_STOP_LIMIT]

        registry.update(
            search_id,
            stage="pubs",
            current=0,
            total=len(pub_search_stop_names),
        )

        searchable_stops = []
        for stop_name in pub_search_stop_names:
            geo_row = stop_geo.filter(pl.col("name") == stop_name)
            if len(geo_row) == 0:
                logger.warning("No coordinates available for stop %s", stop_name)
                continue
            searchable_stops.append((stop_name, float(geo_row["lat"][0]), float(geo_row["lon"][0])))
        pub_search_stop_names = [stop_name for stop_name, _, _ in searchable_stops]
        pubs_by_stop_raw = {stop_name: [] for stop_name in pub_search_stop_names}
        places_api_error = False
        pending_queries = []
        # Form validation has already limited these values to supported types. Preserve
        # the submitted order while ensuring each stop/type coverage is checked once.
        unique_place_types = list(dict.fromkeys(place_types))

        # Check coverage per stop and type before scheduling only cache misses. An empty
        # cached response is meaningful coverage, so distinguish it from a cache miss.
        for stop_name, lat, lon in searchable_stops:
            for place_type in unique_place_types:
                cached = await get_cached_pubs_for_type(
                    db, stop_name, place_type, PLACES_SEARCH_RADIUS_METERS
                )
                if cached is None:
                    pending_queries.append((stop_name, lat, lon, place_type))
                else:
                    pubs_by_stop_raw[stop_name].extend(cached)

        # The semaphore is deliberately scoped to this search invocation. It limits live
        # Google requests without serialising cache reads or response processing.
        places_semaphore = asyncio.Semaphore(PLACES_CONCURRENCY_LIMIT)

        async def fetch_query(stop_name, lat, lon, place_type):
            try:
                async with places_semaphore:
                    pubs = await search_pubs_near_stop(
                        lat, lon, place_type, radius=PLACES_SEARCH_RADIUS_METERS
                    )
                return stop_name, place_type, pubs, None
            except Exception as exc:
                return stop_name, place_type, [], exc

        query_results = await asyncio.gather(*(fetch_query(*query) for query in pending_queries))
        # Cache writes are intentionally sequential: aiosqlite shares one connection and
        # cache_pubs_for_type uses an explicit transaction for each completed query.
        for stop_name, place_type, pubs, error in query_results:
            if error is not None:
                logger.warning("Places API error for %s (%s): %s", stop_name, place_type, error)
                places_api_error = True
                continue
            pubs_by_stop_raw[stop_name].extend(pubs)
            try:
                await cache_pubs_for_type(
                    db, stop_name, place_type, PLACES_SEARCH_RADIUS_METERS, pubs
                )
            except Exception as exc:
                logger.warning(
                    "Could not cache Places response for %s (%s): %s", stop_name, place_type, exc
                )

        for current, _ in enumerate(pub_search_stop_names, start=1):
            registry.update(search_id, current=current)

        # Filter by opening hours and deduplicate
        seen_place_ids: set[str] = set()
        pubs_by_stop = {}
        for stop_name in pub_search_stop_names:
            geo_row = stop_geo.filter(pl.col("name") == stop_name)
            ordered_pubs = order_pubs_for_stop(
                pubs_by_stop_raw[stop_name], float(geo_row["lat"][0]), float(geo_row["lon"][0])
            )
            unique_pubs = []
            for pub in ordered_pubs:
                if pub["place_id"] in seen_place_ids:
                    continue
                if not is_open_during(pub, departure_datetime, return_datetime):
                    continue
                seen_place_ids.add(pub["place_id"])
                unique_pubs.append(pub)
            pubs_by_stop[stop_name] = unique_pubs

        stop_geo_data = []
        for stop_name in top_stops:
            geo_row = stop_geo.filter(pl.col("name") == stop_name)
            if len(geo_row) > 0:
                stop_geo_data.append(
                    {
                        "name": stop_name,
                        "lat": float(geo_row["lat"][0]),
                        "lon": float(geo_row["lon"][0]),
                    }
                )

        pubs_flat = []
        for stop_name, pubs in pubs_by_stop.items():
            for pub in pubs:
                pubs_flat.append(
                    {
                        "stop": stop_name,
                        "name": pub["name"],
                        "lat": pub["lat"],
                        "lon": pub["lon"],
                        "rating": pub["rating"],
                        "rating_count": pub["rating_count"],
                        "url": pub["google_maps_url"],
                    }
                )

        participants_geo = []
        for p, (start, end) in zip(active_participants, stop_pairs):
            for stop_name, label in [(start, "from"), (end, "to")]:
                geo_row = stop_geo.filter(pl.col("name") == stop_name)
                if len(geo_row) > 0:
                    participants_geo.append(
                        {
                            "name": p["name"],
                            "stop": stop_name,
                            "type": label,
                            "lat": float(geo_row["lat"][0]),
                            "lon": float(geo_row["lon"][0]),
                        }
                    )

        warning = None
        if places_api_error:
            warning = "Google Places API limit reached — pub data may be incomplete for some stops."

        # Save results
        results_rows = df_results.rows(named=True)
        results_columns = df_results.columns
        await save_search_results(
            db,
            code,
            {
                "rows": results_rows,
                "columns": results_columns,
                "pubs_by_stop": {k: v for k, v in pubs_by_stop.items()},
                "pub_search_stop_names": pub_search_stop_names,
                "stops_geo": stop_geo_data,
                "pubs_flat": pubs_flat,
                "participants_geo": participants_geo,
                "warning": warning,
            },
        )

        result_html = templates.get_template("partials/results_table.html").render(
            request=request,
            error=None,
            results=df_results,
            pubs_by_stop=pubs_by_stop,
            pub_search_stop_names=set(pub_search_stop_names),
            stops_json=json.dumps(stop_geo_data),
            pubs_json=json.dumps(pubs_flat),
            participants_json=json.dumps(participants_geo),
            warning=warning,
        )

        registry.update(search_id, done=True, result_html=result_html)

    except Exception as e:
        logger.error("Search failed: %s", e, exc_info=True)
        error_html = templates.get_template("partials/results_table.html").render(
            request=request,
            error=f"Search failed: {e}",
            results=None,
        )
        registry.update(search_id, done=True, result_html=error_html)


@router.get("/session/{code}/search-progress/{search_id}")
async def search_progress_stream(request: Request, code: str, search_id: str):
    # Validate search_id format (hex only) to prevent injection
    if not search_id.isalnum() or len(search_id) > 32:
        return StreamingResponse(iter([]), media_type="text/event-stream", status_code=400)

    registry = request.app.state.search_registry
    registry.prune()

    async def event_stream():
        while True:
            if await request.is_disconnected():
                break

            progress = registry.get(search_id, code)

            if progress is None:
                # Search already completed and results were delivered — close silently
                break

            if progress.done:
                html = progress.result_html or ""
                escaped = html.replace("\n", "\ndata: ")
                yield f"event: progress\ndata: {escaped}\n\nevent: complete\ndata: done\n\n"
                registry.pop(search_id, code)
                break

            stage = progress.stage
            current = progress.current
            total = progress.total

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

            progress_html = _render_progress_html(pct, label)
            escaped_html = progress_html.replace("\n", "\ndata: ")
            yield f"event: progress\ndata: {escaped_html}\n\n"
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
        return templates.TemplateResponse(
            request,
            "results.html",
            {
                "session": session,
                "has_results": False,
            },
        )

    data = saved["data"]
    df_results = pl.DataFrame(data["rows"])

    return templates.TemplateResponse(
        request,
        "results.html",
        {
            "session": session,
            "has_results": True,
            "results": df_results,
            "pubs_by_stop": data["pubs_by_stop"],
            "pub_search_stop_names": set(data.get("pub_search_stop_names", [])),
            "stops_json": json.dumps(data["stops_geo"]),
            "pubs_json": json.dumps(data["pubs_flat"]),
            "participants_json": json.dumps(data["participants_geo"]),
            "warning": data.get("warning"),
            "created_at": saved["created_at"],
        },
    )
