import asyncio
import json
import logging
import secrets
import time as _time
from collections import defaultdict
from datetime import datetime
from html import escape

import polars as pl
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.responses import StreamingResponse

from backend.db import (
    begin_search,
    get_participants,
    get_search_results,
    get_session,
    save_search_results,
    save_search_results_if_active,
    update_search_results_if_current,
)
from backend.i18n import make_templates, translate
from backend.optimization import get_actual_time_optimal_stop_pairs, get_optimal_stop_pairs
from backend.places import (
    cache_pubs_for_type,
    get_cached_pubs_for_type,
    is_open_during,
    order_pubs_for_stop,
    search_pubs_near_stop,
)
from backend.reachability import participant_color, participant_text_color
from backend.utils import get_total_minutes_with_retries, validate_date_time

logger = logging.getLogger(__name__)

# Simple per-session rate limiter: max 3 searches per 60 seconds
_search_timestamps: dict[str, list[float]] = defaultdict(list)
_venue_expansion_timestamps: dict[str, list[float]] = defaultdict(list)
_venue_expansion_locks: dict[str, asyncio.Lock] = {}
SEARCH_RATE_LIMIT = 3
SEARCH_RATE_WINDOW = 60  # seconds
VENUE_EXPANSION_RATE_LIMIT = 3
VENUE_EXPANSION_RATE_WINDOW = 60
PUB_DISCOVERY_STOP_LIMIT = 5
PLACES_CONCURRENCY_LIMIT = 4
PLACES_SEARCH_RADIUS_METERS = 500
_PROGRESS_STAGES = {"starting", "candidates", "scraping", "pubs"}
_PLACE_TYPE_LABELS = {
    "pub": "Drinks",
    "bar": "Drinks",
    "cafe": "Coffee",
    "restaurant": "Food",
}


def _is_rate_limited(session_code: str) -> bool:
    now = _time.monotonic()
    timestamps = _search_timestamps[session_code]
    # Prune old entries
    _search_timestamps[session_code] = [t for t in timestamps if now - t < SEARCH_RATE_WINDOW]
    if len(_search_timestamps[session_code]) >= SEARCH_RATE_LIMIT:
        return True
    _search_timestamps[session_code].append(now)
    return False


def _is_venue_expansion_rate_limited(session_code: str) -> bool:
    """Limit uncached venue expansions per session to bound Google API spend."""
    now = _time.monotonic()
    recent = [
        timestamp
        for timestamp in _venue_expansion_timestamps[session_code]
        if now - timestamp < VENUE_EXPANSION_RATE_WINDOW
    ]
    _venue_expansion_timestamps[session_code] = recent
    if len(recent) >= VENUE_EXPANSION_RATE_LIMIT:
        return True
    recent.append(now)
    return False


def _selected_place_type_labels(place_types: list[str], locale: str = "en") -> tuple[str, ...]:
    labels = []
    for place_type in place_types:
        label = _PLACE_TYPE_LABELS.get(place_type)
        if label:
            label = translate(f"session.{label.lower()}", locale)
        if label and label not in labels:
            labels.append(label)
    return tuple(labels)


def _format_place_type_labels(place_type_labels: tuple[str, ...], locale: str = "en") -> str:
    if len(place_type_labels) < 2:
        return "".join(place_type_labels)
    if len(place_type_labels) == 2:
        return (" a " if locale == "cs" else " and ").join(place_type_labels)
    conjunction = " a " if locale == "cs" else ", and "
    return f"{', '.join(place_type_labels[:-1])}{conjunction}{place_type_labels[-1]}"


def _progress_place_type_suffix(place_type_labels: tuple[str, ...], locale: str) -> str:
    labels = _format_place_type_labels(place_type_labels, locale)
    if not labels:
        return ""
    return f" {'pro' if locale == 'cs' else 'for'} {labels}"


def _normalise_progress(
    percentage: object,
    stage: object,
    current: object,
    total: object,
) -> tuple[int, str, int, int]:
    """Return safe values for a progress element and its operational status."""

    def nonnegative_int(value: object) -> int:
        try:
            return max(int(value), 0)
        except (TypeError, ValueError):
            return 0

    safe_percentage = min(nonnegative_int(percentage), 100)
    safe_stage = stage if isinstance(stage, str) and stage in _PROGRESS_STAGES else "starting"
    safe_total = nonnegative_int(total)
    safe_current = nonnegative_int(current)
    if safe_total:
        safe_current = min(safe_current, safe_total)
    else:
        safe_current = 0
    return safe_percentage, safe_stage, safe_current, safe_total


def _progress_percentage(stage: str, current: int, total: int) -> int:
    if stage == "candidates":
        return 5
    if stage == "scraping":
        return 10 + int((current / total) * 70) if total else 10
    if stage == "pubs":
        return 80 + int((current / total) * 18) if total else 80
    return 0


def _progress_announcement(stage: str, place_type_labels: tuple[str, ...], total: int) -> str:
    if stage == "candidates":
        return "Select candidates from the transit matrix."
    if stage == "scraping":
        return "Query DPP journey times."
    if stage == "pubs":
        type_summary = _format_place_type_labels(place_type_labels)
        if type_summary:
            return f"Query nearby places for {type_summary} across {total} top stops."
        return f"Query nearby places across {total} top stops."
    return "Preparing search."


def _render_progress_html(
    percentage: int,
    stage: str,
    current: int,
    total: int,
    place_type_labels: tuple[str, ...] = (),
    locale: str = "en",
) -> str:
    percentage, stage, current, total = _normalise_progress(percentage, stage, current, total)
    return templates.get_template("partials/search_progress.html").render(
        percentage=percentage,
        stage=stage,
        current=current,
        total=total,
        place_type_labels=_progress_place_type_suffix(place_type_labels, locale),
        locale=locale,
    )


def _render_progress_update_html(
    percentage: int,
    stage: str,
    current: int,
    total: int,
    place_type_labels: tuple[str, ...],
    announced_stage: str,
    locale: str = "en",
) -> tuple[str, str]:
    percentage, stage, current, total = _normalise_progress(percentage, stage, current, total)
    progress_html = _render_progress_html(
        percentage, stage, current, total, place_type_labels, locale
    )
    if stage == announced_stage:
        return progress_html, stage
    announcement = escape(_progress_announcement(stage, place_type_labels, total))
    announcement_html = (
        '<p id="search-progress-announcement" class="progress-announcement visually-hidden" '
        'aria-live="polite" hx-swap-oob="true">'
        f"{announcement}</p>"
    )
    return f"{progress_html}\n{announcement_html}", stage


router = APIRouter()
templates = make_templates()
templates.env.filters["participant_text_color"] = participant_text_color


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

    db = request.app.state.db
    participants = await get_participants(db, code)
    if len(participants) < 2:
        return templates.TemplateResponse(
            request,
            "partials/results_table.html",
            {
                "error": "Add one more participant, then choose their stops.",
                "results": None,
            },
        )

    incomplete_participant = next(
        (
            participant
            for participant in participants
            if not participant["start_stop"]
            or (not participant["same_start_end"] and not participant["end_stop"])
        ),
        None,
    )
    if incomplete_participant:
        return templates.TemplateResponse(
            request,
            "partials/results_table.html",
            {
                "error": f"{incomplete_participant['name']} needs start and end stops.",
                "results": None,
            },
        )

    if _is_rate_limited(code):
        return templates.TemplateResponse(
            request,
            "partials/results_table.html",
            {
                "error": "Too many searches. Please wait a minute before trying again.",
                "results": None,
            },
        )

    active_participants = participants
    stop_pairs = [(p["start_stop"], p["end_stop"] or p["start_stop"]) for p in participants]
    participant_names = [p["name"] for p in participants]

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
    locale = request.state.locale
    selected_place_type_labels = _selected_place_type_labels(place_types, locale)
    registry.create(search_id, code, place_type_labels=selected_place_type_labels, locale=locale)
    await begin_search(db, code, search_id)
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
    return f"""<section class="search-progress-panel">
<p id="search-progress-announcement" class="progress-announcement visually-hidden" aria-live="polite">{translate("progress.preparing", locale)}</p>
<div id="search-progress" hx-ext="sse" sse-connect="/session/{code}/search-progress/{search_id}" sse-swap="progress" sse-close="complete" hx-swap="innerHTML">
    {_render_progress_html(0, "starting", 0, 0, selected_place_type_labels, locale)}
</div>
<a class="search-progress-back" href="/session/{code}">{translate("progress.back", locale)}</a>
</section>"""


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
        pending_queries_by_stop = {stop_name: [] for stop_name in pub_search_stop_names}
        # Form validation has already limited these values to supported types. Preserve
        # the submitted order while ensuring each stop/type coverage is checked once.
        unique_place_types = list(dict.fromkeys(place_types))

        registry.update(
            search_id,
            stage="pubs",
            current=0,
            total=len(pub_search_stop_names),
        )

        # Check coverage per stop and type before scheduling only cache misses. An empty
        # cached response is meaningful coverage, so distinguish it from a cache miss.
        for stop_name, lat, lon in searchable_stops:
            for place_type in unique_place_types:
                cached = await get_cached_pubs_for_type(
                    db, stop_name, place_type, PLACES_SEARCH_RADIUS_METERS
                )
                if cached is None:
                    pending_queries_by_stop[stop_name].append((stop_name, lat, lon, place_type))
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

        async def fetch_stop(stop_name, queries):
            return stop_name, await asyncio.gather(*(fetch_query(*query) for query in queries))

        pending_stop_tasks = [
            asyncio.create_task(fetch_stop(stop_name, queries))
            for stop_name, queries in pending_queries_by_stop.items()
            if queries
        ]
        completed_stops = 0

        # Fully cached stops are already checked. Report each before waiting for live work.
        for stop_name in pub_search_stop_names:
            if not pending_queries_by_stop[stop_name]:
                completed_stops += 1
                registry.update(search_id, current=completed_stops)

        try:
            # Cache writes are intentionally sequential: aiosqlite shares one connection and
            # cache_pubs_for_type uses an explicit transaction for each completed query.
            for completed_task in asyncio.as_completed(pending_stop_tasks):
                _, query_results = await completed_task
                for stop_name, place_type, pubs, error in query_results:
                    if error is not None:
                        logger.warning(
                            "Places API error for %s (%s): %s", stop_name, place_type, error
                        )
                        places_api_error = True
                        continue
                    pubs_by_stop_raw[stop_name].extend(pubs)
                    try:
                        await cache_pubs_for_type(
                            db, stop_name, place_type, PLACES_SEARCH_RADIUS_METERS, pubs
                        )
                    except Exception as exc:
                        logger.warning(
                            "Could not cache Places response for %s (%s): %s",
                            stop_name,
                            place_type,
                            exc,
                        )
                completed_stops += 1
                registry.update(search_id, current=completed_stops)
        finally:
            for pending_stop_task in pending_stop_tasks:
                if not pending_stop_task.done():
                    pending_stop_task.cancel()
            if pending_stop_tasks:
                await asyncio.gather(*pending_stop_tasks, return_exceptions=True)

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
            warning = "Google Places API limit reached; pub data may be incomplete for some stops."

        participant_snapshot = [
            {
                "id": participant["id"],
                "name": participant["name"],
                "color": participant_color(participant["id"]),
                "start_stop": start,
                "end_stop": end,
            }
            for participant, (start, end) in zip(active_participants, stop_pairs)
        ]

        # Save results
        results_rows = df_results.rows(named=True)
        results_columns = df_results.columns
        is_current = getattr(registry, "is_current", lambda _id, _code: True)
        if not is_current(search_id, code):
            return
        data = {
            "rows": results_rows,
            "columns": results_columns,
            "pubs_by_stop": {k: v for k, v in pubs_by_stop.items()},
            "pub_search_stop_names": pub_search_stop_names,
            "place_types": unique_place_types,
            "departure_datetime": departure_datetime.isoformat(),
            "return_datetime": return_datetime.isoformat(),
            "stops_geo": stop_geo_data,
            "pubs_flat": pubs_flat,
            "participants_geo": participants_geo,
            "search_direction": direction,
            "participant_snapshot": participant_snapshot,
            "search_id": search_id,
            "search_method": method,
            "warning": warning,
        }
        if hasattr(registry, "is_current"):
            saved = await save_search_results_if_active(db, code, search_id, data)
        else:
            await save_search_results(db, code, data)
            saved = True
        if not saved:
            return

        result_html = templates.get_template("partials/results_table.html").render(
            request=request,
            error=None,
            results=df_results,
            pubs_by_stop=pubs_by_stop,
            pub_search_stop_names=set(pub_search_stop_names),
            session_code=code,
            stops_json=json.dumps(stop_geo_data),
            pubs_json=json.dumps(pubs_flat),
            participant_snapshot=participant_snapshot,
            participant_snapshot_json=json.dumps(participant_snapshot),
            search_direction=direction,
            search_method=method,
            search_id=search_id,
            warning=warning,
        )

        if is_current(search_id, code):
            registry.update(search_id, done=True, result_html=result_html)

    except Exception as e:
        logger.error("Search failed: %s", e, exc_info=True)
        error_html = templates.get_template("partials/results_table.html").render(
            request=request,
            error=f"Search failed: {e}",
            results=None,
        )
        if getattr(registry, "is_current", lambda _id, _code: True)(search_id, code):
            registry.update(search_id, done=True, result_html=error_html)


@router.get("/session/{code}/search-progress/{search_id}")
async def search_progress_stream(request: Request, code: str, search_id: str):
    # Validate search_id format (hex only) to prevent injection
    if not search_id.isalnum() or len(search_id) > 32:
        return StreamingResponse(iter([]), media_type="text/event-stream", status_code=400)

    registry = request.app.state.search_registry
    registry.prune()

    async def event_stream():
        announced_stage = "starting"
        while True:
            if await request.is_disconnected():
                break

            progress = registry.get(search_id, code)

            if progress is None:
                # Search already completed and results were delivered; close silently
                break

            if progress.cancelled:
                registry.pop(search_id, code)
                break

            if progress.done:
                html = progress.result_html or ""
                escaped = html.replace("\n", "\ndata: ")
                yield f"event: progress\ndata: {escaped}\n\nevent: complete\ndata: done\n\n"
                registry.pop(search_id, code)
                break

            _, stage, current, total = _normalise_progress(
                0, progress.stage, progress.current, progress.total
            )
            pct = _progress_percentage(stage, current, total)

            progress_html, announced_stage = _render_progress_update_html(
                pct,
                stage,
                current,
                total,
                progress.place_type_labels,
                announced_stage,
                progress.locale,
            )
            escaped_html = progress_html.replace("\n", "\ndata: ")
            yield f"event: progress\ndata: {escaped_html}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


def _pubs_flat_from_saved(pubs_by_stop: dict[str, list[dict]]) -> list[dict]:
    """Build the map marker payload from the persisted venue lists."""
    return [
        {
            "stop": stop_name,
            "name": pub["name"],
            "lat": pub["lat"],
            "lon": pub["lon"],
            "rating": pub.get("rating"),
            "rating_count": pub.get("rating_count"),
            "url": pub.get("google_maps_url", ""),
        }
        for stop_name, pubs in pubs_by_stop.items()
        for pub in pubs
    ]


def _render_venue_suggestions(
    request: Request,
    code: str,
    stop_name: str,
    pubs: list[dict],
    *,
    searched: bool,
    state: str | None = None,
    saved_data: dict | None = None,
) -> HTMLResponse:
    if state is None:
        if searched and pubs:
            state = "loaded"
        elif searched:
            state = "empty"
        else:
            state = "not-searched"
    context = {
        "session_code": code,
        "stop_name": stop_name,
        "pubs": pubs,
        "searched": searched,
        "venue_state": state,
        "venue_error": None,
        "map_update": saved_data is not None,
    }
    if saved_data is not None:
        context.update(
            {
                "stops_json": json.dumps(saved_data.get("stops_geo", [])),
                "pubs_json": json.dumps(saved_data.get("pubs_flat", [])),
                "participant_snapshot_json": json.dumps(saved_data.get("participant_snapshot", [])),
            }
        )
    return templates.TemplateResponse(request, "partials/venue_suggestions.html", context)


def _stop_coordinates(request: Request, data: dict, stop_name: str) -> tuple[float, float] | None:
    for stop in data.get("stops_geo", []):
        if stop.get("name") == stop_name:
            return float(stop["lat"]), float(stop["lon"])

    geo_row = request.app.state.stop_geo.filter(pl.col("name") == stop_name)
    if len(geo_row) == 0:
        return None
    return float(geo_row["lat"][0]), float(geo_row["lon"][0])


@router.post("/session/{code}/venues", response_class=HTMLResponse)
async def load_venues_for_stop(
    request: Request,
    code: str,
    stop_name: str = Form(..., max_length=200),
):
    """Load and persist venue suggestions for one ranked stop on demand."""
    db = request.app.state.db
    if await get_session(db, code) is None:
        raise HTTPException(status_code=404, detail="Session not found")

    lock = _venue_expansion_locks.setdefault(code, asyncio.Lock())
    async with lock:
        saved = await get_search_results(db, code)
        if saved is None:
            raise HTTPException(status_code=404, detail="Search results not found")
        data = saved["data"]
        original_search_id = data.get("search_id", "")
        if not isinstance(original_search_id, str):
            raise HTTPException(status_code=422, detail="Saved search version is invalid")
        original_created_at = saved["created_at"]
        ranked_stops = {
            row.get("Target Stop") for row in data.get("rows", []) if row.get("Target Stop")
        }
        if stop_name not in ranked_stops:
            raise HTTPException(status_code=404, detail="Stop not found in these results")

        pubs_by_stop = data.setdefault("pubs_by_stop", {})
        searched_stops = data.get("pub_search_stop_names")
        if searched_stops is None:
            searched_stops = list(pubs_by_stop)
        if stop_name in searched_stops:
            return _render_venue_suggestions(
                request,
                code,
                stop_name,
                pubs_by_stop.get(stop_name, []),
                searched=True,
            )

        coordinates = _stop_coordinates(request, data, stop_name)
        if coordinates is None:
            return _render_venue_suggestions(
                request,
                code,
                stop_name,
                [],
                searched=False,
                state="provider-error",
            )
        lat, lon = coordinates

        valid_types = {"pub", "bar", "cafe", "restaurant"}
        saved_place_types = data.get("place_types") or ["pub", "bar", "cafe"]
        place_types = list(
            dict.fromkeys(
                place_type for place_type in saved_place_types if place_type in valid_types
            )
        ) or ["pub", "bar", "cafe"]

        pubs_raw = []
        missing_types = []
        for place_type in place_types:
            cached = await get_cached_pubs_for_type(
                db, stop_name, place_type, PLACES_SEARCH_RADIUS_METERS
            )
            if cached is None:
                missing_types.append(place_type)
            else:
                pubs_raw.extend(cached)

        if missing_types and _is_venue_expansion_rate_limited(code):
            return _render_venue_suggestions(
                request,
                code,
                stop_name,
                [],
                searched=False,
                state="rate-limited",
            )

        semaphore = asyncio.Semaphore(PLACES_CONCURRENCY_LIMIT)

        async def fetch(place_type: str):
            try:
                async with semaphore:
                    pubs = await search_pubs_near_stop(
                        lat,
                        lon,
                        place_type,
                        radius=PLACES_SEARCH_RADIUS_METERS,
                    )
                return place_type, pubs, None
            except Exception as exc:
                return place_type, [], exc

        fetch_results = await asyncio.gather(*(fetch(place_type) for place_type in missing_types))
        had_error = False
        for place_type, pubs, error in fetch_results:
            if error is not None:
                logger.warning(
                    "On-demand Places API error for %s (%s): %s", stop_name, place_type, error
                )
                had_error = True
                continue
            pubs_raw.extend(pubs)
            try:
                await cache_pubs_for_type(
                    db,
                    stop_name,
                    place_type,
                    PLACES_SEARCH_RADIUS_METERS,
                    pubs,
                )
            except Exception as exc:
                logger.warning(
                    "Could not cache on-demand Places response for %s (%s): %s",
                    stop_name,
                    place_type,
                    exc,
                )

        if had_error:
            return _render_venue_suggestions(
                request,
                code,
                stop_name,
                [],
                searched=False,
                state="provider-error",
            )

        departure_value = data.get("departure_datetime")
        return_value = data.get("return_datetime")
        departure_datetime = datetime.fromisoformat(departure_value) if departure_value else None
        return_datetime = datetime.fromisoformat(return_value) if return_value else None
        used_place_ids = {
            pub.get("place_id")
            for existing_stop, pubs in pubs_by_stop.items()
            if existing_stop != stop_name
            for pub in pubs
        }
        pubs = []
        for pub in order_pubs_for_stop(pubs_raw, lat, lon):
            if pub.get("place_id") in used_place_ids:
                continue
            if departure_datetime and return_datetime:
                if not is_open_during(pub, departure_datetime, return_datetime):
                    continue
            pubs.append(pub)

        pubs_by_stop[stop_name] = pubs
        searched_stops.append(stop_name)
        data["pub_search_stop_names"] = searched_stops
        data["pubs_flat"] = _pubs_flat_from_saved(pubs_by_stop)
        updated = await update_search_results_if_current(
            db,
            code,
            data,
            search_id=original_search_id,
            created_at=original_created_at,
        )
        if not updated:
            return _render_venue_suggestions(
                request, code, stop_name, [], searched=False, state="stale"
            )

        return _render_venue_suggestions(
            request,
            code,
            stop_name,
            pubs,
            searched=True,
            saved_data=data,
        )


@router.get("/session/{code}/results", response_class=HTMLResponse)
async def results_page(request: Request, code: str):
    """Shareable results page showing the last search results for a session."""
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
    pub_search_stop_names = data.get("pub_search_stop_names")
    if pub_search_stop_names is None:
        pub_search_stop_names = df_results["Target Stop"].head(PUB_DISCOVERY_STOP_LIMIT).to_list()

    return templates.TemplateResponse(
        request,
        "results.html",
        {
            "session": session,
            "has_results": True,
            "results": df_results,
            "pubs_by_stop": data["pubs_by_stop"],
            "pub_search_stop_names": set(pub_search_stop_names),
            "session_code": code,
            "stops_json": json.dumps(data["stops_geo"]),
            "pubs_json": json.dumps(data["pubs_flat"]),
            "participant_snapshot": data.get("participant_snapshot", []),
            "participant_snapshot_json": json.dumps(data.get("participant_snapshot", [])),
            "search_direction": data.get("search_direction", "round-trip"),
            "search_method": data.get("search_method", "minimize-worst-case"),
            "search_id": data.get("search_id", ""),
            "warning": data.get("warning"),
            "created_at": saved["created_at"],
        },
    )
