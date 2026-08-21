"""Integration tests covering the full search flow, JSON serialization, and caching."""

import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import aiosqlite
import httpx
import polars as pl
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import routers.search as search_router
from backend.app import app
from backend.db import (
    add_participant_stops,
    create_session,
    get_participants,
    get_search_results,
    init_db,
    join_session,
    save_search_results,
)
from backend.places import get_cached_pubs_for_type
from backend.search_registry import SearchRegistry
from routers.search import _search_timestamps


@pytest_asyncio.fixture
async def db():
    conn = await aiosqlite.connect(":memory:")
    await init_db(conn)
    yield conn
    await conn.close()


@pytest_asyncio.fixture(autouse=True)
async def setup_app():
    """Set up app state with minimal test data."""
    db = await aiosqlite.connect(":memory:")
    await init_db(db)

    distance_table = pl.DataFrame(
        {
            "from": ["A", "A", "B", "B"],
            "to": ["B", "A", "A", "B"],
            "distance_in_km": [1.0, 1.0, 1.0, 0.0],
            "total_minutes": [10, 10, 10, 0],
        }
    )
    stop_geo = pl.DataFrame(
        {
            "name": ["A", "B"],
            "lat": [50.08, 50.09],
            "lon": [14.42, 14.43],
        }
    )

    app.state.db = db
    app.state.distance_table = distance_table
    app.state.all_stops = ["A", "B"]
    app.state.stop_geo = stop_geo
    registry = SearchRegistry(result_ttl_seconds=1)
    app.state.search_registry = registry
    _search_timestamps.clear()
    search_router._places_request_tasks.clear()

    yield db

    await registry.shutdown()
    await db.close()


async def _create_session_with_participants(client, stops):
    """Helper: create a session via DB directly, add participants with stops, return session code."""
    db = app.state.db
    session = await create_session(db, "Test", "P1")
    code = session["code"]

    # Get creator participant
    participants = await get_participants(db, code)
    p1 = [p for p in participants if p["name"] == "P1"][0]
    await add_participant_stops(db, code, p1["id"], stops[0][0], stops[0][1])

    # Additional participants
    for i, (start, end) in enumerate(stops[1:], start=2):
        result = await join_session(db, code, f"P{i}")
        await add_participant_stops(db, code, result["id"], start, end)

    return code


# --- save_search_results serialization tests ---


@pytest.mark.asyncio
async def test_save_results_with_polars_types(db):
    """Polars row dicts (may contain non-native types) serialize without error."""
    session = await create_session(db, "Test", "Dan")
    df = pl.DataFrame(
        {
            "Target Stop": ["A", "B"],
            "Worst Case Minutes": [10, 20],
            "Total Minutes": [15, 30],
        }
    )
    results_data = {
        "rows": df.rows(named=True),
        "columns": df.columns,
        "pubs_by_stop": {},
        "stops_geo": [],
        "pubs_flat": [],
        "participants_geo": [],
        "warning": None,
    }
    await save_search_results(db, session["code"], results_data)
    saved = await get_search_results(db, session["code"])
    assert saved is not None
    assert len(saved["data"]["rows"]) == 2
    assert saved["data"]["rows"][0]["Target Stop"] == "A"


@pytest.mark.asyncio
async def test_save_results_with_none_values(db):
    """Results containing None values serialize correctly."""
    session = await create_session(db, "Test", "Dan")
    df = pl.DataFrame(
        {
            "Target Stop": ["A"],
            "Worst Case Minutes": [None],
            "Total Minutes": [None],
        }
    )
    results_data = {
        "rows": df.rows(named=True),
        "columns": df.columns,
        "pubs_by_stop": {
            "A": [
                {
                    "place_id": "x",
                    "name": "Pub",
                    "lat": 50.0,
                    "lon": 14.0,
                    "rating": None,
                    "rating_count": None,
                    "price_level": None,
                    "google_maps_url": "",
                }
            ]
        },
        "stops_geo": [{"name": "A", "lat": 50.0, "lon": 14.0}],
        "pubs_flat": [],
        "participants_geo": [],
        "warning": None,
    }
    await save_search_results(db, session["code"], results_data)
    saved = await get_search_results(db, session["code"])
    assert saved is not None
    assert saved["data"]["rows"][0]["Worst Case Minutes"] is None


@pytest.mark.asyncio
async def test_save_results_with_nested_pub_dicts(db):
    """Pub dicts (containing dicts as values) serialize and deserialize correctly."""
    session = await create_session(db, "Test", "Dan")
    pubs = [
        {
            "place_id": f"id{i}",
            "name": f"Pub {i}",
            "lat": 50.0 + i * 0.01,
            "lon": 14.0 + i * 0.01,
            "rating": 4.5,
            "rating_count": 100,
            "price_level": 2,
            "google_maps_url": f"https://maps.google.com/{i}",
        }
        for i in range(5)
    ]
    results_data = {
        "rows": [{"Target Stop": "A", "Worst Case Minutes": 10, "Total Minutes": 20}],
        "columns": ["Target Stop", "Worst Case Minutes", "Total Minutes"],
        "pubs_by_stop": {"A": pubs},
        "stops_geo": [{"name": "A", "lat": 50.0, "lon": 14.0}],
        "pubs_flat": [
            {
                "stop": "A",
                "name": p["name"],
                "lat": p["lat"],
                "lon": p["lon"],
                "rating": p["rating"],
                "rating_count": p["rating_count"],
                "url": p["google_maps_url"],
            }
            for p in pubs
        ],
        "participants_geo": [
            {"name": "Dan", "stop": "B", "type": "from", "lat": 50.08, "lon": 14.42}
        ],
        "warning": None,
    }
    await save_search_results(db, session["code"], results_data)
    saved = await get_search_results(db, session["code"])
    assert len(saved["data"]["pubs_by_stop"]["A"]) == 5
    assert saved["data"]["pubs_flat"][0]["name"] == "Pub 0"


@pytest.mark.asyncio
async def test_save_results_overwrites_previous(db):
    """Saving results twice for the same session replaces the first."""
    session = await create_session(db, "Test", "Dan")
    await save_search_results(db, session["code"], {"rows": [], "v": 1})
    await save_search_results(db, session["code"], {"rows": [{"x": 1}], "v": 2})
    saved = await get_search_results(db, session["code"])
    assert saved["data"]["v"] == 2
    assert len(saved["data"]["rows"]) == 1


# --- Cache key tests ---


def test_cache_key_uses_string_datetime():
    """get_total_minutes_with_retries cache key works with datetime args."""
    from backend.utils import _cache_key

    dt = datetime(2026, 3, 28, 20, 0)
    key1 = _cache_key("A", "B", dt)
    key2 = _cache_key("A", "B", dt)
    key3 = _cache_key("A", "C", dt)

    assert key1 == key2
    assert key1 != key3
    # All elements must be hashable (usable as dict key)
    assert hash(key1)


# --- Full search endpoint integration tests ---


@pytest.mark.asyncio
async def test_search_requires_two_participants():
    """Search with fewer than 2 participants returns an error."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        code = await _create_session_with_participants(client, [("A", "A")])
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        resp = await client.post(
            f"/session/{code}/search",
            data={
                "departure_date": tomorrow,
                "departure_time": "20:00",
                "return_date": tomorrow,
                "return_time": "23:00",
                "method": "minimize-worst-case",
            },
        )
    assert resp.status_code == 200
    assert "Add one more participant" in resp.text


@pytest.mark.asyncio
async def test_search_requires_every_person_to_have_a_complete_trip_before_rate_limiting(
    monkeypatch,
):
    """An incomplete second participant cannot start or consume a search."""
    transport = ASGITransport(app=app)
    called = False

    def rate_limiter(_code):
        nonlocal called
        called = True
        return False

    monkeypatch.setattr(search_router, "_is_rate_limited", rate_limiter)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        code = await _create_session_with_participants(client, [("A", "A"), ("B", "")])
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        response = await client.post(
            f"/session/{code}/search",
            data={
                "departure_date": tomorrow,
                "departure_time": "20:00",
                "return_date": tomorrow,
                "return_time": "23:00",
            },
        )

    assert "P2 needs start and end stops." in response.text
    assert called is False


async def _wait_for_search(search_id, session_code, timeout=10):
    """Wait for a background search to complete."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        progress = app.state.search_registry.get(search_id, session_code)
        if progress and progress.done:
            return True
        await asyncio.sleep(0.1)
    return False


def _extract_search_id(html: str) -> str:
    """Extract search_id from the progress SSE HTML response."""
    import re

    match = re.search(r"search-progress/([a-f0-9]+)", html)
    return match.group(1) if match else ""


def test_progress_copy_names_each_operation():
    """Progress rendering identifies the active search operation and its work count."""
    assert "Select candidates from the transit matrix" in search_router._render_progress_html(
        5, "candidates", 0, 0
    )
    scraping = search_router._render_progress_html(42, "scraping", 14, 31)
    assert "Query DPP journey times" in scraping
    assert "14 of 31 candidate stops checked" in scraping
    venues = search_router._render_progress_html(85, "pubs", 2, 5)
    assert "Query nearby places" in venues
    assert "2 of 5 stops checked" in venues


def test_progress_rendering_clamps_invalid_progress_values():
    """Unexpected registry values cannot produce an invalid progress element or count."""
    scraping = search_router._render_progress_html(125, "scraping", 100, 3)
    assert 'max="100" value="100"' in scraping
    assert "3 of 3 candidate stops checked" in scraping

    unknown = search_router._render_progress_html(-1, "unexpected", -4, 0)
    assert 'max="100" value="0"' in unknown
    assert "Preparing search." in unknown


def test_progress_event_announces_only_stage_transitions():
    """Count changes replace only visual progress, while a stage change updates the live region."""
    same_stage, announced_stage = search_router._render_progress_update_html(
        42, "scraping", 14, 31, (), "scraping"
    )
    assert announced_stage == "scraping"
    assert "aria-live" not in same_stage
    assert "hx-swap-oob" not in same_stage

    changed_stage, announced_stage = search_router._render_progress_update_html(
        42, "scraping", 14, 31, (), "candidates"
    )
    assert announced_stage == "scraping"
    assert 'aria-live="polite"' in changed_stage
    assert 'hx-swap-oob="true"' in changed_stage
    announcement = changed_stage.split('hx-swap-oob="true"', 1)[1]
    assert "14" not in announcement
    assert "31" not in announcement
    assert "42" not in announcement


def test_nearby_place_progress_names_selected_types_and_top_stops():
    """Venue progress identifies both the selected categories and focused stop count."""
    venues = search_router._render_progress_html(85, "pubs", 2, 5, ("Coffee", "Food"))
    assert "Query nearby places for Coffee and Food across 5 top stops" in venues
    assert "2 of 5 stops checked" in venues


def test_progress_updates_keep_selected_types_after_the_initial_fragment():
    """SSE count updates retain the venue labels chosen when the search started."""
    venues, announced_stage = search_router._render_progress_update_html(
        85, "pubs", 2, 5, ("Coffee", "Food"), "pubs"
    )
    assert announced_stage == "pubs"
    assert "Query nearby places for Coffee and Food across 5 top stops" in venues


class _DirectSearchRegistry:
    """Minimal registry for exercising _run_search without an HTTP background task."""

    def __init__(self):
        self.updates = []

    async def run_blocking(self, func, *args, **kwargs):
        return func(*args, **kwargs)

    def update(self, search_id, **kwargs):
        self.updates.append((search_id, kwargs))


def _six_stop_results():
    return pl.DataFrame(
        {
            "Target Stop": ["A", "B", "C", "D", "E", "F"],
            "Worst Case Minutes": [10, 11, 12, 13, 14, 15],
            "Total Minutes": [20, 21, 22, 23, 24, 25],
        }
    )


async def _run_direct_search(monkeypatch, code, place_types):
    """Run a six-stop venue search with deterministic candidate and transit data."""
    registry = _DirectSearchRegistry()
    app.state.search_registry = registry
    app.state.stop_geo = pl.DataFrame(
        {
            "name": ["A", "B", "C", "D", "E", "F"],
            "lat": [50.00, 50.01, 50.02, 50.03, 50.04, 50.05],
            "lon": [14.00, 14.01, 14.02, 14.03, 14.04, 14.05],
        }
    )
    monkeypatch.setattr(
        search_router, "get_optimal_stop_pairs", lambda *args, **kwargs: list("ABCDEF")
    )
    monkeypatch.setattr(
        search_router,
        "get_actual_time_optimal_stop_pairs",
        lambda *args, **kwargs: _six_stop_results(),
    )

    await search_router._run_search(
        type("Request", (), {"app": app})(),
        code,
        "direct-search",
        "2026-08-10",
        "20:00",
        "2026-08-10",
        "23:00",
        "minimize-worst-case",
        "round-trip",
        [("A", "A"), ("B", "B")],
        ["P1", "P2"],
        [],
        place_types,
    )
    return registry


@pytest.mark.asyncio
async def test_search_live_reranking_uses_only_the_best_ten_matrix_candidates(monkeypatch):
    """A search must not send lower-ranked matrix candidates to the live provider."""
    session = await create_session(app.state.db, "Test", "P1")
    candidates = [f"C{i}" for i in range(12)]
    matrix_rows = []
    for index, candidate in enumerate(candidates):
        for participant_stop in ("A", "B"):
            matrix_rows.extend(
                [
                    {
                        "from": participant_stop,
                        "to": candidate,
                        "distance_in_km": float(index + 1),
                        "total_minutes": index + 1,
                    },
                    {
                        "from": candidate,
                        "to": participant_stop,
                        "distance_in_km": float(index + 1),
                        "total_minutes": index + 1,
                    },
                ]
            )
    app.state.distance_table = pl.DataFrame(matrix_rows)
    app.state.stop_geo = pl.DataFrame(
        {
            "name": candidates,
            "lat": [50.0 + index / 100 for index in range(len(candidates))],
            "lon": [14.0 + index / 100 for index in range(len(candidates))],
        }
    )
    registry = _DirectSearchRegistry()
    app.state.search_registry = registry

    # Candidate discovery is already covered separately. Reverse its output so
    # this test also catches a naive ``candidates[:10]`` implementation.
    monkeypatch.setattr(
        search_router,
        "get_optimal_stop_pairs",
        lambda *args, **kwargs: list(reversed(candidates)),
    )

    def fake_live_reranking(
        method,
        stop_pairs,
        target_stops,
        event_datetime,
        get_total_minutes_func,
        **kwargs,
    ):
        return pl.DataFrame(
            {
                "Target Stop": target_stops,
                "Worst Case Minutes": list(range(1, len(target_stops) + 1)),
                "Total Minutes": list(range(2, len(target_stops) + 2)),
            }
        )

    monkeypatch.setattr(
        search_router,
        "get_actual_time_optimal_stop_pairs",
        fake_live_reranking,
    )

    await search_router._run_search(
        type("Request", (), {"app": app})(),
        session["code"],
        "limited-search",
        "2026-08-22",
        "20:00",
        "2026-08-22",
        "23:00",
        "minimize-worst-case",
        "round-trip",
        [("A", "A"), ("B", "B")],
        ["P1", "P2"],
        [],
        ["pub"],
    )

    saved = await get_search_results(app.state.db, session["code"])
    assert saved is not None
    assert [row["Target Stop"] for row in saved["data"]["rows"]] == candidates[:10]
    scraping_totals = [
        update["total"]
        for _, update in registry.updates
        if update.get("stage") == "scraping" and "total" in update
    ]
    assert scraping_totals == [10]


@pytest.mark.asyncio
async def test_search_defers_venue_discovery_until_a_user_requests_it(monkeypatch):
    """Optimising a plan must not create billable Google Places requests."""
    session = await create_session(app.state.db, "Test", "P1")
    calls = []

    async def fake_search(lat, lon, place_type, radius=500):
        calls.append((lat, lon, place_type))
        return []

    monkeypatch.setattr(search_router, "search_pubs_near_stop", fake_search)
    await _run_direct_search(monkeypatch, session["code"], ["pub", "bar", "cafe"])

    assert calls == []
    saved = await get_search_results(app.state.db, session["code"])
    assert saved is not None
    assert saved["data"]["place_types"] == ["pub", "bar", "cafe"]
    assert saved["data"]["pub_search_stop_names"] == []
    assert saved["data"]["departure_datetime"] == "2026-08-10T20:00:00"
    assert saved["data"]["return_datetime"] == "2026-08-10T23:00:00"


@pytest.mark.asyncio
async def test_identical_concurrent_venue_requests_share_one_provider_call(monkeypatch):
    calls = 0
    release = asyncio.Event()

    async def fake_search(lat, lon, place_type, radius=500):
        nonlocal calls
        calls += 1
        await release.wait()
        return []

    monkeypatch.setattr(search_router, "search_pubs_near_stop", fake_search)
    first = asyncio.create_task(search_router._shared_place_search(50.08, 14.42, "pub", 500))
    second = asyncio.create_task(search_router._shared_place_search(50.08, 14.42, "pub", 500))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert calls == 1
    release.set()
    assert await first == []
    assert await second == []


@pytest.mark.asyncio
@pytest.mark.skip(reason="Venue discovery is now explicitly on demand.")
async def test_duplicate_place_types_do_not_repeat_cache_or_live_queries(monkeypatch):
    """Duplicate submitted venue types keep their first position but run once per stop."""
    session = await create_session(app.state.db, "Test", "P1")
    cache_reads = []
    cache_writes = []
    live_calls = []

    async def fake_get_cached(db, stop_name, place_type, radius=500):
        cache_reads.append((stop_name, place_type, radius))
        return None

    async def fake_cache(db, stop_name, place_type, radius, pubs):
        cache_writes.append((stop_name, place_type, radius))

    async def fake_search(lat, lon, place_type, radius=500):
        live_calls.append((lat, place_type, radius))
        return []

    monkeypatch.setattr(search_router, "get_cached_pubs_for_type", fake_get_cached)
    monkeypatch.setattr(search_router, "cache_pubs_for_type", fake_cache)
    monkeypatch.setattr(search_router, "search_pubs_near_stop", fake_search)

    await _run_direct_search(monkeypatch, session["code"], ["pub", "bar", "pub", "cafe", "bar"])

    expected_queries = {
        (stop_name, place_type)
        for stop_name in ["A", "B", "C", "D", "E"]
        for place_type in ["pub", "bar", "cafe"]
    }
    assert {(stop_name, place_type) for stop_name, place_type, _ in cache_reads} == expected_queries
    assert {
        (stop_name, place_type) for stop_name, place_type, _ in cache_writes
    } == expected_queries
    assert len(cache_reads) == len(expected_queries)
    assert len(cache_writes) == len(expected_queries)
    assert len(live_calls) == len(expected_queries)
    assert [place_type for _, place_type, _ in live_calls[:3]] == ["pub", "bar", "cafe"]


@pytest.mark.asyncio
@pytest.mark.skip(reason="Venue provider work no longer runs during a transit search.")
async def test_one_type_failure_keeps_other_results(monkeypatch):
    """A failed type query neither cancels peers nor hides their venue results."""
    session = await create_session(app.state.db, "Test", "P1")
    pub = {
        "place_id": "pub-id",
        "name": "A Pub",
        "lat": 50.0,
        "lon": 14.0,
        "rating": 4.5,
        "rating_count": 42,
        "price_level": None,
        "google_maps_url": "https://example.com/pub",
        "opening_hours": None,
        "primary_type": "pub",
    }
    request = httpx.Request("POST", "https://places.googleapis.com/v1/places:searchNearby")
    response = httpx.Response(429, request=request)
    calls = []

    async def fake_search(lat, lon, place_type, radius=500):
        calls.append((lat, place_type))
        if place_type == "bar":
            raise httpx.HTTPStatusError("limited", request=request, response=response)
        return [{**pub, "place_id": f"{place_type}-{lat}"}]

    monkeypatch.setattr(search_router, "search_pubs_near_stop", fake_search)
    registry = await _run_direct_search(monkeypatch, session["code"], ["pub", "bar", "cafe"])

    saved = await get_search_results(app.state.db, session["code"])
    assert (
        saved["data"]["warning"]
        == "Google Places API limit reached; pub data may be incomplete for some stops."
    )
    assert [pub["primary_type"] for pub in saved["data"]["pubs_by_stop"]["A"]] == ["pub", "pub"]
    assert len(calls) == 15
    assert await get_cached_pubs_for_type(app.state.db, "A", "bar") is None
    assert await get_cached_pubs_for_type(app.state.db, "A", "pub") is not None
    assert any(update.get("done") for _, update in registry.updates)


@pytest.mark.asyncio
@pytest.mark.skip(reason="Venue provider work no longer runs during a transit search.")
async def test_venue_progress_advances_when_each_stop_group_finishes(monkeypatch):
    """Venue progress advances for a completed stop without waiting for slower top stops."""
    session = await create_session(app.state.db, "Test", "P1")
    registry = _DirectSearchRegistry()
    app.state.search_registry = registry
    app.state.stop_geo = pl.DataFrame(
        {
            "name": ["A", "B", "C", "D", "E", "F"],
            "lat": [50.00, 50.01, 50.02, 50.03, 50.04, 50.05],
            "lon": [14.00, 14.01, 14.02, 14.03, 14.04, 14.05],
        }
    )
    monkeypatch.setattr(
        search_router, "get_optimal_stop_pairs", lambda *args, **kwargs: list("ABCDEF")
    )
    monkeypatch.setattr(
        search_router,
        "get_actual_time_optimal_stop_pairs",
        lambda *args, **kwargs: _six_stop_results(),
    )
    first_stop_release = asyncio.Event()
    later_stops_release = asyncio.Event()
    requests_started = asyncio.Event()
    request_count = 0

    async def fake_search(lat, lon, place_type, radius=500):
        nonlocal request_count
        request_count += 1
        if request_count >= 4:
            requests_started.set()
        if lat == 50.00:
            await first_stop_release.wait()
        else:
            await later_stops_release.wait()
        return []

    monkeypatch.setattr(search_router, "search_pubs_near_stop", fake_search)
    task = asyncio.create_task(
        search_router._run_search(
            type("Request", (), {"app": app})(),
            session["code"],
            "direct-search",
            "2026-08-10",
            "20:00",
            "2026-08-10",
            "23:00",
            "minimize-worst-case",
            "round-trip",
            [("A", "A"), ("B", "B")],
            ["P1", "P2"],
            [],
            ["pub", "cafe"],
        )
    )
    await asyncio.wait_for(requests_started.wait(), timeout=1)
    assert any(update.get("stage") == "pubs" for _, update in registry.updates)

    first_stop_release.set()
    for _ in range(100):
        if any(update.get("current") == 1 for _, update in registry.updates):
            break
        await asyncio.sleep(0.01)
    else:
        later_stops_release.set()
        await task
        pytest.fail("first completed stop did not update venue progress")

    later_stops_release.set()
    await task

    venue_stage_index = next(
        index for index, (_, update) in enumerate(registry.updates) if update.get("stage") == "pubs"
    )
    venue_updates = [
        update["current"]
        for _, update in registry.updates[venue_stage_index:]
        if "current" in update
    ]
    assert venue_updates[0] == 0
    assert venue_updates[-1] == 5


@pytest.mark.asyncio
@pytest.mark.skip(reason="Venue provider work no longer runs during a transit search.")
async def test_registry_shutdown_cancels_pending_venue_stop_tasks(monkeypatch):
    """Registry shutdown leaves no live venue request running after its parent search ends."""
    session = await create_session(app.state.db, "Test", "P1")
    registry = app.state.search_registry
    app.state.stop_geo = pl.DataFrame(
        {
            "name": ["A", "B", "C", "D", "E", "F"],
            "lat": [50.00, 50.01, 50.02, 50.03, 50.04, 50.05],
            "lon": [14.00, 14.01, 14.02, 14.03, 14.04, 14.05],
        }
    )
    monkeypatch.setattr(
        search_router, "get_optimal_stop_pairs", lambda *args, **kwargs: list("ABCDEF")
    )
    monkeypatch.setattr(
        search_router,
        "get_actual_time_optimal_stop_pairs",
        lambda *args, **kwargs: _six_stop_results(),
    )
    release_queries = asyncio.Event()
    query_started = asyncio.Event()
    active_queries = 0
    cancelled_queries = 0

    async def fake_search(lat, lon, place_type, radius=500):
        nonlocal active_queries, cancelled_queries
        active_queries += 1
        query_started.set()
        try:
            await release_queries.wait()
        except asyncio.CancelledError:
            cancelled_queries += 1
            raise
        finally:
            active_queries -= 1
        return []

    monkeypatch.setattr(search_router, "search_pubs_near_stop", fake_search)
    search_id = "shutdown-search"
    registry.create(search_id, session["code"])
    registry.start(
        search_id,
        search_router._run_search(
            type("Request", (), {"app": app})(),
            session["code"],
            search_id,
            "2026-08-10",
            "20:00",
            "2026-08-10",
            "23:00",
            "minimize-worst-case",
            "round-trip",
            [("A", "A"), ("B", "B")],
            ["P1", "P2"],
            [],
            ["pub", "cafe"],
        ),
    )
    await asyncio.wait_for(query_started.wait(), timeout=1)

    try:
        await registry.shutdown()
        await asyncio.sleep(0)
        assert active_queries == 0
        assert cancelled_queries > 0
    finally:
        release_queries.set()
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_lower_ranked_stop_is_marked_unsearched(monkeypatch):
    """Stops outside focused discovery explain why they have no venue status."""
    session = await create_session(app.state.db, "Test", "P1")

    async def fake_search(lat, lon, place_type, radius=500):
        return []

    monkeypatch.setattr(search_router, "search_pubs_near_stop", fake_search)
    await _run_direct_search(monkeypatch, session["code"], ["pub", "bar", "cafe"])

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/session/{session['code']}/results")

    sixth_result = response.text.split('data-result-rank="6"', 1)[1].split("</article>", 1)[0]
    assert "Nearby places not searched" in sixth_result
    assert f'hx-post="/session/{session["code"]}/venues"' in sixth_result
    assert 'name="stop_name" value="F"' in sixth_result
    assert "No pubs found nearby" not in sixth_result


@pytest.mark.asyncio
async def test_on_demand_venues_fetches_selected_types_filters_orders_caches_and_persists(
    monkeypatch,
):
    """A lower-ranked stop can be expanded once, then served from persisted cache."""
    session = await create_session(app.state.db, "Test", "P1")
    app.state.stop_geo = pl.DataFrame(
        {"name": ["A", "F"], "lat": [50.0, 50.05], "lon": [14.0, 14.05]}
    )
    base_pub = {
        "lat": 50.051,
        "lon": 14.051,
        "price_level": None,
        "google_maps_url": "https://example.com/venue",
        "opening_hours": None,
        "primary_type": "cafe",
    }
    await save_search_results(
        app.state.db,
        session["code"],
        {
            "rows": [
                {"Target Stop": name, "Worst Case Minutes": 10, "Total Minutes": 20}
                for name in ["A", "B", "C", "D", "E", "F"]
            ],
            "pubs_by_stop": {name: [] for name in ["A", "B", "C", "D", "E"]},
            "pub_search_stop_names": ["A", "B", "C", "D", "E"],
            "place_types": ["cafe"],
            "departure_datetime": "2026-08-10T20:00:00",
            "return_datetime": "2026-08-10T23:00:00",
            "stops_geo": [],
            "pubs_flat": [],
            "participants_geo": [],
            "warning": None,
        },
    )
    original_saved = await get_search_results(app.state.db, session["code"])
    assert original_saved is not None
    calls = []

    async def fake_search(lat, lon, place_type, radius=500):
        calls.append((lat, lon, place_type, radius))
        return [
            {
                **base_pub,
                "place_id": "tiny-perfect",
                "name": "Tiny Perfect",
                "rating": 5.0,
                "rating_count": 1,
            },
            {
                **base_pub,
                "place_id": "established",
                "name": "Established Cafe",
                "rating": 4.2,
                "rating_count": 100,
            },
            {
                **base_pub,
                "place_id": "closes-early",
                "name": "Closes Early",
                "rating": 4.9,
                "rating_count": 500,
                "opening_hours": [
                    {
                        "open": {"day": 1, "hour": 18, "minute": 0},
                        "close": {"day": 1, "hour": 21, "minute": 0},
                    }
                ],
            },
        ]

    monkeypatch.setattr(search_router, "search_pubs_near_stop", fake_search)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(f"/session/{session['code']}/venues", data={"stop_name": "F"})
        cached_response = await client.post(
            f"/session/{session['code']}/venues", data={"stop_name": "F"}
        )

    assert response.status_code == 200
    assert response.text.index("Established Cafe") < response.text.index("Tiny Perfect")
    assert "Closes Early" not in response.text
    assert len(calls) == 1
    assert cached_response.status_code == 200
    assert "Established Cafe" in cached_response.text

    saved = await get_search_results(app.state.db, session["code"])
    assert saved is not None
    assert saved["created_at"] == original_saved["created_at"]
    assert saved["data"]["pub_search_stop_names"] == ["A", "B", "C", "D", "E", "F"]
    assert [pub["place_id"] for pub in saved["data"]["pubs_by_stop"]["F"]] == [
        "established",
        "tiny-perfect",
    ]
    assert [pub["stop"] for pub in saved["data"]["pubs_flat"]] == ["F", "F"]


@pytest.mark.asyncio
async def test_on_demand_venue_provider_failure_returns_explicit_retry_state(monkeypatch):
    session = await create_session(app.state.db, "Test", "P1")
    await save_search_results(
        app.state.db,
        session["code"],
        {
            "rows": [{"Target Stop": "A", "Worst Case Minutes": 10, "Total Minutes": 20}],
            "pubs_by_stop": {},
            "pub_search_stop_names": [],
            "place_types": ["cafe"],
            "stops_geo": [{"name": "A", "lat": 50.0, "lon": 14.0}],
            "pubs_flat": [],
            "participants_geo": [],
            "warning": None,
        },
    )

    async def failed_search(*args, **kwargs):
        raise RuntimeError("provider secret must not reach the page")

    monkeypatch.setattr(search_router, "search_pubs_near_stop", failed_search)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(f"/session/{session['code']}/venues", data={"stop_name": "A"})

    assert response.status_code == 200
    assert 'data-venue-state="provider-error"' in response.text
    assert "Places could not be loaded" in response.text
    assert "Try again" in response.text
    assert "Transit results are unchanged" in response.text
    assert "provider secret" not in response.text


@pytest.mark.asyncio
async def test_on_demand_venues_rejects_stop_outside_saved_results(monkeypatch):
    """The session-scoped endpoint cannot query arbitrary stop names."""
    session = await create_session(app.state.db, "Test", "P1")
    await save_search_results(
        app.state.db,
        session["code"],
        {
            "rows": [{"Target Stop": "A", "Worst Case Minutes": 10, "Total Minutes": 20}],
            "pubs_by_stop": {"A": []},
            "pub_search_stop_names": ["A"],
            "stops_geo": [],
            "pubs_flat": [],
            "participants_geo": [],
            "warning": None,
        },
    )
    live_search = AsyncMock(return_value=[])
    monkeypatch.setattr(search_router, "search_pubs_near_stop", live_search)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            f"/session/{session['code']}/venues", data={"stop_name": "Not in results"}
        )

    assert response.status_code == 404
    live_search.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_saved_results_keep_existing_pub_suggestions():
    """Saved results created before focused discovery still show their venues."""
    session = await create_session(app.state.db, "Test", "P1")
    await save_search_results(
        app.state.db,
        session["code"],
        {
            "rows": [{"Target Stop": "A", "Worst Case Minutes": 10, "Total Minutes": 20}],
            "pubs_by_stop": {
                "A": [
                    {
                        "place_id": "legacy-pub",
                        "name": "Legacy Pub",
                        "lat": 50.08,
                        "lon": 14.42,
                        "rating": 4.5,
                        "rating_count": 42,
                        "google_maps_url": "https://example.com/legacy-pub",
                    }
                ]
            },
            "stops_geo": [],
            "pubs_flat": [],
            "participants_geo": [],
            "warning": None,
        },
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/session/{session['code']}/results")

    assert response.status_code == 200
    assert "Legacy Pub" in response.text
    assert "Pub suggestions are shown for the top 5 meeting points" not in response.text


@pytest.mark.asyncio
async def test_rating_display_formats_rating_and_review_count_in_collapsed_and_expanded_lists():
    """Rendered venue pills show the same precise rating details before and after Show more."""
    session = await create_session(app.state.db, "Test", "P1")
    pubs = [
        {
            "place_id": "rated-collapsed",
            "name": "Rated collapsed pub",
            "lat": 50.08,
            "lon": 14.42,
            "rating": 4.6,
            "rating_count": 23360,
            "google_maps_url": "https://example.com/rated-collapsed",
        },
        {
            "place_id": "other-one",
            "name": "Other pub one",
            "lat": 50.081,
            "lon": 14.421,
            "rating": 4.4,
            "rating_count": 120,
            "google_maps_url": "https://example.com/other-one",
        },
        {
            "place_id": "other-two",
            "name": "Other pub two",
            "lat": 50.082,
            "lon": 14.422,
            "rating": 4.2,
            "rating_count": 80,
            "google_maps_url": "https://example.com/other-two",
        },
        {
            "place_id": "rated-expanded",
            "name": "Rated expanded pub",
            "lat": 50.083,
            "lon": 14.423,
            "rating": 4.6,
            "rating_count": 23360,
            "google_maps_url": "https://example.com/rated-expanded",
        },
    ]
    await save_search_results(
        app.state.db,
        session["code"],
        {
            "rows": [{"Target Stop": "A", "Worst Case Minutes": 10, "Total Minutes": 20}],
            "pubs_by_stop": {"A": pubs},
            "stops_geo": [],
            "pubs_flat": [],
            "participants_geo": [],
            "warning": None,
        },
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/session/{session['code']}/results")

    assert response.status_code == 200
    assert "4.6★ (23,360)" in response.text
    assert response.text.count("4.6★ (23,360)") == 2


@pytest.mark.asyncio
async def test_search_success_returns_progress():
    """Search returns a progress bar that connects via SSE."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        code = await _create_session_with_participants(client, [("A", "A"), ("B", "B")])

        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        with patch("routers.search.get_total_minutes_with_retries", return_value=15):
            with patch(
                "routers.search.search_pubs_near_stop", new_callable=AsyncMock, return_value=[]
            ):
                resp = await client.post(
                    f"/session/{code}/search",
                    data={
                        "departure_date": tomorrow,
                        "departure_time": "20:00",
                        "return_date": tomorrow,
                        "return_time": "23:00",
                        "method": "minimize-worst-case",
                    },
                )
                assert resp.status_code == 200
                assert "search-progress" in resp.text
                assert "sse-connect" in resp.text
                assert 'sse-close="complete"' in resp.text

                search_id = _extract_search_id(resp.text)
                assert await _wait_for_search(search_id, code)

                progress_response = await client.get(f"/session/{code}/search-progress/{search_id}")
                assert "event: progress" in progress_response.text
                assert "event: complete" in progress_response.text


@pytest.mark.asyncio
async def test_search_results_saved_to_db():
    """After a successful search, results are persisted and the results page works."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        code = await _create_session_with_participants(client, [("A", "A"), ("B", "B")])

        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        with patch("routers.search.get_total_minutes_with_retries", return_value=15):
            with patch(
                "routers.search.search_pubs_near_stop", new_callable=AsyncMock, return_value=[]
            ):
                resp = await client.post(
                    f"/session/{code}/search",
                    data={
                        "departure_date": tomorrow,
                        "departure_time": "20:00",
                        "return_date": tomorrow,
                        "return_time": "23:00",
                        "method": "minimize-total",
                    },
                )
                search_id = _extract_search_id(resp.text)
                assert await _wait_for_search(search_id, code)

        # Check results are saved
        saved = await get_search_results(app.state.db, code)
        assert saved is not None
        assert len(saved["data"]["rows"]) > 0
        assert saved["data"]["search_direction"] == "round-trip"
        assert saved["data"]["participant_snapshot"] == [
            {
                "id": 1,
                "name": "P1",
                "color": "#dff0ff",
                "start_stop": "A",
                "end_stop": "A",
            },
            {
                "id": 2,
                "name": "P2",
                "color": "#ffd447",
                "start_stop": "B",
                "end_stop": "B",
            },
        ]

        # Check shareable results page loads
        results_page = await client.get(f"/session/{code}/results")
    assert results_page.status_code == 200


@pytest.mark.asyncio
async def test_search_rate_limiting():
    """Rate limiter blocks after 3 searches within 60 seconds."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        code = await _create_session_with_participants(client, [("A", "A"), ("B", "B")])
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        search_data = {
            "departure_date": tomorrow,
            "departure_time": "20:00",
            "return_date": tomorrow,
            "return_time": "23:00",
            "method": "minimize-worst-case",
        }

        with patch("routers.search.get_total_minutes_with_retries", return_value=15):
            with patch(
                "routers.search.search_pubs_near_stop", new_callable=AsyncMock, return_value=[]
            ):
                search_ids = []
                for _ in range(3):
                    search_response = await client.post(
                        f"/session/{code}/search",
                        data=search_data,
                    )
                    search_ids.append(_extract_search_id(search_response.text))

                # 4th search should be rate limited
                resp = await client.post(f"/session/{code}/search", data=search_data)
                assert all([await _wait_for_search(search_id, code) for search_id in search_ids])

    assert resp.status_code == 200
    assert "Too many searches" in resp.text


@pytest.mark.asyncio
async def test_results_page_no_results():
    """Results page for a session with no search shows appropriate message."""
    session = await create_session(app.state.db, "Test", "P1")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        results_page = await client.get(f"/session/{session['code']}/results")
    assert results_page.status_code == 200


@pytest.mark.asyncio
async def test_results_page_nonexistent_session():
    """Results page for nonexistent session redirects."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/session/nonexistent/results", follow_redirects=False)
    assert resp.status_code == 303
