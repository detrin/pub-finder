"""Integration tests covering the full search flow, JSON serialization, and caching."""

import asyncio
import json
import time
from datetime import datetime, timedelta
from unittest.mock import patch, AsyncMock

import pytest
import pytest_asyncio
import aiosqlite
import polars as pl
from httpx import ASGITransport, AsyncClient

from backend.db import (
    init_db,
    create_session,
    join_session,
    add_participant_stops,
    get_participants,
    save_search_results,
    get_search_results,
)
from backend.app import app
from backend.utils import get_total_minutes_with_retries


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

    distance_table = pl.DataFrame({
        "from": ["A", "A", "B", "B"],
        "to": ["B", "A", "A", "B"],
        "distance_in_km": [1.0, 1.0, 1.0, 0.0],
        "total_minutes": [10, 10, 10, 0],
    })
    stop_geo = pl.DataFrame({
        "name": ["A", "B"],
        "lat": [50.08, 50.09],
        "lon": [14.42, 14.43],
    })

    app.state.db = db
    app.state.distance_table = distance_table
    app.state.all_stops = ["A", "B"]
    app.state.stop_geo = stop_geo

    yield db

    await db.close()


async def _create_session_with_participants(client, stops):
    """Helper: create a session via DB directly, add participants with stops, return session code."""
    db = app.state.db
    session = await create_session(db, "Test", "P1")
    code = session["code"]

    # Get creator participant
    participants = await get_participants(db, code)
    p1 = [p for p in participants if p["name"] == "P1"][0]
    await add_participant_stops(db, p1["id"], stops[0][0], stops[0][1])

    # Additional participants
    for i, (start, end) in enumerate(stops[1:], start=2):
        result = await join_session(db, code, f"P{i}")
        await add_participant_stops(db, result["id"], start, end)

    return code


# --- save_search_results serialization tests ---


@pytest.mark.asyncio
async def test_save_results_with_polars_types(db):
    """Polars row dicts (may contain non-native types) serialize without error."""
    session = await create_session(db, "Test", "Dan")
    df = pl.DataFrame({
        "Target Stop": ["A", "B"],
        "Worst Case Minutes": [10, 20],
        "Total Minutes": [15, 30],
    })
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
    df = pl.DataFrame({
        "Target Stop": ["A"],
        "Worst Case Minutes": [None],
        "Total Minutes": [None],
    })
    results_data = {
        "rows": df.rows(named=True),
        "columns": df.columns,
        "pubs_by_stop": {"A": [{"place_id": "x", "name": "Pub", "lat": 50.0, "lon": 14.0,
                                 "rating": None, "rating_count": None, "price_level": None,
                                 "google_maps_url": ""}]},
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
        {"place_id": f"id{i}", "name": f"Pub {i}", "lat": 50.0 + i * 0.01,
         "lon": 14.0 + i * 0.01, "rating": 4.5, "rating_count": 100,
         "price_level": 2, "google_maps_url": f"https://maps.google.com/{i}"}
        for i in range(5)
    ]
    results_data = {
        "rows": [{"Target Stop": "A", "Worst Case Minutes": 10, "Total Minutes": 20}],
        "columns": ["Target Stop", "Worst Case Minutes", "Total Minutes"],
        "pubs_by_stop": {"A": pubs},
        "stops_geo": [{"name": "A", "lat": 50.0, "lon": 14.0}],
        "pubs_flat": [{"stop": "A", "name": p["name"], "lat": p["lat"], "lon": p["lon"],
                        "rating": p["rating"], "rating_count": p["rating_count"],
                        "url": p["google_maps_url"]} for p in pubs],
        "participants_geo": [{"name": "Dan", "stop": "B", "type": "from", "lat": 50.08, "lon": 14.42}],
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
    assert "At least 2 participants" in resp.text


async def _wait_for_search(search_id, timeout=10):
    """Wait for a background search to complete."""
    from routers.search import _search_progress, _search_progress_lock
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        with _search_progress_lock:
            progress = _search_progress.get(search_id)
        if progress and progress["done"]:
            return True
        await asyncio.sleep(0.1)
    return False


def _extract_search_id(html: str) -> str:
    """Extract search_id from the progress SSE HTML response."""
    import re
    match = re.search(r"search-progress/([a-f0-9]+)", html)
    return match.group(1) if match else ""


@pytest.mark.asyncio
async def test_search_success_returns_progress():
    """Search returns a progress bar that connects via SSE."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        code = await _create_session_with_participants(client, [("A", "A"), ("B", "B")])

        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        with patch("routers.search.get_total_minutes_with_retries", return_value=15):
            with patch("routers.search.search_pubs_near_stop", new_callable=AsyncMock, return_value=[]):
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

                search_id = _extract_search_id(resp.text)
                await _wait_for_search(search_id)


@pytest.mark.asyncio
async def test_search_results_saved_to_db():
    """After a successful search, results are persisted and the results page works."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        code = await _create_session_with_participants(client, [("A", "A"), ("B", "B")])

        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        with patch("routers.search.get_total_minutes_with_retries", return_value=15):
            with patch("routers.search.search_pubs_near_stop", new_callable=AsyncMock, return_value=[]):
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
                await _wait_for_search(search_id)

        # Check results are saved
        saved = await get_search_results(app.state.db, code)
        assert saved is not None
        assert len(saved["data"]["rows"]) > 0

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
            with patch("routers.search.search_pubs_near_stop", new_callable=AsyncMock, return_value=[]):
                for _ in range(3):
                    await client.post(f"/session/{code}/search", data=search_data)

                # 4th search should be rate limited
                resp = await client.post(f"/session/{code}/search", data=search_data)

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
