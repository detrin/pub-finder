import importlib
import json
import math
import statistics
import time
from pathlib import Path

import aiosqlite
import httpx
import polars as pl
import pytest
import pytest_asyncio
from httpx import ASGITransport

import backend.reachability as reachability
import routers.reachability as reachability_router
from backend.app import app
from backend.db import (
    add_participant_stops,
    create_session,
    get_participants,
    init_db,
    save_search_results,
    update_search_results,
)
from backend.preview import PreviewPayloadCache, PreviewRateLimiter, build_preview_participants
from backend.reachability import build_reachability_payload, participant_color


@pytest_asyncio.fixture(autouse=True)
async def reachability_app_state():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)
    app.state.db = db
    app.state.distance_table = matrix()
    app.state.stop_geo = geo()
    app.state.all_stops = ["A", "B", "C"]
    yield
    await db.close()


def matrix() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "from": ["A", "A", "B", "B", "C", "C"],
            "to": ["A", "B", "A", "C", "B", "C"],
            "distance_in_km": [0, 1, 1, 1, 1, 0],
            "total_minutes": [0, 10, 11, 16, 15, 0],
        }
    )


def geo() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "name": ["A", "B", "C"],
            "lat": [50.0, 50.1, 50.2],
            "lon": [14.0, 14.1, 14.2],
        }
    )


def participants() -> list[dict]:
    return [
        {
            "id": 1,
            "name": "Daniel",
            "color": "#ff6658",
            "start_stop": "A",
            "end_stop": "A",
        },
        {
            "id": 2,
            "name": "Anna",
            "color": "#ffd447",
            "start_stop": "C",
            "end_stop": "C",
        },
    ]


def stop(payload: dict, name: str) -> dict:
    return next(item for item in payload["stops"] if item["name"] == name)


def preview_module():
    try:
        return importlib.import_module("backend.preview")
    except ModuleNotFoundError:
        pytest.fail("backend.preview is not implemented")


def test_preview_cache_preserves_origin_order_in_keys():
    preview = preview_module()
    cache = preview.PreviewPayloadCache()
    payload = {"participants": ["A", "C"]}

    cache.set(("A", "C"), payload)

    assert cache.get(("C", "A")) is None
    assert cache.get(("A", "C")) == payload


def test_preview_cache_misses_an_expired_entry():
    now = [0.0]
    preview = preview_module()
    cache = preview.PreviewPayloadCache(clock=lambda: now[0])
    cache.set(("A",), {"participants": ["A"]})
    now[0] = 300.0

    assert cache.get(("A",)) is None


def test_preview_cache_evicts_the_least_recently_used_entry_at_capacity():
    preview = preview_module()
    cache = preview.PreviewPayloadCache(max_entries=2)
    cache.set(("A",), {"id": "A"})
    cache.set(("B",), {"id": "B"})
    assert cache.get(("A",)) == {"id": "A"}

    cache.set(("C",), {"id": "C"})

    assert cache.get(("B",)) is None
    assert cache.get(("A",)) == {"id": "A"}
    assert cache.get(("C",)) == {"id": "C"}


def test_preview_rate_limiter_rejects_requests_after_its_configured_limit():
    preview = preview_module()
    limiter = preview.PreviewRateLimiter(limit=2)

    assert limiter.allow("client") is True
    assert limiter.allow("client") is True
    assert limiter.allow("client") is False


def test_preview_rate_limiter_prunes_inactive_clients_before_accepting_new_ones():
    now = [0.0]
    preview = preview_module()
    limiter = preview.PreviewRateLimiter(max_clients=1, clock=lambda: now[0])
    assert limiter.allow("first") is True
    now[0] = 60.0

    assert limiter.allow("second") is True


def test_there_only_uses_maximum_participant_time():
    payload = build_reachability_payload(matrix(), geo(), participants(), "there-only")

    assert stop(payload, "B")["participant_minutes"] == [10, 15]
    assert stop(payload, "B")["group_max_minutes"] == 15


def test_back_only_uses_target_to_each_participant_end_stop():
    payload = build_reachability_payload(matrix(), geo(), participants(), "back-only")

    assert stop(payload, "B")["participant_minutes"] == [11, 16]
    assert stop(payload, "B")["group_max_minutes"] == 16


def test_round_trip_sums_directional_pairs():
    payload = build_reachability_payload(matrix(), geo(), participants(), "round-trip")

    assert stop(payload, "B")["participant_minutes"] == [21, 31]
    assert stop(payload, "B")["group_max_minutes"] == 31


def test_round_trip_uses_reverse_direction_only_for_a_missing_matrix_leg():
    sparse_matrix = pl.DataFrame(
        {
            "from": ["A", "B", "C"],
            "to": ["B", "A", "B"],
            "total_minutes": [10, 1, 15],
        }
    )
    sparse_geo = pl.DataFrame({"name": ["B"], "lat": [50.1], "lon": [14.1]})
    participant = {
        "id": 1,
        "name": "P1",
        "color": "#dff0ff",
        "start_stop": "A",
        "end_stop": "C",
    }

    payload = build_reachability_payload(
        sparse_matrix,
        sparse_geo,
        [participant],
        "round-trip",
    )

    assert stop(payload, "B")["participant_minutes"] == [25]
    assert stop(payload, "B")["group_max_minutes"] == 25
    assert stop(payload, "B")["estimated"] is True
    assert payload["estimation"] == {
        "method": "reverse direction where a requested matrix leg is missing",
        "estimated_stops": 1,
    }


def test_sparse_production_return_stop_still_has_round_trip_heatmap_coverage():
    project_root = Path(__file__).parents[1]
    distance_table = pl.read_parquet(project_root / "data/Prague_stops_combinations.parquet")
    stop_geo = pl.read_parquet(project_root / "data/Prague_stops_geo.parquet")
    participant = {
        "id": 1,
        "name": "P1",
        "color": "#dff0ff",
        "start_stop": "Vršovické náměstí",
        "end_stop": "Škola Poštovka",
    }

    payload = build_reachability_payload(
        distance_table,
        stop_geo,
        [participant],
        "round-trip",
    )

    assert payload["coverage"] == {"total_stops": 1444, "complete_stops": 1444}
    assert payload["estimation"]["estimated_stops"] == 1444


def test_missing_pair_marks_group_value_unavailable():
    payload = build_reachability_payload(matrix(), geo(), participants(), "back-only")

    assert stop(payload, "A")["participant_minutes"] == [0, None]
    assert stop(payload, "A")["group_max_minutes"] is None


def test_duplicate_pairs_use_fastest_value_and_emit_each_geo_stop_once():
    duplicate_matrix = pl.concat(
        [
            matrix(),
            pl.DataFrame(
                {
                    "from": ["A", "A"],
                    "to": ["A", "B"],
                    "distance_in_km": [0, 1],
                    "total_minutes": [5, 99],
                }
            ),
        ]
    )
    duplicate_geo = pl.concat(
        [geo().reverse(), pl.DataFrame({"name": ["A"], "lat": [math.nan], "lon": [math.nan]})]
    )

    payload = build_reachability_payload(
        duplicate_matrix, duplicate_geo, [participants()[0]], "there-only"
    )

    assert [item["name"] for item in payload["stops"]] == ["A", "B", "C"]
    assert stop(payload, "A")["participant_minutes"] == [0]
    assert stop(payload, "B")["participant_minutes"] == [10]
    assert stop(payload, "A")["lat"] == 50.0


def test_null_and_non_finite_minutes_are_unavailable():
    sparse_matrix = pl.DataFrame(
        {
            "from": ["A", "A", "A"],
            "to": ["A", "B", "C"],
            "total_minutes": [0.0, None, math.nan],
        }
    )

    payload = build_reachability_payload(sparse_matrix, geo(), [participants()[0]], "there-only")

    assert stop(payload, "A")["participant_minutes"] == [0.0]
    assert stop(payload, "B")["participant_minutes"] == [None]
    assert stop(payload, "C")["participant_minutes"] == [None]


def test_empty_participant_list_returns_unavailable_group_values():
    payload = build_reachability_payload(matrix(), geo(), [], "round-trip")

    assert payload["participants"] == []
    assert payload["coverage"] == {"total_stops": 3, "complete_stops": 0}
    assert all(item["participant_minutes"] == [] for item in payload["stops"])
    assert all(item["group_max_minutes"] is None for item in payload["stops"])


def test_payload_metadata_and_complete_coverage_count_are_stable():
    payload = build_reachability_payload(matrix(), geo().reverse(), participants(), "there-only")

    assert payload["participants"] == participants()
    assert payload["direction"] == "there-only"
    assert payload["dataset"] == "precomputed typical transit times"
    assert payload["coverage"] == {"total_stops": 3, "complete_stops": 1}
    assert [item["name"] for item in payload["stops"]] == ["A", "B", "C"]
    assert isinstance(stop(payload, "A")["lat"], float)
    assert isinstance(stop(payload, "A")["lon"], float)


def test_invalid_direction_is_rejected():
    with pytest.raises(ValueError, match="Invalid search direction"):
        build_reachability_payload(matrix(), geo(), participants(), "sideways")


@pytest.mark.parametrize(
    ("participant_id", "expected"),
    [
        (0, "#ff6658"),
        (1, "#dff0ff"),
        (2, "#ffd447"),
        (6, "#ff6658"),
        (-1, "#dff0ff"),
    ],
)
def test_participant_color_matches_the_session_palette(participant_id: int, expected: str):
    assert participant_color(participant_id) == expected


def test_participant_text_color_meets_normal_text_contrast_for_the_palette():
    text_color = getattr(reachability, "participant_text_color", None)
    assert callable(text_color), "participant_text_color is not implemented"

    def luminance(value: str) -> float:
        channels = [int(value[index : index + 2], 16) / 255 for index in (1, 3, 5)]
        linear = [
            channel / 12.92 if channel <= 0.04045 else ((channel + 0.055) / 1.055) ** 2.4
            for channel in channels
        ]
        return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]

    def contrast(foreground: str, background: str) -> float:
        values = sorted((luminance(foreground), luminance(background)), reverse=True)
        return (values[0] + 0.05) / (values[1] + 0.05)

    choices = [text_color(color) for color in reachability.PARTICIPANT_PALETTE]
    assert choices == ["#17191C"] * 4 + ["#F4F2EB", "#17191C"]
    assert all(
        contrast(foreground, background) >= 4.5
        for foreground, background in zip(choices, reachability.PARTICIPANT_PALETTE, strict=True)
    )


def saved_payload(participant_id: int, *, direction: str = "there-only") -> dict:
    return {
        "rows": [],
        "columns": [],
        "participant_snapshot": [
            {
                "id": participant_id,
                "name": "P1",
                "color": "#dff0ff",
                "start_stop": "A",
                "end_stop": "A",
            }
        ],
        "search_direction": direction,
    }


@pytest.mark.asyncio
async def test_reachability_uses_saved_snapshot_not_later_session_edits():
    session = await create_session(app.state.db, "Test", "P1")
    participant = (await get_participants(app.state.db, session["code"]))[0]
    await save_search_results(
        app.state.db,
        session["code"],
        saved_payload(participant["id"]),
    )
    await add_participant_stops(
        app.state.db,
        session["code"],
        participant["id"],
        "B",
        "B",
    )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/reachability")

    assert response.status_code == 200
    assert response.json()["participants"][0]["start_stop"] == "A"
    assert stop(response.json(), "B")["participant_minutes"] == [10]


@pytest.mark.asyncio
async def test_reachability_returns_not_found_for_unknown_session():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/session/not-a-session/reachability")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_reachability_returns_not_found_without_saved_results():
    session = await create_session(app.state.db, "Test", "P1")

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/reachability")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_reachability_rejects_saved_invalid_direction():
    session = await create_session(app.state.db, "Test", "P1")
    participant = (await get_participants(app.state.db, session["code"]))[0]
    await save_search_results(
        app.state.db,
        session["code"],
        saved_payload(participant["id"], direction="sideways"),
    )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/reachability")

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_reachability_rejects_malformed_direction_type():
    session = await create_session(app.state.db, "Test", "P1")
    await save_search_results(
        app.state.db,
        session["code"],
        {"rows": [], "participant_snapshot": [], "search_direction": []},
    )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/reachability")

    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "malformed_data",
    [
        {"search_direction": "there-only"},
        {"search_direction": "there-only", "participant_snapshot": {}},
        {"search_direction": "there-only", "participant_snapshot": [None]},
        {
            "search_direction": "there-only",
            "participant_snapshot": [
                {"id": 1, "name": "P1", "color": "#dff0ff", "start_stop": "A"}
            ],
        },
    ],
)
async def test_reachability_rejects_malformed_saved_snapshot(malformed_data):
    session = await create_session(app.state.db, "Test", "P1")
    await save_search_results(app.state.db, session["code"], malformed_data)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/reachability")

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_reachability_accepts_an_empty_saved_participant_snapshot():
    session = await create_session(app.state.db, "Test", "P1")
    await save_search_results(
        app.state.db,
        session["code"],
        {"rows": [], "participant_snapshot": [], "search_direction": "round-trip"},
    )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/reachability")

    assert response.status_code == 200
    assert response.json()["participants"] == []
    assert response.json()["coverage"]["complete_stops"] == 0


@pytest.mark.asyncio
async def test_reachability_etag_is_private_conditional_and_invalidated_by_new_search():
    session = await create_session(app.state.db, "Test", "P1")
    participant = (await get_participants(app.state.db, session["code"]))[0]
    data = saved_payload(participant["id"])
    await save_search_results(app.state.db, session["code"], data)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = await client.get(f"/session/{session['code']}/reachability")
        repeated = await client.get(f"/session/{session['code']}/reachability")
        conditional = await client.get(
            f"/session/{session['code']}/reachability",
            headers={"If-None-Match": first.headers["etag"]},
        )
        await save_search_results(app.state.db, session["code"], data)
        replaced = await client.get(f"/session/{session['code']}/reachability")

    assert first.status_code == 200
    assert first.headers["cache-control"] == "private, max-age=300"
    assert first.headers["etag"] == repeated.headers["etag"]
    assert conditional.status_code == 304
    assert conditional.content == b""
    assert conditional.headers["cache-control"] == "private, max-age=300"
    assert replaced.headers["etag"] != first.headers["etag"]


@pytest.mark.asyncio
async def test_reachability_etag_changes_when_saved_direction_changes():
    session = await create_session(app.state.db, "Test", "P1")
    participant = (await get_participants(app.state.db, session["code"]))[0]
    await save_search_results(app.state.db, session["code"], saved_payload(participant["id"]))

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = await client.get(f"/session/{session['code']}/reachability")
        await update_search_results(
            app.state.db,
            session["code"],
            saved_payload(participant["id"], direction="back-only"),
        )
        changed = await client.get(f"/session/{session['code']}/reachability")

    assert changed.status_code == 200
    assert changed.headers["etag"] != first.headers["etag"]


@pytest.mark.asyncio
async def test_reachability_version_rejects_an_old_result_payload_after_rerun():
    session = await create_session(app.state.db, "Test", "P1")
    participant = (await get_participants(app.state.db, session["code"]))[0]
    first = saved_payload(participant["id"])
    first["search_id"] = "first-search"
    second = saved_payload(participant["id"], direction="back-only")
    second["search_id"] = "second-search"
    await save_search_results(app.state.db, session["code"], first)
    await save_search_results(app.state.db, session["code"], second)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        stale = await client.get(f"/session/{session['code']}/reachability?version=first-search")
        current = await client.get(f"/session/{session['code']}/reachability?version=second-search")

    assert stale.status_code == 409
    assert current.status_code == 200


@pytest.mark.asyncio
async def test_preview_returns_one_way_reachability_without_a_session():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/reachability/preview", json={"origins": ["A", "C"]})

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    payload = response.json()
    assert payload["direction"] == "there-only"
    assert [person["start_stop"] for person in payload["participants"]] == ["A", "C"]
    assert stop(payload, "B")["group_max_minutes"] == 15
    async with app.state.db.execute("SELECT COUNT(*) FROM sessions") as cursor:
        assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_preview_does_not_issue_an_analytics_identifier_cookie():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/reachability/preview", json={"origins": ["A"]})

    assert response.status_code == 200
    assert "set-cookie" not in response.headers
    for table in ("analytics_users", "sessions", "participants"):
        async with app.state.db.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
            assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_preview_rejects_oversized_content_length_before_rate_limiting(monkeypatch):
    class CountingLimiter:
        calls = 0

        def allow(self, _client_key):
            self.calls += 1
            return True

    limiter = CountingLimiter()
    calculation_calls = []
    monkeypatch.setattr(reachability_router, "_preview_limiter", limiter)
    monkeypatch.setattr(
        reachability_router,
        "build_reachability_payload",
        lambda *args: calculation_calls.append(args),
    )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/reachability/preview",
            content=b'{"origins":["A"]}',
            headers={
                "Content-Length": "2049",
                "Content-Type": "application/json",
            },
        )

    assert response.status_code == 413
    assert response.headers["cache-control"] == "no-store"
    assert "set-cookie" not in response.headers
    assert limiter.calls == 0
    assert calculation_calls == []
    for table in ("analytics_users", "sessions", "participants"):
        async with app.state.db.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
            assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_preview_rejects_accumulated_streamed_bytes_before_rate_limiting(monkeypatch):
    class CountingLimiter:
        calls = 0

        def allow(self, _client_key):
            self.calls += 1
            return True

    async def oversized_chunks():
        yield b'{"origins":["'
        yield b"A" * 2048
        yield b'"]}'

    limiter = CountingLimiter()
    calculation_calls = []
    monkeypatch.setattr(reachability_router, "_preview_limiter", limiter)
    monkeypatch.setattr(
        reachability_router,
        "build_reachability_payload",
        lambda *args: calculation_calls.append(args),
    )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/reachability/preview",
            content=oversized_chunks(),
            headers={"Content-Type": "application/json"},
        )

    assert response.status_code == 413
    assert response.headers["cache-control"] == "no-store"
    assert "set-cookie" not in response.headers
    assert limiter.calls == 0
    assert calculation_calls == []
    for table in ("analytics_users", "sessions", "participants"):
        async with app.state.db.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
            assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_preview_caps_each_origin_string_with_the_normal_422_contract(monkeypatch):
    long_origin = "A" * 201
    app.state.all_stops = [long_origin]
    calculation_calls = []
    monkeypatch.setattr(
        reachability_router,
        "build_reachability_payload",
        lambda *args: calculation_calls.append(args),
    )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/reachability/preview",
            json={"origins": [long_origin]},
        )

    assert response.status_code == 422
    assert response.headers["cache-control"] == "no-store"
    assert isinstance(response.json()["detail"], list)
    assert "set-cookie" not in response.headers
    assert calculation_calls == []


@pytest.mark.asyncio
async def test_preview_body_guard_preserves_malformed_json_validation_contract():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/reachability/preview",
            content=b'{"origins":',
            headers={"Content-Type": "application/json"},
        )

    assert response.status_code == 422
    assert response.headers["cache-control"] == "no-store"
    assert isinstance(response.json()["detail"], list)


@pytest.mark.asyncio
async def test_preview_returns_explicit_missing_estimates_from_sparse_matrix(monkeypatch):
    monkeypatch.setattr(reachability_router, "_preview_cache", PreviewPayloadCache())
    app.state.distance_table = pl.DataFrame(
        {
            "from": ["A"],
            "to": ["A"],
            "total_minutes": [0],
        }
    )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/reachability/preview", json={"origins": ["A"]})

    assert response.status_code == 200
    assert stop(response.json(), "B")["participant_minutes"] == [None]
    assert stop(response.json(), "B")["group_max_minutes"] is None
    assert response.json()["coverage"] == {"total_stops": 3, "complete_stops": 1}


@pytest.mark.asyncio
async def test_preview_endpoint_cache_reuses_only_the_exact_ordered_origin_key(monkeypatch):
    monkeypatch.setattr(reachability_router, "_preview_cache", PreviewPayloadCache())
    monkeypatch.setattr(
        reachability_router,
        "_preview_limiter",
        PreviewRateLimiter(limit=10),
    )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = await client.post(
            "/reachability/preview",
            json={"origins": ["A", "C"]},
        )
        app.state.distance_table = matrix().with_columns(pl.lit(99).alias("total_minutes"))
        repeated = await client.post(
            "/reachability/preview",
            json={"origins": ["A", "C"]},
        )
        distinct = await client.post(
            "/reachability/preview",
            json={"origins": ["A"]},
        )

    assert stop(first.json(), "B")["group_max_minutes"] == 15
    assert repeated.json() == first.json()
    assert stop(distinct.json(), "B")["group_max_minutes"] == 99


@pytest.mark.asyncio
async def test_preview_calculation_failure_is_generic_stateless_and_not_stored(
    monkeypatch,
    caplog,
):
    private_detail = "origin A triggered a private provider detail"

    def fail_calculation(*args, **kwargs):
        raise RuntimeError(private_detail)

    monkeypatch.setattr(reachability_router, "_preview_cache", PreviewPayloadCache())
    monkeypatch.setattr(reachability_router, "build_reachability_payload", fail_calculation)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.post("/reachability/preview", json={"origins": ["A"]})

    assert response.status_code == 503
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {"detail": "The quick estimate is unavailable"}
    assert private_detail not in response.text
    assert private_detail not in caplog.text
    assert "set-cookie" not in response.headers
    async with app.state.db.execute("SELECT COUNT(*) FROM sessions") as cursor:
        assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "status"),
    [
        ({"origins": []}, 422),
        ({"origins": ["missing"]}, 422),
        ({"origins": ["A", " A "]}, 422),
        ({"origins": ["A"] * 7}, 422),
        ({"origins": "A"}, 422),
        ({}, 422),
        ({"origins": ["A"], "direction": "round-trip"}, 422),
    ],
)
async def test_preview_rejects_invalid_origins(body, status):
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/reachability/preview", json=body)
    assert response.status_code == status
    assert response.headers["cache-control"] == "no-store"
    assert "set-cookie" not in response.headers
    async with app.state.db.execute("SELECT COUNT(*) FROM sessions") as cursor:
        assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_preview_limiter_uses_the_connected_client_not_a_forwarded_header(monkeypatch):
    preview = preview_module()
    router = importlib.import_module("routers.reachability")
    monkeypatch.setattr(router, "_preview_limiter", preview.PreviewRateLimiter(limit=1))

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = await client.post(
            "/reachability/preview",
            json={"origins": ["A"]},
            headers={"X-Forwarded-For": "198.51.100.1"},
        )
        second = await client.post(
            "/reachability/preview",
            json={"origins": ["A"]},
            headers={"X-Forwarded-For": "203.0.113.2"},
        )

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.headers["cache-control"] == "no-store"
    assert "set-cookie" not in second.headers


def test_preview_production_data_latency_benchmark_reports_distribution():
    project_root = Path(__file__).parents[1]
    distance_table = pl.read_parquet(project_root / "data/Prague_stops_combinations.parquet")
    stop_geo = pl.read_parquet(project_root / "data/Prague_stops_geo.parquet")
    origins = distance_table["from"].unique().sort().head(6).to_list()
    results = {}

    for count in (1, 6):
        participants = build_preview_participants(origins[:count])
        build_reachability_payload(distance_table, stop_geo, participants, "there-only")
        samples = []
        payload = None
        for _ in range(20):
            started = time.perf_counter()
            payload = build_reachability_payload(
                distance_table,
                stop_geo,
                participants,
                "there-only",
            )
            samples.append((time.perf_counter() - started) * 1000)
        ordered = sorted(samples)
        results[count] = {
            "p50_ms": round(statistics.median(ordered), 1),
            "p95_ms": round(ordered[math.ceil(len(ordered) * 0.95) - 1], 1),
            "max_ms": round(max(ordered), 1),
            "bytes": len(json.dumps(payload, ensure_ascii=False).encode()),
        }

    print(f"preview production benchmark: {json.dumps(results, sort_keys=True)}")
    assert set(results) == {1, 6}
    assert all(result["bytes"] > 0 for result in results.values())
