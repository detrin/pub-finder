import math

import aiosqlite
import httpx
import polars as pl
import pytest
import pytest_asyncio
from httpx import ASGITransport

from backend.app import app
from backend.db import (
    add_participant_stops,
    create_session,
    get_participants,
    init_db,
    save_search_results,
    update_search_results,
)
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

    payload = build_reachability_payload(
        sparse_matrix, geo(), [participants()[0]], "there-only"
    )

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
