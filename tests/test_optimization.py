from datetime import datetime

import polars as pl
import pytest

from backend.optimization import (
    get_actual_time_optimal_stop_pairs,
    get_optimal_stop_pairs,
    select_live_candidate_stops,
)


def make_distance_table():
    return pl.DataFrame(
        {
            "from": ["A", "A", "A", "B", "B", "B", "C", "C", "C"],
            "to": ["A", "B", "C", "A", "B", "C", "A", "B", "C"],
            "distance_in_km": [0, 5, 10, 5, 0, 7, 10, 7, 0],
            "total_minutes": [0, 15, 30, 15, 0, 20, 30, 20, 0],
        }
    )


def make_asymmetric_distance_table():
    return pl.DataFrame(
        {
            "from": ["A", "A", "B", "B", "X", "Y"],
            "to": ["X", "Y", "X", "Y", "B", "B"],
            "distance_in_km": [1, 1, 1, 1, 1, 1],
            "total_minutes": [1, 100, 1, 100, 100, 1],
        }
    )


def make_live_candidate_ranking_table():
    return pl.DataFrame(
        {
            "from": ["A", "B", "A", "B", "A", "B", "X", "X", "Y", "Y", "Z", "Z"],
            "to": ["X", "X", "Y", "Y", "Z", "Z", "D", "E", "D", "E", "D", "E"],
            "distance_in_km": [1.0] * 12,
            "total_minutes": [5, 5, 8, 3, 2, 10, 9, 9, 1, 2, 4, 7],
        }
    )


def test_get_optimal_stop_pairs_symmetric():
    dt = make_distance_table()
    pairs = [("A", "A"), ("B", "B")]
    result = get_optimal_stop_pairs(
        dt, "minimize-worst-case", pairs, show_top_geo=3, show_top_time=3
    )
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(isinstance(s, str) for s in result)


def test_get_optimal_stop_pairs_asymmetric():
    dt = make_distance_table()
    pairs = [("A", "C"), ("B", "A")]
    result = get_optimal_stop_pairs(dt, "minimize-total", pairs, show_top_geo=3, show_top_time=3)
    assert isinstance(result, list)
    assert len(result) > 0


def test_back_only_candidates_follow_target_to_end_direction():
    result = get_optimal_stop_pairs(
        make_asymmetric_distance_table(),
        "minimize-total",
        [("A", "B")],
        show_top_geo=0,
        show_top_time=1,
        direction="back-only",
    )

    assert result == ["Y"]


def test_round_trip_candidates_include_each_directed_leg():
    result = get_optimal_stop_pairs(
        make_asymmetric_distance_table(),
        "minimize-total",
        [("A", "B")],
        show_top_geo=0,
        show_top_time=1,
        direction="round-trip",
    )

    assert set(result) == {"X", "Y"}


@pytest.mark.parametrize(
    ("method", "direction", "expected"),
    [
        ("minimize-worst-case", "there-only", ["X", "Y"]),
        ("minimize-total", "there-only", ["X", "Y"]),
        ("minimize-worst-case", "back-only", ["Y", "Z"]),
        ("minimize-total", "back-only", ["Y", "Z"]),
        ("minimize-worst-case", "round-trip", ["Y", "X"]),
        ("minimize-total", "round-trip", ["Y", "Z"]),
    ],
)
def test_live_candidate_selection_preserves_direction_and_objective(method, direction, expected):
    """The cap must use the same directional score and objective as live reranking."""
    selected = select_live_candidate_stops(
        make_live_candidate_ranking_table(),
        method,
        [("A", "D"), ("B", "E")],
        ["Z", "X", "Y"],
        limit=2,
        direction=direction,
    )

    assert selected == expected


def test_live_candidate_selection_deduplicates_matrix_pairs():
    """Duplicate source rows must not consume more than one live candidate slot."""
    duplicate = pl.DataFrame(
        {
            "from": ["A"],
            "to": ["X"],
            "distance_in_km": [1.0],
            "total_minutes": [4],
        }
    )
    selected = select_live_candidate_stops(
        pl.concat([make_live_candidate_ranking_table(), duplicate]),
        "minimize-worst-case",
        [("A", "D"), ("B", "E")],
        ["Z", "X", "Y"],
        limit=3,
        direction="round-trip",
    )

    assert selected == ["Y", "X", "Z"]


def test_live_candidate_selection_penalizes_missing_matrix_legs_without_dropping_stops():
    """Sparse matrix coverage must still leave finalists for authoritative live lookup."""
    table = make_live_candidate_ranking_table().filter(
        ~(pl.col("from").is_in(["X", "Y", "Z"]) & pl.col("to").is_in(["D", "E"]))
    )
    selected = select_live_candidate_stops(
        table,
        "minimize-worst-case",
        [("A", "D"), ("B", "E")],
        ["Z", "X", "Y"],
        limit=2,
        direction="round-trip",
    )

    assert selected == ["X", "Y"]


def test_unreachable_stop_does_not_rank_above_reachable_ones():
    """A target stop nobody can reach must not be scored as 0 minutes (free).

    Regression test: get_total_minutes_func returning None for both legs of a
    participant used to make round_trip = (None or 0) + (None or 0) == 0,
    which then sorted the unreachable stop to the very top of the results.
    """

    def get_total_minutes_func(from_stop, to_stop, dt):
        if from_stop == "Unreachable" or to_stop == "Unreachable":
            return None
        return 10

    df = get_actual_time_optimal_stop_pairs(
        method="minimize-worst-case",
        stop_pairs=[("A", "A"), ("B", "B")],
        target_stops=["Unreachable", "Reachable"],
        event_datetime=datetime.now(),
        get_total_minutes_func=get_total_minutes_func,
        participant_names=["p1", "p2"],
    )

    unreachable = df.filter(pl.col("Target Stop") == "Unreachable")
    assert unreachable["Round trip (p1)"][0] == 999
    assert unreachable["Worst Case Minutes"][0] == 999
    assert unreachable["Total Minutes"][0] == 999 * 2

    # Reachable (20 min round trip per participant) must be ranked first.
    assert df["Target Stop"].to_list().index("Reachable") < df["Target Stop"].to_list().index(
        "Unreachable"
    )
