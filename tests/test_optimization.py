from datetime import datetime

import polars as pl
from backend.optimization import get_optimal_stop_pairs, get_actual_time_optimal_stop_pairs


def make_distance_table():
    return pl.DataFrame({
        "from": ["A", "A", "A", "B", "B", "B", "C", "C", "C"],
        "to": ["A", "B", "C", "A", "B", "C", "A", "B", "C"],
        "distance_in_km": [0, 5, 10, 5, 0, 7, 10, 7, 0],
        "total_minutes": [0, 15, 30, 15, 0, 20, 30, 20, 0],
    })


def test_get_optimal_stop_pairs_symmetric():
    dt = make_distance_table()
    pairs = [("A", "A"), ("B", "B")]
    result = get_optimal_stop_pairs(dt, "minimize-worst-case", pairs, show_top_geo=3, show_top_time=3)
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(isinstance(s, str) for s in result)


def test_get_optimal_stop_pairs_asymmetric():
    dt = make_distance_table()
    pairs = [("A", "C"), ("B", "A")]
    result = get_optimal_stop_pairs(dt, "minimize-total", pairs, show_top_geo=3, show_top_time=3)
    assert isinstance(result, list)
    assert len(result) > 0


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
    reachable = df.filter(pl.col("Target Stop") == "Reachable")

    assert unreachable["Round trip (p1)"][0] == 999
    assert unreachable["Worst Case Minutes"][0] == 999
    assert unreachable["Total Minutes"][0] == 999 * 2

    # Reachable (20 min round trip per participant) must be ranked first.
    assert df["Target Stop"].to_list().index("Reachable") < df["Target Stop"].to_list().index("Unreachable")
