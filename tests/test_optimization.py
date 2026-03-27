import polars as pl
from backend.optimization import get_optimal_stop_pairs


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
