import logging
import traceback
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
from tqdm import tqdm

logger = logging.getLogger(__name__)


def get_geo_optimal_stop(
    distance_table: pl.DataFrame,
    method: str, 
    selected_stops: List[str], 
    show_top: int = 20
) -> List[str]:
    dfs = []
    for si, stop in tqdm(
        enumerate(selected_stops),
        desc="Calculating optimal stops",
        total=len(selected_stops),
    ):
        df = (
            distance_table.filter(pl.col("from") == stop)
            .drop("from")
            .with_columns(
                pl.col("to").alias("target_stop"),
                pl.col("distance_in_km").alias(f"distance_in_km_{si}"),
            )
            .select("target_stop", f"distance_in_km_{si}")
        )
        dfs.append(df)

    logger.debug("Joining dataframes ...")
    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.join(dfs[i], on="target_stop")

    logger.debug("Finding optimal stops ...")
    df = df.with_columns(
        pl.max_horizontal(
            *[f"distance_in_km_{si}" for si in range(len(selected_stops))]
        ).alias("worst_case_km"),
        pl.sum_horizontal(
            *[f"distance_in_km_{si}" for si in range(len(selected_stops))]
        ).alias("total_km"),
    )

    if method == "minimize-worst-case":
        df = df.sort("worst_case_km")
    elif method == "minimize-total":
        df = df.sort("total_km")

    return df.head(show_top)["target_stop"].to_list()


def get_time_optimal_stop(
    distance_table: pl.DataFrame,
    method: str, 
    selected_stops: List[str], 
    show_top: int = 20
) -> list[str]:
    dfs = []
    for si, stop in tqdm(
        enumerate(selected_stops),
        desc="Calculating optimal stops",
        total=len(selected_stops),
    ):
        df = (
            distance_table.filter(pl.col("from") == stop)
            .drop("from")
            .with_columns(
                pl.col("to").alias("target_stop"),
                pl.col("total_minutes").alias(f"total_minutes_{si}"),
            )
            .select("target_stop", f"total_minutes_{si}")
        )
        dfs.append(df)

    logger.debug("Joining dataframes ...")
    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.join(dfs[i], on="target_stop")

    logger.debug("Finding optimal stops ...")
    df = df.with_columns(
        pl.max_horizontal(
            *[f"total_minutes_{si}" for si in range(len(selected_stops))]
        ).alias("worst_case_minutes"),
        pl.sum_horizontal(
            *[f"total_minutes_{si}" for si in range(len(selected_stops))]
        ).alias("total_minutes"),
    )

    if method == "minimize-worst-case":
        df = df.sort("worst_case_minutes")
    elif method == "minimize-total":
        df = df.sort("total_minutes")
    else:
        raise ValueError(f"Unknown method: {method}")

    return df.head(show_top)["target_stop"].to_list()


def get_optimal_stop(
    distance_table: pl.DataFrame,
    method: str, 
    selected_stops: List[str], 
    show_top_geo: int = 20, 
    show_top_time: int = 20
) -> List[str]:
    geo_optimal_stops = get_geo_optimal_stop(distance_table, method, selected_stops, show_top_geo)
    time_optimal_stops = get_time_optimal_stop(distance_table, method, selected_stops, show_top_time)
    
    return list(set(geo_optimal_stops) | set(time_optimal_stops))


def get_actual_time_optimal_stop(
    method: str,
    selected_stops: List[str],
    target_stops: List[str],
    event_datetime,
    get_total_minutes_func,
    show_top: int = 20,
) -> pl.DataFrame:
    def process_target_stop(args):
        target_stop, selected_stops, event_datetime, get_total_minutes_func = args
        row = {"target_stop": target_stop}
        for si, from_stop in enumerate(selected_stops):
            try:
                total_minutes = get_total_minutes_func(
                    from_stop, target_stop, event_datetime
                )
                row[f"total_minutes_{si}"] = total_minutes
            except Exception as e:
                logger.warning("Error processing pair (%s, %s): %s", from_stop, target_stop, e)
                traceback.print_exc()
                row[f"total_minutes_{si}"] = None
        return row

    rows = []
    arguments = [
        (target_stop, selected_stops, event_datetime, get_total_minutes_func) 
        for target_stop in target_stops
    ]
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(process_target_stop, arg): arg[0] for arg in arguments
        }
        for future in tqdm(as_completed(futures), total=len(arguments)):
            try:
                result = future.result()
                rows.append(result)
            except Exception as e:
                logger.error("An error occurred with target_stop=%s: %s", futures[future], e)

    df_times = pl.DataFrame(rows).with_columns(
        pl.max_horizontal(
            *[f"total_minutes_{si}" for si in range(len(selected_stops))]
        ).alias("worst_case_minutes"),
        pl.sum_horizontal(
            *[f"total_minutes_{si}" for si in range(len(selected_stops))]
        ).alias("total_minutes"),
    )

    if method == "minimize-worst-case":
        df_times = df_times.sort("worst_case_minutes")
    elif method == "minimize-total":
        df_times = df_times.sort("total_minutes")

    df_times = df_times.rename(
        {
            "target_stop": "Target Stop",
            "worst_case_minutes": "Worst Case Minutes", 
            "total_minutes": "Total Minutes"
        }
    )
    for si in range(len(selected_stops)):
        df_times = df_times.rename({f"total_minutes_{si}": f"t{si+1} mins"})

    df_times = df_times.drop_nulls()

    return df_times.head(show_top)


def get_optimal_stop_pairs(
    distance_table: pl.DataFrame,
    method: str,
    stop_pairs: List[tuple[str, str]],
    show_top_geo: int = 20,
    show_top_time: int = 20,
    direction: str = "round-trip",
) -> List[str]:
    """Get candidate stops considering start and/or end stops based on direction."""
    start_stops = [pair[0] for pair in stop_pairs]
    end_stops = [pair[1] for pair in stop_pairs]
    if direction == "there-only":
        relevant = list(set(start_stops))
    elif direction == "back-only":
        relevant = list(set(end_stops))
    else:
        relevant = list(set(start_stops + end_stops))
    geo_candidates = get_geo_optimal_stop(distance_table, method, relevant, show_top_geo)
    time_candidates = get_time_optimal_stop(distance_table, method, relevant, show_top_time)
    return list(set(geo_candidates) | set(time_candidates))


def get_actual_time_optimal_stop_pairs(
    method: str,
    stop_pairs: List[tuple[str, str]],
    target_stops: List[str],
    event_datetime,
    get_total_minutes_func,
    show_top: int = 20,
    participant_names: Optional[List[str]] = None,
    return_datetime=None,
    progress_callback=None,
    direction: str = "round-trip",
) -> pl.DataFrame:
    """Like get_actual_time_optimal_stop but computes round trips (to meeting point + back to end stop).
    direction controls which leg(s) to optimize: 'round-trip', 'there-only', 'back-only'."""
    if return_datetime is None:
        return_datetime = event_datetime

    skip_to = direction == "back-only"
    skip_from = direction == "there-only"

    def process_target_stop(args):
        target_stop, stop_pairs, departure_dt, return_dt, get_total_minutes_func = args
        row = {"target_stop": target_stop}
        for si, (start, end) in enumerate(stop_pairs):
            try:
                to_minutes = None
                from_minutes = None
                if not skip_to:
                    to_minutes = get_total_minutes_func(start, target_stop, departure_dt)
                if not skip_from:
                    if start == end:
                        from_minutes = get_total_minutes_func(target_stop, start, return_dt)
                    else:
                        from_minutes = get_total_minutes_func(target_stop, end, return_dt)
                round_trip = (to_minutes or 0) + (from_minutes or 0)
                row[f"to_minutes_{si}"] = to_minutes
                row[f"from_minutes_{si}"] = from_minutes
                row[f"round_trip_{si}"] = round_trip
            except Exception as e:
                logger.warning("Error processing pair (%s, %s, %s): %s", start, target_stop, end, e)
                traceback.print_exc()
                row[f"to_minutes_{si}"] = None
                row[f"from_minutes_{si}"] = None
                row[f"round_trip_{si}"] = None
        return row

    rows = []
    total = len(target_stops)
    arguments = [
        (target_stop, stop_pairs, event_datetime, return_datetime, get_total_minutes_func)
        for target_stop in target_stops
    ]
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(process_target_stop, arg): arg[0] for arg in arguments
        }
        for i, future in enumerate(tqdm(as_completed(futures), total=total), 1):
            try:
                result = future.result()
                rows.append(result)
            except Exception as e:
                logger.error("An error occurred with target_stop=%s: %s", futures[future], e)
            if progress_callback:
                progress_callback("scraping", i, total)

    # Choose which columns to use for ranking based on direction
    if direction == "there-only":
        rank_cols = [f"to_minutes_{si}" for si in range(len(stop_pairs))]
    elif direction == "back-only":
        rank_cols = [f"from_minutes_{si}" for si in range(len(stop_pairs))]
    else:
        rank_cols = [f"round_trip_{si}" for si in range(len(stop_pairs))]

    # Fill null transit times with 999 so stops with partial failures still rank (just low)
    all_time_cols = []
    for si in range(len(stop_pairs)):
        all_time_cols.extend([f"to_minutes_{si}", f"from_minutes_{si}", f"round_trip_{si}"])

    df_times = pl.DataFrame(rows)
    existing_cols = set(df_times.columns)
    fill_exprs = [pl.col(c).fill_null(999) for c in all_time_cols if c in existing_cols]
    if fill_exprs:
        df_times = df_times.with_columns(fill_exprs)

    df_times = df_times.with_columns(
        pl.max_horizontal(*rank_cols).alias("worst_case_minutes"),
        pl.sum_horizontal(*rank_cols).alias("total_minutes"),
    )

    if method == "minimize-worst-case":
        df_times = df_times.sort("worst_case_minutes")
    elif method == "minimize-total":
        df_times = df_times.sort("total_minutes")

    rename_map = {
        "target_stop": "Target Stop",
        "worst_case_minutes": "Worst Case Minutes",
        "total_minutes": "Total Minutes",
    }
    for si in range(len(stop_pairs)):
        name = participant_names[si] if participant_names and si < len(participant_names) else f"p{si+1}"
        rename_map[f"to_minutes_{si}"] = f"To ({name})"
        rename_map[f"from_minutes_{si}"] = f"From ({name})"
        rename_map[f"round_trip_{si}"] = f"Round trip ({name})"

    df_times = df_times.rename(rename_map)
    # Only drop rows where the ranking metric is null (not all columns)
    df_times = df_times.filter(
        pl.col("Worst Case Minutes").is_not_null() & pl.col("Total Minutes").is_not_null()
    )

    return df_times.head(show_top)
