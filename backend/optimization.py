import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

import polars as pl
from tqdm import tqdm

logger = logging.getLogger(__name__)
MISSING_TRANSIT_MINUTES = 999


def get_geo_optimal_stop(
    distance_table: pl.DataFrame,
    method: str,
    selected_stops: List[str],
    show_top: int = 20,
    reverse: bool = False,
) -> List[str]:
    dfs = []
    filter_column = "to" if reverse else "from"
    candidate_column = "from" if reverse else "to"
    for si, stop in tqdm(
        enumerate(selected_stops),
        desc="Calculating optimal stops",
        total=len(selected_stops),
    ):
        df = (
            distance_table.filter(pl.col(filter_column) == stop)
            .with_columns(
                pl.col(candidate_column).alias("target_stop"),
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
        pl.max_horizontal(*[f"distance_in_km_{si}" for si in range(len(selected_stops))]).alias(
            "worst_case_km"
        ),
        pl.sum_horizontal(*[f"distance_in_km_{si}" for si in range(len(selected_stops))]).alias(
            "total_km"
        ),
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
    show_top: int = 20,
    reverse: bool = False,
) -> list[str]:
    dfs = []
    filter_column = "to" if reverse else "from"
    candidate_column = "from" if reverse else "to"
    for si, stop in tqdm(
        enumerate(selected_stops),
        desc="Calculating optimal stops",
        total=len(selected_stops),
    ):
        df = (
            distance_table.filter(pl.col(filter_column) == stop)
            .with_columns(
                pl.col(candidate_column).alias("target_stop"),
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
        pl.max_horizontal(*[f"total_minutes_{si}" for si in range(len(selected_stops))]).alias(
            "worst_case_minutes"
        ),
        pl.sum_horizontal(*[f"total_minutes_{si}" for si in range(len(selected_stops))]).alias(
            "total_minutes"
        ),
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
    show_top_time: int = 20,
) -> List[str]:
    geo_optimal_stops = get_geo_optimal_stop(distance_table, method, selected_stops, show_top_geo)
    time_optimal_stops = get_time_optimal_stop(
        distance_table, method, selected_stops, show_top_time
    )

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
                total_minutes = get_total_minutes_func(from_stop, target_stop, event_datetime)
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
        futures = {executor.submit(process_target_stop, arg): arg[0] for arg in arguments}
        for future in tqdm(as_completed(futures), total=len(arguments)):
            try:
                result = future.result()
                rows.append(result)
            except Exception as e:
                logger.error("An error occurred with target_stop=%s: %s", futures[future], e)

    df_times = pl.DataFrame(rows).with_columns(
        pl.max_horizontal(*[f"total_minutes_{si}" for si in range(len(selected_stops))]).alias(
            "worst_case_minutes"
        ),
        pl.sum_horizontal(*[f"total_minutes_{si}" for si in range(len(selected_stops))]).alias(
            "total_minutes"
        ),
    )

    if method == "minimize-worst-case":
        df_times = df_times.sort("worst_case_minutes")
    elif method == "minimize-total":
        df_times = df_times.sort("total_minutes")

    df_times = df_times.rename(
        {
            "target_stop": "Target Stop",
            "worst_case_minutes": "Worst Case Minutes",
            "total_minutes": "Total Minutes",
        }
    )
    for si in range(len(selected_stops)):
        df_times = df_times.rename({f"total_minutes_{si}": f"t{si + 1} mins"})

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
    candidates: list[str] = []

    def add_candidates(selected_stops: list[str], *, reverse: bool) -> None:
        relevant = list(dict.fromkeys(selected_stops))
        candidates.extend(
            get_geo_optimal_stop(
                distance_table,
                method,
                relevant,
                show_top_geo,
                reverse=reverse,
            )
        )
        candidates.extend(
            get_time_optimal_stop(
                distance_table,
                method,
                relevant,
                show_top_time,
                reverse=reverse,
            )
        )

    if direction in {"there-only", "round-trip"}:
        add_candidates(start_stops, reverse=False)
    if direction in {"back-only", "round-trip"}:
        add_candidates(end_stops, reverse=True)

    return list(dict.fromkeys(candidates))


def select_live_candidate_stops(
    distance_table: pl.DataFrame,
    method: str,
    stop_pairs: List[tuple[str, str]],
    candidate_stops: List[str],
    limit: int,
    direction: str = "round-trip",
) -> List[str]:
    """Rank matrix candidates for live lookup and return at most ``limit`` stops."""
    if limit < 1:
        raise ValueError("Live candidate limit must be positive")
    if method not in {"minimize-worst-case", "minimize-total"}:
        raise ValueError(f"Unknown method: {method}")
    if direction not in {"round-trip", "there-only", "back-only"}:
        raise ValueError(f"Unknown direction: {direction}")

    unique_candidates = list(dict.fromkeys(candidate_stops))
    if not unique_candidates or not stop_pairs:
        return []

    ranked = pl.DataFrame(
        {
            "target_stop": unique_candidates,
            "candidate_order": range(len(unique_candidates)),
        }
    )
    participant_score_columns = []

    for index, (start_stop, end_stop) in enumerate(stop_pairs):
        participant = pl.DataFrame({"target_stop": unique_candidates})
        leg_columns = []

        if direction != "back-only":
            to_column = f"to_minutes_{index}"
            to_times = distance_table.filter(
                (pl.col("from") == start_stop) & pl.col("to").is_in(unique_candidates)
            ).select(
                pl.col("to").alias("target_stop"),
                pl.col("total_minutes").alias(to_column),
            )
            to_times = to_times.group_by("target_stop").agg(pl.col(to_column).min())
            participant = participant.join(to_times, on="target_stop", how="left").with_columns(
                pl.col(to_column).fill_null(MISSING_TRANSIT_MINUTES)
            )
            leg_columns.append(to_column)

        if direction != "there-only":
            from_column = f"from_minutes_{index}"
            from_times = distance_table.filter(
                (pl.col("to") == end_stop) & pl.col("from").is_in(unique_candidates)
            ).select(
                pl.col("from").alias("target_stop"),
                pl.col("total_minutes").alias(from_column),
            )
            from_times = from_times.group_by("target_stop").agg(pl.col(from_column).min())
            participant = participant.join(from_times, on="target_stop", how="left").with_columns(
                pl.col(from_column).fill_null(MISSING_TRANSIT_MINUTES)
            )
            leg_columns.append(from_column)

        score_column = f"participant_score_{index}"
        participant = participant.with_columns(
            pl.sum_horizontal(*leg_columns).alias(score_column)
        ).select("target_stop", score_column)
        ranked = ranked.join(participant, on="target_stop", how="inner")
        participant_score_columns.append(score_column)

    aggregate = (
        pl.max_horizontal(*participant_score_columns)
        if method == "minimize-worst-case"
        else pl.sum_horizontal(*participant_score_columns)
    )
    return (
        ranked.with_columns(aggregate.alias("matrix_score"))
        .sort("matrix_score", "candidate_order")
        .head(limit)["target_stop"]
        .to_list()
    )


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

                if skip_to:
                    round_trip = from_minutes
                elif skip_from:
                    round_trip = to_minutes
                elif to_minutes is None or from_minutes is None:
                    # A required leg genuinely failed (no route found) -> unreachable.
                    # Leave as None so the fill_null(999) step below applies the same
                    # sentinel used for the To/From columns, instead of silently
                    # treating the missing leg as free.
                    round_trip = None
                else:
                    round_trip = to_minutes + from_minutes

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
        futures = {executor.submit(process_target_stop, arg): arg[0] for arg in arguments}
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

    # Fill null transit times so stops with partial failures still rank (just low)
    all_time_cols = []
    for si in range(len(stop_pairs)):
        all_time_cols.extend([f"to_minutes_{si}", f"from_minutes_{si}", f"round_trip_{si}"])

    df_times = pl.DataFrame(rows)
    existing_cols = set(df_times.columns)
    fill_exprs = [
        pl.col(c).fill_null(MISSING_TRANSIT_MINUTES) for c in all_time_cols if c in existing_cols
    ]
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
        name = (
            participant_names[si]
            if participant_names and si < len(participant_names)
            else f"p{si + 1}"
        )
        rename_map[f"to_minutes_{si}"] = f"To ({name})"
        rename_map[f"from_minutes_{si}"] = f"From ({name})"
        rename_map[f"round_trip_{si}"] = f"Round trip ({name})"

    df_times = df_times.rename(rename_map)
    # Only drop rows where the ranking metric is null (not all columns)
    df_times = df_times.filter(
        pl.col("Worst Case Minutes").is_not_null() & pl.col("Total Minutes").is_not_null()
    )

    return df_times.head(show_top)
