from typing import List
import polars as pl
import traceback
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


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

    print("Joining dataframes ...")
    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.join(dfs[i], on="target_stop")

    print("Finidng optimal stops ...")
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

    print("Joining dataframes ...")
    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.join(dfs[i], on="target_stop")

    print("Finding optimal stops ...")
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
                print(f"Error processing pair ({from_stop}, {target_stop}): {e}")
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
                print(f"An error occurred with target_stop={futures[future]}: {e}")

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
) -> List[str]:
    """Get candidate stops considering both start and end stops.
    Combines all unique start+end stops for candidate selection."""
    start_stops = [pair[0] for pair in stop_pairs]
    end_stops = [pair[1] for pair in stop_pairs]
    all_unique = list(set(start_stops + end_stops))
    geo_candidates = get_geo_optimal_stop(distance_table, method, all_unique, show_top_geo)
    time_candidates = get_time_optimal_stop(distance_table, method, all_unique, show_top_time)
    return list(set(geo_candidates) | set(time_candidates))


def get_actual_time_optimal_stop_pairs(
    method: str,
    stop_pairs: List[tuple[str, str]],
    target_stops: List[str],
    event_datetime,
    get_total_minutes_func,
    show_top: int = 20,
) -> pl.DataFrame:
    """Like get_actual_time_optimal_stop but computes round trips (to meeting point + back to end stop)."""
    def process_target_stop(args):
        target_stop, stop_pairs, event_datetime, get_total_minutes_func = args
        row = {"target_stop": target_stop}
        for si, (start, end) in enumerate(stop_pairs):
            try:
                to_minutes = get_total_minutes_func(start, target_stop, event_datetime)
                if start == end:
                    from_minutes = to_minutes
                else:
                    from_minutes = get_total_minutes_func(target_stop, end, event_datetime)
                round_trip = (to_minutes or 0) + (from_minutes or 0)
                row[f"to_minutes_{si}"] = to_minutes
                row[f"from_minutes_{si}"] = from_minutes
                row[f"round_trip_{si}"] = round_trip
            except Exception as e:
                print(f"Error processing pair ({start}, {target_stop}, {end}): {e}")
                traceback.print_exc()
                row[f"to_minutes_{si}"] = None
                row[f"from_minutes_{si}"] = None
                row[f"round_trip_{si}"] = None
        return row

    rows = []
    arguments = [
        (target_stop, stop_pairs, event_datetime, get_total_minutes_func)
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
                print(f"An error occurred with target_stop={futures[future]}: {e}")

    df_times = pl.DataFrame(rows).with_columns(
        pl.max_horizontal(
            *[f"round_trip_{si}" for si in range(len(stop_pairs))]
        ).alias("worst_case_minutes"),
        pl.sum_horizontal(
            *[f"round_trip_{si}" for si in range(len(stop_pairs))]
        ).alias("total_minutes"),
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
        rename_map[f"to_minutes_{si}"] = f"To (p{si+1})"
        rename_map[f"from_minutes_{si}"] = f"From (p{si+1})"
        rename_map[f"round_trip_{si}"] = f"Round trip (p{si+1})"

    df_times = df_times.rename(rename_map)
    df_times = df_times.drop_nulls()

    return df_times.head(show_top)
