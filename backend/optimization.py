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
