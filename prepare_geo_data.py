import json
import glob
from typing import List, Dict
import polars as pl
import geopy.distance


def extract_unique_stops(json_directory: str) -> pl.DataFrame:
    json_pattern = f"{json_directory}/Prague_stops_gps_*.json"
    json_files = glob.glob(json_pattern)

    if not json_files:
        raise FileNotFoundError(f"No JSON files found matching pattern: {json_pattern}")

    records: List[Dict] = []

    for file_path in json_files:
        with open(file_path, "r", encoding="utf-8") as file:
            try:
                data = json.load(file)
                group_stops = data.get("group_stops", [])

                for stop in group_stops:
                    record = {
                        "name": stop.get("name"),
                        "fullName": stop.get("fullName"),
                        "lat": float(stop.get("lat", 0.0)),
                        "lon": float(stop.get("lon", 0.0)),
                        "lineType": stop.get("lineType"),
                    }
                    records.append(record)

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from file {file_path}: {e}")

    if not records:
        raise ValueError("No records found in the provided JSON files.")

    df = (
        pl.DataFrame(records)
        .unique(subset=["fullName", "lineType"])
        .group_by("name")
        .agg(
            pl.col("lat").mean().alias("lat"),
            pl.col("lon").mean().alias("lon"),
        )
    )

    return df


if __name__ == "__main__":
    json_dir = "data"
    stops_geo_data = extract_unique_stops(json_dir)
    stops_file = "Prague_stops.txt"
    with open(stops_file, "r", encoding="utf-8") as f:
        stops = [line.strip() for line in f if line.strip()]

    stops_df = pl.DataFrame(stops, schema=["name"])
    prague_stops = stops_df.join(stops_geo_data, on="name", how="inner").sort("name")

    for stop in stops:
        if stop not in prague_stops["name"]:
            print(f"Stop '{stop}' not found in Prague_stops.")

    missings = []
    for stop_name in ["Praha hl", "Praha Masarykovo n"]:
        missing = stops_geo_data.filter(pl.col("name").str.contains(stop_name))
        print(missing)

    if len(missing) > 0:
        prague_stops = pl.concat([prague_stops, *missings])

    print(prague_stops)

    prague_stops.write_csv("Prague_stops_geo.csv")
