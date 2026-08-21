from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

import polars as pl

VALID_DIRECTIONS = {"there-only", "back-only", "round-trip"}
PARTICIPANT_PALETTE = ("#ff6658", "#dff0ff", "#ffd447", "#4dc694", "#2458df", "#b9a8ff")
_DARK_PARTICIPANT_TEXT = "#17191C"
_LIGHT_PARTICIPANT_TEXT = "#F4F2EB"


def participant_color(participant_id: int) -> str:
    """Return the stable UI colour assigned to a participant ID."""
    return PARTICIPANT_PALETTE[abs(int(participant_id or 0)) % len(PARTICIPANT_PALETTE)]


def participant_text_color(background: str) -> str:
    """Return the higher-contrast text colour for a participant colour."""
    if not isinstance(background, str) or re.fullmatch(r"#[0-9a-fA-F]{6}", background) is None:
        return _DARK_PARTICIPANT_TEXT

    def luminance(color: str) -> float:
        channels = [int(color[index : index + 2], 16) / 255 for index in (1, 3, 5)]
        linear = [
            channel / 12.92 if channel <= 0.04045 else ((channel + 0.055) / 1.055) ** 2.4
            for channel in channels
        ]
        return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]

    background_luminance = luminance(background)

    def contrast(color: str) -> float:
        brighter, darker = sorted((luminance(color), background_luminance), reverse=True)
        return (brighter + 0.05) / (darker + 0.05)

    return max((_DARK_PARTICIPANT_TEXT, _LIGHT_PARTICIPANT_TEXT), key=contrast)


def _geo_frame(stop_geo: pl.DataFrame) -> pl.DataFrame:
    return (
        stop_geo.select(
            pl.col("name").cast(pl.String),
            pl.col("lat").cast(pl.Float64, strict=False),
            pl.col("lon").cast(pl.Float64, strict=False),
        )
        .filter(
            pl.col("name").is_not_null()
            & pl.col("lat").is_not_null()
            & pl.col("lat").is_finite()
            & pl.col("lon").is_not_null()
            & pl.col("lon").is_finite()
        )
        .unique(subset="name", keep="first", maintain_order=True)
        .sort("name")
    )


def _participant_frame(
    table: pl.DataFrame,
    participant: Mapping[str, Any],
    index: int,
    direction: str,
) -> pl.DataFrame:
    there = _leg_frame(
        table,
        participant["start_stop"],
        primary_from_anchor=True,
        value_name="there",
    )
    back = _leg_frame(
        table,
        participant["end_stop"],
        primary_from_anchor=False,
        value_name="back",
    )

    value_column = f"participant_{index}"
    estimated_column = f"participant_{index}_estimated"
    if direction == "there-only":
        return there.select(
            "name",
            pl.col("there").alias(value_column),
            pl.col("there_estimated").alias(estimated_column),
        )
    if direction == "back-only":
        return back.select(
            "name",
            pl.col("back").alias(value_column),
            pl.col("back_estimated").alias(estimated_column),
        )

    return there.join(back, on="name", how="full", coalesce=True).select(
        "name",
        pl.when(pl.col("there").is_not_null() & pl.col("back").is_not_null())
        .then(pl.col("there") + pl.col("back"))
        .otherwise(None)
        .alias(value_column),
        pl.when(pl.col("there").is_not_null() & pl.col("back").is_not_null())
        .then(pl.col("there_estimated") | pl.col("back_estimated"))
        .otherwise(False)
        .alias(estimated_column),
    )


def _leg_frame(
    table: pl.DataFrame,
    anchor_stop: str,
    *,
    primary_from_anchor: bool,
    value_name: str,
) -> pl.DataFrame:
    """Use the requested direction, falling back to its reverse when absent."""
    finite_minutes = pl.col("total_minutes").cast(pl.Float64, strict=False).is_finite()
    if primary_from_anchor:
        primary_filter = pl.col("from") == anchor_stop
        primary_name = "to"
        reverse_filter = pl.col("to") == anchor_stop
        reverse_name = "from"
    else:
        primary_filter = pl.col("to") == anchor_stop
        primary_name = "from"
        reverse_filter = pl.col("from") == anchor_stop
        reverse_name = "to"

    primary_column = f"{value_name}_primary"
    reverse_column = f"{value_name}_reverse"
    primary = (
        table.filter(primary_filter)
        .select(
            pl.col(primary_name).alias("name"),
            pl.when(finite_minutes)
            .then(pl.col("total_minutes"))
            .otherwise(None)
            .alias(primary_column),
        )
        .group_by("name")
        .agg(pl.col(primary_column).min())
    )
    reverse = (
        table.filter(reverse_filter)
        .select(
            pl.col(reverse_name).alias("name"),
            pl.when(finite_minutes)
            .then(pl.col("total_minutes"))
            .otherwise(None)
            .alias(reverse_column),
        )
        .group_by("name")
        .agg(pl.col(reverse_column).min())
    )
    return primary.join(reverse, on="name", how="full", coalesce=True).select(
        "name",
        pl.coalesce(primary_column, reverse_column).alias(value_name),
        (pl.col(primary_column).is_null() & pl.col(reverse_column).is_not_null()).alias(
            f"{value_name}_estimated"
        ),
    )


def build_reachability_payload(
    distance_table: pl.DataFrame,
    stop_geo: pl.DataFrame,
    participants: Sequence[Mapping[str, Any]],
    direction: str,
) -> dict[str, Any]:
    """Build approximate per-stop journey values from the precomputed matrix."""
    if direction not in VALID_DIRECTIONS:
        raise ValueError(f"Invalid search direction: {direction}")

    result = _geo_frame(stop_geo)
    participant_columns = []
    estimated_columns = []
    for index, participant in enumerate(participants):
        column = f"participant_{index}"
        estimated_column = f"participant_{index}_estimated"
        participant_columns.append(column)
        estimated_columns.append(estimated_column)
        result = result.join(
            _participant_frame(distance_table, participant, index, direction),
            on="name",
            how="left",
        )

    if participant_columns:
        participant_values = [pl.col(column) for column in participant_columns]
        participant_estimates = [pl.col(column).fill_null(False) for column in estimated_columns]
        result = result.with_columns(
            pl.concat_list(participant_values).alias("participant_minutes"),
            pl.when(pl.all_horizontal(*(value.is_not_null() for value in participant_values)))
            .then(pl.max_horizontal(*participant_values))
            .otherwise(None)
            .alias("group_max_minutes"),
            pl.when(pl.all_horizontal(*(value.is_not_null() for value in participant_values)))
            .then(pl.any_horizontal(*participant_estimates))
            .otherwise(False)
            .alias("estimated"),
        )
    else:
        result = result.with_columns(
            pl.lit([], dtype=pl.List(pl.Int64)).alias("participant_minutes"),
            pl.lit(None, dtype=pl.Int64).alias("group_max_minutes"),
            pl.lit(False).alias("estimated"),
        )

    stops = result.select(
        "name", "lat", "lon", "participant_minutes", "group_max_minutes", "estimated"
    ).to_dicts()
    complete_stops = result.select(pl.col("group_max_minutes").is_not_null().sum()).item()
    estimated_stops = result.select(pl.col("estimated").sum()).item()

    return {
        "participants": [dict(participant) for participant in participants],
        "direction": direction,
        "dataset": "precomputed typical transit times",
        "coverage": {
            "total_stops": len(stops),
            "complete_stops": int(complete_stops),
        },
        "estimation": {
            "method": "reverse direction where a requested matrix leg is missing",
            "estimated_stops": int(estimated_stops),
        },
        "stops": stops,
    }
