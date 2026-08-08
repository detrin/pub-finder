from __future__ import annotations

import json
import math
from datetime import datetime
from typing import Optional

import aiosqlite
import httpx

from backend.config import GOOGLE_PLACES_API_KEY

PRICE_LEVEL_MAP = {
    "PRICE_LEVEL_FREE": 0,
    "PRICE_LEVEL_INEXPENSIVE": 1,
    "PRICE_LEVEL_MODERATE": 2,
    "PRICE_LEVEL_EXPENSIVE": 3,
    "PRICE_LEVEL_VERY_EXPENSIVE": 4,
}

# Google Places API day mapping: 0=Sunday, 1=Monday, ..., 6=Saturday
# Python weekday(): 0=Monday, ..., 6=Sunday
_PYTHON_TO_GOOGLE_DAY = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 0}


def parse_places_response(data: dict) -> list[dict]:
    """Parse Google Places API response into list of pub dicts."""
    pubs = []
    for place in data.get("places", []):
        hours = place.get("regularOpeningHours", {})
        periods = hours.get("periods", [])

        pubs.append(
            {
                "place_id": place.get("id", ""),
                "name": place.get("displayName", {}).get("text", ""),
                "lat": place.get("location", {}).get("latitude", 0),
                "lon": place.get("location", {}).get("longitude", 0),
                "rating": place.get("rating"),
                "rating_count": place.get("userRatingCount"),
                "price_level": PRICE_LEVEL_MAP.get(place.get("priceLevel"), None),
                "google_maps_url": place.get("googleMapsUri", ""),
                "opening_hours": periods if periods else None,
                "primary_type": place.get("primaryType", ""),
            }
        )
    return pubs


def is_open_during(pub: dict, arrival: datetime, departure: datetime) -> bool:
    """Check if a pub is open for the full window between arrival and departure.

    If opening_hours is None (not available), assume open.
    """
    periods = pub.get("opening_hours")
    if not periods:
        return True

    arrival_day = _PYTHON_TO_GOOGLE_DAY[arrival.weekday()]
    arrival_minutes = arrival.hour * 60 + arrival.minute
    departure_day = _PYTHON_TO_GOOGLE_DAY[departure.weekday()]
    departure_minutes = departure.hour * 60 + departure.minute

    for period in periods:
        open_info = period.get("open", {})
        close_info = period.get("close")

        open_day = open_info.get("day")
        open_minutes = open_info.get("hour", 0) * 60 + open_info.get("minute", 0)

        # No close info means open 24 hours
        if close_info is None:
            return True

        close_day = close_info.get("day")
        close_minutes = close_info.get("hour", 0) * 60 + close_info.get("minute", 0)

        # Check if arrival falls within this period
        if open_day == close_day:
            # Same day period
            if (
                arrival_day == open_day
                and arrival_minutes >= open_minutes
                and departure_day == close_day
                and departure_minutes <= close_minutes
            ):
                return True
        else:
            # Overnight period (e.g. open Friday 18:00, close Saturday 02:00)
            if arrival_day == open_day and arrival_minutes >= open_minutes:
                if departure_day == close_day and departure_minutes <= close_minutes:
                    return True
                if departure_day == open_day and departure_minutes >= arrival_minutes:
                    return True
            if arrival_day == close_day and arrival_minutes < close_minutes:
                if departure_day == close_day and departure_minutes <= close_minutes:
                    return True

    return False


def _distance_meters(lat: float, lon: float, pub_lat: float, pub_lon: float) -> float:
    """Return the great-circle distance between a stop and a pub in metres."""
    earth_radius_m = 6_371_000
    lat_delta = math.radians(pub_lat - lat)
    lon_delta = math.radians(pub_lon - lon)
    haversine = (
        math.sin(lat_delta / 2) ** 2
        + math.cos(math.radians(lat))
        * math.cos(math.radians(pub_lat))
        * math.sin(lon_delta / 2) ** 2
    )
    return 2 * earth_radius_m * math.asin(math.sqrt(haversine))


def order_pubs_for_stop(pubs: list[dict], lat: float, lon: float) -> list[dict]:
    """Deduplicate and deterministically order nearby pubs for one stop."""
    unique_pubs = {}
    for pub in pubs:
        unique_pubs.setdefault(pub["place_id"], pub)

    pubs_with_distance = []
    for pub in unique_pubs.values():
        ordered_pub = {
            **pub,
            "distance_m": _distance_meters(lat, lon, pub["lat"], pub["lon"]),
        }
        pubs_with_distance.append(ordered_pub)

    return sorted(
        pubs_with_distance,
        key=lambda pub: (
            pub["distance_m"],
            -(pub.get("rating") or 0),
            -(pub.get("rating_count") or 0),
            pub["place_id"],
        ),
    )


async def search_pubs_near_stop(
    lat: float,
    lon: float,
    place_type: Optional[str] = None,
    radius: int = 500,
    place_types: Optional[list[str]] = None,
) -> list[dict]:
    """Search Google Places API, retaining the route's legacy combined-type call."""
    is_single_type_search = place_type is not None
    included_types = [place_type] if is_single_type_search else place_types
    if included_types is None:
        included_types = ["bar", "pub", "cafe"]

    url = "https://places.googleapis.com/v1/places:searchNearby"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": (
            "places.id,places.displayName,places.location,places.rating,"
            "places.userRatingCount,places.priceLevel,places.googleMapsUri,"
            "places.regularOpeningHours,places.primaryType"
        ),
    }
    body = {
        "includedTypes": included_types,
        "maxResultCount": 20,
        "locationRestriction": {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
                "radius": radius,
            }
        },
    }
    if is_single_type_search:
        body["rankPreference"] = "DISTANCE"

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(url, json=body, headers=headers)
        resp.raise_for_status()
        pubs = parse_places_response(resp.json())
        return order_pubs_for_stop(pubs, lat, lon) if is_single_type_search else pubs


async def get_cached_pubs(
    db: aiosqlite.Connection,
    stop_name: str,
    place_types: Optional[list[str]] = None,
) -> list[dict]:
    """Get cached pubs for a stop (within 90 day TTL), filtered by place types."""
    cursor = await db.execute(
        "SELECT place_id, name, lat, lon, rating, rating_count, price_level, "
        "google_maps_url, opening_hours, primary_type "
        "FROM pub_cache WHERE stop_name = ? AND cached_at > datetime('now', '-90 days')",
        (stop_name,),
    )
    rows = await cursor.fetchall()
    pubs = [
        {
            "place_id": r[0],
            "name": r[1],
            "lat": r[2],
            "lon": r[3],
            "rating": r[4],
            "rating_count": r[5],
            "price_level": r[6],
            "google_maps_url": r[7],
            "opening_hours": json.loads(r[8]) if r[8] else None,
            "primary_type": r[9] or "",
        }
        for r in rows
    ]
    if place_types:
        pubs = [p for p in pubs if p["primary_type"] in place_types or not p["primary_type"]]
    return pubs


async def get_cached_pubs_for_type(
    db: aiosqlite.Connection, stop_name: str, place_type: str, radius: int = 500
) -> list[dict] | None:
    """Get a fresh cache entry for one Google Places query, if it exists."""
    async with db.execute(
        "SELECT 1 FROM pub_cache_queries "
        "WHERE stop_name = ? AND place_type = ? AND radius = ? "
        "AND cached_at > datetime('now', '-90 days')",
        (stop_name, place_type, radius),
    ) as cursor:
        query_row = await cursor.fetchone()
    if query_row is None:
        return None

    async with db.execute(
        "SELECT cache.place_id, cache.name, cache.lat, cache.lon, cache.rating, "
        "cache.rating_count, cache.price_level, cache.google_maps_url, "
        "cache.opening_hours, cache.primary_type "
        "FROM pub_cache_matches AS matches "
        "JOIN pub_cache AS cache "
        "ON cache.stop_name = matches.stop_name AND cache.place_id = matches.place_id "
        "WHERE matches.stop_name = ? AND matches.place_type = ? AND matches.radius = ?",
        (stop_name, place_type, radius),
    ) as cursor:
        rows = await cursor.fetchall()
    return [
        {
            "place_id": row[0],
            "name": row[1],
            "lat": row[2],
            "lon": row[3],
            "rating": row[4],
            "rating_count": row[5],
            "price_level": row[6],
            "google_maps_url": row[7],
            "opening_hours": json.loads(row[8]) if row[8] else None,
            "primary_type": row[9] or "",
        }
        for row in rows
    ]


async def cache_pubs(db: aiosqlite.Connection, stop_name: str, pubs: list[dict]):
    """Cache pubs for a stop in SQLite."""
    for pub in pubs:
        hours_json = json.dumps(pub["opening_hours"]) if pub.get("opening_hours") else None
        await db.execute(
            "INSERT OR REPLACE INTO pub_cache "
            "(stop_name, place_id, name, lat, lon, rating, rating_count, price_level, "
            "google_maps_url, opening_hours, primary_type, cached_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            (
                stop_name,
                pub["place_id"],
                pub["name"],
                pub["lat"],
                pub["lon"],
                pub["rating"],
                pub["rating_count"],
                pub.get("price_level"),
                pub["google_maps_url"],
                hours_json,
                pub.get("primary_type", ""),
            ),
        )
    await db.commit()


async def cache_pubs_for_type(
    db: aiosqlite.Connection,
    stop_name: str,
    place_type: str,
    radius: int,
    pubs: list[dict],
) -> None:
    """Atomically cache one completed Google Places query, including empty results."""
    try:
        await db.execute("BEGIN")
        for pub in pubs:
            hours_json = json.dumps(pub["opening_hours"]) if pub.get("opening_hours") else None
            await db.execute(
                "INSERT INTO pub_cache "
                "(stop_name, place_id, name, lat, lon, rating, rating_count, price_level, "
                "google_maps_url, opening_hours, primary_type, cached_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now')) "
                "ON CONFLICT(stop_name, place_id) DO UPDATE SET "
                "name = excluded.name, lat = excluded.lat, lon = excluded.lon, "
                "rating = excluded.rating, rating_count = excluded.rating_count, "
                "price_level = excluded.price_level, google_maps_url = excluded.google_maps_url, "
                "opening_hours = excluded.opening_hours, primary_type = excluded.primary_type, "
                "cached_at = excluded.cached_at",
                (
                    stop_name,
                    pub["place_id"],
                    pub["name"],
                    pub["lat"],
                    pub["lon"],
                    pub["rating"],
                    pub["rating_count"],
                    pub.get("price_level"),
                    pub["google_maps_url"],
                    hours_json,
                    pub.get("primary_type", ""),
                ),
            )
        await db.execute(
            "DELETE FROM pub_cache_matches WHERE stop_name = ? AND place_type = ? AND radius = ?",
            (stop_name, place_type, radius),
        )
        await db.executemany(
            "INSERT INTO pub_cache_matches (stop_name, place_type, radius, place_id) "
            "VALUES (?, ?, ?, ?)",
            [(stop_name, place_type, radius, pub["place_id"]) for pub in pubs],
        )
        await db.execute(
            "INSERT INTO pub_cache_queries (stop_name, place_type, radius, cached_at) "
            "VALUES (?, ?, ?, datetime('now')) "
            "ON CONFLICT(stop_name, place_type, radius) DO UPDATE SET "
            "cached_at = excluded.cached_at",
            (stop_name, place_type, radius),
        )
        await db.commit()
    except Exception:
        await db.rollback()
        raise
