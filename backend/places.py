import httpx
import aiosqlite
from config import GOOGLE_PLACES_API_KEY

PRICE_LEVEL_MAP = {
    "PRICE_LEVEL_FREE": 0,
    "PRICE_LEVEL_INEXPENSIVE": 1,
    "PRICE_LEVEL_MODERATE": 2,
    "PRICE_LEVEL_EXPENSIVE": 3,
    "PRICE_LEVEL_VERY_EXPENSIVE": 4,
}


def parse_places_response(data: dict) -> list[dict]:
    """Parse Google Places API response into list of pub dicts."""
    pubs = []
    for place in data.get("places", []):
        pubs.append({
            "place_id": place.get("id", ""),
            "name": place.get("displayName", {}).get("text", ""),
            "lat": place.get("location", {}).get("latitude", 0),
            "lon": place.get("location", {}).get("longitude", 0),
            "rating": place.get("rating"),
            "rating_count": place.get("userRatingCount"),
            "price_level": PRICE_LEVEL_MAP.get(place.get("priceLevel"), None),
            "google_maps_url": place.get("googleMapsUri", ""),
        })
    return pubs


async def search_pubs_near_stop(lat: float, lon: float, radius: int = 500) -> list[dict]:
    """Search Google Places API for bars/pubs/restaurants near coordinates."""
    url = "https://places.googleapis.com/v1/places:searchNearby"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.location,places.rating,places.userRatingCount,places.priceLevel,places.googleMapsUri",
    }
    body = {
        "includedTypes": ["bar", "pub", "restaurant"],
        "maxResultCount": 10,
        "locationRestriction": {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
                "radius": radius,
            }
        },
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(url, json=body, headers=headers)
        resp.raise_for_status()
        return parse_places_response(resp.json())


async def get_cached_pubs(db: aiosqlite.Connection, stop_name: str) -> list[dict]:
    """Get cached pubs for a stop (within 24h TTL)."""
    cursor = await db.execute(
        "SELECT place_id, name, lat, lon, rating, rating_count, price_level, google_maps_url "
        "FROM pub_cache WHERE stop_name = ? AND cached_at > datetime('now', '-24 hours')",
        (stop_name,),
    )
    rows = await cursor.fetchall()
    return [
        {"place_id": r[0], "name": r[1], "lat": r[2], "lon": r[3],
         "rating": r[4], "rating_count": r[5], "price_level": r[6], "google_maps_url": r[7]}
        for r in rows
    ]


async def cache_pubs(db: aiosqlite.Connection, stop_name: str, pubs: list[dict]):
    """Cache pubs for a stop in SQLite."""
    for pub in pubs:
        await db.execute(
            "INSERT OR REPLACE INTO pub_cache (stop_name, place_id, name, lat, lon, rating, rating_count, price_level, google_maps_url, cached_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            (stop_name, pub["place_id"], pub["name"], pub["lat"], pub["lon"],
             pub["rating"], pub["rating_count"], pub["price_level"], pub["google_maps_url"]),
        )
    await db.commit()
