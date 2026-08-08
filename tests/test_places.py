import aiosqlite
import pytest
import pytest_asyncio

import backend.places as places
from backend.db import init_db
from backend.places import (
    cache_pubs,
    cache_pubs_for_type,
    get_cached_pubs,
    get_cached_pubs_for_type,
    parse_places_response,
    search_pubs_near_stop,
)

PUB = {
    "place_id": "ChIJ_test1",
    "name": "U Fleku",
    "lat": 50.0789,
    "lon": 14.4186,
    "rating": 4.3,
    "rating_count": 5421,
    "price_level": 2,
    "google_maps_url": "https://maps.google.com/?cid=123",
    "opening_hours": None,
    "primary_type": "",
}
PUB_A = {**PUB, "place_id": "ChIJ_test_a", "name": "Pub A"}
PUB_B = {**PUB, "place_id": "ChIJ_test_b", "name": "Pub B"}
PUB_C = {**PUB, "place_id": "ChIJ_test_c", "name": "Pub C"}

MOCK_PLACES_RESPONSE = {
    "places": [
        {
            "id": "ChIJ_test1",
            "displayName": {"text": "U Fleku"},
            "location": {"latitude": 50.0789, "longitude": 14.4186},
            "rating": 4.3,
            "userRatingCount": 5421,
            "priceLevel": "PRICE_LEVEL_MODERATE",
            "googleMapsUri": "https://maps.google.com/?cid=123",
        },
        {
            "id": "ChIJ_test2",
            "displayName": {"text": "Lokál"},
            "location": {"latitude": 50.0801, "longitude": 14.4200},
            "rating": 4.5,
            "userRatingCount": 3200,
            "priceLevel": "PRICE_LEVEL_INEXPENSIVE",
            "googleMapsUri": "https://maps.google.com/?cid=456",
        },
    ]
}


def fake_client(captured: dict, response_data: dict):
    """Provide an httpx client double that records the request payload."""

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return response_data

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            captured["client_kwargs"] = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, *, json, headers):
            captured.update({"url": url, "json": json, "headers": headers})
            return FakeResponse()

    return FakeAsyncClient


@pytest_asyncio.fixture
async def db():
    conn = await aiosqlite.connect(":memory:")
    await init_db(conn)
    yield conn
    await conn.close()


def test_parse_places_response():
    pubs = parse_places_response(MOCK_PLACES_RESPONSE)
    assert len(pubs) == 2
    assert pubs[0]["name"] == "U Fleku"
    assert pubs[0]["rating"] == 4.3
    assert pubs[0]["rating_count"] == 5421
    assert pubs[0]["price_level"] == 2
    assert pubs[0]["place_id"] == "ChIJ_test1"


@pytest.mark.asyncio
async def test_cache_and_retrieve_pubs(db):
    pubs = [
        {
            "place_id": "ChIJ_test1",
            "name": "U Fleku",
            "lat": 50.0789,
            "lon": 14.4186,
            "rating": 4.3,
            "rating_count": 5421,
            "price_level": 2,
            "google_maps_url": "https://maps.google.com/?cid=123",
        },
    ]
    await cache_pubs(db, "Národní třída", pubs)
    cached = await get_cached_pubs(db, "Národní třída")
    assert len(cached) == 1
    assert cached[0]["name"] == "U Fleku"


@pytest.mark.asyncio
async def test_cached_empty_query_is_not_a_miss(db):
    await cache_pubs_for_type(db, "Muzeum", "pub", 500, [])
    assert await get_cached_pubs_for_type(db, "Muzeum", "pub", 500) == []


@pytest.mark.asyncio
async def test_cache_is_scoped_to_requested_type(db):
    await cache_pubs_for_type(db, "Muzeum", "cafe", 500, [PUB])
    assert await get_cached_pubs_for_type(db, "Muzeum", "pub", 500) is None


@pytest.mark.asyncio
async def test_refresh_replaces_only_refreshed_type_matches(db):
    await cache_pubs_for_type(db, "Muzeum", "pub", 500, [PUB_A])
    await cache_pubs_for_type(db, "Muzeum", "cafe", 500, [PUB_B])
    await cache_pubs_for_type(db, "Muzeum", "pub", 500, [PUB_C])
    assert await get_cached_pubs_for_type(db, "Muzeum", "pub", 500) == [PUB_C]
    assert await get_cached_pubs_for_type(db, "Muzeum", "cafe", 500) == [PUB_B]


@pytest.mark.asyncio
async def test_cache_matches_are_isolated_by_radius(db):
    await cache_pubs_for_type(db, "Muzeum", "pub", 500, [PUB_A])
    await cache_pubs_for_type(db, "Muzeum", "pub", 1000, [PUB_B])
    assert await get_cached_pubs_for_type(db, "Muzeum", "pub", 500) == [PUB_A]
    assert await get_cached_pubs_for_type(db, "Muzeum", "pub", 1000) == [PUB_B]


def test_parse_empty_response():
    pubs = parse_places_response({})
    assert pubs == []


def test_parse_missing_fields():
    data = {
        "places": [
            {
                "id": "test",
                "displayName": {"text": "Bar"},
                "location": {"latitude": 50.0, "longitude": 14.0},
            }
        ]
    }
    pubs = parse_places_response(data)
    assert pubs[0]["rating"] is None
    assert pubs[0]["price_level"] is None


@pytest.mark.asyncio
async def test_nearby_search_uses_one_type_and_distance_ranking(monkeypatch):
    """A combined-type or relevance-ranked request could hide nearby pubs."""
    captured = {}
    monkeypatch.setattr("backend.places.httpx.AsyncClient", fake_client(captured, {"places": []}))

    await search_pubs_near_stop(50.08, 14.43, "pub")

    assert captured["json"]["includedTypes"] == ["pub"]
    assert captured["json"]["maxResultCount"] == 20
    assert captured["json"]["rankPreference"] == "DISTANCE"


def test_order_pubs_for_stop_deduplicates_and_breaks_distance_ties():
    """Unordered duplicate results would produce unstable venue suggestions."""
    pubs = [
        {**PUB, "place_id": "far", "lat": 50.09, "lon": 14.43, "rating": 5.0},
        {
            **PUB,
            "place_id": "tie-low",
            "lat": 50.08,
            "lon": 14.43,
            "rating": 4.0,
            "rating_count": 100,
        },
        {
            **PUB,
            "place_id": "tie-high-count",
            "lat": 50.08,
            "lon": 14.43,
            "rating": 4.0,
            "rating_count": 200,
        },
        {
            **PUB,
            "place_id": "tie-high-rating",
            "lat": 50.08,
            "lon": 14.43,
            "rating": 4.5,
            "rating_count": 1,
        },
        {
            **PUB,
            "place_id": "tie-zulu",
            "lat": 50.08,
            "lon": 14.43,
            "rating": 3.0,
            "rating_count": 1,
        },
        {
            **PUB,
            "place_id": "tie-alpha",
            "lat": 50.08,
            "lon": 14.43,
            "rating": 3.0,
            "rating_count": 1,
        },
        {**PUB, "place_id": "far", "lat": 50.07, "lon": 14.43, "rating": 1.0},
    ]

    ordered = places.order_pubs_for_stop(pubs, 50.08, 14.43)

    assert [pub["place_id"] for pub in ordered] == [
        "tie-high-rating",
        "tie-high-count",
        "tie-low",
        "tie-alpha",
        "tie-zulu",
        "far",
    ]
    assert all(isinstance(pub["distance_m"], float) for pub in ordered)
