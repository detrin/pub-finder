import pytest
import pytest_asyncio
import aiosqlite
from backend.db import init_db
from backend.places import parse_places_response, get_cached_pubs, cache_pubs


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
        {"place_id": "ChIJ_test1", "name": "U Fleku", "lat": 50.0789, "lon": 14.4186,
         "rating": 4.3, "rating_count": 5421, "price_level": 2, "google_maps_url": "https://maps.google.com/?cid=123"},
    ]
    await cache_pubs(db, "Národní třída", pubs)
    cached = await get_cached_pubs(db, "Národní třída")
    assert len(cached) == 1
    assert cached[0]["name"] == "U Fleku"


def test_parse_empty_response():
    pubs = parse_places_response({})
    assert pubs == []


def test_parse_missing_fields():
    data = {"places": [{"id": "test", "displayName": {"text": "Bar"}, "location": {"latitude": 50.0, "longitude": 14.0}}]}
    pubs = parse_places_response(data)
    assert pubs[0]["rating"] is None
    assert pubs[0]["price_level"] is None
