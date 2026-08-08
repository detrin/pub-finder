# Balanced Google Places Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the five best meeting stops type-balanced Google venue suggestions without allowing one type to consume Nearby Search's 20-result ceiling.

**Architecture:** Add query coverage and query-to-place associations beside the existing payload cache. Search each selected type independently, at most four calls at a time, then merge, deduplicate, and order the resulting places per stop.

**Tech Stack:** Python 3.12, FastAPI, aiosqlite, httpx, Polars, pytest, pytest-asyncio.

## Global Constraints

- Keep Google Places, Leaflet, and the existing 90-day payload-cache policy unchanged.
- Search only `top_stops[:5]`; each selected type gets one 500 m, 20-result, `DISTANCE` Nearby Search request.
- A valid empty response is cached; a failed request is not.
- Never run more than four live Places requests during one search.
- Rows below the five-stop cutoff must not claim that no venues were found.

---

## File Structure

- `backend/db.py`: query-coverage and query-to-place tables.
- `backend/places.py`: one-type HTTP request, query-aware cache, cache write, distance ordering.
- `routers/search.py`: top-five task orchestration, partial failure, render state.
- `templates/partials/results_table.html`: searched vs unsearched venue state.
- `tests/test_places.py`: cache and request contract tests.
- `tests/test_integration.py`: route orchestration and HTML behavior.
- `README.md`, `templates/how_it_works.html`: focused top-five venue copy.

## Task 1: Query-aware cache

**Files:** Modify `backend/db.py`, `backend/places.py`, `tests/test_places.py`.

**Interfaces:**

```python
async def get_cached_pubs_for_type(
    db: aiosqlite.Connection, stop_name: str, place_type: str, radius: int = 500
) -> list[dict] | None: ...

async def cache_pubs_for_type(
    db: aiosqlite.Connection, stop_name: str, place_type: str, radius: int, pubs: list[dict]
) -> None: ...
```

- [ ] **Step 1: Write failing cache tests**

```python
@pytest.mark.asyncio
async def test_cached_empty_query_is_not_a_miss(db):
    await cache_pubs_for_type(db, "Muzeum", "pub", 500, [])
    assert await get_cached_pubs_for_type(db, "Muzeum", "pub", 500) == []

@pytest.mark.asyncio
async def test_cache_is_scoped_to_requested_type(db):
    await cache_pubs_for_type(db, "Muzeum", "cafe", 500, [PUB])
    assert await get_cached_pubs_for_type(db, "Muzeum", "pub", 500) is None
```

- [ ] **Step 2: Run the tests and confirm failure**

Run: `uv run pytest tests/test_places.py -q`

Expected: FAIL because the functions and tables do not exist.

- [ ] **Step 3: Implement schema and atomic writes**

```sql
CREATE TABLE IF NOT EXISTS pub_cache_queries (
  stop_name TEXT NOT NULL, place_type TEXT NOT NULL, radius INTEGER NOT NULL,
  cached_at TEXT NOT NULL, PRIMARY KEY (stop_name, place_type, radius)
);
CREATE TABLE IF NOT EXISTS pub_cache_matches (
  stop_name TEXT NOT NULL, place_type TEXT NOT NULL, place_id TEXT NOT NULL,
  PRIMARY KEY (stop_name, place_type, place_id)
);
```

Join a fresh query row through matches to `pub_cache`. `None` is a miss; an empty list means a fresh query row with no matches. In one transaction, upsert payloads, replace only that query's matches, and upsert its timestamp.

- [ ] **Step 4: Add and pass refresh isolation test**

```python
@pytest.mark.asyncio
async def test_refresh_replaces_only_refreshed_type_matches(db):
    await cache_pubs_for_type(db, "Muzeum", "pub", 500, [PUB_A])
    await cache_pubs_for_type(db, "Muzeum", "cafe", 500, [PUB_B])
    await cache_pubs_for_type(db, "Muzeum", "pub", 500, [PUB_C])
    assert await get_cached_pubs_for_type(db, "Muzeum", "pub", 500) == [PUB_C]
    assert await get_cached_pubs_for_type(db, "Muzeum", "cafe", 500) == [PUB_B]
```

Run: `uv run pytest tests/test_places.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/db.py backend/places.py tests/test_places.py
git commit -m "feat: track cached place queries by type"
```

## Task 2: One-type distance-ranked Nearby Search

**Files:** Modify `backend/places.py`, `tests/test_places.py`.

**Interfaces:**

```python
async def search_pubs_near_stop(
    lat: float, lon: float, place_type: str, radius: int = 500
) -> list[dict]: ...

def order_pubs_for_stop(pubs: list[dict], lat: float, lon: float) -> list[dict]: ...
```

- [ ] **Step 1: Write the failing HTTP request contract**

```python
@pytest.mark.asyncio
async def test_nearby_search_uses_one_type_and_distance_ranking(monkeypatch):
    captured = {}
    monkeypatch.setattr("backend.places.httpx.AsyncClient", fake_client(captured, {"places": []}))
    await search_pubs_near_stop(50.08, 14.43, "pub")
    assert captured["json"]["includedTypes"] == ["pub"]
    assert captured["json"]["maxResultCount"] == 20
    assert captured["json"]["rankPreference"] == "DISTANCE"
```

- [ ] **Step 2: Run it and confirm failure**

Run: `uv run pytest tests/test_places.py::test_nearby_search_uses_one_type_and_distance_ranking -q`

Expected: FAIL because the current API accepts a combined type list and omits rank preference.

- [ ] **Step 3: Implement the narrow request and ordering**

Use `includedTypes: [place_type]`, `maxResultCount: 20`, and `rankPreference: "DISTANCE"`. Compute `distance_m` from the stop, deduplicate by `place_id`, and sort by `(distance_m, -rating_or_zero, -rating_count_or_zero, place_id)`.

- [ ] **Step 4: Run and commit**

Run: `uv run pytest tests/test_places.py -q`

Expected: PASS.

```bash
git add backend/places.py tests/test_places.py
git commit -m "feat: rank type-specific nearby searches by distance"
```

## Task 3: Bounded top-five orchestration

**Files:** Modify `routers/search.py`, `tests/test_integration.py`.

**Interfaces:** Consumes the Task 1 cache and Task 2 request functions. Produces `pubs_by_stop` only for searched stops and `pub_search_stop_names` for the template.

- [ ] **Step 1: Write failing integration tests**

```python
@pytest.mark.asyncio
async def test_search_queries_each_type_for_only_five_stops(monkeypatch):
    calls = []
    async def fake_search(lat, lon, place_type, radius=500):
        calls.append(place_type)
        return []
    monkeypatch.setattr("routers.search.search_pubs_near_stop", fake_search)
    # Mock six ranked stops and selected ["pub", "bar", "cafe"].
    # Assert exactly 15 calls and no sixth-stop lookup.

@pytest.mark.asyncio
async def test_one_type_failure_keeps_other_results(monkeypatch):
    async def fake_search(lat, lon, place_type, radius=500):
        if place_type == "bar":
            raise httpx.HTTPStatusError("limited", request=REQUEST, response=RESPONSE)
        return [PUB]
    # Assert pub/cafe results persist and the incomplete-data warning is present.
```

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/test_integration.py -q`

Expected: FAIL because the current route performs one sequential combined query for every ranked stop and suppresses later calls after a failure.

- [ ] **Step 3: Implement bounded query tasks**

```python
PUB_DISCOVERY_STOP_LIMIT = 5
PLACES_CONCURRENCY_LIMIT = 4
places_semaphore = asyncio.Semaphore(PLACES_CONCURRENCY_LIMIT)
```

For every selected `(top_stops[:5], place_type)` pair, return a cache hit, successful response, or local failure tuple. Protect only the live HTTP call with the semaphore; use `asyncio.gather()` to allow the remaining pairs to finish. Do not create a cache-coverage row on failure. Merge per-stop type lists, then apply existing opening-hours and cross-stop ID de-duplication.

- [ ] **Step 4: Run and commit**

Run: `uv run pytest tests/test_integration.py -q`

Expected: PASS, including 15 calls for six mocked stops and partial results after one failed type.

```bash
git add routers/search.py tests/test_integration.py
git commit -m "feat: balance venue discovery across place types"
```

## Task 4: Accurate cutoff UI and documentation

**Files:** Modify `routers/search.py`, `templates/partials/results_table.html`, `README.md`, `templates/how_it_works.html`, `tests/test_integration.py`.

- [ ] **Step 1: Write the failing HTML test**

```python
async def test_lower_ranked_stop_is_marked_unsearched(monkeypatch):
    # Mock six results and inspect rendered HTML.
    # Assert the sixth row says "Pub suggestions are shown for the top 5 meeting points"
    # and does not say "No pubs found nearby".
```

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/test_integration.py::test_lower_ranked_stop_is_marked_unsearched -q`

Expected: FAIL because every row currently renders the empty-pubs state.

- [ ] **Step 3: Implement render state and copy**

Pass `pub_search_stop_names=set(top_stops[:PUB_DISCOVERY_STOP_LIMIT])` to the results template. Render the current venues/empty state only for members of that set; render the exact cutoff message otherwise. Update README and How It Works to state that type-balanced discovery runs for the top five transit-ranked stops.

- [ ] **Step 4: Run complete verification**

```bash
uv sync --locked --extra dev
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
node --test tests_js/*.test.mjs
uv run pip-audit
uv run python -m compileall -q backend routers data_preparation tests
node --check static/app.js
git diff --check
```

Expected: every command exits 0.

- [ ] **Step 5: Commit and request review**

```bash
git add README.md routers/search.py templates/how_it_works.html templates/partials/results_table.html tests/test_integration.py
git commit -m "docs: explain focused venue discovery"
git status --short --branch
```

Request a read-only review before branch integration.

## Plan Self-Review

- Task 1 covers fresh, empty, and type-scoped cache states.
- Task 2 covers Google request shape and stable ordering.
- Task 3 covers top-five scope, four-call concurrency, and local failures.
- Task 4 covers truthful UI copy, documentation, and project-wide verification.
- The interfaces are consistent: cache miss is always `None`; valid empty is always `[]`; route rendering consumes `pub_search_stop_names`.

