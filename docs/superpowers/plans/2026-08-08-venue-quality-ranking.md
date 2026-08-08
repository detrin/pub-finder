# Venue Quality Ranking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rank each stop's venue suggestions by a confidence-adjusted rating score and show every rating as one decimal plus its review count.

**Architecture:** Keep all quality logic in `backend.places.order_pubs_for_stop`, which already deduplicates and enriches Google Places results. It will add a deterministic Bayesian-smoothed score from Google’s aggregate rating and review count, then rank quality first and walking distance second. The Jinja template only formats already-stored fields, so cached and saved results share the display change.

**Tech Stack:** Python 3.12, FastAPI/Jinja2, pytest.

## Global Constraints

- Use `prior_rating = 4.0` and `prior_weight = 25` exactly.
- Keep Google’s raw `rating` and `rating_count` unchanged for display and persistence.
- Give missing rating or count no quality score and sort those venues after scored venues.
- Sort by descending quality score, ascending walking distance, descending raw rating, descending review count, then place ID.
- Display ratings exactly as `4.6★ (23,360)` in both collapsed and expanded venue lists.
- Do not change Google queries, cache behavior, transit ranking, or venue limits.

---

### Task 1: Confidence-adjusted venue ordering

**Files:**
- Modify: `backend/places.py:116-138`
- Test: `tests/test_places.py:195-247`

**Interfaces:**
- Produces: `venue_quality_score(rating: float | None, rating_count: int | None) -> float | None`.
- Updates: `order_pubs_for_stop(pubs: list[dict], lat: float, lon: float) -> list[dict]` to attach `quality_score` to every returned venue.
- Consumes: parsed Google `rating` and `rating_count` values already stored in each venue dict.

- [ ] **Step 1: Write failing ordering tests**

```python
def test_order_pubs_for_stop_prefers_confident_quality_over_tiny_sample():
    pubs = [
        {**PUB, "place_id": "perfect-three", "rating": 5.0, "rating_count": 3},
        {**PUB, "place_id": "trusted", "rating": 4.8, "rating_count": 100},
    ]

    ordered = places.order_pubs_for_stop(pubs, 50.08, 14.43)

    assert [pub["place_id"] for pub in ordered] == ["trusted", "perfect-three"]
    assert ordered[0]["quality_score"] > ordered[1]["quality_score"]


def test_venue_quality_score_returns_none_for_missing_evidence():
    assert places.venue_quality_score(None, 10) is None
    assert places.venue_quality_score(4.5, None) is None
```

- [ ] **Step 2: Run the focused tests to verify RED**

Run: `uv run pytest tests/test_places.py -k 'confident_quality or missing_evidence' -v`

Expected: FAIL because `venue_quality_score` does not exist and distance remains the primary ordering key.

- [ ] **Step 3: Add the score helper and change the deterministic sort key**

```python
QUALITY_PRIOR_RATING = 4.0
QUALITY_PRIOR_WEIGHT = 25


def venue_quality_score(rating: float | None, rating_count: int | None) -> float | None:
    if rating is None or rating_count is None or rating_count < 0:
        return None
    return (
        rating_count * rating + QUALITY_PRIOR_WEIGHT * QUALITY_PRIOR_RATING
    ) / (rating_count + QUALITY_PRIOR_WEIGHT)
```

Copy each venue before enriching it with both `distance_m` and `quality_score`. Sort scored venues before unscored venues using `quality_score is None` as the first key, then use the exact global constraint ordering.

- [ ] **Step 4: Run focused and full backend tests**

Run: `uv run pytest tests/test_places.py -q && uv run pytest -q && uv run ruff check backend/places.py tests/test_places.py`

Expected: PASS with all tests green and no lint errors.

- [ ] **Step 5: Commit the ordering change**

```bash
git add backend/places.py tests/test_places.py
git commit -m "feat: rank venues by confidence-adjusted quality"
```

### Task 2: Consistent rating and review-count presentation

**Files:**
- Modify: `templates/partials/results_table.html:96-122`
- Test: `tests/test_integration.py`

**Interfaces:**
- Consumes: `pub["rating"]` as a number or `None`, and `pub["rating_count"]` as an integer or `None`.
- Produces: identical rating markup in the first three visible venue pills and the expanded venue list.

- [ ] **Step 1: Write a failing rendered-results test**

```python
response = await client.get(f"/session/{code}/results")

assert "4.6★ (23,360)" in response.text
assert response.text.count("4.6★ (23,360)") == 2
```

Use a saved result fixture with at least four venues so one matching rating appears in the collapsed list and one appears inside “Show more.”

- [ ] **Step 2: Run the focused test to verify RED**

Run: `uv run pytest tests/test_integration.py -k 'rating_display' -v`

Expected: FAIL because the template renders unformatted raw ratings and omits review counts in expanded items.

- [ ] **Step 3: Format both template branches identically**

```jinja2
{{ "%.1f" | format(pub.rating) }}&#9733; ({{ "{:,}".format(pub.rating_count) }})
```

Render the span only when both rating and rating count are available. Keep the existing link title, updating it to use the same one-decimal rating and count when present.

- [ ] **Step 4: Run all checks**

Run: `uv run pytest -q && uv run ruff check . && node --test tests_js/app.test.mjs && git diff --check`

Expected: PASS.

- [ ] **Step 5: Commit the display change**

```bash
git add templates/partials/results_table.html tests/test_integration.py
git commit -m "feat: show precise venue ratings and review counts"
```

## Self-review

- Spec coverage: Task 1 implements the exact smoothing constants, uncertain-value behavior, and sort order. Task 2 implements one-decimal ratings and grouped review counts in both template branches.
- Placeholder scan: no TODO/TBD or unspecified implementation steps.
- Type consistency: `venue_quality_score` receives nullable parsed values and returns the nullable `quality_score` added by `order_pubs_for_stop`; the template continues to use raw persisted values.
