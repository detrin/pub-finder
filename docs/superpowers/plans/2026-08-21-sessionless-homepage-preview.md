# Sessionless Homepage Reachability Preview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the homepage illustration with a stateless, additive one-way reachability preview and carry its selected starting stops into a newly created session.

**Architecture:** A bounded FastAPI JSON endpoint validates one to six canonical stop names and reuses `build_reachability_payload` without database or provider calls. A homepage-only JavaScript controller owns the accessible combobox, selection state, request cancellation, and session-form handoff while reusing the existing Leaflet reachability controller for rendering.

**Tech Stack:** Python 3, FastAPI, Pydantic, aiosqlite, Polars, Jinja2, vanilla ES modules, Leaflet, pytest, Node's built-in test runner

**Spec:** `docs/superpowers/specs/2026-08-21-sessionless-homepage-preview-design.md`

## Global Constraints

- The preview is approximate, one-way, based on precomputed typical transit times, and never described as live, exact, optimal, date-specific, or a route.
- It performs no database write, DPP request, Google Places request, cookie write, local-storage write, or hidden session creation.
- Accept one to six canonical, unique origins; never send the approximately 2.1-million-row matrix to the browser.
- The first origin renders individual reach; two or more render group maximum reach.
- The homepage has no date, return, direction, venue, ranking, participant-tab, or threshold controls.
- The native session form remains the only creation action and redirects to `/session/{code}`.
- Submitted origins are untrusted and must be normalized and validated again before atomic session creation.
- Preserve at least two unnamed participant slots; carry only `start_stop` and leave `end_stop` empty.
- All new visible copy ships in English and Czech.
- Render stop names with Jinja escaping or DOM `textContent`; never use `innerHTML`.
- Preserve keyboard access, 44 px targets, visible focus, reduced motion, light/dark themes, and visible OpenStreetMap attribution.
- Do not add analytics events as part of this implementation.

---

### Task 1: Stateless preview service and endpoint

**Files:**
- Create: `backend/preview.py`
- Modify: `routers/reachability.py`
- Test: `tests/test_reachability.py`

**Interfaces:**
- Produces: `normalize_preview_origins(raw_origins, allowed_stops, *, reject_duplicates) -> tuple[str, ...]`
- Produces: `build_preview_participants(origins) -> list[dict[str, object]]`
- Produces: `PreviewPayloadCache.get(key)`, `PreviewPayloadCache.set(key, payload)`, and `PreviewRateLimiter.allow(client_key)`
- Produces: `POST /reachability/preview` with body `{"origins": [str, ...]}` and the existing reachability payload response

- [ ] **Step 1: Write failing validation and route tests**

Add literal behavior tests covering the public boundary:

```python
@pytest.mark.asyncio
async def test_preview_returns_one_way_reachability_without_a_session():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/reachability/preview", json={"origins": ["A", "C"]})

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    payload = response.json()
    assert payload["direction"] == "there-only"
    assert [person["start_stop"] for person in payload["participants"]] == ["A", "C"]
    assert stop(payload, "B")["group_max_minutes"] == 15
    async with app.state.db.execute("SELECT COUNT(*) FROM sessions") as cursor:
        assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "status"),
    [
        ({"origins": []}, 422),
        ({"origins": ["missing"]}, 422),
        ({"origins": ["A", " A "]}, 422),
        ({"origins": ["A"] * 7}, 422),
        ({"origins": "A"}, 422),
    ],
)
async def test_preview_rejects_invalid_origins(body, status):
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/reachability/preview", json=body)
    assert response.status_code == status
```

Add focused unit tests proving cache keys preserve origin order, expired entries miss, the LRU stays bounded, and the limiter returns `False` after its configured count.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
.venv/bin/pytest -q tests/test_reachability.py -k 'preview'
```

Expected: failures because `/reachability/preview` and `backend.preview` do not exist.

- [ ] **Step 3: Implement the preview boundary**

Create focused primitives in `backend/preview.py`:

```python
from collections import OrderedDict, defaultdict, deque
from collections.abc import Collection, Sequence
from dataclasses import dataclass
import time

MAX_PREVIEW_ORIGINS = 6


class PreviewValidationError(ValueError):
    pass


def normalize_preview_origins(
    raw_origins: Sequence[str],
    allowed_stops: Collection[str],
    *,
    reject_duplicates: bool,
) -> tuple[str, ...]:
    if isinstance(raw_origins, (str, bytes)) or not 1 <= len(raw_origins) <= MAX_PREVIEW_ORIGINS:
        raise PreviewValidationError("Choose between one and six starting stops.")
    allowed = set(allowed_stops)
    normalized: list[str] = []
    seen: set[str] = set()
    for value in raw_origins:
        if not isinstance(value, str):
            raise PreviewValidationError("Starting stops are invalid.")
        stop = value.strip()
        if not stop or stop not in allowed:
            raise PreviewValidationError("Choose stops from the Prague stop list.")
        if stop in seen:
            if reject_duplicates:
                raise PreviewValidationError("Choose each starting stop once.")
            continue
        seen.add(stop)
        normalized.append(stop)
    return tuple(normalized)


def build_preview_participants(origins: Sequence[str]) -> list[dict[str, object]]:
    return [
        {
            "id": index + 1,
            "name": stop,
            "color": participant_color(index),
            "start_stop": stop,
            "end_stop": "",
        }
        for index, stop in enumerate(origins)
    ]
```

Implement `PreviewPayloadCache` as a monotonic-time `OrderedDict` with a 5-minute TTL and 64-entry maximum. Implement `PreviewRateLimiter` with per-client deques, 30 accepted requests per 60 seconds, and pruning on every call. Do not log keys or origins.

In `routers/reachability.py`, add a strict request model and route:

```python
class PreviewRequest(BaseModel):
    origins: list[str] = Field(min_length=1, max_length=MAX_PREVIEW_ORIGINS)


@router.post("/reachability/preview")
async def preview_reachability(request: Request, body: PreviewRequest) -> Response:
    client_key = _preview_client_key(request)
    if not _preview_limiter.allow(client_key):
        raise HTTPException(status_code=429, detail="Too many preview requests")
    try:
        origins = normalize_preview_origins(
            body.origins,
            request.app.state.all_stops,
            reject_duplicates=True,
        )
    except PreviewValidationError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    payload = _preview_cache.get(origins)
    if payload is None:
        payload = await run_in_threadpool(
            build_reachability_payload,
            request.app.state.distance_table,
            request.app.state.stop_geo,
            build_preview_participants(origins),
            "there-only",
        )
        _preview_cache.set(origins, payload)
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})
```

    Derive the limiter key from `request.client.host or "unknown"`; do not trust a caller-supplied forwarding header. Bound retained limiter keys and prune inactive windows so spoofed or high-cardinality clients cannot grow memory without limit. Keep preview limiter and cache globals resettable from tests.

- [ ] **Step 4: Run focused and neighboring tests**

Run:

```bash
.venv/bin/pytest -q tests/test_reachability.py tests/test_routers.py -k 'preview or reachability'
```

Expected: PASS.

- [ ] **Step 5: Commit the backend preview boundary**

```bash
git add backend/preview.py routers/reachability.py tests/test_reachability.py
git commit -m "Add stateless reachability preview API"
```

---

### Task 2: Atomic session carry-over

**Files:**
- Modify: `backend/db.py`
- Modify: `routers/session.py`
- Test: `tests/test_routers.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Consumes: `normalize_preview_origins(..., reject_duplicates=False)` from Task 1
- Changes: `create_session(db, session_name, creator_name="", initial_stops=()) -> dict`
- Changes: `POST /session/create` accepts repeated `preview_stops` form values

- [ ] **Step 1: Write failing carry-over and rollback tests**

Add route coverage with literal expectations:

```python
@pytest.mark.asyncio
async def test_create_session_carries_preview_origins_into_unnamed_slots():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/session/create",
            data={
                "session_name": "Friday crew",
                "preview_stops": ["A", "B", "C"],
            },
            follow_redirects=False,
        )

    code = response.headers["location"].removeprefix("/session/")
    participants = await get_participants(app.state.db, code)
    assert [person["name"] for person in participants] == ["", "", ""]
    assert [person["start_stop"] for person in participants] == ["A", "B", "C"]
    assert [person["end_stop"] for person in participants] == ["", "", ""]
    assert [person["same_start_end"] for person in participants] == [False, False, False]
```

Also add tests that zero stops still creates exactly two blank slots, one stop creates two slots with only the first start populated, duplicate submitted values are deduplicated, an unknown stop redirects with `error=preview_stops_invalid`, and no session exists after that validation failure. In `tests/test_db.py`, inject an `executemany` failure and assert both the session and participants roll back.

- [ ] **Step 2: Run carry-over tests and verify RED**

```bash
.venv/bin/pytest -q tests/test_routers.py tests/test_db.py -k 'preview_origin or preview_stop or create_session'
```

Expected: failures because the form values and `initial_stops` parameter are not implemented.

- [ ] **Step 3: Make session creation atomic and populate starts**

Change the database helper to accept validated stops and create the session plus slots in one savepoint:

```python
async def create_session(
    db: aiosqlite.Connection,
    session_name: str,
    creator_name: str = "",
    initial_stops: Sequence[str] = (),
) -> dict:
    code = secrets.token_hex(16)
    now = datetime.now(timezone.utc).isoformat()
    await db.execute("SAVEPOINT create_session")
    try:
        await db.execute(
            "INSERT INTO sessions (code, name, creator_name, created_at) VALUES (?, ?, ?, ?)",
            (code, session_name, creator_name, now),
        )
        if creator_name:
            rows = [(code, creator_name, "", "", 1, now)]
        else:
            slot_count = max(2, len(initial_stops))
            rows = [
                (
                    code,
                    "",
                    initial_stops[index] if index < len(initial_stops) else "",
                    "",
                    0 if index < len(initial_stops) else 1,
                    now,
                )
                for index in range(slot_count)
            ]
        await db.executemany(
            "INSERT INTO participants "
            "(session_code, name, start_stop, end_stop, same_start_end, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
    except Exception:
        await db.execute("ROLLBACK TO create_session")
        await db.execute("RELEASE create_session")
        raise
    await db.execute("RELEASE create_session")
    await db.commit()
    return {"code": code, "name": session_name, "creator_name": creator_name, "created_at": now}
```

In the route, parse repeated fields with `preview_stops: list[str] = Form(default=[])`, validate before calling `create_session`, and redirect to `/?error=preview_stops_invalid` without writing on failure. Do not treat hidden form values as trusted because the homepage generated them.

- [ ] **Step 4: Run carry-over and concurrency regression tests**

```bash
.venv/bin/pytest -q tests/test_routers.py tests/test_db.py -k 'create_session or participant or preview_stop'
```

Expected: PASS, including the existing minimum-two-slot and concurrent participant-operation tests.

- [ ] **Step 5: Commit session carry-over**

```bash
git add backend/db.py routers/session.py tests/test_routers.py tests/test_db.py
git commit -m "Carry preview stops into new sessions"
```

---

### Task 3: Reusable payload-driven map controller

**Files:**
- Modify: `static/reachability-map.js`
- Test: `tests/js/reachability-map.test.js`

**Interfaces:**
- Produces: `validateReachabilityPayload(payload) -> payload`
- Changes: `createReachabilityMap(root, { payload })` initializes without fetching
- Changes: `ReachabilityMapController.setPayload(payload)` validates every replacement
- Produces: `ReachabilityMapController.clearPayload()` removes field and participant markers without destroying the Leaflet map

- [ ] **Step 1: Write failing payload-mode tests**

```javascript
test("payload mode creates a reusable map without fetching", async () => {
    const harness = createHarness();
    let fetchCount = 0;
    const controller = await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        payload,
        fetch: async () => { fetchCount += 1; },
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
    });

    assert.equal(fetchCount, 0);
    controller.setPayload({ ...payload, participants: [], stops: [] });
    assert.equal(harness.createdGroups[0].layers.length, 0);
});
```

Add a test proving malformed replacement payloads throw before changing the current controller payload, and a test proving `clearPayload()` hides the field and clears origin markers.

- [ ] **Step 2: Run the map test and verify RED**

```bash
node --test tests/js/reachability-map.test.js
```

Expected: failure because the factory requires `reachabilityUrl` and no clear method exists.

- [ ] **Step 3: Add payload initialization and safe replacement**

Rename and export the existing validator, then validate before assignment:

```javascript
export function validateReachabilityPayload(payload) {
    if (
        payload == null
        || typeof payload !== "object"
        || !Array.isArray(payload.participants)
        || !Array.isArray(payload.stops)
    ) throw new TypeError("Reachability response is invalid");
    const ids = new Set();
    for (const participant of payload.participants) {
        if (
            participant == null
            || typeof participant !== "object"
            || !["number", "string"].includes(typeof participant.id)
            || ids.has(String(participant.id))
        ) throw new TypeError("Reachability response is invalid");
        ids.add(String(participant.id));
    }
    for (const stop of payload.stops) {
        if (
            stop == null
            || typeof stop.name !== "string"
            || !Number.isFinite(stop.lat)
            || !Number.isFinite(stop.lon)
            || !Array.isArray(stop.participant_minutes)
            || stop.participant_minutes.length !== payload.participants.length
            || !stop.participant_minutes.every(isNullableFinite)
            || !isNullableFinite(stop.group_max_minutes)
        ) throw new TypeError("Reachability response is invalid");
    }
    return payload;
}

setPayload(payload) {
    const validated = validateReachabilityPayload(payload);
    this.payload = validated;
    this.participantId = null;
    this.layerValues = selectLayerValues(validated, null);
    this.renderParticipants();
    this.scheduleRedraw();
}

clearPayload() {
    this.setPayload({ participants: [], stops: [] });
    this.hideField();
}
```

In `createReachabilityMap`, accept exactly one data source. If `options.payload` is supplied, validate it and skip fetch. Otherwise retain the existing required URL and fetch behavior. Preserve abort wiring, error events, results, venues, tile attribution, and destroy semantics.

- [ ] **Step 4: Run all reachability JavaScript tests**

```bash
node --test tests/js/reachability-core.test.js tests/js/reachability-map.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit reusable map behavior**

```bash
git add static/reachability-map.js tests/js/reachability-map.test.js
git commit -m "Allow reachability maps to accept live payloads"
```

---

### Task 4: Homepage preview controller

**Files:**
- Create: `static/home-preview.js`
- Create: `tests/js/home-preview.test.js`

**Interfaces:**
- Consumes: `createReachabilityMap(root, { payload })`, `controller.setPayload(payload)`, and `controller.clearPayload()` from Task 3
- Produces: `createHomePreview(root, dependencies) -> Promise<HomePreviewController>`
- Produces: `initHomePreview(target=document)` idempotent page initializer

- [ ] **Step 1: Write failing controller tests with real state transitions**

Build a small DOM harness and inject only fetch and map creation. Assert user-visible state, not mock call existence:

```javascript
test("first stop renders individual reach and second stop renders shared reach", async () => {
    const harness = createPreviewHarness(["Anděl", "Dejvická"]);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    controller.addOrigin("Anděl");
    requests.resolveNext(payloadFor(["Anděl"]));
    await harness.flush();
    assert.equal(harness.heading.textContent, "Approximate reach from Anděl");
    assert.equal(harness.prompt.textContent, "Add another stop to see where everyone can reach.");

    controller.addOrigin("Dejvická");
    requests.resolveNext(payloadFor(["Anděl", "Dejvická"]));
    await harness.flush();
    assert.equal(harness.heading.textContent, "Shared reach for 2 starting points");
    assert.equal(harness.hiddenInputs.length, 2);
});
```

Add separate tests for duplicate rejection, six-stop cap, chip removal, repeated hidden inputs named `preview_stops`, keyboard combobox navigation, Escape dismissal, newest-response wins, abort silence, genuine failure clearing the map, and idempotent initialization.

- [ ] **Step 2: Run the new JavaScript test and verify RED**

```bash
node --test tests/js/home-preview.test.js
```

Expected: module-not-found failure for `static/home-preview.js`.

- [ ] **Step 3: Implement the controller**

Use one exported controller with explicit state:

```javascript
const EMPTY_PAYLOAD = Object.freeze({ participants: [], stops: [] });
const MAX_ORIGINS = 6;

export async function createHomePreview(root, dependencies = {}) {
    const fetchRequest = dependencies.fetch ?? globalThis.fetch;
    const createMap = dependencies.createMap ?? createReachabilityMap;
    const stops = parseStops(root.dataset.stops);
    const selected = [];
    let requestVersion = 0;
    let activeRequest = null;
    const map = await createMap(root.querySelector("[data-preview-map]"), {
        payload: EMPTY_PAYLOAD,
    });

    async function refresh() {
        const version = ++requestVersion;
        activeRequest?.abort();
        activeRequest = new AbortController();
        if (!selected.length) {
            map.clearPayload();
            renderEmpty();
            return;
        }
        renderUpdating();
        try {
            const response = await fetchRequest("/reachability/preview", {
                method: "POST",
                headers: { "Content-Type": "application/json", Accept: "application/json" },
                body: JSON.stringify({ origins: selected }),
                signal: activeRequest.signal,
            });
            if (!response.ok) throw new Error(`Preview request failed: ${response.status}`);
            const payload = await response.json();
            if (version !== requestVersion) return;
            map.setPayload(payload);
            renderReady();
        } catch (error) {
            if (error?.name === "AbortError" || version !== requestVersion) return;
            map.clearPayload();
            renderFailure();
        }
    }

    return { addOrigin, removeOrigin, destroy };
}
```

All chip, option, status, and hidden-input content must be created with DOM nodes and `textContent`. Filter a maximum of 50 matching canonical stops, prioritize prefix matches, prevent duplicate requests, and attach a letter label to every chip/marker through participant order. Keep the prior field dimmed during updates and clear it only for empty or genuine failure states.

- [ ] **Step 4: Run the homepage controller and full JS suites**

```bash
npm run test:js
```

Expected: PASS.

- [ ] **Step 5: Commit the homepage controller**

```bash
git add static/home-preview.js tests/js/home-preview.test.js
git commit -m "Add additive homepage preview controller"
```

---

### Task 5: Responsive homepage UI and localized copy

**Files:**
- Modify: `routers/home.py`
- Modify: `backend/i18n.py`
- Modify: `templates/home.html`
- Modify: `templates/base.html`
- Modify: `static/app.css`
- Test: `tests/test_ui.py`

**Interfaces:**
- Consumes: `initHomePreview` from Task 4
- Produces: `[data-home-preview]` DOM contract and repeated hidden `preview_stops` fields inside the existing create form

- [ ] **Step 1: Write failing rendered-page tests**

Add assertions against the rendered behavior contract:

```python
@pytest.mark.asyncio
async def test_home_renders_sessionless_preview_before_the_single_create_form():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")

    page = BeautifulSoup(response.text, "html.parser")
    preview = page.select_one("[data-home-preview]")
    create_form = page.select_one('form[action="/session/create"][method="post"]')
    assert preview is not None
    assert preview.select_one("[data-preview-map]") is not None
    assert preview.select_one('[role="combobox"]') is not None
    assert preview.get("data-stops") == '["A", "B"]'
    assert page.select_one(".home-preview svg") is None
    main_elements = list(page.select_one("main").descendants)
    assert main_elements.index(preview) < main_elements.index(create_form)
    assert "Approximate · one way" in preview.get_text(" ", strip=True)
    assert "No selected date" in preview.get_text(" ", strip=True)
```

Add a Czech response test for all visible preview and plan-support labels, plus structure tests for the live region, semantic selected-origin list, legend including `no estimate`, handoff target, and homepage-only module script.

- [ ] **Step 2: Run the UI tests and verify RED**

```bash
.venv/bin/pytest -q tests/test_ui.py -k 'home'
```

Expected: failures because the homepage still contains the static SVG.

- [ ] **Step 3: Render the approved structure**

Pass `all_stops` from `routers/home.py`:

```python
return templates.TemplateResponse(
    request,
    "home.html",
    {
        "error": request.query_params.get("error"),
        "all_stops": getattr(request.app.state, "all_stops", []),
    },
)
```

Replace only the right-hand static illustration with the preview card. Its root carries safely serialized canonical stops and localized state strings in data attributes:

```html
<section class="home-estimate" data-home-preview
    data-stops='{{ all_stops | tojson }}'
    data-one-heading="{{ t('home.preview_one') }}"
    data-group-heading="{{ t('home.preview_group') }}">
    <header class="home-estimate__header">
        <span>{{ t("home.quick_estimate") }}</span>
        <span class="home-estimate__badge">{{ t("home.approximate_one_way") }}</span>
    </header>
    <label for="home-stop-search">{{ t("home.starting_stops") }}</label>
    <input id="home-stop-search" type="search" role="combobox"
        aria-autocomplete="list" aria-expanded="false"
        aria-controls="home-stop-options" data-preview-search>
    <ul id="home-stop-options" role="listbox" hidden data-preview-options></ul>
    <ul data-preview-selections></ul>
    <p aria-live="polite" data-preview-status></p>
    <h2 data-preview-heading>{{ t("home.preview_empty") }}</h2>
    <p data-preview-prompt></p>
    <div data-preview-map aria-hidden="true"></div>
    <!-- fixed semantic legend and permanent disclosure -->
</section>
```

Add a hidden-field container and carry-over status inside the existing create form. The controller appends one `<input type="hidden" name="preview_stops">` per selection. The handoff link targets and focuses `#session-name`.

Add all English and Czech translation keys to `backend/i18n.py`. Add a homepage-only script block in `home.html` rather than loading the controller on every page:

```html
{% block scripts %}
<script type="module" src="/static/home-preview.js?v=1"></script>
{% endblock %}
```

Update the CSS version in `base.html`. Replace the old SVG-specific rules with the approved flat framed card, map height, chips, combobox options, dimmed updating state, fixed legend, and responsive order. Preserve current fonts, tokens, ink borders, offset shadows, focus visibility, reduced motion, and both themes.

- [ ] **Step 4: Run UI, localization, and JavaScript tests**

```bash
.venv/bin/pytest -q tests/test_ui.py tests/test_routers.py -k 'home or language or create_session'
npm run test:js
```

Expected: PASS.

- [ ] **Step 5: Commit the integrated homepage**

```bash
git add routers/home.py backend/i18n.py templates/home.html templates/base.html static/app.css tests/test_ui.py
git commit -m "Replace homepage illustration with quick estimate"
```

---

### Task 6: Integration, resilience, and release verification

**Files:**
- Modify: `templates/how_it_works.html`
- Modify: `tests/test_integration.py`
- Modify: `tests/test_ui.py`
- Modify: `tests/test_reachability.py`

**Interfaces:**
- Verifies the complete homepage preview → session handoff boundary
- Does not introduce new production interfaces

- [ ] **Step 1: Write the end-to-end boundary test**

```python
@pytest.mark.asyncio
async def test_preview_then_create_session_preserves_starts_without_preview_persistence(monkeypatch):
    provider_calls = []
    monkeypatch.setattr(search_router, "scrape_route", lambda *args, **kwargs: provider_calls.append(args))
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        preview = await client.post("/reachability/preview", json={"origins": ["A", "C"]})
        async with app.state.db.execute("SELECT COUNT(*) FROM sessions") as cursor:
            assert (await cursor.fetchone())[0] == 0
        created = await client.post(
            "/session/create",
            data={"session_name": "Friday crew", "preview_stops": ["A", "C"]},
            follow_redirects=False,
        )

    assert preview.status_code == 200
    assert provider_calls == []
    code = created.headers["location"].removeprefix("/session/")
    participants = await get_participants(app.state.db, code)
    assert [(person["start_stop"], person["end_stop"]) for person in participants] == [
        ("A", ""),
        ("C", ""),
    ]
```

Add a rendered-page assertion that the preview disclosure links to the expanded `How it works` explanation, and update that page to describe the homepage preview separately from live refinement.

- [ ] **Step 2: Run the integration test and verify RED**

```bash
.venv/bin/pytest -q tests/test_integration.py -k 'preview_then_create'
```

Expected: failure until the complete route, form, and database path are connected.

- [ ] **Step 3: Complete resilience and documentation wiring**

Add a `Homepage quick estimate` section to `templates/how_it_works.html` and link the homepage disclosure to it. The section states: `The homepage quick estimate uses precomputed typical one-way transit times. It does not use a selected date, account for service changes, include a return trip, or call live DPP or Google services. Create a plan for date-specific journey queries and ranked meeting points.`

Run a local production-data measurement with one and six origins:

```bash
.venv/bin/python -c 'import json,time,polars as pl; from backend.preview import build_preview_participants; from backend.reachability import build_reachability_payload; d=pl.read_parquet("data/Prague_stops_combinations.parquet"); g=pl.read_parquet("data/Prague_stops_geo.parquet"); origins=d["from"].unique().sort().head(6).to_list();
for count in (1,6):
 start=time.perf_counter(); payload=build_reachability_payload(d,g,build_preview_participants(origins[:count]),"there-only"); elapsed=(time.perf_counter()-start)*1000; print(count,round(elapsed,1),len(json.dumps(payload,ensure_ascii=False)))'
```

Record the measured one-origin and six-origin latency and serialized byte count in the implementation commit message or handoff. If local six-origin latency exceeds 500 ms, profile the Polars joins before release rather than moving the matrix client-side.

- [ ] **Step 4: Run complete verification**

```bash
.venv/bin/pytest -q
npm run test:js
.venv/bin/ruff check backend routers tests
.venv/bin/ruff format --check backend routers tests
git diff --check
```

Expected: all commands exit 0. If the repository has documented pre-existing format failures outside touched lines, report them separately and do not bulk-format unrelated user work.

- [ ] **Step 5: Perform a browser accessibility and responsive smoke test**

Verify at 1366×768 and 390×844:

1. Add one stop using keyboard only and confirm immediate field plus prompt.
2. Add a second stop and confirm shared-reach copy.
3. Remove a stop and confirm all map/form state updates.
4. Confirm persistent approximation badge, disclosure, legend, and attribution.
5. Follow the handoff link and confirm focus lands on the plan name.
6. Submit and confirm `/session/{code}` loads with carried start stops and empty end stops.
7. Repeat in Czech, dark theme, and reduced-motion mode.
8. Simulate preview failure and confirm the plan form remains usable.

- [ ] **Step 6: Request final read-only code review and fix all Critical or Important findings**

Review the complete implementation against the spec, with explicit attention to privacy, stale responses, sparse matrix coverage, atomic session creation, accessibility, and unrelated working-tree files.

- [ ] **Step 7: Commit the verified integration**

```bash
git add templates/how_it_works.html tests/test_integration.py tests/test_ui.py tests/test_reachability.py
git commit -m "Verify homepage preview flow"
```
