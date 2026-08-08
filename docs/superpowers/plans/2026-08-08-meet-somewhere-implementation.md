# Meet Somewhere Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign Pub Finder as Meet Somewhere and add an approximate longest-journey reachability layer while preserving the FastAPI, Jinja, HTMX, SSE, Leaflet, search, autosave, and venue-cache architecture.

**Architecture:** FastAPI and Jinja continue to own pages and HTMX fragments. Focused no-build JavaScript modules own theme, session interactions, results state, recent sessions, and the Leaflet map. A read-only FastAPI endpoint derives reachability values from the saved-search participant snapshot and the already loaded Polars matrix; the browser renders those values as a classified Leaflet canvas overlay.

**Tech Stack:** Python 3.12, FastAPI, Jinja2, HTMX 2, HTMX SSE, Polars, aiosqlite, Leaflet 1.9, ES modules, Node built-in test runner, pytest, Ruff.

## Global Constraints

- Work directly on `main`, as requested by the repository owner.
- Preserve the latest-wins stop autosave and stop-picker target-resolution fixes.
- Preserve the current search ranking algorithm, DPP querying, Google Places cache duration, request concurrency, and rate limits.
- Keep HTMX and SSE as the server-rendered state mechanism. Do not add React, Vue, Svelte, or a build tool.
- Use Bricolage Grotesque for display and body copy and DM Mono for data and labels. Self-host WOFF2 assets under `static/fonts/`.
- Use the approved light and dark tokens from `docs/superpowers/specs/2026-08-08-meet-somewhere-ui-redesign.md`.
- Use factual, terse interface copy. Do not use em dashes anywhere.
- The initial heatmap supports longest journey for Everyone and one selected participant. Do not add average or fairness-gap layers.
- Heatmap values come from the saved-search participant snapshot, never from later session edits.
- Ranked results remain available if reachability data or venue lookup fails.
- Minimum touch target is 44 by 44 CSS pixels. Honour `prefers-reduced-motion` and WCAG AA contrast.
- Do not stage or modify `docs/superpowers/plans/2026-03-27-fastapi-migration.md`.

## File Structure

### Create

- `backend/reachability.py`: pure Polars reachability computation and payload construction.
- `routers/reachability.py`: saved-search reachability JSON endpoint.
- `static/theme-init.js`: blocking theme selection before first paint.
- `static/theme.js`: theme toggle interaction.
- `static/session.js`: invite copy, stop picker, autosave feedback, remove confirmation, and session controls.
- `static/results.js`: HTMX/SSE result initialization, selected result, venue updates, and mobile Map/List state.
- `static/reachability-core.js`: pure layer selection and time-band helpers.
- `static/reachability-map.js`: Leaflet map and canvas reachability overlay.
- `static/history.js`: recent-session local storage.
- `static/fonts/bricolage-grotesque-latin.woff2`: self-hosted variable font.
- `static/fonts/dm-mono-400-latin.woff2`: self-hosted regular font.
- `static/fonts/dm-mono-500-latin.woff2`: self-hosted medium font.
- `templates/partials/search_progress.html`: staged SSE progress component.
- `templates/partials/system_message.html`: local empty, warning, and error component.
- `tests/test_reachability.py`: pure and endpoint reachability tests.
- `tests/test_ui.py`: template, copy, shell, and accessibility-hook tests.
- `tests/js/reachability-core.test.js`: pure ES-module tests.
- `package.json`: Node test script and ES-module mode only, with no runtime packages.

### Modify

- `backend/app.py`: register reachability router, load any required metadata, update CSP, and replace script includes.
- `routers/home.py`: provide home and system-state context where required.
- `routers/search.py`: persist the saved-search participant snapshot and render the progress partial.
- `routers/session.py`: provide participant display colours and any explicit UI state required by templates.
- `templates/base.html`: Meet Somewhere shell, navigation, fonts, scripts, footer, and titles.
- `templates/home.html`: split hero, start form, join disclosure, and recent sessions.
- `templates/join.html`: direct invitation screen.
- `templates/session.html`: session shell, stop dialog, plan controls, remove confirmation, and search region.
- `templates/results.html`: shareable result shell and no-results state.
- `templates/how_it_works.html`: technical document layout and accurate dataset facts.
- `templates/feedback.html`: factual feedback context around the existing Google Form.
- `templates/partials/session_participants_inner.html`: participant workspace rows.
- `templates/partials/stop_form.html`: compact autosaving stop controls.
- `templates/partials/results_table.html`: results split view and map data hooks.
- `templates/partials/venue_suggestions.html`: all loaded, loading, empty, rate-limit, and provider-error states.
- `static/app.css`: complete approved light, dark, responsive, print-like component system.
- `static/favicon.svg`: Meet Somewhere transit-line mark.
- `tests/test_routers.py`: route, CSP, session, and copy assertions.
- `tests/test_integration.py`: progress, saved snapshot, result, and venue partial assertions.
- `README.md`: visible Meet Somewhere name and reachability explanation.

### Remove after replacements pass

- `static/app.js`: responsibilities move to focused ES modules.
- Oat CSS and JavaScript CDN references from `templates/base.html`.

---

### Task 1: Establish the Meet Somewhere shell and design tokens

**Files:**
- Create: `static/theme-init.js`
- Create: `static/theme.js`
- Create: `static/fonts/bricolage-grotesque-latin.woff2`
- Create: `static/fonts/dm-mono-400-latin.woff2`
- Create: `static/fonts/dm-mono-500-latin.woff2`
- Modify: `templates/base.html`
- Modify: `static/app.css`
- Modify: `static/favicon.svg`
- Modify: `backend/app.py`
- Test: `tests/test_ui.py`
- Test: `tests/test_routers.py`

**Interfaces:**
- Produces CSS tokens, `.site-nav`, `.brand`, `.brand-mark`, `.site-footer`, focus styles, and `data-theme` behaviour used by every later task.
- Produces `initThemeToggle(): void` from `static/theme.js`.
- Preserves `localStorage["pubfinder_theme"]` for existing users.

- [ ] **Step 1: Write failing shell and CSP tests**

```python
# tests/test_ui.py
import aiosqlite
import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport

from backend.app import app
from backend.db import create_session, init_db, save_search_results


@pytest_asyncio.fixture(autouse=True)
async def ui_app_state():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)
    app.state.db = db
    app.state.all_stops = ["A", "B"]
    yield
    await db.close()


@pytest.mark.asyncio
async def test_home_uses_meet_somewhere_shell():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")

    assert response.status_code == 200
    assert "Meet Somewhere" in response.text
    assert 'class="brand"' in response.text
    assert "/static/theme-init.js" in response.text
    assert "oat.min.css" not in response.text
    assert "oat.min.js" not in response.text
```

Add to `tests/test_routers.py`:

```python
@pytest.mark.asyncio
async def test_csp_allows_only_self_hosted_fonts_and_existing_map_assets():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")

    csp = response.headers["content-security-policy"]
    assert "font-src 'self'" in csp
    assert "fonts.googleapis.com" not in csp
    assert "https://unpkg.com" in csp
```

- [ ] **Step 2: Run the focused tests and confirm failure**

Run: `uv run pytest tests/test_ui.py tests/test_routers.py::test_csp_allows_only_self_hosted_fonts_and_existing_map_assets -q`

Expected: FAIL because the shell still says Pub Finder, includes Oat, and has no theme modules.

- [ ] **Step 3: Download and verify the three self-hosted font files**

```bash
font_tmp_dir="$(mktemp -d)"
curl -fsSL 'https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,400..800&family=DM+Mono:wght@400;500&display=swap' \
  -H 'User-Agent: Mozilla/5.0' \
  -o "$font_tmp_dir/fonts.css"
rg -o 'https://fonts.gstatic.com/[^)]+' "$font_tmp_dir/fonts.css"
```

Download the Latin WOFF2 URLs reported by that stylesheet into the exact `static/fonts/` paths above. Verify each with `file static/fonts/*.woff2`; every result must report Web Open Font Format.

- [ ] **Step 4: Replace the global shell and theme bootstrap**

Use this script contract:

```javascript
// static/theme-init.js
(function () {
  const saved = localStorage.getItem("pubfinder_theme");
  const theme = saved === "dark" || saved === "light"
    ? saved
    : (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  document.documentElement.dataset.theme = theme;
  document.documentElement.style.colorScheme = theme;
})();
```

```javascript
// static/theme.js
export function initThemeToggle() {
  const button = document.querySelector("[data-theme-toggle]");
  if (!button || button.dataset.bound === "true") return;
  button.dataset.bound = "true";
  button.addEventListener("click", () => {
    const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    document.documentElement.style.colorScheme = next;
    localStorage.setItem("pubfinder_theme", next);
    button.setAttribute("aria-label", next === "dark" ? "Use light theme" : "Use dark theme");
  });
}
```

Load `theme-init.js` in `<head>` without `defer`. Load later UI scripts as `type="module"` at the end of `body`. Remove Oat CSS and JS references. Keep HTMX, HTMX SSE, Leaflet, and their current allowed origins.

Define approved tokens using `:root` and `[data-theme="dark"]`. Add `@font-face` rules, a 44px control minimum for touch layouts, and the 3px yellow `:focus-visible` ring.

Because later tasks replace templates incrementally, add temporary compatibility rules for the existing `.row`, `.col-*`, `.card`, and `[data-field]` structures before removing Oat. Mark that CSS section `Legacy layout compatibility` and delete it in Task 10 after the final template migration. This keeps How It Works, Feedback, session, and results usable between commits.

- [ ] **Step 5: Run shell tests, Ruff, and the existing suite**

Run:

```bash
uv run pytest tests/test_ui.py tests/test_routers.py -q
uv run ruff check backend routers tests
uv run pytest -q
```

Expected: all commands exit 0.

- [ ] **Step 6: Commit the shell**

```bash
git add backend/app.py templates/base.html static/app.css static/favicon.svg \
  static/theme-init.js static/theme.js static/fonts tests/test_ui.py tests/test_routers.py
git commit -m "feat: add Meet Somewhere design system"
```

### Task 2: Redesign home, join, and recent sessions

**Files:**
- Create: `static/history.js`
- Modify: `templates/home.html`
- Modify: `templates/join.html`
- Modify: `static/app.css`
- Modify: `routers/home.py`
- Test: `tests/test_ui.py`
- Test: `tests/test_routers.py`

**Interfaces:**
- Consumes global tokens and shell from Task 1.
- Produces `rememberSession({code, name}): void` and `renderRecentSessions(): void`.
- Preserves POST `/session/create` and GET `/session/join` field names and redirects.

- [ ] **Step 1: Write failing home and invitation tests**

```python
@pytest.mark.asyncio
async def test_home_has_one_primary_start_form_and_secondary_join_path():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")

    assert "Pick a place that works for everyone." in response.text
    assert 'name="session_name"' in response.text
    assert 'name="creator_name"' in response.text
    assert 'data-join-disclosure' in response.text
    assert 'data-session-history' in response.text


@pytest.mark.asyncio
async def test_direct_invite_names_the_session_and_requires_only_name():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/join?code={session['code']}")

    assert "You’re invited to Friday crew." in response.text
    assert 'name="code"' in response.text
    assert 'name="name"' in response.text
    assert 'name="session_name"' not in response.text
```

- [ ] **Step 2: Run tests and confirm the old card layout fails them**

Run: `uv run pytest tests/test_ui.py -q`

Expected: FAIL on approved headline, disclosure hook, history hook, and invitation copy.

- [ ] **Step 3: Replace home and join templates**

Use this home hierarchy and preserve exact form fields:

```html
<section class="home-hero">
  <div class="home-hero__copy">
    <span class="sticker">Made for Prague</span>
    <h1>Pick a place that works for everyone.</h1>
    <p>Add people, choose a time, and rank meeting points using Prague public transport times.</p>
  </div>
  <div class="home-preview" aria-label="Three routes meeting at Náměstí Míru">
    <!-- inline SVG route preview, marked aria-hidden except for this container label -->
  </div>
</section>
<section class="start-plan">
  <form hx-post="/session/create" hx-target="body">
    <input name="session_name" required>
    <input name="creator_name" required>
    <button>Start planning</button>
  </form>
</section>
<details class="join-plan" data-join-disclosure>
  <summary>Join with a code</summary>
  <form action="/session/join" method="get">
    <input name="code" required>
    <input name="name" required>
    <button>Join plan</button>
  </form>
</details>
<section hidden data-session-history aria-labelledby="recent-plans-title"></section>
```

- [ ] **Step 4: Add bounded recent-session storage**

```javascript
// static/history.js
const KEY = "meet_somewhere_recent_sessions";

export function rememberSession(session) {
  const current = JSON.parse(localStorage.getItem(KEY) || "[]");
  const next = [session, ...current.filter(item => item.code !== session.code)].slice(0, 5);
  localStorage.setItem(KEY, JSON.stringify(next));
}

export function renderRecentSessions() {
  const root = document.querySelector("[data-session-history]");
  if (!root) return;
  const sessions = JSON.parse(localStorage.getItem(KEY) || "[]");
  if (!sessions.length) return;
  root.hidden = false;
  root.replaceChildren(...sessions.map(({code, name}) => {
    const link = document.createElement("a");
    link.href = `/session/${encodeURIComponent(code)}`;
    link.textContent = name;
    return link;
  }));
}
```

Pass session code and name through existing session page data attributes and call `rememberSession` there.

- [ ] **Step 5: Verify routes and responsive home layout**

Run:

```bash
uv run pytest tests/test_ui.py tests/test_routers.py -q
uv run pytest -q
```

At 390px width, verify the hero, both inputs, primary button, and join disclosure fit without horizontal scrolling.

- [ ] **Step 6: Commit entry screens**

```bash
git add routers/home.py templates/home.html templates/join.html static/app.css static/history.js tests
git commit -m "feat: redesign meetup entry flow"
```

### Task 3: Redesign the session workspace and stop dialogs

**Files:**
- Create: `static/session.js`
- Modify: `routers/session.py`
- Modify: `templates/session.html`
- Modify: `templates/partials/session_participants_inner.html`
- Modify: `templates/partials/stop_form.html`
- Modify: `static/app.css`
- Test: `tests/test_ui.py`
- Test: `tests/test_routers.py`

**Interfaces:**
- Consumes existing HTMX participant and stop endpoints unchanged.
- Produces idempotent `initSessionUi(): void`.
- Module-private bindings have exact signatures `bindInviteCopy(root: Element): void`, `bindStopPicker(root: Element): void`, `bindReturnCheckboxes(root: Element): void`, `bindRemoveConfirmation(root: Element): void`, and `bindOccasionPresets(root: Element): void`.
- Uses participant colour derived from stable participant ID modulo the approved palette.
- Preserves `[data-stop-input]`, `[data-same-start-end]`, participant ID fields, `hx-sync="this:replace"`, and SSE target IDs.

- [ ] **Step 1: Add failing session-markup tests**

```python
@pytest.mark.asyncio
async def test_session_workspace_exposes_autosave_and_dialog_hooks(setup_app):
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}")

    assert 'class="session-workspace"' in response.text
    assert 'data-stop-dialog' in response.text
    assert 'data-remove-dialog' in response.text
    assert 'aria-live="polite"' in response.text
    assert "Find somewhere" in response.text
```

- [ ] **Step 2: Run the focused test and confirm failure**

Run: `uv run pytest tests/test_ui.py::test_session_workspace_exposes_autosave_and_dialog_hooks -q`

Expected: FAIL on new workspace and dialog hooks.

- [ ] **Step 3: Recompose the session template without changing form contracts**

Use one `.session-workspace` containing `.participants-panel` and `.plan-panel`. Move each participant's start and end controls into its participant row. Keep the server partial root IDs exactly:

```html
<div id="session-participants" hx-ext="sse" ...>
  <div id="session-participants-inner">
    <section class="participants-panel">...</section>
  </div>
</div>
<section class="plan-panel">
  <form hx-post="/session/{{ session.code }}/search" ...>...</form>
</section>
```

Render participant state with explicit text:

```html
<span class="save-state htmx-indicator" aria-live="polite">saving</span>
{% if participant.start_stop %}<span class="save-state save-state--saved">saved</span>{% endif %}
```

Keep existing field names for dates, times, method, direction, and `place_types`. Occasion buttons update the four existing checkboxes through `session.js`; the checkboxes remain in the DOM as the submitted source of truth.

- [ ] **Step 4: Port stop-picker and participant controls into an idempotent module**

```javascript
// static/session.js
export function participantColor(id) {
  const palette = ["#ff6658", "#dff0ff", "#ffd447", "#4dc694", "#2458df", "#b9a8ff"];
  return palette[Math.abs(Number(id) || 0) % palette.length];
}

export function initSessionUi() {
  const root = document.querySelector("[data-session-workspace]");
  if (!root || root.dataset.bound === "true") return;
  root.dataset.bound = "true";
  bindInviteCopy(root);
  bindStopPicker(root);
  bindReturnCheckboxes(root);
  bindRemoveConfirmation(root);
  bindOccasionPresets(root);
}
```

Move the existing latest-target resolution logic into `bindStopPicker`. Keep `activeParticipantId` and `activeFieldName`, re-resolve the input after every HTMX swap, and dispatch one bubbling `change` event after a valid selection.

For removal, open one `<dialog data-remove-dialog>`, set the participant name and hidden ID, and submit the existing remove endpoint only after confirmation.

- [ ] **Step 5: Verify autosave, return toggling, stop selection, and SSE swaps**

Run:

```bash
uv run pytest tests/test_routers.py tests/test_integration.py -q
uv run pytest -q
```

Manually verify in two browser tabs that an SSE participant update does not close the active stop dialog, retarget the stop to another participant, or overwrite text input focus.

- [ ] **Step 6: Commit the session workspace**

```bash
git add routers/session.py templates/session.html templates/partials/session_participants_inner.html \
  templates/partials/stop_form.html static/session.js static/app.css tests
git commit -m "feat: redesign session workspace"
```

### Task 4: Persist the search snapshot and compute reachability

**Files:**
- Create: `backend/reachability.py`
- Create: `routers/reachability.py`
- Create: `tests/test_reachability.py`
- Modify: `backend/app.py`
- Modify: `routers/search.py`
- Modify: `tests/test_integration.py`

**Interfaces:**
- Produces `build_reachability_payload(distance_table, stop_geo, participants, direction) -> dict`.
- Produces GET `/session/{code}/reachability`.
- Saved result data gains `search_direction` and `participant_snapshot`.
- `participant_snapshot` entries contain `id`, `name`, `color`, `start_stop`, and `end_stop`.

- [ ] **Step 1: Write failing pure computation tests**

```python
# tests/test_reachability.py
import aiosqlite
import httpx
import polars as pl
import pytest
import pytest_asyncio
from httpx import ASGITransport

from backend.app import app
from backend.db import (
    add_participant_stops,
    create_session,
    get_participants,
    init_db,
    save_search_results,
)
from backend.reachability import build_reachability_payload


@pytest_asyncio.fixture(autouse=True)
async def reachability_app_state():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)
    app.state.db = db
    app.state.distance_table = matrix()
    app.state.stop_geo = geo()
    app.state.all_stops = ["A", "B", "C"]
    yield
    await db.close()


def matrix():
    return pl.DataFrame(
        {
            "from": ["A", "A", "B", "B", "C", "C"],
            "to":   ["A", "B", "A", "C", "B", "C"],
            "distance_in_km": [0, 1, 1, 1, 1, 0],
            "total_minutes": [0, 10, 11, 16, 15, 0],
        }
    )


def geo():
    return pl.DataFrame({"name": ["A", "B", "C"], "lat": [50.0, 50.1, 50.2], "lon": [14.0, 14.1, 14.2]})


def participants():
    return [
        {"id": 1, "name": "Daniel", "color": "#ff6658", "start_stop": "A", "end_stop": "A"},
        {"id": 2, "name": "Anna", "color": "#ffd447", "start_stop": "C", "end_stop": "C"},
    ]


def stop(payload, name):
    return next(item for item in payload["stops"] if item["name"] == name)


def test_there_only_uses_maximum_participant_time():
    payload = build_reachability_payload(matrix(), geo(), participants(), "there-only")
    assert stop(payload, "B")["participant_minutes"] == [10, 15]
    assert stop(payload, "B")["group_max_minutes"] == 15


def test_round_trip_sums_directional_pairs():
    payload = build_reachability_payload(matrix(), geo(), participants(), "round-trip")
    assert stop(payload, "B")["participant_minutes"] == [21, 31]
    assert stop(payload, "B")["group_max_minutes"] == 31


def test_missing_pair_marks_group_value_unavailable():
    payload = build_reachability_payload(matrix(), geo(), participants(), "back-only")
    assert stop(payload, "A")["group_max_minutes"] is None
```

- [ ] **Step 2: Run tests and confirm import failure**

Run: `uv run pytest tests/test_reachability.py -q`

Expected: FAIL because `backend.reachability` does not exist.

- [ ] **Step 3: Implement the pure Polars computation**

```python
# backend/reachability.py
from __future__ import annotations

import polars as pl


VALID_DIRECTIONS = {"there-only", "back-only", "round-trip"}


def _participant_frame(table: pl.DataFrame, participant: dict, index: int, direction: str) -> pl.DataFrame:
    there = (
        table.filter(pl.col("from") == participant["start_stop"])
        .select(pl.col("to").alias("name"), pl.col("total_minutes").alias("there"))
    )
    back = (
        table.filter(pl.col("to") == participant["end_stop"])
        .select(pl.col("from").alias("name"), pl.col("total_minutes").alias("back"))
    )
    joined = there.join(back, on="name", how="full", coalesce=True)
    expression = {
        "there-only": pl.col("there"),
        "back-only": pl.col("back"),
        "round-trip": pl.when(pl.col("there").is_not_null() & pl.col("back").is_not_null())
            .then(pl.col("there") + pl.col("back"))
            .otherwise(None),
    }[direction]
    return joined.select("name", expression.alias(f"participant_{index}"))
```

Join participant frames to `stop_geo`, produce ordered `participant_minutes`, and set `group_max_minutes` to `None` if any participant value is missing. Return `dataset: "precomputed typical transit times"` and coverage counts.

- [ ] **Step 4: Persist an immutable search snapshot**

In `_run_search`, save:

```python
"search_direction": direction,
"participant_snapshot": [
    {
        "id": p["id"],
        "name": p["name"],
        "color": participant_color(p["id"]),
        "start_stop": start,
        "end_stop": end,
    }
    for p, (start, end) in zip(active_participants, stop_pairs)
],
```

Define one Python `participant_color(participant_id: int) -> str` helper in `backend/reachability.py` using the same palette as `static/session.js`.

- [ ] **Step 5: Write and implement endpoint tests**

```python
@pytest.mark.asyncio
async def test_reachability_uses_saved_snapshot_not_later_session_edits():
    session = await create_session(app.state.db, "Test", "P1")
    participant = (await get_participants(app.state.db, session["code"]))[0]
    snapshot = [{"id": participant["id"], "name": "P1", "color": "#dff0ff", "start_stop": "A", "end_stop": "A"}]
    await save_search_results(app.state.db, session["code"], {
        "rows": [], "columns": [], "participant_snapshot": snapshot,
        "search_direction": "there-only"
    })
    await add_participant_stops(app.state.db, session["code"], participant["id"], "B", "B")

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/reachability")

    assert response.status_code == 200
    assert response.json()["participants"][0]["start_stop"] == "A"
```

The endpoint returns 404 for an invalid session or no saved results, and 422 for a saved snapshot with an invalid direction. Add `Cache-Control: private, max-age=300` and an ETag derived from session code, saved `created_at`, and direction.

- [ ] **Step 6: Run reachability and integration tests**

Run:

```bash
uv run pytest tests/test_reachability.py tests/test_integration.py -q
uv run ruff check backend routers tests
uv run pytest -q
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit reachability backend**

```bash
git add backend/reachability.py routers/reachability.py backend/app.py routers/search.py \
  tests/test_reachability.py tests/test_integration.py
git commit -m "feat: add saved-search reachability data"
```

### Task 5: Build the Leaflet reachability module with pure JS tests

**Files:**
- Create: `package.json`
- Create: `static/reachability-core.js`
- Create: `static/reachability-map.js`
- Create: `tests/js/reachability-core.test.js`
- Modify: `templates/base.html`
- Modify: `static/app.css`

**Interfaces:**
- Produces `selectLayerValues(payload, participantId): Array<number|null>`.
- Produces `classifyTime(value, threshold, step): number|null` where 0 is darkest and 3 is lightest.
- Produces `createReachabilityMap(root, options): ReachabilityMapController`.
- Controller methods: `setParticipant(id|null)`, `setThreshold(minutes)`, `setResults(stops)`, `setVenues(venues)`, and `destroy()`.

- [ ] **Step 1: Add failing Node tests**

```json
{
  "private": true,
  "type": "module",
  "scripts": {"test:js": "node --test tests/js/*.test.js"}
}
```

```javascript
// tests/js/reachability-core.test.js
import test from "node:test";
import assert from "node:assert/strict";
import { classifyTime, selectLayerValues } from "../../static/reachability-core.js";

const payload = {
  participants: [{id: 1}, {id: 2}],
  stops: [
    {group_max_minutes: 35, participant_minutes: [20, 35]},
    {group_max_minutes: null, participant_minutes: [40, null]},
  ],
};

test("Everyone uses group maximum values", () => {
  assert.deepEqual(selectLayerValues(payload, null), [35, null]);
});

test("participant view uses the matching array index", () => {
  assert.deepEqual(selectLayerValues(payload, 1), [20, 40]);
  assert.deepEqual(selectLayerValues(payload, 2), [35, null]);
});

test("time bands get lighter above the threshold", () => {
  assert.equal(classifyTime(20, 35, 15), 0);
  assert.equal(classifyTime(35, 35, 15), 0);
  assert.equal(classifyTime(50, 35, 15), 1);
  assert.equal(classifyTime(80, 35, 15), 3);
  assert.equal(classifyTime(null, 35, 15), null);
});
```

- [ ] **Step 2: Run Node tests and confirm module failure**

Run: `npm run test:js`

Expected: FAIL because `static/reachability-core.js` does not exist.

- [ ] **Step 3: Implement pure layer helpers**

```javascript
// static/reachability-core.js
export function selectLayerValues(payload, participantId) {
  if (participantId == null) return payload.stops.map(stop => stop.group_max_minutes);
  const index = payload.participants.findIndex(person => person.id === participantId);
  if (index < 0) throw new RangeError(`Unknown participant ${participantId}`);
  return payload.stops.map(stop => stop.participant_minutes[index] ?? null);
}

export function classifyTime(value, threshold, step = 15) {
  if (value == null || !Number.isFinite(value)) return null;
  return Math.max(0, Math.min(3, Math.ceil((value - threshold) / step)));
}
```

- [ ] **Step 4: Prototype and measure the canvas interpolation**

Implement a pure `interpolateGrid(points, width, height, power = 2)` that computes inverse-distance weighted values on a low-resolution grid. Use at most a 96 by 96 grid and the nearest eight observed stops per cell. Add a Node benchmark fixture with 1,444 synthetic stops and assert one grid computation completes below 150ms on the development machine. Record the measured value in the plan checkbox when executing.

If the nearest-eight search exceeds the budget, build a fixed screen-space bucket index before interpolation. Do not switch to a point-density heatmap, because stop density must not alter journey values.

- [ ] **Step 5: Implement the Leaflet controller**

```javascript
// static/reachability-map.js
import { classifyTime, selectLayerValues } from "./reachability-core.js";

export async function createReachabilityMap(root, options) {
  const response = await fetch(options.reachabilityUrl, {headers: {Accept: "application/json"}});
  if (!response.ok) throw new Error(`Reachability request failed: ${response.status}`);
  const payload = await response.json();
  const map = L.map(root).setView([50.0755, 14.4378], 12);
  const controller = new ReachabilityMapController(map, payload, options);
  controller.render();
  return controller;
}
```

The controller creates separate Leaflet layer groups for participants, ranked stops, venues, and the canvas field. Canvas pixels use four discrete lilac bands. Stop dots remain visible. Pan and zoom redraw through one `requestAnimationFrame` callback. `destroy()` removes listeners and the Leaflet instance so HTMX reinitialization cannot leak maps.

On fetch or render error, dispatch `reachability:error` on the map root and continue rendering markers.

- [ ] **Step 6: Run JS tests and verify the prototype budget**

Run:

```bash
npm run test:js
node --test tests/js/reachability-core.test.js
```

Expected: all tests pass and the benchmark is below 150ms.

- [ ] **Step 7: Commit map infrastructure**

```bash
git add package.json static/reachability-core.js static/reachability-map.js \
  static/app.css templates/base.html tests/js
git commit -m "feat: add reachability map renderer"
```

### Task 6: Redesign search progress with exact operational copy

**Files:**
- Create: `templates/partials/search_progress.html`
- Modify: `routers/search.py`
- Modify: `static/app.css`
- Modify: `tests/test_integration.py`

**Interfaces:**
- Consumes existing SearchRegistry fields `stage`, `current`, and `total`.
- Produces one progress fragment used for initial response and SSE updates.
- Preserves SSE event names `progress` and `complete`.

- [ ] **Step 1: Write failing progress-copy tests**

```python
def test_progress_copy_names_each_operation():
    assert "Select candidates from the transit matrix" in _render_progress_html(5, "candidates", 0, 0)
    scraping = _render_progress_html(42, "scraping", 14, 31)
    assert "Query DPP journey times" in scraping
    assert "14 of 31 candidate stops checked" in scraping
    venues = _render_progress_html(85, "pubs", 2, 5)
    assert "Query nearby places" in venues
    assert "2 of 5 stops checked" in venues
```

- [ ] **Step 2: Run the focused test and confirm signature failure**

Run: `uv run pytest tests/test_integration.py -k progress_copy -q`

Expected: FAIL because `_render_progress_html` still accepts only percentage and one label.

- [ ] **Step 3: Render all progress states from one Jinja partial**

Change the helper signature to:

```python
def _render_progress_html(
    percentage: int,
    stage: str,
    current: int,
    total: int,
) -> str:
    return templates.get_template("partials/search_progress.html").render(
        percentage=percentage,
        stage=stage,
        current=current,
        total=total,
    )
```

The partial contains the three fixed stage names, dynamic counts, semantic `<progress max="100" value="...">`, and one `aria-live="polite"` status sentence. Do not announce every percentage change.

- [ ] **Step 4: Run progress and full integration tests**

Run:

```bash
uv run pytest tests/test_integration.py -q
uv run pytest -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit progress UI**

```bash
git add routers/search.py templates/partials/search_progress.html static/app.css tests/test_integration.py
git commit -m "feat: redesign search progress"
```

### Task 7: Build the results workspace and venue states

**Files:**
- Create: `static/results.js`
- Modify: `templates/partials/results_table.html`
- Modify: `templates/partials/venue_suggestions.html`
- Modify: `templates/results.html`
- Modify: `static/app.css`
- Modify: `tests/test_ui.py`
- Modify: `tests/test_integration.py`
- Modify: `tests/test_places.py`

**Interfaces:**
- Consumes `createReachabilityMap` from Task 5.
- Produces idempotent `initResultsUi(target = document): void`.
- Module-private bindings have exact signatures `bindRankSelection(root: Element): void` and `bindMobileViews(root: Element): void`. Rank handlers read the module-level `controller` only when invoked, after asynchronous initialization has assigned it.
- Results root exposes `data-reachability-url`, selected rank, map stop JSON, venue JSON, and participant JSON.
- Venue HTMX out-of-band map data remains supported.

- [ ] **Step 1: Write failing result and venue-state tests**

```python
@pytest.mark.asyncio
def saved_result_fixture():
    return {
        "rows": [{
            "Target Stop": "B",
            "Worst Case Minutes": 15,
            "Total Minutes": 25,
            "To (Daniel)": 10,
            "From (Daniel)": 11,
            "Round trip (Daniel)": 21,
        }],
        "columns": [
            "Target Stop", "Worst Case Minutes", "Total Minutes",
            "To (Daniel)", "From (Daniel)", "Round trip (Daniel)",
        ],
        "pubs_by_stop": {"B": []},
        "pub_search_stop_names": ["B"],
        "place_types": ["pub", "bar"],
        "stops_geo": [{"name": "B", "lat": 50.1, "lon": 14.1}],
        "pubs_flat": [],
        "participants_geo": [],
        "participant_snapshot": [{
            "id": 1, "name": "Daniel", "color": "#ff6658",
            "start_stop": "A", "end_stop": "A",
        }],
        "search_direction": "round-trip",
        "warning": None,
    }


async def test_saved_results_render_split_workspace_and_reachability_url():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    await save_search_results(app.state.db, session["code"], saved_result_fixture())
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/results")

    assert 'class="results-workspace"' in response.text
    assert f'data-reachability-url="/session/{session["code"]}/reachability"' in response.text
    assert "Longest journey" in response.text
    assert "Approximate from typical transit times" in response.text


def test_rate_limited_venue_partial_preserves_transit_context():
    from routers.search import templates

    html = templates.get_template("partials/venue_suggestions.html").render(
        session_code="code",
        stop_name="B",
        pubs=[],
        searched=False,
        venue_error="rate-limited",
        map_update=False,
    )
    assert "Venue request limit reached" in html
    assert "Try again in one minute" in html
    assert "Transit results are unchanged" in html
```

- [ ] **Step 2: Run focused tests and confirm layout failure**

Run: `uv run pytest tests/test_ui.py tests/test_integration.py -k 'results or venue' -q`

Expected: FAIL on split workspace, reachability URL, and factual error copy.

- [ ] **Step 3: Replace result markup with a split workspace**

Use this root contract:

```html
<section class="results-workspace"
  data-results-root
  data-reachability-url="/session/{{ session_code }}/reachability">
  <aside class="results-rail">
    <div class="reach-controls" aria-label="Reachability layer">
      <div data-participant-tabs>...</div>
      <label>Longest journey <input type="range" data-threshold></label>
    </div>
    <div class="ranked-stops">...</div>
  </aside>
  <section class="results-map-panel">
    <div id="map" data-results-map></div>
    <p class="map-source">Approximate from typical transit times</p>
  </section>
</section>
```

Each rank button controls one adjacent detail region using `aria-expanded` and `aria-controls`. Render top result expanded on the server. Preserve all existing values and venue include calls.

- [ ] **Step 4: Implement idempotent result behaviour**

```javascript
// static/results.js
import { createReachabilityMap } from "./reachability-map.js";

let controller = null;

export async function initResultsUi(target = document) {
  const root = target.matches?.("[data-results-root]")
    ? target
    : target.querySelector?.("[data-results-root]");
  if (!root || root.dataset.bound === "true") return;
  root.dataset.bound = "true";
  bindRankSelection(root);
  bindMobileViews(root);
  try {
    controller?.destroy();
    controller = await createReachabilityMap(root.querySelector("[data-results-map]"), {
      reachabilityUrl: root.dataset.reachabilityUrl,
      stops: JSON.parse(root.querySelector("[data-map-data]").dataset.stops || "[]"),
      venues: JSON.parse(root.querySelector("[data-map-data]").dataset.venues || "[]"),
    });
  } catch (error) {
    root.querySelector("[data-reachability-error]").hidden = false;
  }
}
```

Call `initResultsUi` on DOM ready, `htmx:afterSwap`, and `htmx:sseMessage`. On rank selection, update `aria-expanded`, controller result emphasis, and mobile sheet content. On venue out-of-band updates, call `controller.setVenues` without rebuilding the map.

- [ ] **Step 5: Implement every venue state**

Map backend messages to fixed UI copy in the template context rather than matching arbitrary strings in JavaScript. The partial must render one of `loaded`, `loading`, `empty`, `not-searched`, `rate-limited`, or `provider-error`. Keep HTMX button disabling and the existing endpoint.

- [ ] **Step 6: Run Python and JS tests**

Run:

```bash
uv run pytest tests/test_ui.py tests/test_integration.py tests/test_places.py -q
npm run test:js
uv run pytest -q
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit results workspace**

```bash
git add templates/results.html templates/partials/results_table.html \
  templates/partials/venue_suggestions.html static/results.js static/app.css tests
git commit -m "feat: redesign results and venue discovery"
```

### Task 8: Redesign technical, feedback, and system pages

**Files:**
- Create: `templates/partials/system_message.html`
- Modify: `templates/how_it_works.html`
- Modify: `templates/feedback.html`
- Modify: `templates/results.html`
- Modify: `routers/home.py`
- Modify: `static/app.css`
- Modify: `tests/test_ui.py`
- Modify: `tests/test_routers.py`

**Interfaces:**
- Produces reusable system-message variants `empty`, `warning`, and `error` with one action.
- Preserves the existing Google Form URL and frame CSP allowance.

- [ ] **Step 1: Write failing documentation and system-state tests**

```python
@pytest.mark.asyncio
async def test_how_it_works_uses_verified_dataset_facts():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/how-it-works")

    assert "1,444" in response.text
    assert "2,083,035" in response.text
    assert "precomputed typical times" in response.text
    assert "live DPP" in response.text
    assert "1,463" not in response.text


@pytest.mark.asyncio
async def test_invalid_session_redirect_renders_specific_home_message():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test",
        follow_redirects=True,
    ) as client:
        response = await client.get("/session/not-valid/results")

    assert "This invite link is not valid." in response.text
    assert "Start a new plan" in response.text
```

- [ ] **Step 2: Run tests and confirm inaccurate old documentation**

Run: `uv run pytest tests/test_ui.py tests/test_routers.py -q`

Expected: FAIL on dataset facts and dedicated system messages.

- [ ] **Step 3: Implement technical-document and feedback layouts**

The How It Works page uses sections for Problem, Objective functions, Candidate selection, Live refinement, Venue search, Heatmap, and Limitations. Copy the exact formulas and facts from the approved specification.

Keep the Google Form iframe URL unchanged. Add only surrounding factual instructions:

```html
<section class="feedback-intro">
  <p>Include what you expected, what happened, your browser and device, and the session code if available.</p>
</section>
<div class="external-form-frame">{{ existing iframe unchanged }}</div>
```

- [ ] **Step 4: Render local system messages**

Pass `error=session_not_found` from the existing redirect into the home template and render:

```html
{% with kind="error", title="This invite link is not valid.",
  body="Check the link with the person who created the plan.",
  action_href="/", action_label="Start a new plan" %}
  {% include "partials/system_message.html" %}
{% endwith %}
```

Use the same partial for no saved results and reachability unavailability. Search and venue errors stay in their local result regions.

- [ ] **Step 5: Run route, UI, CSP, and full tests**

Run:

```bash
uv run pytest tests/test_ui.py tests/test_routers.py -q
uv run pytest -q
```

Expected: all tests pass and feedback CSP still allows only `https://docs.google.com` frames.

- [ ] **Step 6: Commit system pages**

```bash
git add templates/how_it_works.html templates/feedback.html templates/results.html \
  templates/partials/system_message.html routers/home.py static/app.css tests
git commit -m "feat: redesign technical and system pages"
```

### Task 9: Complete responsive, dark-theme, and accessibility behaviour

**Files:**
- Modify: `static/app.css`
- Modify: `static/session.js`
- Modify: `static/results.js`
- Modify: `templates/base.html`
- Modify: `templates/session.html`
- Modify: `templates/partials/results_table.html`
- Test: `tests/test_ui.py`
- Test: `tests/js/reachability-core.test.js`

**Interfaces:**
- Mobile result view values are `map` and `list` in `data-mobile-view`.
- Dialog close restores focus to the invoking control.
- Reduced motion disables route drawing, field fades, and sheet transitions.

- [ ] **Step 1: Add failing accessibility-hook tests**

```python
@pytest.mark.asyncio
async def test_session_dialogs_have_labels_and_live_regions():
    session = await create_session(app.state.db, "Test", "Daniel")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}")

    assert 'aria-labelledby="stop-picker-title"' in response.text
    assert 'aria-labelledby="remove-participant-title"' in response.text
    assert 'role="status"' in response.text
    assert 'aria-live="polite"' in response.text
```

- [ ] **Step 2: Run the focused accessibility tests**

Run: `uv run pytest tests/test_ui.py -k 'dialog or accessibility' -q`

Expected: FAIL until all hooks are present.

- [ ] **Step 3: Implement mobile composition and interaction**

At `max-width: 720px`:

- Home hero and form become one column.
- Session participants precede plan preferences.
- Stop picker becomes a bottom sheet with `max-height: min(80dvh, 680px)`.
- Results render Map and List toggle controls.
- Map view uses a bottom detail sheet.
- List view hides the map and shows all ranks.
- Participant chips use horizontal scrolling rather than wrapping into several rows.

Use `100dvh` fallbacks and account for `env(safe-area-inset-bottom)`.

- [ ] **Step 4: Implement focus restoration and reduced motion**

Store `dialog.returnValue` and the invoking element before `showModal()`. On `close`, call `invoker.focus()` only if the node is still connected. Add:

```css
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    scroll-behavior: auto !important;
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;
    transition-duration: 0.01ms !important;
  }
}
```

- [ ] **Step 5: Verify four viewports and both themes in the browser**

Capture screenshots at 1440x900, 834x1112, 390x844, and 360x800 for home, session, stop picker, search progress, results map, results list, How It Works, and one error state. Repeat home, session, and results in dark theme.

For each screenshot, confirm no horizontal overflow, clipped buttons, overlapping labels, illegible map controls, or light-theme-only surfaces.

- [ ] **Step 6: Run all automated checks and commit**

Run:

```bash
uv run ruff check backend routers tests
uv run pytest -q
npm run test:js
```

Expected: all commands exit 0.

```bash
git add static/app.css static/session.js static/results.js templates tests
git commit -m "fix: complete responsive and accessible UI"
```

### Task 10: Remove the legacy script, audit copy, update docs, and perform final QA

**Files:**
- Remove: `static/app.js`
- Modify: `templates/base.html`
- Modify: `README.md`
- Modify: all changed templates and scripts found by the audits below
- Test: full Python and JavaScript suites

**Interfaces:**
- Final app loads only `theme-init.js`, `theme.js`, `history.js`, `session.js`, and `results.js`, with `results.js` importing map modules.
- No user-visible `Pub Finder` brand or em dash remains.

- [ ] **Step 1: Run copy and legacy audits before deletion**

Run:

```bash
rg -nP "Pub Finder|pub finder|\\x{2014}|\\x{2013}|Checking the journeys that matter|Finding somewhere fair|Best balance|cooling down" \
  templates static README.md backend routers
rg -n "app\.js|oat\.min|onclick=|onchange=" templates backend
```

Expected before cleanup: only intentional historical or package references may remain. Every user-facing match must be changed. Inline event attributes must have zero matches because CSP blocks them.

- [ ] **Step 2: Remove `static/app.js` after confirming every responsibility moved**

Use this checklist against the old file before deletion:

- Theme toggle is in `theme.js`.
- Map creation and marker updates are in `reachability-map.js`.
- HTMX and SSE result lifecycle is in `results.js`.
- Invite copy, return checkbox, SSE form protection, and stop picker are in `session.js`.
- Recent session storage is in `history.js`.

Then delete `static/app.js` and remove its `<script>` tag.

- [ ] **Step 3: Update README with exact product and heatmap behaviour**

Replace the opening with:

```markdown
# Meet Somewhere

Meet Somewhere ranks Prague meeting points using public transport journey times for every person in a group. It can minimize the longest individual journey or total group travel time, then show nearby pubs, bars, cafes, and restaurants.

The optional reachability layer is derived from 2,083,035 precomputed directional stop pairs. It is approximate. Ranked results use live DPP queries for the selected departure and return times.
```

Keep the current setup, environment variable, cache, rate-limit, and data-preparation documentation accurate.

- [ ] **Step 4: Run complete automated verification**

Run:

```bash
uv run ruff check backend routers tests
uv run pytest -q
npm run test:js
git diff --check
```

Expected: all commands exit 0, with zero pytest or Node test failures.

- [ ] **Step 5: Run a local production-style server and browser smoke test**

Run:

```bash
uv run uvicorn backend.app:app --host 127.0.0.1 --port 8765
```

In a browser, complete this exact flow:

1. Create `Friday crew` as Daniel.
2. Add Matěj and Anna.
3. Select distinct valid starts and same-stop returns.
4. Verify each stop saves once and survives an SSE update.
5. Select Drinks, round trip, and minimize longest journey.
6. Run search and inspect every progress stage.
7. Open results, switch Everyone to each participant, and change the threshold.
8. Select multiple ranked stops and expand travel details.
9. Load venues on a lower-ranked stop.
10. Copy the share link and open it in a second tab.
11. Toggle dark theme and reload.
12. Repeat the stop picker and results Map/List flow at 390px width.

- [ ] **Step 6: Inspect console, network, and visual output**

Require:

- Zero uncaught JavaScript errors.
- Zero failed local asset requests.
- Reachability endpoint returns 200 and is not refetched during participant switches.
- Venue partial swaps update markers without recreating the Leaflet map.
- No duplicate HTMX handlers after repeated swaps.
- No horizontal overflow at the four required viewports.
- All dialogs restore focus.
- All route titles and visible brand references say Meet Somewhere.

- [ ] **Step 7: Commit final cleanup and push main**

```bash
git add README.md backend routers templates static package.json tests
git status --short
git commit -m "feat: complete Meet Somewhere redesign"
git push origin main
```

Before committing, confirm `.superpowers/` and `docs/superpowers/plans/2026-03-27-fastapi-migration.md` are not staged.
