# Pub Finder Audit Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix every blocker and secondary issue from the 2026-08-07 Pub Finder audit while preserving passwordless share-link collaboration.

**Architecture:** Keep the existing FastAPI, SQLite, HTMX/SSE, Leaflet, and Polars stack. Tighten session boundaries at the database layer, make candidate selection explicitly direction-aware, move background-search ownership into an application-scoped registry, and render map data through safe DOM APIs. Lock the Python environment and enforce the same test, lint, formatting, and audit gates in CI and Docker.

**Tech Stack:** Python 3.12, FastAPI, Starlette, aiosqlite, Polars, Jinja2, HTMX/SSE, Leaflet, pytest, Ruff, uv, GitHub Actions.

## Global Constraints

- Preserve the no-account, high-entropy share-link session model.
- Scope participant mutations by both session code and participant ID.
- Treat transit stop-pair data as directed for every candidate-selection stage.
- Never interpolate untrusted map data into HTML strings.
- Do not add Redis, Celery, accounts, roles, or unrelated visual redesigns.
- Every behavior change begins with a failing regression test and ends with a focused passing test.
- The final environment must install reproducibly from `uv.lock` on Python 3.12.

---

### Task 1: Enforce the Session Capability Boundary

**Files:**
- Modify: `backend/db.py:173-179`
- Modify: `routers/session.py:111-145`
- Modify: `tests/test_db.py`
- Modify: `tests/test_routers.py`

**Interfaces:**
- Consumes: `session_code: str`, `participant_id: int`, validated stop names.
- Produces: `add_participant_stops(db, session_code, participant_id, start_stop, end_stop) -> bool`.

- [ ] **Step 1: Write a database regression test that catches cross-session updates**

```python
@pytest.mark.asyncio
async def test_add_stops_cannot_update_participant_from_another_session(db):
    first = await create_session(db, "First", "Alice")
    second = await create_session(db, "Second", "Bob")
    alice = (await get_participants(db, first["code"]))[0]

    updated = await add_participant_stops(
        db,
        second["code"],
        alice["id"],
        start_stop="Anděl",
        end_stop="Florenc",
    )

    assert updated is False
    alice_after = (await get_participants(db, first["code"]))[0]
    assert alice_after["start_stop"] == ""
    assert alice_after["end_stop"] == ""
```

- [ ] **Step 2: Run the database test and verify RED**

Run: `uv run pytest tests/test_db.py::test_add_stops_cannot_update_participant_from_another_session -v`

Expected: FAIL because the current function has no `session_code` argument and updates solely by participant ID.

- [ ] **Step 3: Scope the update and return whether it matched**

```python
async def add_participant_stops(
    db: aiosqlite.Connection,
    session_code: str,
    participant_id: int,
    start_stop: str,
    end_stop: str,
) -> bool:
    same = 1 if start_stop == end_stop else 0
    result = await db.execute(
        "UPDATE participants SET start_stop = ?, end_stop = ?, same_start_end = ? "
        "WHERE id = ? AND session_code = ?",
        (start_stop, end_stop, same, participant_id, session_code),
    )
    await db.commit()
    return result.rowcount > 0
```

Update every caller and test helper to pass the session code.

- [ ] **Step 4: Add a route-level regression test**

Create two sessions through the real database, POST session B's URL with session A's participant ID, and assert the response reports `Participant not found in this session` while session A remains unchanged.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run: `uv run pytest tests/test_db.py tests/test_routers.py -v`

Expected: PASS with no cross-session mutation.

- [ ] **Step 6: Commit the capability-boundary fix**

```bash
git add backend/db.py routers/session.py tests/test_db.py tests/test_routers.py tests/test_integration.py
git commit -m "fix: scope participant updates to sessions"
```

### Task 2: Correct Directed Candidate Selection

**Files:**
- Modify: `backend/optimization.py:12-116,187-206`
- Modify: `tests/test_optimization.py`

**Interfaces:**
- Consumes: the directed `from`, `to`, `distance_in_km`, and `total_minutes` columns.
- Produces: `get_geo_optimal_stop(..., reverse: bool = False)`, `get_time_optimal_stop(..., reverse: bool = False)`, and direction-correct `get_optimal_stop_pairs(...)`.

- [ ] **Step 1: Write asymmetric return-only and round-trip regression tests**

```python
def make_asymmetric_distance_table():
    return pl.DataFrame({
        "from": ["A", "A", "B", "B", "X", "Y"],
        "to": ["X", "Y", "X", "Y", "B", "B"],
        "distance_in_km": [1, 1, 1, 1, 1, 1],
        "total_minutes": [10, 20, 100, 1, 1, 100],
    })


def test_back_only_candidates_follow_target_to_end_direction():
    result = get_optimal_stop_pairs(
        make_asymmetric_distance_table(),
        "minimize-total",
        [("A", "B")],
        show_top_geo=0,
        show_top_time=1,
        direction="back-only",
    )
    assert result == ["X"]


def test_round_trip_candidates_include_best_directed_return_stop():
    result = get_optimal_stop_pairs(
        make_asymmetric_distance_table(),
        "minimize-total",
        [("A", "B")],
        show_top_geo=0,
        show_top_time=1,
        direction="round-trip",
    )
    assert "X" in result
```

- [ ] **Step 2: Run the new tests and verify RED**

Run: `uv run pytest tests/test_optimization.py -k 'back_only or round_trip_candidates' -v`

Expected: the return-only test selects `Y`, proving the current code traverses `B -> target` instead of `target -> B`.

- [ ] **Step 3: Add a direction-aware internal selector**

Refactor the duplicated geographic/time selectors around this column choice:

```python
filter_column = "to" if reverse else "from"
candidate_column = "from" if reverse else "to"

df = (
    distance_table.filter(pl.col(filter_column) == stop)
    .with_columns(
        pl.col(candidate_column).alias("target_stop"),
        pl.col(value_column).alias(f"{metric_prefix}_{index}"),
    )
    .select("target_stop", f"{metric_prefix}_{index}")
)
```

The public geographic/time functions keep their existing parameters and add `reverse: bool = False`.

- [ ] **Step 4: Build candidates per leg**

```python
if direction in {"there-only", "round-trip"}:
    candidates.extend(select_candidates(start_stops, reverse=False))
if direction in {"back-only", "round-trip"}:
    candidates.extend(select_candidates(end_stops, reverse=True))
return list(dict.fromkeys(candidates))
```

- [ ] **Step 5: Run optimization tests and verify GREEN**

Run: `uv run pytest tests/test_optimization.py -v`

Expected: all directed and unreachable-stop tests pass.

- [ ] **Step 6: Commit the direction fix**

```bash
git add backend/optimization.py tests/test_optimization.py
git commit -m "fix: preserve direction in candidate selection"
```

### Task 3: Make Template Rendering Compatible With Current Dependencies

**Files:**
- Modify: `routers/search.py:84-164,361-391`
- Modify: `tests/test_integration.py`

**Interfaces:**
- Consumes: current Starlette `Jinja2Templates.TemplateResponse(request, name, context)`.
- Produces: successful error, rate-limit, empty-result, and populated-result responses under the locked dependency set.

- [ ] **Step 1: Establish the failing integration cases in a fresh current environment**

Keep and tighten the existing tests for fewer than two participants, the fourth rate-limited search, a result page without results, and a populated result page. Assert status `200` and the expected rendered message/content for each path.

- [ ] **Step 2: Run the four tests and verify RED**

Run: `uv run pytest tests/test_integration.py -k 'requires_two or rate_limiting or results_page' -v`

Expected: FAIL with `TypeError: unhashable type: 'dict'` from the legacy positional `TemplateResponse` calls.

- [ ] **Step 3: Convert every search-router response to request-first form**

```python
return templates.TemplateResponse(
    request,
    "partials/results_table.html",
    {"error": message, "results": None},
)
```

Apply the same signature to both `results.html` response branches. Do not place `request` redundantly inside the context.

- [ ] **Step 4: Run focused integration tests and verify GREEN**

Run: `uv run pytest tests/test_integration.py -k 'requires_two or rate_limiting or results_page' -v`

Expected: all four paths return `200` with their rendered content.

- [ ] **Step 5: Commit the compatibility fix**

```bash
git add routers/search.py tests/test_integration.py
git commit -m "fix: use current template response API"
```

### Task 4: Own the Background Search Lifecycle

**Files:**
- Create: `backend/search_registry.py`
- Modify: `backend/app.py:16-46`
- Modify: `routers/search.py:23-41,149-358`
- Modify: `tests/test_integration.py`
- Create: `tests/test_search_registry.py`

**Interfaces:**
- Produces: `SearchRegistry(result_ttl_seconds: float = 900.0)` with `create`, `update`, `get`, `pop`, `prune`, `start`, `wait_all`, and `shutdown` methods.
- Stores: `SearchProgress(session_code, stage, current, total, done, result_html, updated_at)`.
- Application state: `app.state.search_registry`.

- [ ] **Step 1: Write registry lifecycle tests**

```python
@pytest.mark.asyncio
async def test_registry_tracks_and_releases_completed_task():
    registry = SearchRegistry()
    registry.create("search-1", "session-1")
    release = asyncio.Event()

    async def work():
        await release.wait()

    task = registry.start("search-1", work())
    assert registry.task_count == 1
    release.set()
    await task
    await asyncio.sleep(0)
    assert registry.task_count == 0


def test_registry_prunes_only_expired_completed_results():
    clock = FakeClock(100.0)
    registry = SearchRegistry(result_ttl_seconds=30, clock=clock)
    registry.create("search-1", "session-1")
    registry.update("search-1", done=True, result_html="done")
    clock.advance(31)
    assert registry.prune() == 1
    assert registry.get("search-1", "session-1") is None


@pytest.mark.asyncio
async def test_registry_shutdown_cancels_outstanding_tasks():
    registry = SearchRegistry()
    registry.create("search-1", "session-1")
    task = registry.start("search-1", asyncio.Event().wait())
    await registry.shutdown()
    assert task.cancelled()
```

- [ ] **Step 2: Run registry tests and verify RED**

Run: `uv run pytest tests/test_search_registry.py -v`

Expected: import failure because `backend.search_registry` does not exist.

- [ ] **Step 3: Implement the registry with a thread-safe progress lock**

Use a `threading.Lock` for progress because the live-timetable progress callback executes inside `asyncio.to_thread`. Store task handles only from the event-loop thread. `get(search_id, session_code)` must return `None` when the stored capability does not match.

- [ ] **Step 4: Connect the registry to application lifespan**

```python
registry = SearchRegistry()
app.state.search_registry = registry

yield

await registry.shutdown()
await db.close()
```

Ensure task cancellation happens before SQLite closes.

- [ ] **Step 5: Replace module globals and detached task creation**

The search route calls `registry.prune()`, `registry.create(search_id, code)`, and `registry.start(search_id, _run_search(...))`. `_run_search`, its thread callback, and the SSE route read/update the registry. The SSE route pops the result after delivery and rejects a search ID belonging to another session.

- [ ] **Step 6: Make integration fixtures await work before database teardown**

Create a fresh registry in the autouse fixture, keep external patches active until each extracted search ID reaches `done`, and call `await registry.shutdown()` before `await db.close()`. In the rate-limit test, collect and await the first three search IDs before leaving the patch context.

- [ ] **Step 7: Run lifecycle and integration tests and verify GREEN**

Run: `uv run pytest tests/test_search_registry.py tests/test_integration.py -v`

Expected: no DPP/Places network retries, no closed-database errors, and all tasks removed or cancelled before teardown.

- [ ] **Step 8: Commit lifecycle ownership**

```bash
git add backend/search_registry.py backend/app.py routers/search.py tests/test_search_registry.py tests/test_integration.py
git commit -m "fix: manage background search lifecycle"
```

### Task 5: Harden Map Rendering and Repair Results/Feedback UX

**Files:**
- Modify: `static/app.js:45-147`
- Modify: `templates/partials/results_table.html:21-32`
- Modify: `templates/session.html:6-23,114-123`
- Modify: `templates/partials/stop_form.html:1-20`
- Modify: `backend/app.py:49-64`
- Modify: `tests/test_routers.py`

**Interfaces:**
- Produces: `createPopupContent(title, detailLines, link)` returning a DOM node containing text-only untrusted fields.
- Produces: page-load and HTMX map initialization through `initMap()`.
- Produces: CSP with `script-src-attr 'none'` and `frame-src https://docs.google.com`.

- [ ] **Step 1: Write route/header regression tests**

```python
@pytest.mark.asyncio
async def test_feedback_csp_allows_google_form_and_blocks_inline_handlers(client):
    response = await client.get("/feedback")
    policy = response.headers["content-security-policy"]
    assert "frame-src https://docs.google.com" in policy
    assert "script-src-attr 'none'" in policy


@pytest.mark.asyncio
async def test_session_page_does_not_emit_inline_event_handlers(client):
    response = await client.get(session_url)
    assert " onclick=" not in response.text
    assert " onchange=" not in response.text
```

- [ ] **Step 2: Run the header/template tests and verify RED**

Run: `uv run pytest tests/test_routers.py -k 'feedback_csp or inline_event' -v`

Expected: CSP directives are absent and session HTML still contains inline handlers.

- [ ] **Step 3: Build popup DOM safely**

```javascript
function createPopupContent(title, detailLines, link) {
    const root = document.createElement("div");
    const heading = document.createElement("strong");
    heading.textContent = String(title || "");
    root.appendChild(heading);

    detailLines.forEach(function (line) {
        root.appendChild(document.createElement("br"));
        root.appendChild(document.createTextNode(String(line)));
    });

    if (link) {
        try {
            const parsed = new URL(link, window.location.origin);
            if (parsed.protocol === "https:") {
                const anchor = document.createElement("a");
                anchor.href = parsed.href;
                anchor.target = "_blank";
                anchor.rel = "noopener noreferrer";
                anchor.textContent = "Google Maps";
                root.appendChild(document.createElement("br"));
                root.appendChild(anchor);
            }
        } catch (_) {
            // Invalid external URLs are omitted.
        }
    }
    return root;
}
```

Use this node for stop, pub, and participant `bindPopup` calls. Keep icon HTML static.

- [ ] **Step 4: Remove inline event handlers and scripts that race `app.js`**

Give the invite button a stable ID and `data-invite-url`, then bind its click listener in `app.js`. Bind return-checkbox changes through delegated JavaScript. Remove the results-partial `initMap()` script. Call `initMap()` once at the end of `app.js` and retain the existing HTMX after-swap call.

- [ ] **Step 5: Update CSP**

Append `script-src-attr 'none'; frame-src https://docs.google.com;` while keeping `frame-ancestors 'none'` and the existing external asset origins.

- [ ] **Step 6: Run focused tests and verify GREEN**

Run: `uv run pytest tests/test_routers.py -v`

Expected: Feedback is embeddable only from Google Forms, inline event attributes are absent, and existing pages still render.

- [ ] **Step 7: Verify both browser regressions locally**

Start with a temporary database and controlled saved result data. Confirm a direct `/session/{code}/results` load creates Leaflet markers. Open a participant marker whose name is `<img src=x onerror=document.body.dataset.xss=1>` and assert `document.body.dataset.xss` remains unset while the literal payload is displayed as text.

- [ ] **Step 8: Commit frontend/security hardening**

```bash
git add static/app.js templates/partials/results_table.html templates/session.html templates/partials/stop_form.html backend/app.py tests/test_routers.py
git commit -m "fix: render map popups safely"
```

### Task 6: Lock Dependencies, Clean Quality Gates, and Add CI

**Files:**
- Modify: `pyproject.toml`
- Create: `uv.lock`
- Modify: `Dockerfile`
- Create: `.github/workflows/ci.yml`
- Modify: `.gitignore`
- Modify: Python files reported by Ruff formatting/checks.

**Interfaces:**
- Produces: `uv sync --locked --extra dev` as the canonical development/CI install.
- Produces: a Docker environment installed via `uv sync --locked --no-dev --no-install-project`.

- [ ] **Step 1: Add explicit development tools and Ruff configuration**

```toml
[project.optional-dependencies]
dev = [
    "pip-audit>=2.9",
    "pytest>=8.3",
    "pytest-asyncio>=0.25",
    "ruff>=0.12",
]

[tool.ruff]
target-version = "py312"
line-length = 100

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

- [ ] **Step 2: Generate and verify the lock**

Run: `uv lock --upgrade`

Run: `uv sync --locked --extra dev`

Expected: both commands exit successfully and install the same current FastAPI/Starlette API used by Task 3.

- [ ] **Step 3: Resolve Ruff findings and format all tracked Python**

Remove the unused `app` import in `backend/__main__.py`, unused progress and test variables/imports, and mark MD5 as non-security use with `hashlib.md5(..., usedforsecurity=False)`. Run `uv run ruff format .`, then `uv run ruff check --fix .`; inspect every changed file to ensure changes are mechanical or part of the planned fixes.

- [ ] **Step 4: Install locked runtime dependencies in Docker**

```dockerfile
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv \
    && uv sync --locked --no-dev --no-install-project
COPY . .
CMD ["/app/.venv/bin/python", "-m", "backend"]
```

Run the module without reload in `backend/__main__.py` so the production container owns one process.

- [ ] **Step 5: Add GitHub Actions**

The workflow checks out the repository, installs uv and Python 3.12, runs `uv sync --locked --extra dev`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run pytest -q`, and `uv run pip-audit`.

- [ ] **Step 6: Add local-artifact ignore patterns**

Ignore `pub_finder.db`, `.playwright-mcp/`, and root-level PNG browser captures so future local verification does not leave a noisy worktree. Do not delete or move the user's existing artifacts in the main checkout.

- [ ] **Step 7: Run all quality gates**

Run:

```bash
uv sync --locked --extra dev
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
uv run pip-audit
uv run python -m compileall -q backend routers data_preparation tests
```

Expected: zero test failures, zero lint/format errors, zero known dependency vulnerabilities, and successful compilation.

- [ ] **Step 8: Commit reproducibility and CI**

```bash
git add pyproject.toml uv.lock Dockerfile .github/workflows/ci.yml .gitignore backend routers data_preparation tests
git commit -m "ci: lock and verify application dependencies"
```

### Task 7: Final Runtime, Security, and Requirement Audit

**Files:**
- Verify all modified files.
- Update: `README.md` only if canonical setup commands changed from `pip install` to `uv sync`.

**Interfaces:**
- Consumes: every requirement in `docs/superpowers/specs/2026-08-08-audit-remediation-design.md`.
- Produces: evidence that each blocker and secondary issue is fixed in the final branch.

- [ ] **Step 1: Re-run the complete locked verification suite from a clean environment**

Create a fresh temporary uv environment or remove only the isolated worktree's `.venv`, run `uv sync --locked --extra dev`, and execute the full quality-gate command set from Task 6.

- [ ] **Step 2: Run focused security/correctness regressions**

Run the cross-session route test, asymmetric direction tests, search-registry lifecycle tests, TemplateResponse integration tests, and CSP tests explicitly with `-v` so each requirement has named evidence.

- [ ] **Step 3: Run the application with an isolated temporary database**

Set `DATABASE_PATH` to a freshly created temporary path and `GOOGLE_PLACES_API_KEY` to empty. Start on `127.0.0.1` at an unused port, confirm `/`, `/feedback`, and a controlled results URL return `200`, then stop the process cleanly and verify no child/reloader process remains.

- [ ] **Step 4: Repeat real-browser verification**

At desktop and mobile viewports, verify the home/session/result surfaces, direct results-map initialization, the Feedback iframe, invite copying, return-checkbox behavior, and safe popup rendering. Confirm the browser console has no CSP or JavaScript errors attributable to the application.

- [ ] **Step 5: Run final diff and secret checks**

Run:

```bash
git diff main...HEAD --check
git status --short --branch
git diff --stat main...HEAD
git grep -nE '(AIza[0-9A-Za-z_-]{30,}|BEGIN (RSA|OPENSSH|EC) PRIVATE KEY)' -- ':!uv.lock'
```

Expected: no whitespace errors, no secrets, and only planned source, test, lock, workflow, documentation, and configuration files.

- [ ] **Step 6: Request code review**

Use `superpowers:requesting-code-review`, address every substantive finding, and rerun affected verification commands.

- [ ] **Step 7: Commit final documentation adjustments**

```bash
git add README.md docs/superpowers
git commit -m "docs: document reproducible development workflow"
```

Skip this commit when README and plan tracking require no final changes.
