# Sessionless Homepage Preview Final Fix Report

Date: 2026-08-22

Baseline: `5907f9b`

Scope: the six findings from the final cross-task review of
`review-f0cf4b0..5907f9b.diff`.

## Outcome

The homepage preview now has a complete stateless display boundary, a bounded
public request body, current-selection failure recovery, theme-aware map
redrawing, and localized EN/CS method copy. Feature-owned Python files are Ruff
clean and formatted. The full Python and JavaScript suites pass.

## RED evidence

### 1. Homepage privacy boundary

- A fresh `GET /` set `_uid`, scheduled analytics, and created an
  `analytics_users` row.
- Loading `static/history.js` with no legacy state called
  `localStorage.setItem` with an empty history value.
- The former analytics cookie test explicitly required a cookie from `GET /`,
  contradicting the design privacy boundary.
- New integration coverage failed until a full `GET /` followed by
  `POST /reachability/preview` produced no cookie, analytics event, provider
  call, analytics row, session, or participant.

### 2. Preview request bounds

Before the ASGI guard, the new boundary tests observed the following:

- an oversized claimed `Content-Length` reached the route and returned `200`;
- an oversized streamed body was buffered and returned framework validation
  instead of `413`;
- a 201-character allowlisted origin returned `200`;
- malformed JSON under the proposed cap already retained FastAPI's `422`
  detail-list contract.

Three of the four focused tests were RED.

### 3. Failure recovery

- After A succeeded and adding B failed, the renderer retained only marker A.
- After A+B succeeded and removing A failed, the renderer retained stale A and
  B markers instead of relabeling B to A.
- The heading likewise retained the last successful selection rather than the
  current chip order.

The focused tests captured both marker diffs before the controller change.

### 4. Feature-owned style

The baseline checks reported:

- `backend/preview.py`: Ruff `I001` import ordering;
- `backend/db.py`: Ruff formatting;
- `tests/test_db.py`: Ruff formatting;
- `routers/search.py:90`: Ruff formatting, pre-existing and outside this
  feature-owned fix wave.

### 5. Theme redraw

- A ready map registered no theme listener (`0` instead of `1`).
- Clicking the theme toggle emitted no stable theme-change event (`0` instead
  of `1`).
- Consequently a ready canvas kept colors read before the toggle until another
  unrelated map redraw.

### 6. Czech method-note copy

The Czech `/how-it-works` response rendered `Homepage quick estimate` in both
the table of contents and the new explanatory section.

## GREEN changes and decisions

### Privacy

- `AnalyticsMiddleware` returns directly for only `GET`/`HEAD /` and exact
  `POST /reachability/preview`. Session pages, other public pages, and tool
  routes retain existing analytics behavior.
- History migration first reads and validates the legacy store. With no actual
  valid legacy sessions it performs no `setItem` or `removeItem`; existing
  current history is still rendered without being rewritten.
- Valid legacy state is still normalized, deduplicated, bounded, merged after
  current history, and removed only after a successful write.
- A preview-controller storage tripwire covers add, successful render, and
  removal without any browser-storage write.

### Request limits

- `MAX_PREVIEW_BODY_BYTES` documents a 2,048-byte public preview limit.
- A route-specific raw ASGI middleware checks an oversized `Content-Length`
  and independently accumulates every received chunk. It never trusts the
  header as the only bound.
- Oversized requests return JSON `413` with `Cache-Control: no-store` before
  the preview rate limiter, Pydantic buffering/parsing, payload calculation,
  analytics identity creation, or database activity.
- Individual origin strings are capped at 200 characters in both the Pydantic
  request model and domain normalization.
- Under-cap malformed JSON and ordinary validation failures preserve the
  existing FastAPI `422` detail-list response.

### Failure recovery

- Coordinates are remembered only from a payload that passed the strict shared
  reachability validator and matched the exact requested participant order.
- Following a later failure, the controller creates a new validator-checked,
  marker-only payload from the current selected origins and those trusted
  coordinates, applies current A-F labels, then clears only field values.
- Synthesis runs only after at least one payload has validated. A malformed
  first response still never reaches the renderer.
- Ready, coverage, and failure states share current-selection heading logic, so
  status, heading, chips, and marker order cannot diverge.

### Theme lifecycle

- The theme toggle emits one `themechange` event with the selected theme after
  the DOM theme variables change.
- Every reachability controller, whether payload-backed (homepage) or
  URL-backed (results), schedules its existing coalesced redraw on that event.
- `destroy()` removes the listener and remains idempotent.
- Cache versions were bumped for the theme, history, map, homepage-preview, and
  results module dependency graph.

### Localization

- Only the branch-added homepage quick-estimate title/body were moved to EN/CS
  i18n keys.
- Unrelated pre-existing English content on the technical page was deliberately
  left unchanged.

### Formatting

- Ruff import ordering is fixed in `backend/preview.py`.
- Ruff formatted the requested feature-owned `backend/db.py` and
  `tests/test_db.py`, along with the newly changed Python middleware.
- `routers/search.py` was not modified.

## Verification

### Focused

- Privacy/router/integration/history focused tests: green.
- Body-boundary focused tests: `4 passed`.
- Failure-recovery focused tests: `3 passed`.
- Theme map/toggle focused tests: green.
- EN/CS method-note focused tests: `2 passed`.
- Combined feature Python selection: `190 passed, 4 skipped`.
- Combined feature JavaScript selection: green after narrowing trusted recovery
  to post-validation failures.

### Full repository

- `source .venv/bin/activate && .venv/bin/pytest -q`:
  `262 passed, 4 skipped`.
- `npm run test:js`: `77 passed`.
- `source .venv/bin/activate && ruff check .`: `All checks passed!`.
- `git diff --check`: passed.
- `source .venv/bin/activate && ruff format --check .`: feature-owned files
  pass; the command remains non-zero only for the unchanged, pre-existing
  `routers/search.py:90` single-line signature formatting difference
  (`39 files already formatted`).

### Browser smoke

The app was started on a local-only port with a temporary SQLite database.

- Desktop: selected Anděl, received a ready field, current A marker, Czech
  heading/status, and preserved plan handoff.
- Theme: after ready, toggling light to dark changed `--mint` from `#4DC694` to
  `#6FD5A7`; the controller redraw behavior is covered directly by the map test.
- Mobile: at 390 × 844 there was no horizontal overflow (`clientWidth` and
  `scrollWidth` both `390`), and the ready preview remained accessible.
- Add failure: after the local server was stopped, adding Dejvická showed chips
  and tooltips A/B, the current two-origin heading, an empty prompt, and the
  failure status.
- Removal failure: removing Anděl while still offline relabeled Dejvická to A
  and changed to the one-origin heading without restoring stale heat data.
- Czech method note: the localized heading was present exactly once and the
  English added heading was absent.

The browser workflow did not inspect private cookie or storage state. Those
privacy properties are covered by ASGI integration tests and JavaScript
write-tripwire tests. The local smoke process inherited the repository's
configured analytics environment; visiting the intentionally non-exempt
`/how-it-works` page emitted one normal GA request. The homepage and preview
paths themselves remained exempt, as verified by the integration suite.

## Security review

- The body limit is enforced before framework JSON buffering and does not rely
  on `Content-Length` alone.
- Stop inputs remain strict strings, length-bounded, canonical-allowlisted,
  unique, and count-bounded.
- No selected-origin list is added to logs.
- Synthetic marker data is constructed only from current canonical selections
  and coordinates from an already validated response.
- All visible stop and localization strings continue through Jinja escaping or
  DOM `textContent`; no `innerHTML` path was introduced.
- No secret, credential, capability URL, or new identifier is stored or logged.

## Remaining concern

There is no feature blocker. Repository-wide `ruff format --check .` remains
non-zero solely because `routers/search.py:90` was already unformatted before
this fix wave and was explicitly excluded from modification.
