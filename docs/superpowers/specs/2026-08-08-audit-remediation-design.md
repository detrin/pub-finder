# Pub Finder Audit Remediation Design

## Objective

Bring Pub Finder from a polished prototype to a reproducible, defensible public release by fixing every blocker and secondary problem identified in the 2026-08-07 audit without changing the product's no-account, share-link collaboration model.

## Scope

This remediation covers:

1. Cross-session participant mutation.
2. Stored/client-side XSS in Leaflet popups.
3. Incorrect directionality in return-trip candidate selection.
4. Fresh-install breakage caused by legacy `TemplateResponse` calls and unconstrained dependency resolution.
5. Blank maps on directly opened shareable result pages.
6. A Feedback page blocked by the application's Content Security Policy.
7. Detached background searches, abandoned progress entries, and integration tests that leak real network work past teardown.
8. Existing Ruff and formatting failures.
9. Missing dependency lock and continuous-integration checks.

## Product and Security Model

Pub Finder remains a passwordless collaborative application. A high-entropy session code is the capability that grants access to a session. Anyone with that code may view and collaborate in that session; no accounts, roles, or participant-specific authentication are introduced.

The capability boundary must nevertheless be enforced consistently. Every participant read or mutation initiated under a session URL must scope database operations by both `session_code` and `participant_id`. A participant identifier from one session must be unusable from another session.

## Data and Route Boundaries

The database API for changing participant stops will accept the session code explicitly and update with `WHERE id = ? AND session_code = ?`. It will return whether a matching participant was updated. The route will treat a missing match as a not-found/error response rather than silently returning a successful participant list.

All `TemplateResponse` calls will use the current request-first Starlette/FastAPI signature. The application will target current compatible dependency versions and commit a `uv.lock`, so local development, CI, and Docker resolve the same environment.

## Safe Map Rendering

Untrusted names and stop labels will never be concatenated into HTML strings. Leaflet popup content will be assembled with DOM elements and `textContent`. Google Maps links will be created only from parsed `https:` URLs; invalid or non-HTTPS values will not become anchors.

The backend will continue serializing map data as JSON in escaped HTML attributes, but the browser will treat every string from that JSON as text. The CSP will add `script-src-attr 'none'` as defense in depth so injected event-handler attributes cannot execute even if a future rendering regression reintroduces HTML parsing.

The popup behavior will be verified with a real browser against controlled local data, including the participant-name payload that reproduced the audit finding.

## Directed Candidate Selection

Candidate selection will preserve the direction of each journey leg:

- Outbound candidates score `participant_start -> target`.
- Return candidates score `target -> participant_end`.
- Round-trip searches union candidates from both directed calculations before live timetable refinement.

The precomputed table is directed, so the return calculation must filter rows by `to == participant_end` and use `from` as the candidate target. Geographic and transit-time candidate calculations will share the same direction-aware shape. Regression fixtures will be intentionally asymmetric so reversing an edge produces the wrong result and fails the test.

## Background Search Lifecycle

The application will own search-task and progress state instead of allowing untracked `asyncio.create_task` calls.

- Each submitted search registers its task handle and a timestamped progress record.
- Completed tasks remove their task handle through a done callback.
- Progress records receive a bounded retention period and are pruned whenever searches or progress streams are accessed.
- Disconnecting an SSE consumer does not immediately cancel a useful search, but its result expires automatically if never consumed.
- Application shutdown cancels and awaits all outstanding search tasks before closing SQLite.
- Tests await registered tasks and patch external transit/Places calls for the full lifetime of the task, preventing network retries after the patch or database fixture has closed.

This remains an in-process design suitable for the current single-process deployment. Redis or a durable job queue is outside this remediation.

## Results and Feedback UX

`app.js` will initialize an existing map on normal page load as well as after HTMX swaps. The results partial will not depend on an inline script that runs before `app.js` exists. Reinitialization remains idempotent through the existing map-data hash.

The CSP will explicitly allow Google Forms under `frame-src https://docs.google.com`. `frame-ancestors 'none'` remains unchanged, so third parties still cannot embed Pub Finder itself.

## Quality and Delivery

Ruff will be configured in `pyproject.toml` as a first-class development dependency and all tracked Python files will be formatted. Unused imports and dead assignments will be removed without unrelated refactoring.

GitHub Actions will run on pushes and pull requests using Python 3.12. The workflow will install from `uv.lock`, run Ruff checks and formatting verification, run the full pytest suite, and audit dependencies. Docker will install from the lock rather than resolving broad version ranges at image-build time.

## Test Strategy

Every production behavior change starts with a regression test that fails against the audited code:

- Integration test proving one session cannot update another session's participant.
- Asymmetric optimization tests for return-only and round-trip candidate selection.
- Integration tests for current `TemplateResponse` behavior on error and results paths.
- Search lifecycle tests proving tasks are tracked, awaited, pruned, and cancelled on shutdown without real network access.
- Route/header tests for the Feedback iframe CSP allowance and inline-handler prohibition.
- Browser verification for direct results-map initialization and safe participant/pub popups.

The final gate is a clean fresh environment with the locked dependencies, a passing full test suite, clean Ruff check and formatting output, successful dependency/security scans, successful application startup, and browser verification of the two previously broken pages.

## Non-Goals

- User accounts, passwords, roles, or per-participant authentication.
- Redis, Celery, or another distributed task queue.
- A redesign of the current visual language or session workflow.
- Broad refactoring outside code touched by the audited defects.
- Changes to the transit or Google Places product semantics beyond the identified correctness and lifecycle defects.
