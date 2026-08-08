# Task 9 report

Date: 2026-08-08

## Outcome

- Added reusable native-dialog focus restoration that resets `returnValue`, records the invoking control, restores only connected controls, and keeps the existing post-swap stop focus path intact.
- Kept mobile result view values at `map` and `list`, and connected each toggle to its controlled map or ranked-list region.
- Completed the 720px mobile composition for navigation, home, session setup, stop picker, and results.
- Added `vh` fallbacks before `dvh` sizing, bottom safe-area handling, horizontal participant chip scrolling, the selected-result bottom sheet, and mobile 44px controls.
- Added explicit dark surfaces for setup panels, cards, dialogs, Leaflet controls, and the Google Form surround.
- Added route drawing with reduced-motion removal. Reduced motion also removes reachability-field and result-sheet transitions.
- Added document-level horizontal overflow containment and retained visible yellow focus rings.

## TDD evidence

The exact dialog label and live-region test from the brief was added. Those hooks were already present at the base revision. A new executable interaction test then failed because `showModalWithFocusReturn` was not exported or implemented:

```text
TypeError: showModalWithFocusReturn is not a function
1 failed, 2 passed
```

After the minimum dialog helper and integrations were added:

```text
3 passed, 20 deselected
```

## Automated verification

```text
uv run pytest tests/test_ui.py -q
23 passed

uv run pytest -q
126 passed

npm run test:js
22 passed

uv run ruff check backend routers tests
All checks passed
```

Additional checks passed for JavaScript syntax, all Jinja template compilation, balanced CSS braces, required mobile and dark selectors, DOM control relationships, `git diff --check`, and changed-line em/en dash absence. Dark-theme text and action color pairs audited between 6.31:1 and 15.68:1.

## Viewport and state audit

The static audit covered the CSS and rendered DOM paths used at:

| Viewport | Applied composition audited |
| --- | --- |
| 1440x900 | Desktop home, two-column session, persistent split results |
| 834x1112 | Tablet navigation and narrowed split results |
| 390x844 | Mobile home and form, session stack, stop sheet, progress, map sheet, full list |
| 360x800 | Narrow mobile controls, single-column forms, safe-area sheets, overflow containment |

State hooks audited: home, session, stop picker, all three search-progress stages, results map, results list, How It Works, feedback iframe surround, invalid/empty/error system messages, and dark home/session/results surfaces.

## Browser limitation

No screenshots were captured. The requested in-app browser control surface was callable, but selecting it returned exactly `Browser is not available: iab`; the runtime browser inventory was empty. No alternate browser was substituted. Visual pixel comparison therefore remains for a browser-enabled environment.

## Concerns

- Automated and structural checks cannot replace pixel inspection. The viewport and dark-theme states still need the requested screenshot sweep when the in-app browser is available.
