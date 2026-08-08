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
- Reinitialized the theme toggle after HTMX shell replacement without duplicate click or lifecycle handlers.
- Kept the interpolation workload within a proportional 96px grid while rendering the reachability field at the map's intrinsic output resolution. Observation dots now keep circular geometry on tall mobile maps.
- Applied a theme-independent dark foreground to coral, yellow, mint, sky, green, and orange surfaces. Participant initials and tabs select the higher-contrast foreground for each stable participant colour.

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

Browser follow-up produced two more red-green regressions:

```text
theme replacement shell: missing htmx:afterSwap lifecycle handler
non-square map: expected intrinsic canvas width 680, received 96
```

The replacement-shell test now covers dark/light state, accessible action labels, local storage, and duplicate-handler prevention. The 340x654 map test now covers proportional interpolation cells, a 680x1308 output canvas at 2x density, correctly scaled observation coordinates, circular dot radius, and the unchanged core 96x96 bound.

The contrast review added two more red-green regressions. The first found accent state selectors that overrode the safe button foreground with dark-theme `--ink`. The second failed because participant palette foreground selection did not exist. The resulting tests execute WCAG contrast calculations, require at least 4.5:1 for normal text, audit all accent selector overrides, and cover every stable participant colour.

## Automated verification

```text
uv run pytest tests/test_ui.py -q
24 passed

uv run pytest -q
128 passed

npm run test:js
24 passed

uv run ruff check backend routers tests
All checks passed

node tests/js/reachability-core.bench.js
41.16ms best of 3 for 1,444 stops on a 96x96 grid
```

Additional checks passed for JavaScript syntax, all Jinja template compilation, balanced CSS braces, required mobile and dark selectors, DOM control relationships, `git diff --check`, and changed-line em/en dash absence. Accent foreground pairs now measure from 5.30:1 for light text on the stable participant blue to 15.14:1 for dark text on the light-theme sky.

## Viewport and state audit

The static audit covered the CSS and rendered DOM paths used at:

| Viewport | Applied composition audited |
| --- | --- |
| 1440x900 | Desktop home, two-column session, persistent split results |
| 834x1112 | Tablet navigation and narrowed split results |
| 390x844 | Mobile home and form, session stack, stop sheet, progress, map sheet, full list |
| 360x800 | Narrow mobile controls, single-column forms, safe-area sheets, overflow containment |

State hooks audited: home, session, stop picker, all three search-progress stages, results map, results list, How It Works, feedback iframe surround, invalid/empty/error system messages, and dark home/session/results surfaces.

## Browser evidence and limitation

This agent's requested in-app browser control surface was callable, but selecting it returned exactly `Browser is not available: iab`; its runtime browser inventory was empty. The root task's browser was available and inspected the 390x844 flow. It observed zero horizontal page overflow and a 654px map height, while identifying two real defects: the theme toggle stopped after the HTMX body replacement, and the low-resolution reachability canvas stretched dots into vertical bars. Both defects now have red-green regressions and fixes. The rest of that screenshot was reported as visually sound.

## Concerns

- The full requested viewport, state, theme, and reduced-motion screenshot matrix is not yet complete. Do not treat the single 390x844 root-browser pass as full visual acceptance.
