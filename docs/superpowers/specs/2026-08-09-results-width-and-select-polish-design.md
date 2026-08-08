# Results Width and Select Polish Design

## Goal

Polish the approved Meet Somewhere interface without changing its structure or interaction model. Native select controls should match the retro visual language, and search results should use more of the available width on desktop.

## Select controls

- Keep native `select` elements so keyboard navigation, mobile pickers and assistive technology behavior remain intact.
- Apply the existing paper surface, two-pixel ink border, compact mono typography and tactile shadow used by other controls.
- Remove the inconsistent browser arrow and replace it with a small CSS chevron inside a bordered accent area on the right.
- Preserve a minimum 44-pixel target and provide clear hover, focus-visible and disabled states.
- Apply the treatment consistently to Method and Direction. The selector should be reusable for future selects.

## Desktop results layout

- Keep the standard application container at its current maximum width.
- Allow `#results-section` to break out symmetrically from that container on viewports that have spare space.
- Cap the results workspace at 1600 pixels and retain 16-pixel minimum page gutters.
- At wide desktop widths, make the ranked-results and reachability rail 520 pixels wide. This is wider than the 480-pixel settings card above it.
- Preserve at least approximately 1000 pixels for the map at a 1920-pixel viewport.
- Use a narrower 420-pixel rail below 1300 pixels so laptop-size maps retain useful width.
- Use the existing intermediate and mobile breakpoints. At 720 pixels and below, retain the current Map/List switch and single-panel layout.
- Apply the same breakout behavior to inline search results and the standalone saved-results page.

## Scope boundaries

- No changes to search behavior, result ordering, maps, HTMX flow, copy or data handling.
- No new frontend framework or custom JavaScript select component.
- No global container widening.

## Verification

- Add focused UI assertions for the reusable select styling and capped centered results breakout.
- Run the existing Python and JavaScript test suites.
- Inspect the session and standalone results pages at 1920, 1440, 1280 and 390 pixel viewport widths.
- At 1920 pixels, confirm the results workspace is 1600 pixels, the reachability rail is 520 pixels and the rendered map is wider than 1000 pixels.
- Confirm visible keyboard focus and native select operation.
