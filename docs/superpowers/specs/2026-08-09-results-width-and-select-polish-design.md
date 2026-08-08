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
- Cap the results workspace at approximately 1360 pixels and retain safe page gutters.
- Keep the ranked-results rail within its current usable range while giving the map the additional width.
- Use the existing intermediate and mobile breakpoints. At 720 pixels and below, retain the current Map/List switch and single-panel layout.
- Apply the same breakout behavior to inline search results and the standalone saved-results page.

## Scope boundaries

- No changes to search behavior, result ordering, maps, HTMX flow, copy or data handling.
- No new frontend framework or custom JavaScript select component.
- No global container widening.

## Verification

- Add focused UI assertions for the reusable select styling and capped centered results breakout.
- Run the existing Python and JavaScript test suites.
- Inspect the session and standalone results pages at wide desktop, intermediate desktop and mobile widths.
- Confirm visible keyboard focus and native select operation.
