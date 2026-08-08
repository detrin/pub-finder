# Meet Somewhere UI Redesign

Date: 2026-08-08

Status: Approved visual direction, pending written-spec review

## Summary

Redesign Pub Finder as **Meet Somewhere**, a playful social planner for finding a group meeting point in Prague. Preserve FastAPI, Jinja, HTMX, SSE, Leaflet, the existing search algorithm, session model, autosave behaviour, venue cache, and on-demand venue lookup.

The redesign replaces the generic card-based UI with an authored visual system built around participants, transit lines, and shared plans. It also adds an approximate reachability layer derived from the existing precomputed transit matrix. The default group metric is the longest participant journey at each stop.

No SPA migration is included. HTMX remains appropriate because the application is primarily server-rendered forms and partial updates. Leaflet and a small map controller own the interactive geographic state.

## Product position

Meet Somewhere answers one question:

> Which meeting point gives this group the best public transport journeys?

Pubs become one venue preference rather than the product identity. The supported occasion presets map to existing Google Places types:

| UI preset | Existing place types |
| --- | --- |
| Drinks | `pub`, `bar` |
| Coffee | `cafe` |
| Food | `restaurant` |
| Anything | `pub`, `bar`, `cafe`, `restaurant` |

The interface must not imply support for venue categories the backend does not query.

## Design principles

### Social before technical

Participants remain visible throughout setup and results. Each participant receives a stable colour used for their initials, origin marker, route line, and time chip.

### Explain the result

The ranked stop list is always actionable. The heatmap explains the ranking but does not replace it. Selecting a stop highlights it on the map and exposes participant times and nearby venues.

### Factual copy

Use terse, Hacker News-style interface copy:

- Name the operation: `Query DPP journey times`.
- Include exact counts when available: `14 of 31 candidate stops checked`.
- Name the metric: `Minimize longest journey`.
- State errors and recovery directly: `Venue request limit reached. Try again in one minute.`
- Avoid marketing filler and vague emotional language.
- Do not use em dashes anywhere in user-facing copy or documentation.

### One expressive element

The reachability field is the only soft visual element. Layout, controls, cards, and status components use flat colour, black rules, and restrained offset shadows.

## Visual system

### Typography

- Display and body: Bricolage Grotesque, weights 400, 600, 700, and 800.
- Data, labels, statuses, and keyboard hints: DM Mono, weights 400 and 500.
- Self-host WOFF2 font files under `static/fonts/` to avoid an additional runtime dependency and CSP exception.
- Fall back to system sans-serif and monospace fonts.

### Light theme

| Token | Value | Use |
| --- | --- | --- |
| Ink | `#17191C` | Text, borders, offset shadows |
| Paper | `#FFFEFA` | Primary surfaces |
| Sky | `#DFF0FF` | Secondary controls and setup areas |
| Blue | `#2458DF` | Participant and route colour |
| Coral | `#FF6658` | Primary actions and participant colour |
| Yellow | `#FFD447` | Focus, status, labels, participant colour |
| Mint | `#4DC694` | Selected result and success |
| Lilac | `#B9A8FF` | Reachability field |
| Muted text | `#686D71` | Supporting copy |

### Dark theme

| Token | Value |
| --- | --- |
| Background | `#18191D` |
| Surface | `#1C1D21` |
| Text | `#F4F2EB` |
| Muted text | `#AEB0B4` |
| Coral | `#FF7869` |
| Mint | `#6FD5A7` |
| Lilac | `#C9BBFF` |
| Sky | `#B7DDED` |
| Yellow | `#FFE071` |

The existing theme preference remains in local storage. Map geography stays legible in both themes. The dark theme may filter the base tiles or use a dark-compatible tile layer, but attribution must remain visible.

### Shape and depth

- Primary application frame: 2px ink border and modest radius.
- Controls: 1.5px to 2px ink border.
- Primary buttons: 3px offset ink shadow.
- Feature cards and dialogs: 4px to 7px offset ink shadow.
- Pills are reserved for participant and occasion selectors. They are not the default shape for every control.
- Do not use decorative gradients. The reachability field may use stepped translucent colour bands.

### Brand mark

The Meet Somewhere mark consists of two short intersecting transit lines, coral and blue, with ink outlines. It can be implemented as an inline SVG and reused for the favicon. The visible wordmark is `meet somewhere` in lowercase.

## Global application shell

Desktop navigation contains:

- Meet Somewhere wordmark linked to home.
- How it works.
- Feedback.
- GitHub.
- Theme toggle.

On narrow screens, retain the wordmark and theme toggle, and place secondary links in a compact menu. The footer contains creator attribution and the GitHub link without promotional copy.

Replace the Oat CSS dependency with the application stylesheet. Oat layout and card classes conflict with the approved visual system and are not needed once templates use project-specific components.

## Screen specifications

### Home

Desktop uses a split hero:

- Left: `Pick a place that works for everyone.`
- Supporting copy: `Add people, choose a time, and rank meeting points using Prague public transport times.`
- Right: a static or lightly animated transit diagram with three participant lines meeting at a ranked stop.
- The diagram contains a concrete sample result and sample times.

The primary start form sits immediately below the hero and contains plan name, creator name, and `Start planning`.

Joining is secondary. `Join with a code` expands or reveals the code and name fields. A direct invitation URL bypasses the code field.

Recent sessions, when present in local storage, appear as a short list below the entry controls.

### Invitation page

Show the session name before the name input:

- Heading: `You’re invited to {session name}.`
- Show existing participant initials when available.
- Ask only for the joining participant's name.
- State that no account is required and that names and selected stops are visible to the group.

### Session workspace

Desktop uses two columns:

- Left: participants and their stop selectors.
- Right: date, time, return configuration, occasion, optimization method, and direction.

Each participant row contains:

- Stable coloured initials.
- Name.
- Start and end stop selectors.
- `Return to the same stop` checkbox.
- Local `saving` and `saved` status.
- Remove control when more than one participant exists.

The add-participant field stays at the end of the participant list.

The plan panel contains:

- Departure date and time.
- Return date and time.
- Occasion preset.
- Optimization method, with `Minimize longest journey` as the default label.
- Direction, with plain labels for round trip, there only, and back only.
- Primary action: `Find somewhere`.

If a participant is incomplete, keep the primary action disabled and name the missing person in the adjacent status text.

### Stop picker

Desktop uses a centred dialog. Mobile uses a bottom sheet.

The picker contains:

- Participant and direction context, for example `Daniel · starting from`.
- Search input focused on open.
- Up to 50 filtered results.
- Stop name and available mode or line metadata when it is already available. If line metadata is not available in the current dataset, omit it rather than adding another data source in this redesign.
- Keyboard navigation using Arrow Up, Arrow Down, Enter, and Escape.
- Backdrop and close-button dismissal.

Selecting a valid stop closes the picker and triggers the existing latest-wins autosave.

Participant removal uses a confirmation dialog because it deletes that participant's selected stops. Copy states the exact participant name and consequence.

### Search progress

Keep the session header and provide three explicit stages:

1. `Select candidates from the transit matrix`, with `2,083,035 precomputed stop pairs`.
2. `Query DPP journey times`, with `{current} of {total} candidate stops checked`.
3. `Query nearby places`, with the selected types and number of top stops.

Show a numeric progress bar. The page updates through the existing SSE stream. Provide `Back to the plan`; leaving the results area must not cancel the server search unless cancellation is explicitly implemented later.

### Results workspace

Desktop uses a persistent split view:

- Left rail: reachability controls and scrollable ranked stop list.
- Right: sticky Leaflet map with participant markers, ranked stop markers, and optional reachability layer.

The left rail contains:

- View selector: Everyone or one participant.
- Read-only search-method label. Changing the ranking method requires returning to the plan and running a new search.
- Reachability layer label: `Longest journey`. The initial implementation does not add average-time or fairness-gap layers.
- Maximum-time threshold control for the heatmap.
- Ranked stop cards.

Each ranked stop card contains:

- Rank and stop name.
- Longest journey time.
- Total journey time where useful.
- Compact participant time chips.
- Expandable directional or round-trip details.
- Nearby venues or an on-demand venue action.

Selecting a result updates map emphasis without navigation. The first result is selected initially.

### Shareable results

Use the same results workspace and data. Replace edit controls with `Open plan`. The share URL continues to expose the last saved result set for the session.

### Venue suggestions

Venue rows are typographic because the existing data does not contain photos. Each row contains:

- Venue name.
- Walking distance or link.
- Rating formatted to one decimal place.
- Review count in parentheses.
- Open-at-selected-time status when available.
- Google Maps link.

The UI must cover:

- Cached results.
- Live loading.
- No suitable venues.
- Not searched, with `Find nearby places`.
- Rate limit, with a one-minute recovery instruction.
- Provider failure, with `Try again`.

Venue errors never remove or invalidate transit rankings.

### How it works

Use a technical-document layout with a desktop table of contents. Include:

- Problem definition.
- Both objective functions.
- 1,444 geocoded stops.
- 2,083,035 precomputed directional stop pairs.
- Candidate selection and live DPP refinement.
- Return-direction handling.
- Venue search and cache behaviour.
- Heatmap approximation and limitations.
- Repository link.

Keep factual distinctions between precomputed typical times and live queried times.

### Feedback

Retain the current Google Form embed. The surrounding page states which information is useful: expected result, actual result, browser and device, and optional session code. The Google Form itself keeps its own styling because cross-origin iframe content cannot inherit application CSS.

### Empty and error states

Provide designed states for:

- No saved results.
- Invalid session or invite link.
- Incomplete participant setup.
- DPP search failure.
- Partial Google Places failure.
- Venue expansion rate limit.
- Empty venue result.
- Heatmap data unavailable.

Preserve all valid surrounding state. Give one specific recovery action.

## Reachability layer

### Metric

For participant `i` and target stop `t`, define an approximate precomputed journey value `T_i(t)` based on the selected direction:

- There only: precomputed minutes from the participant's start stop to `t`.
- Back only: precomputed minutes from `t` to the participant's end stop.
- Round trip: the sum of both values.

The Everyone layer uses:

```text
G(t) = max_i T_i(t)
```

This means a 35-minute highlighted region contains stops where every participant's approximate journey is at most 35 minutes under the selected direction.

The one-person layer uses `T_i(t)` for the selected participant.

### Data source and honesty

- Use `data/Prague_stops_combinations.parquet` and `data/Prague_stops_geo.parquet`.
- Do not make live DPP or Google API requests when changing heatmap participant, threshold, or visibility.
- Label the layer `Approximate from typical transit times`.
- Keep stop points visible so users can see where observations exist.
- Heatmap interpolation must not be presented as precise continuous transit coverage.
- Ranked results remain based on the existing live refinement.

### Server boundary

Add a read-only JSON endpoint scoped to a valid session and its current participant stops. The response includes:

- Participant IDs, names, colours, start stops, and end stops.
- Stop name, latitude, and longitude.
- Approximate value for each participant at each stop.
- Group maximum for each stop.
- Search direction used to compute values.
- Dataset label and coverage metadata.

The endpoint computes values with Polars from the already loaded matrix. Cache responses by session code plus a hash of participant stops and direction. Session updates invalidate the relevant cache key naturally because the hash changes.

### Client boundary

Leaflet owns:

- Base map.
- Participant markers.
- Ranked stop markers.
- Venue markers.
- Reachability canvas overlay.
- Map selection and viewport state.

HTMX owns server-rendered participant, search-progress, results, and venue fragments. After an HTMX or SSE results swap, the map controller reads the new data attributes, fetches reachability JSON once, and initializes the overlay.

Switching Everyone or a participant and changing the threshold redraw locally from the fetched values.

### Rendering

Render the field on a Leaflet-aligned canvas:

- Use a low-resolution interpolation grid for performance.
- Interpolate only between observed stop values.
- Classify the raster into discrete time bands rather than a decorative density gradient.
- Darker lilac indicates a shorter journey, and lighter lilac indicates a longer journey.
- A threshold masks or deemphasizes values above the selected maximum.
- Debounce redraws during pan and zoom.
- Respect reduced-motion settings.

The implementation plan must include a short prototype and performance check before finalizing the interpolation method.

## Client structure

Keep a no-build JavaScript architecture with focused files:

- `static/theme.js`: theme initialization and toggle.
- `static/session.js`: invite copy, stop picker, stop autosave feedback, participant confirmation, and session-local controls.
- `static/results.js`: HTMX and SSE result lifecycle, result selection, venue map updates, and mobile result sheets.
- `static/reachability-map.js`: Leaflet setup, layers, fetched matrix data, field rendering, and participant view state.
- `static/history.js`: recent session storage and rendering.

Expose small initialization functions and make every initializer idempotent because HTMX and SSE can replace fragments repeatedly.

## HTMX and state flow

1. Participant or stop changes submit through the existing HTMX forms.
2. The server returns the participant fragment.
3. SSE keeps other clients synchronized.
4. Search submission returns the SSE progress fragment.
5. Progress events replace only the search-results area.
6. Completion swaps in the result workspace.
7. The result initializer creates the map and fetches reachability data.
8. Venue expansion replaces only the selected stop's venue section and sends updated marker data out of band.

Focus, open dialogs, and active form inputs must not be disrupted by participant SSE updates. Preserve the current latest-wins stop autosave and stop-picker target resolution fixes.

## Responsive behaviour

### Breakpoints

- Wide desktop: full split views.
- Tablet: narrower rail and map, with nonessential labels shortened.
- Mobile: single-column task flow.

### Mobile home and setup

- Hero text precedes the start form.
- The decorative map preview is reduced or moved below the form.
- Session participants become vertical rows.
- Date and time preferences follow participants as a separate section.
- The stop picker becomes a bottom sheet above the on-screen keyboard.

### Mobile results

- Provide Map and List views rather than shrinking the desktop split pane.
- Map opens with the top-ranked stop selected.
- A bottom sheet shows the selected result and can expand for journeys and venues.
- List view shows all ranked stops without a map behind it.
- Participant time chips scroll horizontally when needed.

### Edge cases

- Long Czech stop names truncate only in compact selectors. Full text remains available in the picker and accessible name.
- Five or more participants remain a vertical list in setup.
- Result time chips may scroll horizontally.
- Dialogs stay within the viewport and preserve close controls above the keyboard.

## Accessibility

- Minimum pointer target: 44 by 44 CSS pixels on touch layouts.
- Visible focus: 3px yellow outer ring, not colour alone.
- Semantic buttons for actions and links for navigation.
- Every dialog has a labelled heading, explicit close control, Escape support, and focus restoration.
- Colour is never the only status or metric indicator.
- Heatmap includes a textual legend and summary.
- Meet WCAG AA contrast for body text and controls in both themes.
- Honour `prefers-reduced-motion`.
- Retain live regions for saving and search progress without announcing every SSE poll.

## Motion

Motion is limited to:

- A short route-line convergence on the home preview.
- Progress-bar updates.
- Map layer opacity transitions.
- Bottom-sheet movement on mobile.

No decorative ambient animation. Reduced-motion mode removes route drawing and replaces movement with immediate state changes.

## Error handling

- Stop autosave failure stays on the affected participant row and retains the selected input value for retry.
- Search failure replaces only the search-results area and provides `Run search again`.
- Reachability failure hides the layer, retains ranked results and map markers, and states `Approximate transit layer unavailable`.
- Venue failure stays inside the relevant stop card.
- Rate limits give a concrete retry interval.
- Invalid session links show a dedicated state with `Start a new plan`.

## Testing and acceptance criteria

### Automated behaviour

- Existing test suite continues to pass.
- Template tests verify Meet Somewhere titles, navigation, approved copy, and required component hooks.
- Session tests cover participant add and remove, latest-wins stop autosave, return-stop behaviour, and incomplete-person gating.
- Search tests cover all progress labels and result initialization data.
- Reachability unit tests cover there-only, back-only, round-trip, group maximum, individual selection, missing pairs, and cache-key invalidation.
- Venue partial tests cover loaded, empty, not searched, rate-limited, and provider-error states.
- Accessibility smoke tests cover dialog labels, button names, focus restoration, and live-region behaviour.

### Visual verification

Verify at minimum:

- Desktop: 1440 by 900.
- Tablet: 834 by 1112.
- Mobile: 390 by 844 and 360 by 800.
- Light and dark themes.
- Default and reduced motion.
- One, three, and six participants.
- Long session and stop names.
- Empty, loading, error, and populated result states.

Compare local screenshots against the approved mockup passes. Inspect every route and dialog in the browser before completion.

### Performance

- Initial HTML remains server-rendered and usable without waiting for reachability data.
- Heatmap data fetch does not block ranked results.
- Cached heatmap response should be generated in under 200 ms locally.
- Heatmap interaction should remain responsive during participant switching and threshold changes.
- Map redraw is debounced during pan and zoom.

## Out of scope

- React, Vue, Svelte, or another SPA migration.
- User accounts.
- Real-time collaborative map cursors.
- Voting on meeting points or venues.
- Venue photos.
- New venue categories beyond the current Places types.
- Live isochrones from an external routing provider.
- Changes to the existing search ranking algorithm.
- Changes to Google Places cache duration or request limits.

## Framework decision

HTMX remains the correct default for this redesign. The application has server-owned session state, form submissions, partial replacement, SSE progress, and a single interactive map island. A larger client framework would add duplicate state ownership without improving the current workflow.

Reconsider a SPA only if the product later requires several of the following at once: draggable route editing, optimistic multi-user collaboration, offline planning, client-owned undo history, or complex local itinerary state.
