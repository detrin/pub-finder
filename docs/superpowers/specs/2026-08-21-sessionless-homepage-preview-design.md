# Sessionless Homepage Reachability Preview

Date: 2026-08-21

Status: Approved visual direction, pending written-spec review

## Summary

Replace the homepage's static route illustration with an interactive Prague reachability preview. A visitor adds starting stops and immediately sees an approximate one-way reachability field built from the existing precomputed typical-time matrix. The preview has no database session, date, return journey, venue search, or ranked meeting points.

The existing `Start a plan` form remains the only session-creation action. Stops selected in the preview carry into the new session so the transition to live, date-specific DPP planning does not discard the visitor's work.

## Product boundary

The homepage preview answers:

> Where can these starting points approximately reach using typical one-way transit times?

The session planner answers:

> Which meeting points rank best for these people at a selected date and time, optionally including the trip home?

The preview must never be described as live, exact, optimal, date-specific, or a route. It demonstrates shared reachability, not a ranked recommendation.

## Validated design decisions

- The preview replaces the current static hero illustration. It is not appended as another homepage section.
- Desktop keeps the split hero: product copy on the left, interactive estimate on the right, and the plan form immediately below.
- Mobile uses the same experience-first order: headline, complete preview, plan form, join disclosure, and recent plans.
- The first selected stop immediately renders its individual approximate reach.
- With two or more selected stops, the field changes to shared reach and uses the longest one-way journey among selected origins at every destination.
- The preview uses fixed approximate time bands. It has no threshold slider.
- The existing plan-name form is the sole session-creation form.
- Selected preview stops carry into the new session.
- The preview does not persist across a reload and does not create a hidden or temporary database session.

## Homepage content hierarchy

### Hero copy

- Eyebrow: `Made for Prague`
- Heading: `Find a place that works for everyone.`
- Supporting copy: `Add starting stops and see their approximate shared reach across Prague, then create a plan when the date, return journey, and live ranking matter.`

Localized Czech copy must ship with the English copy. Final punctuation may be tightened during implementation, but the accuracy claims must not change.

### Preview card

- Label: `Quick estimate`
- Persistent badge: `Approximate · one way`
- Field label: `Starting stops`
- Input placeholder: `Add a Prague stop`
- Zero-stop state: `Add a starting stop to see its reach.`
- One-stop heading: `Approximate reach from {stop}`
- One-stop prompt: `Add another stop to see where everyone can reach.`
- Group heading: `Shared reach for {count} starting points`
- Metric explanation: `Colour shows the longest estimated journey among the selected starts.`
- Permanent disclosure: `Based on typical transit times. No selected date, service changes, or trip home.`
- Handoff link: `Plan with live DPP times ↓`

The handoff link scrolls to and focuses the existing plan-name field. It does not create a session.

### Time legend

Use fixed, explicitly approximate bands based on the existing visualization scale:

- `about 35 min`
- `36–50 min`
- `51–65 min`
- `over 65 min`
- `no estimate`

`No estimate` must be visually distinguishable from the longest-time band. The interface must not imply that missing matrix coverage means unreachable.

### Plan form

- Heading: `Start a plan`
- Support: `Choose a date and time, rank meeting points, and include the trip home.`
- Field: `Plan name`
- Button: `Start planning`
- Conditional carry-over status: `{count} selected starts will be added to this plan.`

The button label stays stable. The conditional status explains the handoff without introducing a competing session action.

## Interaction states

### Adding and removing origins

- Use a searchable, keyboard-operable stop combobox rather than map-click selection.
- Each unique selection becomes a removable chip and a labelled map marker.
- Duplicate stops are rejected without issuing a calculation request.
- A visitor may select at most six unique origins in the preview. At the limit, explain that larger groups should continue in a session.
- Recalculate only after a stop is added or removed, never on search keystrokes.
- Selections live only in page memory until the visitor submits the plan form.

### Updating

- Preserve the last valid field while a newer calculation is pending, dim it, and expose a textual `Updating estimate…` status.
- Only the newest response may update the map. Abort superseded requests and ignore their completion.
- Update the status through a polite live region without moving focus.

### Failure

- An aborted superseded request is not an error and produces no message.
- On a genuine request or validation failure, clear the old field so it cannot be mistaken for the current selection, retain the selected chips and origin markers, and show `The quick estimate is unavailable. You can still create a plan.`
- When a selected origin has insufficient matrix coverage, identify that stop, keep other selections intact, and offer removal. Do not silently substitute a different stop.
- The session form remains usable when the preview is unavailable.

## Stateless preview API

Add a dedicated sessionless endpoint, conceptually:

```http
POST /reachability/preview
Content-Type: application/json

{"origins": ["Anděl", "Dejvická"]}
```

The endpoint:

1. Requires one to six origins.
2. Normalizes whitespace, rejects duplicates after normalization, and validates every complete stop name against the application's canonical stop list.
3. Creates in-memory participant-shaped inputs with `start_stop` populated and uses the existing `there-only` reachability calculation.
4. Calls the existing `build_reachability_payload` function with the precomputed distance table and stop geography.
5. Returns no session code and performs no database write, live DPP request, or Google Places request.

The response reuses the established reachability payload shape so the existing validation and rendering logic remain authoritative. Preview-specific copy and state live outside that data contract.

Protect the CPU-bound matrix calculation with the following boundaries:

- Maximum six origins and a bounded request body.
- Per-IP request throttling separate from live session-search limits.
- A small in-memory TTL/LRU cache keyed by the ordered normalized origin tuple.
- Calculation timing measured in tests and observable in server logs without logging raw IP addresses or session identifiers.

POST responses use `Cache-Control: no-store`; application-level caching remains server-side. This avoids intermediaries retaining a visitor's selected locations.

## Map and client architecture

Add a homepage-specific controller responsible for:

- Stop combobox state.
- Selected-origin chips and hidden form values.
- Preview request cancellation and newest-response enforcement.
- Empty, loading, ready, and error states.
- Textual accessibility announcements.

Reuse the existing reachability payload validator, interpolation, time classification, map markers, and `ReachabilityMapController`. Refactor the map factory only as much as required to allow a validated payload to be supplied or replaced without requiring a session reachability URL. Do not duplicate interpolation or color-band logic in a homepage script.

Leaflet remains responsible for map geometry and OpenStreetMap attribution. The homepage map introduces tile requests as soon as it initializes; retain visible attribution and document this behavior in the site's privacy notice when that notice is added.

## Session carry-over

The homepage form submits selected origins as repeated hidden form values alongside `session_name`.

On session creation:

1. Treat submitted origins as untrusted input.
2. Normalize, deduplicate, cap at six, and validate against the canonical stop list.
3. Create at least two unnamed participant slots, preserving the existing minimum-slot invariant.
4. Create one unnamed participant slot per selected origin when more than two origins are supplied.
5. Populate only `start_stop` for the corresponding participant. Leave `end_stop` empty and do not infer a return destination from the one-way preview.
6. Redirect to the canonical `/session/{code}` URL through the existing native form navigation.

Session creation and participant initialization must be atomic. A validation failure returns to the homepage with a clear error and creates no partial session. Concurrent session operations must continue to preserve the minimum two-slot invariant and unique participant-name behavior.

## Responsive behavior

### Desktop

- Keep the existing two-column hero footprint.
- The preview occupies the former illustration column.
- The plan form remains directly below the hero and should remain visible near the first viewport at common laptop sizes.

### Mobile

- Order: headline, preview, plan form, join disclosure, recent plans.
- Map height: approximately 180–220 px.
- Place search and chips above the map.
- Chips wrap without horizontal page scrolling.
- The map must not trap vertical page scrolling before deliberate map interaction.
- All controls retain at least 44 px touch targets.

## Accessibility

- Implement the stop search as a labelled ARIA combobox with `aria-expanded`, `aria-controls`, listbox semantics, arrow-key navigation, Enter selection, and Escape dismissal.
- Render origins as a semantic list. Each remove button has an accessible name such as `Remove Anděl`.
- Match chip labels to map markers with letters or numbers; do not rely on color alone.
- Treat the interpolated canvas as decorative. Provide the current state, metric, and time-band meaning in text.
- Announce concise updates such as `Estimate updated for three starting stops` through `aria-live="polite"`.
- Preserve visible focus, reduced-motion behavior, light and dark themes, and existing 44 px control targets.
- Do not automatically move focus after a map update.

## Security and privacy

- The preview endpoint accepts stop names only, not URLs, redirects, HTML, arbitrary matrix expressions, or file paths.
- Validate against the server-owned canonical stop allowlist before calculation.
- Render labels through Jinja auto-escaping or DOM `textContent`; never insert stop names with `innerHTML`.
- Do not create capability URLs, cookies, browser storage, database records, live provider calls, or analytics identifiers merely to display the preview.
- Do not log selected origin lists at normal log levels. They can reveal approximate participant locations.
- Existing session capability-link behavior is unchanged.

## Performance

- Do not send the approximately 2.1-million-row transit matrix to the browser.
- Return only the existing per-stop reachability payload needed by the renderer.
- Abort obsolete client requests and prevent out-of-order rendering.
- Keep the last valid field visible only while a newer request is pending; clear it on a genuine failure.
- Target a visible updated field within 500 ms at p75 under expected production load.
- Measure payload size and calculation latency for one and six origins before release.

## Testing

### Backend

- One origin produces a `there-only` payload without database or provider calls.
- Two or more origins use group maximum journey time.
- Unknown, duplicate, empty, malformed, and more-than-six origin lists are rejected.
- Sparse matrix coverage retains the existing reverse-direction estimate behavior and explicit missing values.
- Rate limiting and cache keys cannot collide across different origin lists.
- Session creation carries validated origins into participant start stops, retains at least two slots, leaves end stops empty, and is atomic on invalid input.

### Client

- Adding the first stop renders individual reach and the add-another prompt.
- Adding the second stop changes the state to shared reach.
- Duplicate selection and the six-origin limit are handled accessibly.
- Removing a stop recalculates and updates chips, markers, hidden fields, and status.
- A superseded response cannot overwrite a newer selection.
- Failure clears stale field data but leaves session creation available.
- Keyboard users can search, select, review, and remove every origin.
- The approximation badge and disclosure remain present in every ready state.

### UI and integration

- Homepage hierarchy matches the approved desktop and mobile order.
- Submitting the homepage without preview stops preserves current two-empty-slot behavior.
- Submitting with preview stops redirects to `/session/{code}` and shows those start stops in unnamed participant slots.
- No preview flow creates a session until the plan form is submitted.
- Full Python and JavaScript suites remain green.

## Product validation

Before broad promotion, test the preview with five to seven Prague public-transport users:

- At least 80% can add two origins and explain the map without instruction.
- At least 80% identify it as typical-time, one-way, and not date-specific.
- At least 80% choose `Start a plan` when asked to plan for a specific date with a return trip.
- Median time from landing to a two-origin field is under 30 seconds.
- Completed live session searches per landing visitor do not decline materially; a decline greater than 10% is a rollout stop condition.

Analytics instrumentation is not required for implementation. If these events are later collected, they must follow the site's analytics notice and consent or opt-out decision.

## Out of scope

- Live DPP requests from the homepage.
- Dates, times, direction controls, return trips, participant names, venue types, venue pins, or meeting-point rankings in the preview.
- A threshold slider or participant tabs.
- Persisting preview state across reloads.
- Creating hidden temporary sessions.
- Replacing Jinja/HTMX with a SPA.
