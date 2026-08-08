# Stop autosave design

**Date:** 2026-08-08

## Goal

Remove the manual Save button from each participant’s stop form. A selected valid transit stop must persist immediately, so a participant never has to make a second confirming action.

## Interaction model

The stop picker is the only way to edit the read-only stop fields and only exposes entries from the server-provided valid-stop list. It already emits a bubbling `change` event when a user chooses an entry.

Each stop form will use that event as its HTMX trigger:

- selecting From posts the form immediately;
- selecting To posts the form immediately;
- toggling return posts the form immediately because it changes whether the backend copies From into To;
- no request is sent while merely filtering the picker, because the picker’s search input is outside the stop form.

For a return journey, selecting From autosaves both values because the backend already sets `end_stop = start_stop` when `same_start_end` is true. For a one-way or distinct-return journey, selecting From first deliberately persists partial valid state; selecting To subsequently completes it. This preserves a user’s first valid choice if they leave before choosing the second.

## Template and server behavior

`stop_form.html` will replace the submit button with a non-interactive, accessible saving indicator. The existing `POST /session/{code}/stops` endpoint remains the single validation and persistence boundary. No new endpoint, client-side stop whitelist, or database migration is required.

The form will preserve the current target (`#session-participants-inner`) and swap behavior. An in-flight request renders “Saving…” through HTMX’s indicator class; the returned participant partial is the confirmation of the saved server state.

## Error handling

The server continues rejecting unknown stops and re-renders the form with its existing error message. Autosave is only triggered by a picker selection or return toggle, so normal interaction cannot submit arbitrary typed values. A network failure keeps the current DOM rather than falsely marking the values as saved.

## Testing

- Rendered session markup must have an autosave trigger for stop-field and return-checkbox changes, no submit Save control, and an accessible saving indicator.
- Existing route tests continue proving server-side stop validation and session ownership.
- The JavaScript test suite must remain green; no picker behavior changes are required.

## Scope

This affects only stop-form submission UX. It does not alter stop validation, participant management, search rate limiting, transit search, or session data model.
