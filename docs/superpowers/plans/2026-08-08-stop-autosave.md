# Stop Autosave Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist each participant’s selected valid stops automatically without a Save button.

**Architecture:** The existing stop picker emits `change` on read-only valid-stop fields. Configure each existing HTMX stop form to post on stop-field and return-checkbox changes, preserving the existing server endpoint, target, swap, and validation. Replace the manual submit button with an HTMX saving indicator.

**Tech Stack:** FastAPI/Jinja2, HTMX, JavaScript, pytest, Node test runner.

## Global Constraints

- Autosave only on picker-generated stop-field changes and return-checkbox changes; never while filtering the picker.
- Preserve `POST /session/{code}/stops` as the sole validation and persistence boundary.
- Selecting From with return enabled persists both From and To through the existing server behavior.
- Selecting From with return disabled may persist partial valid state; selecting To persists the completed state.
- Remove the manual Save button and render an accessible `Saving…` HTMX indicator.
- Do not change participant management, stop validation, search rate limiting, transit search, or the data model.

---

### Task 1: Trigger stop persistence from valid selections

**Files:**
- Modify: `templates/partials/stop_form.html`
- Test: `tests/test_routers.py`

**Interfaces:**
- Consumes: bubbling `change` events already dispatched by `static/app.js` when a picker item is selected.
- Consumes: existing `POST /session/{code}/stops` endpoint and `#session-participants-inner` response target.
- Produces: a stop form with automatic HTMX submissions and no submit Save control.

- [ ] **Step 1: Write the failing markup test**

```python
response = await client.get(f"/session/{session['code']}")

assert 'hx-trigger="change from:[data-stop-input], change from:[data-same-start-end]"' in response.text
assert "Saving…" in response.text
assert ">Save<" not in response.text
```

- [ ] **Step 2: Run the focused test to verify RED**

Run: `uv run pytest tests/test_routers.py -k autosave -v`

Expected: FAIL because the current form has no `hx-trigger`, still contains `Save`, and no saving indicator.

- [ ] **Step 3: Change the stop form markup**

```jinja2
<form hx-post="/session/{{ session_code }}/stops"
      hx-trigger="change from:[data-stop-input], change from:[data-same-start-end]"
      hx-target="#session-participants-inner" hx-swap="innerHTML"
      hx-indicator="find .stop-save-status" class="stop-form">
...
<span class="stop-save-status htmx-indicator" role="status" aria-live="polite">Saving…</span>
```

Remove only the Save button. Do not add a trigger to the picker filter input, which is outside this form.

- [ ] **Step 4: Run focused and complete checks**

Run: `uv run pytest tests/test_routers.py -q && uv run pytest -q && uv run ruff check . && node --test tests_js/app.test.mjs && git diff --check`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add templates/partials/stop_form.html tests/test_routers.py
git commit -m "feat: autosave selected stops"
```

## Self-review

- Spec coverage: the only task applies autosave to valid selection and return-toggle changes, removes the button, preserves the endpoint, and adds accessible saving feedback.
- Placeholder scan: no incomplete implementation instructions.
- Type consistency: no new application interfaces or data structures are introduced.
