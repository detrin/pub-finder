# Results Width and Select Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make native select controls fit the Meet Somewhere retro aesthetic and let results use a centered 1360-pixel desktop canvas without changing mobile behavior.

**Architecture:** Keep native HTML controls and implement the polish entirely in the existing stylesheet. Give results a viewport-aware symmetric breakout from the standard 1120-pixel container, capped at 1360 pixels with fixed page gutters.

**Tech Stack:** FastAPI, Jinja, HTMX, CSS, pytest, browser computed-style checks

## Global Constraints

- Keep native `select` behavior.
- Do not widen the global container.
- Keep the current results mobile Map/List interaction at 720 pixels and below.
- Do not change search logic, maps, copy or data handling.
- Do not add em dashes or en dashes.

---

### Task 1: Retro select controls

**Files:**
- Modify: `static/app.css`
- Test: browser computed-style verification against a rendered session page

**Interfaces:**
- Consumes: existing native `select` elements and global form-control tokens
- Produces: consistent native selects with custom CSS arrow, control shadow and visible focus

- [x] **Step 1: Record the current computed styles**

At desktop width, load a session page and record `appearance`, `backgroundImage`, `fontFamily`, `boxShadow`, `minHeight` and focus outline for the Method select.

- [x] **Step 2: Verify the current control misses the desired contract**

Confirm at least the custom arrow and tactile shadow are absent before editing the stylesheet.

- [x] **Step 3: Add the minimal reusable select styling**

Add a global `select` rule that uses native semantics with `appearance: none`, enough right padding, a CSS-encoded chevron, mono typography and the existing control shadow. Add hover and disabled states while preserving the global `:focus-visible` rule.

- [x] **Step 4: Verify the rendered control**

Reload and verify the computed properties, keyboard focus, option opening and a minimum 44-pixel height.

### Task 2: Centered desktop results breakout

**Files:**
- Modify: `static/app.css`
- Test: browser geometry verification against inline and standalone results

**Interfaces:**
- Consumes: `#results-section`, `.container` and `.results-workspace`
- Produces: a centered results canvas capped at 1360 pixels with 16-pixel minimum gutters

- [x] **Step 1: Record current desktop geometry**

At a 1440-pixel viewport, confirm the results section is constrained to the 1120-pixel container.

- [x] **Step 2: Add the centered breakout rule**

Set the section width to `min(1360px, calc(100vw - 2rem))`, center it relative to its containing block, and retain the existing vertical spacing.

- [x] **Step 3: Verify desktop and intermediate geometry**

At 1440 and 1280 pixels, confirm equal left and right gutters, no horizontal overflow and a map wider than its previous constrained state.

- [x] **Step 4: Verify mobile behavior**

At 390 pixels, confirm the results section fits the viewport and the existing Map/List switch remains visible and functional.

### Task 3: Regression verification and delivery

**Files:**
- Modify: `static/app.css`

**Interfaces:**
- Consumes: Tasks 1 and 2
- Produces: verified, committed and pushed UI polish on `main`

- [x] **Step 1: Run automated tests**

Run the complete Python and JavaScript test commands documented by the repository and require zero failures.

- [x] **Step 2: Check the diff**

Run `git diff --check`, inspect the focused stylesheet diff, and scan changed project files for em dashes and en dashes.

- [ ] **Step 3: Commit and push**

Commit only the intended plan and stylesheet changes without staging the pre-existing untracked files, then push `main` to `origin`.
