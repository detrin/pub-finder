# Wide Results Pane Correction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give both the reachability rail and map enough width on large desktop screens.

**Architecture:** Increase the centered results breakout to 1600 pixels and give the reachability rail a 520-pixel desktop width. Add a laptop breakpoint at 1300 pixels that reduces the rail to 420 pixels before the existing compact and mobile layouts take over.

**Tech Stack:** CSS Grid, FastAPI/Jinja, browser geometry checks, pytest, Node test runner

## Global Constraints

- At 1920 pixels, render a 1600-pixel workspace, a 520-pixel rail and a map wider than 1000 pixels.
- At 1440 pixels, preserve a useful map width without horizontal overflow.
- At 1280 pixels, use the 420-pixel laptop rail.
- At 390 pixels, retain the existing Map/List switch and single-pane behavior.
- Push the verified change to `main`.

---

### Task 1: Correct wide and laptop grid widths

**Files:**
- Modify: `static/app.css`
- Modify: `templates/base.html`

- [x] Record the failing 1920-pixel geometry for workspace, rail and map.
- [x] Increase the workspace cap from 1360 to 1600 pixels.
- [x] Increase the wide rail from 390 to 520 pixels.
- [x] Add a maximum-1300-pixel breakpoint with a 420-pixel rail.
- [x] Bump the stylesheet cache version.

### Task 2: Browser and regression verification

**Files:**
- Modify: this plan only to record completion

- [x] Verify exact geometry at 1920, 1440, 1280 and 390 pixels.
- [x] Inspect the wide results workspace visually in the browser.
- [x] Run all Python and JavaScript tests.
- [x] Run `git diff --check` and scan changed project files for em dashes and en dashes.
- [ ] Commit and push `main`.
