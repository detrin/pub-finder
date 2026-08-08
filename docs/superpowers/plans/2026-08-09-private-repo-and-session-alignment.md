# Private Repository and Session Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the repository private, remove its footer link and keep the sticky settings card within the people card's desktop boundary.

**Architecture:** Keep the existing two-column grid and sticky settings behavior. Stretch the people card to the grid row height so the visual and sticky boundaries agree, and make the footer change only in the shared Jinja shell.

**Tech Stack:** FastAPI, Jinja, CSS Grid, pytest, browser geometry checks, GitHub CLI

## Global Constraints

- Retain the GitHub link in navigation.
- Retain sticky settings on desktop and static settings on mobile.
- Do not add JavaScript or nested scrolling for layout alignment.
- Work on and push `main`.

---

### Task 1: Footer repository link

**Files:**
- Modify: `templates/base.html`
- Test: `tests/test_ui.py`

- [x] Add a rendered-shell test that scopes assertions to `.site-footer` and confirms the repository URL is absent while Daniel Herman's link remains.
- [x] Run the focused test and confirm it fails on the repository URL.
- [x] Remove only the footer repository link and separator.
- [x] Run the focused test and confirm it passes.

### Task 2: Session card boundary

**Files:**
- Modify: `static/app.css`
- Modify: `templates/base.html`

- [x] Record failing desktop geometry showing the settings bottom below the people-card bottom.
- [x] Stretch the participant wrapper chain and bump the stylesheet cache version.
- [x] Confirm both card bottoms align with one participant before and after scroll.
- [x] Confirm a taller people list still bounds the sticky settings card.
- [x] Confirm the mobile layout remains single-column and non-sticky.

### Task 3: Delivery and repository privacy

**Files:**
- Modify: this plan only to record completed steps

- [x] Run all Python and JavaScript tests plus `git diff --check`.
- [ ] Commit and push the code and tests to `main`.
- [ ] Change `detrin/pub-finder` visibility to private with GitHub CLI.
- [ ] Query GitHub and confirm the reported visibility is `PRIVATE`.
