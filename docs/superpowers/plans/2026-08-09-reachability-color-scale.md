# Reachability Color Scale Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the low-contrast single-color reachability overlay with four distinct travel-time colors.

**Architecture:** Keep the existing classification and interpolation pipeline. Map its four band indexes to fixed brand-aligned colors at a shared opacity, and use matching endpoint colors in the HTML legend.

**Tech Stack:** Canvas 2D, CSS custom properties, Node test runner, browser visual checks

## Global Constraints

- Preserve map labels and controls.
- Preserve the current time thresholds and interaction behavior.
- Verify light and dark themes.
- Push `main` after all tests pass.

---

### Task 1: Renderer regression test

- [x] Change the renderer test to require four distinct colors and a 0.48 opacity.
- [x] Run it against the old renderer and confirm it fails because only lilac is emitted.

### Task 2: Color scale and legend

- [x] Map the four time bands to mint, yellow, orange and red.
- [x] Update the legend endpoints to mint and red.
- [x] Bump CSS and JavaScript asset versions.
- [x] Run the focused renderer test and confirm it passes.

### Task 3: Verification and delivery

- [x] Inspect the populated wide map in light and dark themes.
- [x] Run all Python and JavaScript tests.
- [x] Run `git diff --check` and scan changed project files for em dashes and en dashes.
- [ ] Commit and push `main`.
