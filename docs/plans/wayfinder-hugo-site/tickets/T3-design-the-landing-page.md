---
id: T3
title: Design the landing page
label: wayfinder:prototype
status: closed
assignee: opus (work session 2026-07-24)
closed: 2026-07-24
blocked-by: [T1]
---

## Question

What does the front page (`content/_index.md`) say and show? It is the
page a recruiter or interviewer lands on: it must present the project
in one screen — what the system is (flow-graph framework, Citi Bike
scenario, simulator + demand forecasting), what is technically
interesting about it, and where to go next. Open points: does it show a
figure or screenshot (the Streamlit UI, a journal diagram); does it
carry a short results line (forecast quality, run scale); how much of
the current `docs/README.md` routing ("to run it / to understand it /
to change it") survives on it. Resolve by prototyping in the live site.

## Resolution

The landing page is built live as `docs/site/content/_index.md`; it builds
clean with `hugo v0.164.0+extended`. Structure and the three open points
(user's calls, 2026-07-24):

1. **Figure: the Streamlit screenshot.** `docs/assets/ui_overview.png` was
   copied to `docs/site/static/images/ui_overview.png` and shown as the hero
   image (the run browser). Not a diagram, not text-only. (The move ticket
   T10 / folder-fate ticket T7 own where site images finally live; the copy
   into `static/images/` is the working home for now.)
2. **A results line, with concrete run numbers.** A "By the numbers" block
   carries the January 2026 evaluation figures: 168 hourly periods, 424,191
   trips, 66,895 bikes, 128,537 docks, five runs passing every invariant —
   sourced from `docs/reports/model_evaluation_202601.md`. Not just system
   scale.
3. **Four section links, not the README's three.** "Where to go next" points
   at the sections fixed by T2: scenario → architecture → decisions →
   getting-started (via `relref`). The README's "run / understand / change"
   triple is dropped in favour of the menu structure.

The page also opens with what the system is (flow-graph platform, Citi Bike,
exact historical replay) and a "What is technically interesting" list (the
append-only journal, exact replay, two-level forecast evaluation, the ML
toolkit). Links use `relref` so they survive the later page move.
