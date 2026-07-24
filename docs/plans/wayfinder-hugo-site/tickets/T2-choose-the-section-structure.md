---
id: T2
title: Choose the section structure
label: wayfinder:prototype
status: closed
assignee: opus (work session 2026-07-24)
closed: 2026-07-24
blocked-by: [T1]
---

## Question

How do the `docs/` folders map onto the hugo-book left-hand menu, in
what order, and under what names? The portfolio reader comes first, so
the order the template used (introduction → usage → architecture →
research → reference) may not fit. Open points: does "Key Components"
keep its name or become "Architecture"; where does the Citi Bike
scenario page sit (own section vs part of the introduction); do the
three `decisions/` records get a menu section or fold into the pages
they explain; does `getting-started` stay first or move below the
showcase material. Resolve by prototyping: build the menu as `_index.md`
stubs with weights in the live site and react to it.

## Resolution

The menu is ordered for the portfolio reader (user's call, 2026-07-24):
the concrete scenario and the internals come first; the run-it-yourself
material moves below. Six top-level sections under `content/docs/`, each a
`_index.md` stub with a weight:

1. **The Citi Bike scenario** (`scenario/`, weight 1) — own section, first.
   Leads the menu instead of folding into an introduction.
2. **Architecture** (`architecture/`, weight 2) — was `key-components/`.
   Renamed to "Architecture" (not "How it works" / "Key Components").
3. **Design decisions** (`decisions/`, weight 3) — own section, kept
   visible; not folded into the pages they explain.
4. **Getting started** (`getting-started/`, weight 4) — moved below the
   showcase material.
5. **How-to guides** (`how-to/`, weight 5).
6. **Reference** (`reference/`, weight 6) — API, command table, and
   (pending T4) the Notations dictionary.

Folder-to-section map for the later page move: `scenarios/` → `scenario/`,
`key-components/` → `architecture/`, `decisions/` → `decisions/`,
`getting-started/` → `getting-started/`, `how-to/` → `how-to/`,
`reference/` → `reference/`. Stubs build clean with local
`hugo v0.164.0+extended`. Notations placement (T4) and notebook rendering
(T6) are deliberately out of this ticket. Unblocks the page move (T10).
