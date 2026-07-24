---
id: T12
title: Delete the retired docs/ leftovers
label: wayfinder:task
status: closed
assignee: vlzm
blocked-by: []
---

## Question

After the page move (T10), a few `docs/` files remain whose fate T7 already
settled as **delete**, and which nothing else depends on:

- `docs/README.md` — the old docs index; the site's `content/docs/_index.md`
  replaces it.
- `docs/README_ru.md` — its English partner is gone (T7: a Russian companion
  goes when its English partner moves into the site).
- `docs/archive/overview.md` (and the `docs/archive/` dir).

Delete them. This does not cover `docs/assets/` (coupled to the root README
rewrite — see T11) or the private folders `plans/`, `reports/`, `method/`,
`interview/`, which T7 keeps. When this and T11 are both done, the only
`docs/` left is `site/` plus the private folders — the destination's "old
`docs/` tree is gone" is reached.

## Resolution

Deleted `docs/README.md`, `docs/README_ru.md`, and `docs/archive/overview.md`
via `git rm` (the empty `docs/archive/` dir went with its only file). Checked
first for live references: the only mentions are historical prose in old
`docs/plans/*` planning documents (kept private per T7), not code, build
config, or the site — safe to remove.

`docs/` now holds only `site/` and the private folders (`interview/`,
`method/`, `plans/`, `reports/`). With T11 (which removed `docs/assets/`) this
completes the destination's "the old `docs/` tree is gone". The map is done.
