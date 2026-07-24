---
id: T10
title: Move the docs pages into the site sections
label: wayfinder:task
status: open
assignee:
blocked-by: [T2, T4, T6]
---

## Question

Move the in-scope `docs/` pages into the site sections fixed by T2 and
adapt each one. The folder-to-section map (from T2): `scenarios/` →
`scenario/`, `key-components/` → `architecture/`, `decisions/` →
`decisions/`, `getting-started/` → `getting-started/`, `how-to/` →
`how-to/`, `reference/` → `reference/`. For each moved page: add hugo
front matter (`title`, `weight` for order within its section, `math: true`
/ `mermaid: true` where needed), fix relative links (cross-section links,
images in `assets/`), and remove the page from the old `docs/` tree. The
Notations link target comes from T4; notebook links come from T6 — hence
the block on both. Leaves the landing page (T3) and the root `README.md`
rewrite (still fog) out of this ticket.

From T6: build the two notebook pages by running the committed
`nbconvert --to markdown` export script (Option 1 leaf bundles under
`content/docs/scenario/`), and repoint `scenarios/citibike.md` at those
pages instead of the raw `.ipynb`. Precondition: `forecast_pipeline.ipynb`
must be run once so it has saved outputs, otherwise its page is code-only.
