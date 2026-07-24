---
id: T6
title: Decide how notebooks appear on the site
label: wayfinder:grilling
status: closed
assignee: vlzm (session 2026-07-24)
closed: 2026-07-24
blocked-by: [T5]
---

## Question

Given the researched options (ticket T5), which one do the two
canonical notebooks get on the site? The scenario page
(`scenarios/citibike.md`) links to both notebooks today; a portfolio
reader should see the runs without cloning the repo. Decide the
mechanism and whether it is a one-time export or a repeatable build
step.

## Resolution (2026-07-24)

Decided with the user (grilling). Two facts checked in the notebooks
reframed the T5 research first:

- `test_pipeline.ipynb` (17 cells) outputs 6 pandas HTML tables, one
  matplotlib PNG, and text. **No plotly.** So the research's central
  worry — interactive plotly breaking in a Markdown export — does not
  apply here. Option 1 is the easy, clean case.
- `forecast_pipeline.ipynb` (6 cells) has **no saved outputs** — it has
  not been run (or outputs were stripped).

**Mechanism — Option 1 only (nbconvert → Markdown leaf bundle).** Each
notebook becomes a Hugo leaf bundle under `content/docs/scenario/`
(e.g. `scenario/test-pipeline/index.md` with `index_files/` alongside).
`nbconvert --to markdown` extracts the matplotlib PNG automatically;
pandas tables render as raw HTML via the site's `unsafe = true`. Native
site page — nav, dark mode, search, selectable text, KaTeX. **No** HTML
escape hatch (Option 2) — with no plotly there is nothing interactive to
preserve, and a second per-notebook artifact is not worth it. Screenshots
(Option 4) rejected: text would not be selectable or searchable.

**Cadence — a committed script (Makefile target), run by hand, that does
not execute the notebooks.** The script runs `nbconvert --to markdown`
on the already-executed notebooks and prepends Hugo front matter
(`title`, `weight`), writing into `content/docs/scenario/`. The user runs
the notebooks in Jupyter as usual; the script only picks up saved
outputs. **No `nbconvert --execute` in CI** — a full run is 9–35 min,
needs the DVC data, and hits the torch+lightgbm OpenMP conflict; the CI
runner is the wrong place for it.

**Precondition passed to T10:** because the script converts only saved
outputs, `forecast_pipeline.ipynb` must be run once (by the user) before
its page shows results; until then its page is code-only. T10, when it
builds the scenario section, runs this export script to create the two
notebook pages and points `scenarios/citibike.md` at them instead of at
the raw `.ipynb` files.

Wide-table overflow may still need a few lines of CSS in the theme
override — a small implementation detail for whoever builds the export,
not a separate decision.
