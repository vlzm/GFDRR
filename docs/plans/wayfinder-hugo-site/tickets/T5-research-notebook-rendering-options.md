---
id: T5
title: Research notebook rendering options
label: wayfinder:research
status: closed
assignee: research-agent (charting session 2026-07-24)
closed: 2026-07-24
blocked-by: []
---

## Question

The canonical scenario is two Jupyter notebooks
(`notebooks/test_pipeline.ipynb`, `notebooks/forecast_pipeline.ipynb`).
What are the realistic ways to show a notebook on a hugo-book site, and
what does each cost? Cover at least: convert to Markdown with
`nbconvert` (by hand or a build step) — what happens to plots and wide
tables; link to the notebook on GitHub and rely on GitHub's renderer;
embed static screenshots of key cells. For each option: maintenance
burden when the notebook changes, and how it looks to a reader.
Findings go to `research/notebook-rendering.md` next to this map.

## Resolution (2026-07-24)

Findings: [research/notebook-rendering.md](../research/notebook-rendering.md).
Five options compared. Short version: `nbconvert --to markdown` into a
Hugo leaf bundle gives a native-looking site page (front matter must be
added by a small script; plotly figures must be exported as static PNG);
`nbconvert --to html --embed-images` into `static/` keeps plotly
interactive but looks like Jupyter, not the site; GitHub/nbviewer links
are unreliable for a portfolio reader; screenshots are curated but
high-maintenance; nb2hugo is dead, Quarto works but is heavy for two
notebooks. Researcher's recommendation: markdown bundle as the primary
page plus an embedded-HTML "full interactive notebook" link. The
decision is ticket T6.
