# 04 — ui_shared reductions: precompute vs render-time helper

Type: grilling
Status: resolved
Blocked by: 01

## Decision

Every reduction stays in `app/` as a render-time helper; the artifact contract
does not grow. Each depends on a UI-chosen input (period, commodity, the A/B
pair, facility subset, top-N), which the builder cannot precompute, so §12's
rule is already satisfied. Only the `PANEL_VALUES` / `METRICS` import repoints
to `gbp`. The `views/home.py` all-runs summary may read a small `gbp.artifacts`
helper over every `meta.json`, but that is an index across runs, not a new
in-artifact table. See `docs/plans/app_to_gbp_seam.md`.

## Question

`app/ui_shared.py` (556 lines) is mostly genuine presentation (Plotly, color
ramps, KPI row) that correctly stays in `app/`. But it also holds a cluster of
dataframe reductions run at render time. Decide whether each becomes a
**precomputed artifact table** or a **gbp domain helper** called at render.

The reductions in question:
- `aggregate_flow_totals` (`ui_shared.py:366`) — groups `flow_totals` by
  period / commodity / facility.
- `panel_commodity_slice` (`:261`) and `panel_slice_pair` (`:291`) — groupby-
  sum and the A/B diff join between two runs.
- `top_facilities` (`:436`) — ranks origins.
- `arc_map_rows` (`:449`) — aggregates arcs into map rows.
- Related page-level computation: `views/home.py:30` rebuilds an all-runs
  summary table from every `meta.json` on each render (a precompute-able
  index); `views/station_map.py` builds per-facility diff columns via
  `panel_slice_pair`.

The genuine fork (why this is blocked by 01):
- **Precompute** — the builder (ticket 01) writes these as new artifact
  tables, and the page only reads + draws. Matches Notations §12 "must not
  compute anything the artifact builder can precompute", but grows the
  artifact contract.
- **Render-time gbp helper** — the reductions move to a gbp domain function
  the page calls each render. Smaller contract, keeps per-run-pair diffs
  (which the builder cannot precompute, since the pair is chosen in the UI)
  out of the artifact.

Decisions to reach: for each reduction, precompute-into-artifact vs
render-time-gbp-helper vs stays-in-app; and where the A/B pair diff (which
depends on a UI-chosen pair) sits.

Consult: `/grilling`, `prototype` (a rough table shape helps the call),
`ousterhout-review`. Do not edit code — record the per-reduction verdict.
