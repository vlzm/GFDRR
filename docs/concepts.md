# The five concepts

Every other page of this documentation assumes five words: period, demand,
station inventory, flow journal, run artifact. This page defines each one in a
few sentences with one small example. The exact contracts live in
[Notations.md](../Notations.md) — open it by section when you need a column
list, not before.

## Period

A period is one step of the simulation clock. Everything in a run — a
departure, a docking, a redirect — happens in some period, identified by
`period_id`: 0, 1, 2, … By default one period is one hour (`period_len`), and
period 0 starts at `t0`, the hour of the earliest historical trip.

Example: with `t0 = 2026-01-01 00:00`, period 5 covers 05:00–06:00 of
January 1. A trip that departs in period 5 and docks in period 6 took one
period.

Exact contract: [Notations.md §6](../Notations.md#6-time).

## Demand

Demand is the number of trips users wanted to start, per period, station, and
bike type: a table with the columns `period_id`, `facility_id`,
`commodity_category`, `quantity`. The simulator reads this table the same way
whether the quantities come from history or from a forecast model. Wanted does
not mean happened: demand splits exactly into `departed` plus `lost` with
`reason = "stockout"`.

Example: demand of 5 at station `s1` in period 3, but only 3 bikes are docked
there — 3 trips depart and 2 are lost to a stockout.

Exact contract: [Notations.md §2](../Notations.md#2-core-state) and
[§7](../Notations.md#7-departures).

## Station inventory

Inventory is how many bikes each station holds at a moment: one row per
station and bike type, columns `facility_id`, `commodity_category`,
`quantity`. It changes only by whole bikes — `-1` when a bike leaves a dock,
`+1` when a bike docks — and every change is recorded as a flow event, so
inventory at any moment can be recomputed from the initial inventory plus the
journal.

Example: `s1` starts with 5 bikes; one trip departs in period 0; from that
step on `s1` holds 4.

Exact contract: [Notations.md §2](../Notations.md#2-core-state).

## Flow journal

The flow journal is the run's event table and its single source of truth: one
row per event of one bike's movement, and rows are only appended, never
edited. `event_type` is one of four outcomes — `departed`, `arrived`,
`redirected`, `lost`. Everything else the project shows (inventories, maps,
costs) is computed from the journal.

Example: the smallest journal — one trip `s1 → s2`, one flow, two rows
(showing five of the columns; §0 lists them all):

| flow_id | event_type | source_id | planned_target_id | period_id |
|---|---|---|---|---|
| sim_0_0 | departed | s1 | s2 | 0 |
| sim_0_0 | arrived | s1 | s2 | 1 |

Exact contract: [Notations.md §0](../Notations.md#0-the-flow-event-schema-the-symbol-table)
(the columns) and [§1](../Notations.md#1-the-four-flow-outcomes-and-the-two-reasons)
(the four outcomes).

## Run artifact

A run artifact is a finished run saved to disk: the folder
`data/runs/<run_name>/`, built once by `app/artifacts.py`. It holds
`meta.json` (the run's parameters, totals, and invariant violations) and five
parquet tables; the two you meet first are `flows.parquet` — the journal —
and `panel.parquet` — per period and station: inventory, demand, departures,
losses. The web interface only reads these files; it never simulates.

Example: `python app/runner.py --run-name demo` writes `data/runs/demo/`, and
`streamlit run app/main.py` lists it.

Exact contract: [Notations.md §12](../Notations.md#12-run-artifacts-the-files-the-ui-reads).

## Where to go next

Scenarios 1–3 of the [scenario catalog](explanation/scenarios.md) show these
concepts working together on real journal tables, each re-run by a test.
