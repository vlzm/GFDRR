---
title: "Visualization"
weight: 7
---

# Visualization — from a finished run to the screen

This document explains `app/`, the visualization side of the framework: the
path from a finished run to the screen.
The app has three parts, in the order a run passes through them:

1. `runner.py` runs one scenario end to end and saves the result.
2. `artifacts.py` builds the run artifact: the folder of tables the UI reads.
3. `main.py`, `ui_shared.py` and `views/` are the Streamlit web interface — a
   pure reader of saved artifacts.

The artifact contract is
[Notations.md §12](../reference/notations.md#12-run-artifacts-the-files-the-ui-reads).
Each function's exact behavior is in its docstring — this page gives the map
and the design.

## Code Map

| File | Main role |
|---|---|
| `gbp/consumers/run.py` | Owns `RunRequest`, `build_graph_data`, and `run_scenario`; `app/runner.py` is the thin terminal CLI over it (`python app/runner.py --help` lists the flags). |
| `gbp/artifacts.py` | Owns the `build_*` functions, the `METRICS` table, and save/load. |
| `domains/citybike/ml/ops/evaluation.py` | The two-level evaluation (Notations.md §17): a second terminal entry point, `python -m domains.citybike.ml.ops.evaluation` ([ml-toolkit.md](ml-toolkit.md)). |
| `main.py` | The Streamlit entry point: the page list and navigation. |
| `backend.py` | The one place the app chooses its backend — local files, or HTTP when `API_URL` is set ([api.md](../reference/api.md)). |
| `ui_shared.py` | Shared page helpers: cached typed loaders, scenario pickers, the KPI row, charts. |
| `views/*.py` | One file per page. Every page reads saved tables and draws them. |

One run flows through the app like this:

```text
run_scenario()                    (sizes, runs, validates)
  -> build_run_tables()           (five tables from one finalized journal)
  -> save_run()                   (parquet files first, meta.json last)
  -> data/runs/<run_name>/        (the run artifact)
  -> backend.current()            (disk, or HTTP when API_URL is set)
  -> ui_shared typed loaders      (load_panel, load_meta, ...)
  -> views/*                      (slice and draw)
```

## The Run Artifact

One saved run is a folder `data/runs/<run_name>/` with six files:

| File | One row per | Built by |
|---|---|---|
| `meta.json` | — (parameters, `inputs`, `code_version`, invariant `violations`, whole-run `totals`, the sized state) | `build_meta` |
| `flows.parquet` | flow event (the journal widened with the measures) | `flows_with_measures` |
| `panel.parquet` | `(period_id, facility_id, commodity_category)` | `flows_to_panel` (model layer) |
| `arcs.parquet` | arc — a `(flow_id, move_id)` physical edge of a trip | `build_arcs` |
| `flow_totals.parquet` | flow, with its whole-trip values | `build_flow_totals` |
| `facilities.parquet` | facility, with coordinates and capacity | `build_facilities` |

Saved-run pages read these files only; the trip data is read once, when a
run is created. `DATA_DIR` moves the data folder.

## The Main Idea

The run path (`gbp/consumers/run.py`) splits the work by runtime. `build_graph_data` is the slow step
— it loads the CSV into `RawModelData` and resolves `ResolvedModelData`
([data-model.md](data-model.md)), takes minutes, and is independent of the
run parameters, so callers run it once and reuse it. `run_scenario` does one
run against loaded data described by one `RunRequest`: the forecast
substitution if asked (`--demand-source forecast`), then `run_and_save` —
the shared step that applies the truck fleet on a shallow copy if rebalancing
is on, sizes and runs the scenario (`run_sized_scenario`), and saves the
artifact (`artifacts.save_scenario_run`). The two-level evaluation
(`evaluate.py`) runs through the same `run_and_save`, so a run is built one
way from either entry. It must not modify `graph_data` — the Run page shares
one cached copy across runs.

`artifacts.py` computes everything once, at save time. `build_run_tables`
builds the five tables from one finalized journal; the panel itself is a
model-layer read-model (`flows_to_panel`,
[flow-journal.md](flow-journal.md)). A metric — one value the UI can show —
is described once in the `METRICS` list; the panel columns, the picker
labels, the KPI row, and `meta["totals"]` are all derived from it.

The UI reads through one front door: the typed accessors in `ui_shared.py`
(`load_panel`, `load_meta`, ...), which go through `backend.current()` —
local files, or the API client when `API_URL` is set ([api.md](../reference/api.md)).
Pages never import `api_client` and never check `API_URL` themselves. Every
page follows one pattern: pick the scenario pair, load its tables through
the cache, slice, draw. The one exception is the Run scenario page, which
starts a run through `backend.current().run_and_wait`.

The pages, and which artifact tables each reads:

| Page | File | Reads |
|---|---|---|
| Overview & compare | `views/home.py` | `meta.json` of every run |
| Run scenario | `views/run_scenario.py` | — (starts a run, then shows its meta) |
| Station map | `views/station_map.py` | `panel`, `facilities` |
| Trips map | `views/trips_map.py` | `arcs`, `facilities`, `panel` |
| Truck trips | `views/truck_trips.py` | `arcs` (the `rebalance` rows), `facilities` |
| Costs | `views/costs.py` | `flow_totals` |
| Distance & duration | `views/distance_duration.py` | `flow_totals` |
| Single facility | `views/facility_detail.py` | `panel`, `facilities` |
| Model monitoring | `views/model_monitoring.py` | not run artifacts: `data/ml/monitoring/` ([ml-toolkit.md](ml-toolkit.md)) |
| Download data | `views/downloads.py` | `flow_totals`, `panel` as CSV |

## Why It Is Built This Way

### Saved-Run Pages Read Saved Files

Every journal-level computation happens once, in `build_run_tables`, when
the run is saved. Pages only load parquet files, slice, and draw. This keeps
every page fast regardless of run size, and makes runs comparable: two
scenarios saved months apart are drawn from tables built by the same
builders. The rejected alternative — pages computing from the journal on
demand — would make page speed depend on run size and let two pages compute
the same value differently.

### `meta.json` Is Written Last

`save_run` writes the parquet files first and `meta.json` last, and
`list_runs` only shows folders that have a `meta.json`. An interrupted save
leaves a folder the UI never lists, instead of a half-readable run.

### One `METRICS` Table

Labels, KPI tiles, panel columns and `meta["totals"]` used to be four lists
to keep in sync. Describing each metric once and deriving the four from the
one list keeps them consistent.

### The Fleet And The Sized State Never Touch `graph_data`

Loading the graph data takes minutes, so the Run page caches one
`ResolvedModelData` and reuses it for every run. That only works because
`run_scenario` treats it as read-only: the truck fleet goes onto a shallow
copy, and the sized inventory and capacities stay inside
`run_sized_scenario`.

### `t0` Travels In `meta.json`

The UI shows wall-clock times, but it never loads the trip data where the
clock comes from. The runner saves `t0` and `period_len_hours` into
`meta.json`, and `period_start_time` rebuilds any period's start from those
two numbers.
