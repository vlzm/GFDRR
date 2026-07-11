# The app, step by step

This document explains `app/` — the path from a finished run to the screen.

The app has three parts, in the order a run passes through them:

1. `runner.py` runs one scenario end to end and saves the result.
2. `artifacts.py` builds the run artifact: the folder of tables the UI reads.
3. `main.py`, `ui_shared.py` and `views/` are the Streamlit web interface — a
   pure reader of saved artifacts. It never simulates and never recomputes
   what the artifact builder has already computed.

The artifact contract is
[Notations.md §12](../../Notations.md#12-run-artifacts-the-files-the-ui-reads).
What happens inside the simulator during a run is
[simulator.md](simulator.md); the journal functions the builders call are
explained in [flow_journal.md](flow_journal.md).

The UI has two backends. Without `API_URL` it reads run artifacts from the
local disk, and the Run scenario page runs scenarios in the same process.
With `API_URL` set it fetches the same runs from the run-artifact API over
HTTP, and the Run scenario page starts runs on the server. This document
describes the local backend; the switch and the server side are in
[api.md](api.md).

## Code Map

| File | Main role |
|---|---|
| `runner.py` | Owns `build_graph_data` and `run_scenario`; the terminal entry point. |
| `artifacts.py` | Owns the `build_*` functions, the `METRICS` table, and save/load. |
| `evaluate.py` | The two-level evaluation (Notations.md §17): a second terminal entry point that runs the simulator on the actual demand and on each model's forecast, saves normal run artifacts, and writes `data/ml/evaluation/<month>/comparison.csv`. |
| `main.py` | The Streamlit entry point: the page list and navigation. |
| `backend.py` | The one place the app chooses its backend — local files, or HTTP when `API_URL` is set — for reading runs and for starting them. |
| `ui_shared.py` | Shared page helpers: cached loaders, scenario pickers, the KPI row, colors, charts. |
| `views/*.py` | One file per page. Every page reads saved tables and draws them. |

## The Run Artifact

One saved run is a folder `data/runs/<run_name>/` with six files:

| File | One row per | Built by |
|---|---|---|
| `meta.json` | — (parameters, `inputs` — raw file names, `code_version` — git commit, invariant `violations`, whole-run `totals`, the sized state: `initial_inventory_bikes`, `station_capacity_docks`) | `build_meta` |
| `flows.parquet` | flow event (the journal widened with the measures) | `flows_with_measures` |
| `panel.parquet` | `(period_id, facility_id, commodity_category)` | `flows_to_panel` (model layer) |
| `arcs.parquet` | arc — a `(flow_id, move_id)` physical edge of a trip | `build_arcs` |
| `flow_totals.parquet` | flow, with its whole-trip values | `build_flow_totals` |
| `facilities.parquet` | facility, with coordinates and capacity | `build_facilities` |

Saved-run pages read these files. They do not read the trip data. The Run
scenario page reads the trip data (the raw CSV, or its processed parquet copy
in `data/processed/` — Notations.md §15) only when it creates a new artifact.
`DATA_DIR` moves the data folder; without it, `data/` at the repository root
is used (`artifacts.data_dir`).

## Step 1: `runner.py` — Run And Save

Two functions split the work by runtime.

`build_graph_data(trips_path, ...)` is the slow step: it loads the raw CSV
into `RawModelData` and resolves it into `ResolvedModelData`
(see [dataloader.md](dataloader.md)). It takes minutes and is independent of
the run parameters, so callers run it once and reuse the result.

`run_scenario(graph_data, run_name=..., demand_scale_factor=..., ...)` does
one run against loaded data:

1. If `demand_source="forecast"`, load the named forecast from
   `data/ml/forecasts/` and put its demand table in place of the historical
   one (`apply_forecast_demand`, see [dataloader.md](dataloader.md)). The
   run becomes a forecast run (Notations.md §11): its period grid is the
   forecast horizon, and the OD matrix is the historical one pooled per
   hour of week.
2. If rebalancing is on, apply the truck fleet to a shallow copy
   (`apply_truck_fleet`) and append the two rebalancing phases to
   `canonical_phases()`.
3. Call `run_sized_scenario`: size the initial state against
   `sizing_scale_factor`, run the demand at `demand_scale_factor`, collect
   the invariant violations. It is called with `validate=False`, so a
   violated invariant is recorded in `meta.json` instead of raising.
4. Save the folder with `artifacts.save_scenario_run` — the one operation
   that builds the five tables (`build_run_tables`) and `meta.json`
   (`build_meta`) from the run result and the scenario data, then writes
   them (`save_run`). Besides the run parameters, `meta.json` records where
   the run came from: `inputs` (the raw file name, taken from
   `data.trips_path`), `code_version` (the git commit, `-dirty` when the
   working tree had uncommitted changes), `demand_source`, and — for a
   forecast run — `forecast_name`.

Note: `run_scenario` must not modify `graph_data`. The Run page shares one
cached `ResolvedModelData` across runs, so the sized state stays inside
`run_sized_scenario` and the truck fleet is applied to a shallow copy.

The terminal entry point wraps the same two calls:

```bash
python app/runner.py --run-name demand_x2 --demand-scale 2.0 --periods 50
python app/runner.py --run-name with_trucks --rebalancing --truck-homes depot_1,depot_1,depot_3
```

The full flag list of `app/runner.py`:

| Flag | Meaning | Default |
|---|---|---|
| `--run-name` | Artifact folder name. Required. | — |
| `--demand-scale` | Demand multiplier the run faces (`demand_scale_factor`). | `1.0` |
| `--sizing-scale` | Demand the state is sized for (`sizing_scale_factor`). | `1.0` |
| `--periods` | How many periods to step. | `50` |
| `--trips-path` | Path to the raw trip CSV. | `data/raw/202601-citibike-tripdata_1.csv` |
| `--demand-source` | Where the demand table comes from: `history` or `forecast`. | `history` |
| `--forecast-name` | Saved forecast to run on (a folder under `data/ml/forecasts/`); required with `--demand-source forecast`. | — |
| `--rebalancing` | Run with the two overnight rebalancing phases. | off |
| `--truck-homes` | Home depot per truck, comma-separated; the list length is the fleet size. | 5 trucks at `depot_1` |
| `--truck-capacity` | Bikes one truck can carry. | `20` |
| `--routing` | Distance and travel-time mode: `haversine` or `osrm`. | `haversine` |
| `--osrm-url` | OSRM server URL; read only with `--routing osrm`. | `http://127.0.0.1:5000` |

`--routing osrm` needs a running OSRM server; the setup is
[osrm_setup.md](../guides/osrm_setup.md).

## Step 2: `artifacts.py` — Build The Tables

`build_run_tables(journal, ...)` builds everything from one finalized
journal:

```python
priced = flows_with_measures(journal, routes=routes, rates=rates, period_len=period_len)
arcs = build_arcs(journal, routes, facilities_geo)
panel = flows_to_panel(journal, initial_inventory)[PANEL_KEYS + PANEL_VALUES]
return {
    "flows": priced,
    "panel": panel,
    "arcs": arcs,
    "flow_totals": build_flow_totals(priced, arcs),
    "facilities": build_facilities(facilities, facilities_geo, facilities_capacities),
}
```

### The panel (`flows_to_panel`)

The facility period panel is a read-model of the journal and lives in the
model layer: `gbp.model.flows_to_panel`. For each `(period_id, facility_id,
commodity_category)` it gives the period's values side by side. It starts from
`get_inventory_df` (the `quantity_sop` / `quantity_eop` columns) and merges
one marginal per column: `departed`, `arrived`, `redirected`, `lost_demand`,
`lost_dock_full`. Then `demand = departed + lost_demand`. Every map view and
hover box is a slice of this one table.

After each merge the read-model checks that no events were dropped:

```python
if int(panel[name].sum()) != int(grouped[name].sum()):
    raise ValueError(f"panel dropped {name} events outside the inventory grid")
```

A mismatch means an event happened at a `(facility, commodity)` pair the
inventory grid does not know — a real data error, caught at build time.
`artifacts.py` only selects the columns (`PANEL_KEYS + PANEL_VALUES`), so a
metric named in `METRICS` but missing from the model fails loudly at build
time.

### `build_arcs`

One row per arc: it pairs each arc's opening `departed` with the event that
closed the arc (`arrived`, `redirected` or `lost`) on `(flow_id, move_id)`.
`target_id` is where the arc actually ended: the realized target when it
docked, the planned target when it bounced or was lost there. Each row
carries `distance_km` (measured by the run's routing mode) and the endpoint
coordinates, so the trips map draws arcs without joining another table. A
stockout `lost` has no `departed` row, so it produces no arc.

### `build_flow_totals`

One row per flow with its whole-trip values. It joins three sources: the
flow's opening event (origin `source_id`, `start_period`), its terminal event
(`event_type`, `reason`, `end_period`, `duration_periods`, `cost`), and the
sum of its arcs' `distance_km`. A flow with two terminal events raises — that
would be a double close. Two kinds of rows are not here: a stockout loss (it
has no flow; it lives in the panel as `lost_demand`) and a flow still riding
when the run ends (no terminal event yet).

### The `METRICS` Table

A metric is one value the UI can show. The `Metric` dataclass describes each
one once — the column name (`name`), the display title (`title`), the short
label for the map hover box (`short`), the unit, and whether it is a panel
column, enters `meta["totals"]`, or gets a KPI tile:

```python
Metric("lost_demand", "Lost demand", "Lost (stockout)",
       panel_value=True, panel_total=True, kpi=True, more_is_worse=True),
```

The full picker label — the title plus the column name in braces, here
"Lost demand (lost_demand)" — is not stored; the property `Metric.label`
builds it from the two fields.

`PANEL_VALUES`, the UI label dictionaries, the KPI row and the totals in
`meta.json` are all built from this one list. A metric with `panel_value=True`,
`panel_total=True`, or `kpi=True` is used in the matching place. A metric whose
total comes from `flow_totals` instead of the panel sets `flow_value` (the
column) and `flow_agg` (`"sum"` or `"mean"`) — `cost`, `distance_km` and
`mean_duration_periods` are described this way, and `build_totals` computes
every total from the list. Adding a metric does not require separate label,
picker, KPI and totals lists.

### Save And Load

Before writing anything, `save_run` checks every table against its pandera
schema (`RUN_TABLE_SCHEMAS`); a wrong column set raises "run tables break
their schemas" and no file is written. Then it writes the five parquet files
first and `meta.json` last, so a folder with a `meta.json` is always a
complete artifact — `list_runs` keys on that file.

`load_run_table` and `load_run_meta` are the raw file reads. Pages never
call `load_run_table` themselves: they go through the `ui_shared` typed
accessors, which reach it via the disk backend (`backend.py`). Outside the
UI it is called by `app/evaluate.py`, which compares the panels of its
evaluation runs. `load_run_meta` has more callers: the API endpoints in
`app/api.py` and the runner's terminal entry point, which prints the totals
of the run it just saved.

A run started from the Run scenario page goes through the free-name rule:
the disk backend (`backend.py`) calls `next_free_run_name`; with `API_URL`
set the server resolves the name (see [api.md](api.md)). A taken name gets a
`_version_2`, `_version_3`, … suffix either way, so that page does not
overwrite a saved run. A caller that passes an existing `run_name` directly
to `save_run` replaces the files in that folder.

## Step 3: The Streamlit App

`main.py` registers the pages and runs navigation. Everything else pages
share lives in `ui_shared.py`:

- the run-artifact loader — the one front door to a saved run. Typed
  accessors per table (`load_panel`, `load_arcs`, `load_flow_totals`,
  `load_facilities`, `load_meta`) plus `rebalancing_settings` for the
  rebalancing block of `meta.json`. The accessors read through
  `backend.current()` (`backend.py`), where the disk-or-API choice is made
  once. On the disk backend the cache key includes the file's modification
  time, so a rewritten artifact invalidates itself. With `API_URL` set the
  key is a constant, because a served artifact never changes
  ([api.md](api.md)).
  Old-artifact fallbacks live here: `load_arcs(run, flow_type=...)` handles
  arcs saved before the `flow_type` column existed.
- `pick_scenario_pair` — the sidebar pickers for scenario A and the optional
  comparison scenario B. The picks live in `st.session_state`, so every page
  shows the same pair.
- `kpi_row` and `validation_badge` — the whole-run totals as tiles (with the
  B − A delta when B is chosen) and the green/red invariant badge.
  `delta_b_minus_a` owns the comparison convention (direction B − A, the
  ASCII sign `st.metric` reads) for every difference tile.
- `panel_slice` / `panel_commodity_slice` — the one place that defines the
  "All types" pick: it sums the panel value columns over the bike types.
- `period_start_time` — turns a period id into wall-clock time from the
  `t0` and `period_len_hours` saved in `meta.json`.
- the palette, `sequential_colors` / `diverging_colors` for the maps, and
  `aggregate_flow_totals` / `level_line_chart` for the cost and
  distance/duration charts.
- `FlowTotalsView` / `flow_totals_page` — the shared body of a `flow_totals`
  metric page (whole-run tiles with the B − A difference, or the per-period
  chart with the facility multiselect). The Costs and Distance & duration
  pages are configs over this one module.
- `arc_map_rows` / `arc_deck` — the shared arc map: group arcs into map rows
  (the endpoint coordinates ride on every arc row) and draw the pydeck
  `ArcLayer`. The Trips map and Truck trips pages keep only their filter,
  colors, and tooltip.

The pages, and which artifact tables each reads:

| Page | File | Reads |
|---|---|---|
| Overview & compare | `views/home.py` | `meta.json` of every run |
| Run scenario | `views/run_scenario.py` | — (starts a run through `backend.current()`, then shows the new run's meta) |
| Station map | `views/station_map.py` | `panel`, `facilities` |
| Trips map | `views/trips_map.py` | `arcs`, `facilities`, `panel` (for the bike-type filter) |
| Truck trips | `views/truck_trips.py` | `arcs` (the `rebalance` rows), `facilities` |
| Costs | `views/costs.py` | `flow_totals` |
| Distance & duration | `views/distance_duration.py` | `flow_totals` |
| Single facility | `views/facility_detail.py` | `panel`, `facilities` |
| Model monitoring | `views/model_monitoring.py` | not run artifacts: the metrics table and drift reports in `data/ml/monitoring/`, saved by `python -m gbp.ml.monitoring` |
| Download data | `views/downloads.py` | `flow_totals`, `panel` as CSV downloads |

"Run scenario" is the one page that starts anything slow: it calls
`backend.current().run_and_wait(...)` with `on_progress=st.write`, so the
stages appear in a status box. On the disk backend that runs
`runner.run_scenario` in this process (with `build_graph_data` cached by
`st.cache_resource`, one load per CSV path); on the API backend it queues
the run on the server and polls its status. Every other page follows one
pattern: pick the scenario pair, load its tables through the cache, slice,
draw.

## Why It Is Built This Way

### Saved-Run Pages Read Saved Files

Every journal-level computation happens once, in `build_run_tables`, when the
run is saved. Pages only load parquet files, slice them, and draw. This keeps
every page fast regardless of run size, and it makes runs comparable: two
scenarios saved months apart are drawn from tables built by the same
builders.

### `meta.json` Is Written Last

`save_run` writes the parquet files first and `meta.json` last, and
`list_runs` only shows folders that have a `meta.json`. An interrupted save
leaves a folder the UI never lists, instead of a half-readable run.

### One `METRICS` Table

Labels, KPI tiles, panel columns and `meta["totals"]` used to be four lists
to keep in sync. Describing each metric once and deriving the four from the
one list keeps them consistent. A metric marked with `panel_value=True`,
`panel_total=True`, or `kpi=True` is used in the matching UI or totals output.

### The Fleet And The Sized State Never Touch `graph_data`

Loading the graph data takes minutes, so the Run page caches one
`ResolvedModelData` and reuses it for every run. That only works because
`run_scenario` treats it as read-only: the truck fleet goes onto a shallow
copy, and the sized inventory and capacities stay inside
`run_sized_scenario`.

### `t0` Travels In `meta.json`

The UI shows wall-clock times ("Period 37 starts 2026-02-02 13:00"), but it
never loads the trip data where the clock comes from. The runner saves `t0`
and `period_len_hours` into `meta.json`, and `period_start_time` rebuilds any
period's start from those two numbers.
