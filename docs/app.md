# The app, step by step

This document explains `app/` — the path from a finished run to the screen.

The app has three parts, in the order a run passes through them:

1. `runner.py` runs one scenario end to end and saves the result.
2. `artifacts.py` builds the run artifact: the folder of tables the UI reads.
3. `main.py`, `ui_shared.py` and `views/` are the Streamlit web interface — a
   pure reader of saved artifacts. It never simulates and never recomputes
   what the artifact builder has already computed.

The artifact contract is
[Notations.md §12](../Notations.md#12-run-artifacts-the-files-the-ui-reads).
What happens inside the simulator during a run is
[simulator.md](simulator.md); the journal functions the builders call are
explained in [flow_journal.md](flow_journal.md).

## Code Map

| File | Main role |
|---|---|
| `runner.py` | Owns `build_graph_data` and `run_scenario`; the terminal entry point. |
| `artifacts.py` | Owns the `build_*` functions, the `METRICS` table, and save/load. |
| `main.py` | The Streamlit entry point: the page list and navigation. |
| `ui_shared.py` | Shared page helpers: cached loaders, scenario pickers, the KPI row, colors, charts. |
| `views/*.py` | One file per page. Every page reads saved tables and draws them. |

## The Run Artifact

One saved run is a folder `data/runs/<run_name>/` with six files:

| File | One row per | Built by |
|---|---|---|
| `meta.json` | — (parameters, invariant `violations`, whole-run `totals`) | `build_meta` |
| `flows.parquet` | flow event (the journal widened with the measures) | `flows_with_measures` |
| `panel.parquet` | `(period_id, facility_id, commodity_category)` | `build_panel` |
| `arcs.parquet` | arc — a `(flow_id, move_id)` physical edge of a trip | `build_arcs` |
| `flow_totals.parquet` | flow, with its whole-trip values | `build_flow_totals` |
| `facilities.parquet` | facility, with coordinates and capacity | `build_facilities` |

Saved-run pages read these files. They do not read the raw CSV. The Run
scenario page reads the raw CSV only when it creates a new artifact. `DATA_DIR`
moves the data folder; without it, `data/` at the repository root is used
(`artifacts.data_dir`).

## Step 1: `runner.py` — Run And Save

Two functions split the work by runtime.

`build_graph_data(trips_path, ...)` is the slow step: it loads the raw CSV
into `RawModelData` and resolves it into `ResolvedModelData`
(see [dataloader.md](dataloader.md)). It takes minutes and is independent of
the run parameters, so callers run it once and reuse the result.

`run_scenario(graph_data, run_name=..., demand_scale_factor=..., ...)` does
one run against loaded data:

1. If rebalancing is on, apply the truck fleet to a shallow copy
   (`apply_truck_fleet`) and append the two rebalancing phases to
   `canonical_phases()`.
2. Call `run_sized_scenario`: size the initial state against
   `sizing_scale_factor`, run the demand at `demand_scale_factor`, collect
   the invariant violations. It is called with `validate=False`, so a
   violated invariant is recorded in `meta.json` instead of raising.
3. Build the five tables with `artifacts.build_run_tables`.
4. Build `meta.json` with `artifacts.build_meta`.
5. Save the folder with `artifacts.save_run`.

Note: `run_scenario` must not modify `graph_data`. The Run page shares one
cached `ResolvedModelData` across runs, so the sized state stays inside
`run_sized_scenario` and the truck fleet is applied to a shallow copy.

The terminal entry point wraps the same two calls:

```bash
python app/runner.py --run-name demand_x2 --demand-scale 2.0 --periods 50
python app/runner.py --run-name with_trucks --rebalancing --truck-homes depot_1,depot_1,depot_3
```

## Step 2: `artifacts.py` — Build The Tables

`build_run_tables(journal, ...)` builds everything from one finalized
journal:

```python
priced = flows_with_measures(journal, routes=routes, rates=rates, period_len=period_len)
arcs = build_arcs(journal, routes, facilities_geo)
return {
    "flows": priced,
    "panel": build_panel(journal, initial_inventory),
    "arcs": arcs,
    "flow_totals": build_flow_totals(priced, arcs),
    "facilities": build_facilities(facilities, facilities_geo, facilities_capacities),
}
```

### `build_panel`

The facility period panel: for each `(period_id, facility_id,
commodity_category)`, the period's values side by side. It starts from
`get_inventory_df` (the `quantity_sop` / `quantity_eop` columns) and merges
one marginal per column: `departed`, `arrived`, `redirected`, `lost_demand`,
`lost_dock_full`. Then `demand = departed + lost_demand`. Every map view and
hover box is a slice of this one table.

After each merge the builder checks that no events were dropped:

```python
if int(panel[name].sum()) != int(grouped[name].sum()):
    raise ValueError(f"panel dropped {name} events outside the inventory grid")
```

A mismatch means an event happened at a `(facility, commodity)` pair the
inventory grid does not know — a real data error, caught at build time.

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
one once — column name, full label, short label, unit, and whether it is a
panel column, enters `meta["totals"]`, or gets a KPI tile:

```python
Metric("lost_demand", "Lost demand (lost_demand)", "Lost (stockout)",
       panel_value=True, panel_total=True, kpi=True, more_is_worse=True),
```

`PANEL_VALUES`, the UI label dictionaries, the KPI row and the totals in
`meta.json` are all built from this one list. A metric with `panel_value=True`,
`panel_total=True`, or `kpi=True` is used in the matching place. Adding a metric
does not require separate label, picker, KPI and totals lists.

### Save And Load

`save_run` writes the five parquet files first and `meta.json` last, so a
folder with a `meta.json` is always a complete artifact — `list_runs` keys on
that file. `load_run_table` and `load_run_meta` are the only artifact read
paths.

The Run scenario page calls `next_free_run_name` before it runs. A taken name
gets a `_version_2`, `_version_3`, … suffix there, so that page does not
overwrite a saved run. A caller that passes an existing `run_name` directly to
`save_run` replaces the files in that folder.

## Step 3: The Streamlit App

`main.py` registers the pages and runs navigation. Everything else pages
share lives in `ui_shared.py`:

- `load_table` / `load_meta` — the cached read path. The cache key includes
  the file's modification time, so a rewritten artifact invalidates itself.
- `pick_scenario_pair` — the sidebar pickers for scenario A and the optional
  comparison scenario B. The picks live in `st.session_state`, so every page
  shows the same pair.
- `kpi_row` and `validation_badge` — the whole-run totals as tiles (with the
  B − A delta when B is chosen) and the green/red invariant badge.
- `panel_slice` / `panel_commodity_slice` — the one place that defines the
  "All types" pick: it sums the panel value columns over the bike types.
- `period_start_time` — turns a period id into wall-clock time from the
  `t0` and `period_len_hours` saved in `meta.json`.
- the palette, `sequential_colors` / `diverging_colors` for the maps, and
  `aggregate_flow_totals` / `level_line_chart` for the cost and
  distance/duration charts.

The pages, and which artifact tables each reads:

| Page | File | Reads |
|---|---|---|
| Overview & compare | `views/home.py` | `meta.json` of every run |
| Run scenario | `views/run_scenario.py` | — (calls `runner.run_scenario`, then shows the new run's meta) |
| Station map | `views/station_map.py` | `panel`, `facilities` |
| Trips map | `views/trips_map.py` | `arcs`, `facilities`, `panel` (for the bike-type filter) |
| Truck trips | `views/truck_trips.py` | `arcs` (the `rebalance` rows), `facilities` |
| Costs | `views/costs.py` | `flow_totals` |
| Distance & duration | `views/distance_duration.py` | `flow_totals` |
| Single facility | `views/facility_detail.py` | `panel`, `facilities` |
| Download data | `views/downloads.py` | `flow_totals`, `panel` as CSV downloads |

"Run scenario" is the one page that computes anything slow: it caches
`build_graph_data` with `st.cache_resource` (one load per CSV path) and calls
`runner.run_scenario` with `on_progress=st.write`, so the stages appear in a
status box. Every other page follows one pattern: pick the scenario pair,
load its tables through the cache, slice, draw.

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
