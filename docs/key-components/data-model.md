# The data loaders

This document explains `gbp/loaders/`: the code that turns the raw Citi Bike
trip CSV into `ResolvedModelData`, the input tables the simulator reads. The
simulator reads them through its contract `ScenarioInputs`
(`gbp/consumers/simulator/inputs.py`); `ResolvedModelData` is one supplier of
that contract.

The terms are the same as in [`Notations.md`](../../Notations.md). What the
simulator does with these tables is [simulation-engine.md](simulation-engine.md); the journal
functions used here are explained in [flow-journal.md](flow-journal.md).
Each function's exact behavior is in its docstring — this page gives the map
and the design.

## Code Map

| File | Main role |
|---|---|
| `download.py` | Downloads the published monthly trip CSVs into `data/raw/` and loads files of either published schema into the one trips schema. |
| `dataloader_raw.py` | Reads the trip CSV, builds raw tables, and owns `RawModelData`. |
| `dataloader_graph.py` | Builds `ResolvedModelData` from `RawModelData`; owns `apply_truck_fleet`, `apply_forecast_demand`, and `attach_simulation`. |

One scenario flows through the two files like this:

```text
raw trip CSV
  -> RawModelData          (raw tables with raw column names)
  -> ResolvedModelData     (canonical tables: entities, attributes,
                            period grid, historical journal, marginals,
                            base replay initial inventory, routes)
  -> the simulator runs
  -> attach_simulation()   (fills the simulated_* marginals)
```

## The Main Idea

`RawModelData` loads everything once: the cleaned trip table (with a
processed-parquet copy in `data/processed/`, Notations.md §15), the station
table derived from the trip endpoints, and the synthetic depots, trucks, and
rates generated from a seed. Everything there still uses the raw column
names. `ResolvedModelData` renames them to the canonical schema
([Notations.md §4](../../Notations.md#4-facility-and-its-roles-in-a-trip)) —
`station_id`/`depot_id` become `facility_id`, `truck_id` becomes
`resource_id`, `rideable_type` becomes `commodity_category` — in the small
`get_*` functions at the top of `dataloader_graph.py`. Past this boundary,
only the canonical names exist.

`ResolvedModelData.__init__` then builds the scenario tables in dependency
order (its class docstring lists the order): the time grid, the historical
flow journal, the base replay initial inventory, the historical marginals,
and `routes` — the object that answers `distance_km` and `duration_periods`
for facility pairs
([Notations.md §13](../../Notations.md#13-routing-distance-and-travel-time-between-facilities)).

The historical journal is built with the same builders the simulator uses
(`departed_events`, `arrived_events`, `finalize_flows`): each completed trip
is one flow with two events, `flow_id` gets a `hist_` prefix, and the order
columns come from `stamp_history_ordering`
([flow-journal.md](flow-journal.md)). The marginals — departures, arrivals,
the OD matrix — are computed from that journal with the read-models from
`flows.py`; `historical_demand_df` equals `historical_departures_df`,
because in history every wanted trip departed.

Two sizing helpers live here and are called by `size_state_for_demand` after
a sizing run ([decision record](../decisions/sizing-run.md)):
`get_replay_initial_inventory_df` lifts each station's lowest per-step
inventory to zero, and `get_replay_capacities_df` sets capacity to each
station's peak occupancy. Their docstrings explain the per-step subtlety.

The loaders check tables as soon as they build them, so bad data fails at
load time, not mid-run: the raw trips against `TRIPS_SCHEMA`, the historical
journal against the journal schema, and every engine-facing table against
`ENGINE_TABLE_SCHEMAS`, plus one cross-table assert (period-0 inventory
equals the initial inventory).

## The Two Run-Parameter Substitutions

Two functions return a shallow copy of the resolved data with a few fields
replaced; everything else stays shared.

`apply_truck_fleet(resolved, truck_homes, ...)` rebuilds only the three
resource tables. The truck fleet is a run parameter, not part of the loaded
data; the Run page and the `--truck-homes` runner flag call it.

`apply_forecast_demand(resolved, forecast_demand_df, forecast_periods_df)`
makes the copy a forecast run
([decision record](../decisions/forecast-replaces-only-demand.md)): the
forecast demand table goes into the `historical_demand_df` slot — the one
demand slot the engine reads — together with the forecast period grid and an
OD matrix pooled from history by hour of week
(`map_od_matrix_by_hour_of_week`). It is a load boundary like the loader
itself: schema checks plus two cross-table checks, described in its
docstring. The callers are `app/runner.py` (`--demand-source forecast`) and
`app/evaluate.py`.

## After A Run: `attach_simulation`

`attach_simulation(resolved, simulated_flows_df, ...)` fills the
`simulated_*` attributes from a finished run's journal. Every simulated
marginal is derived with the same read-model function as its historical
twin, so the two sets are directly comparable: in a base replay they are
equal, table by table.

## Why It Is Built This Way

### Stations Come From The Trips

There is no station registry in the raw data. Deriving stations from the
trip endpoints guarantees that every station the journal mentions exists in
the facility tables, and keeps unused stations out. Depots and trucks have
no source data at all, so they are synthesized from a seed — deterministic
for a given scenario.

### The Historical Journal Uses The Simulator's Builders

`get_historical_flows_df` could have built its rows by hand. Calling the
shared builders instead is what makes the base replay checkable row by row:
the historical journal and a replay run's journal are produced by the same
primitives, so any difference between them is a real behavior difference,
never a formatting one.

### The Initial State Is Computed, Not Loaded

Today's real station inventory is a current observation, unrelated to the
historical month being replayed; limiting demand against it would create
stockouts that never happened. The loader instead computes the smallest
starting inventory under which history replays cleanly — a pure function of
the journal ([decision record](../decisions/sizing-run.md)).

### One Read-Model, Two Views

Every `historical_*` marginal and its `simulated_*` twin are produced by one
function from `flows.py`. The container just calls it twice, on two
journals, so the base replay check compares tables that share one
definition.
