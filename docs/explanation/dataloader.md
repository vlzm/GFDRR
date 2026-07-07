# The data loaders, step by step

This document explains `gbp/loaders/`: the code that turns the raw Citi Bike
trip CSV into `ResolvedModelData`, the input tables the simulator reads.

The loaders do three things:

1. They read the raw trip CSV and derive the entity tables from it:
   stations, depots, trucks, and bike categories, with their capacities, costs,
   and rates.
2. They rename the raw columns to the canonical schema and build the
   historical flow journal and its marginals. A marginal is a table computed
   from the journal, such as inventory, departures, arrivals, or the OD matrix.
3. They compute the initial inventory for the base replay and provide the
   sizing helpers used by `size_state_for_demand`.

The terms are the same as in [`Notations.md`](../../Notations.md). What the
simulator does with these tables is [simulator.md](simulator.md). The journal
functions used here are explained in [flow_journal.md](flow_journal.md).

## Code Map

| File | Main role |
|---|---|
| `dataloader_raw.py` | Reads the trip CSV, derives raw entity tables, and owns `RawModelData`. |
| `dataloader_graph.py` | Builds `ResolvedModelData` from `RawModelData`; also owns `apply_truck_fleet` and `attach_simulation`. |

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

## Step 1: `RawModelData` (`dataloader_raw.py`)

`RawModelData.__init__` loads everything once and stores the results as
attributes. In order:

1. `load_trips_raw_df(trips_path)` loads the trips. The first load parses the
   CSV, drops rows with a missing start time, end time, station id, or
   coordinate, and writes the cleaned table to
   `data/processed/<csv name>.parquet` (Notations.md §15). Later loads read
   that parquet copy instead, which is much faster. The copy counts as fresh
   only while it is newer than its CSV; delete the `processed` folder to force
   a rebuild — for example after changing the cleaning code.
2. `get_stations(trips_raw_df)` builds the station table from the trips
   themselves: every distinct `start_station_id` or `end_station_id` becomes
   one station, with its coordinates. There is no separate station registry.
3. Every station gets the constant dock capacity 100 and a fixed cost of 0.
   The real capacities are not loaded. A sized run replaces this capacity table
   before the simulator starts.
4. `get_depots(rng, n)` synthesizes depots at random coordinates inside the
   city box — one row per depot with `depot_id`, `lat`, `lng`. Their random
   capacities come from `get_depots_capacities` and their random fixed costs
   from `get_depots_costs`. The Citi Bike data has no depots, so all three
   tables are generated from the seed.
5. `get_trips_df` keeps only the trip columns the rest of the pipeline needs.
6. `get_trucks_df(n_trucks)` builds the truck table. Every truck starts at
   `depot_1`; a run can replace the fleet later with `apply_truck_fleet`.
7. `get_trucks_rates_df` and `get_trucks_capacities_df` set the truck rate and
   bike capacity.
8. `get_bike_rates_df` sets the price per hour for the two bike categories.

Everything here still uses the raw column names: `station_id`, `depot_id`,
`truck_id`, `ride_id`, `rideable_type`.

## The Raw-To-Canonical Boundary

`ResolvedModelData` renames the raw names to the canonical schema
([Notations.md §4](../../Notations.md#4-facility-and-its-roles-in-a-trip)):

| Raw | Canonical |
|---|---|
| `station_id`, `depot_id` | `facility_id` (with `facility_category` = `station` / `depot`) |
| `truck_id` | `resource_id` |
| `rideable_type` | `commodity_category` |

The rename happens in the small `get_*` functions at the top of
`dataloader_graph.py` (`get_facilities_df`, `get_resources_df`,
`get_facilities_geo_df`, and so on). Past this boundary, only the canonical
names exist.

## Step 2: `ResolvedModelData` (`dataloader_graph.py`)

`ResolvedModelData.__init__` builds the scenario tables in this order:

| Order | What is built | Names |
|---|---|---|
| 1 | Entities | `facilities_df`, `resources_df`, `commodities_categories_df` |
| 2 | Attributes | `facilities_geo_df`, `facilities_capacities_df`, `resources_capacities_df`, `facilities_costs_df`, `resources_rates_df`, `commodities_categories_rates_df` |
| 3 | The time grid | `period_len`, `t0`, `periods_df` |
| 4 | The historical journal and empty resource observations | `historical_flows_df`, `historical_resources_df` |
| 5 | The base replay initial inventory | `initial_inventory_df` |
| 6 | The historical marginals | `historical_inventory_df`, `historical_demand_df`, `historical_departures_df`, `historical_arrivals_df`, `historical_od_matrix_df` |
| 7 | Riding speed and routes | `trip_speed_km_per_period`, `routing_mode`, `routes` |
| 8 | Empty simulated attributes | `simulated_flows_df = None` and the other `simulated_*` |

The simulator reads this subset: `periods_df`, `initial_inventory_df`,
`historical_demand_df`, `historical_od_matrix_df`,
`facilities_capacities_df`, `facilities_geo_df`, and `routes`.

### The Time Grid

```python
self.t0 = raw.trips_df["started_at"].min().floor("h")
self.periods_df = get_periods_df(raw.trips_df, self.t0, period_len)
```

`t0` is the earliest trip start, floored to the hour. Period `k` runs from
`t0 + k * period_len` for one `period_len` (default one hour).
`to_period_id(ts, t0, period_len)` maps any timestamp to its period, and
`periods_df` lists every period with its start and end timestamps, out to the
last trip's end.

### The Historical Journal

`get_historical_flows_df(trips_df, t0, period_len)` turns each completed trip
into one flow with two events:

```python
departed = departed_events(trips)
arrived = arrived_events(trips, trips["planned_end_period"])
journal = pd.concat([departed, arrived], ignore_index=True)
journal["phase_rank"] = phase_rank_by_timing(journal)
flows = finalize_flows(journal)
violations = check_journal_schema(flows)
if violations:
    raise ValueError(
        "historical flow journal breaks the journal schema:\n" + "\n".join(violations)
    )
return flows
```

The finished journal is checked against the journal schema before it is
returned, so bad input data fails at load time instead of surfacing later as
a run-end violation.

The rows are built with the same builders the simulator uses, and ordered by
the same `finalize_flows`. History contains only trips that actually happened,
so there are no `lost` or `redirected` events and every flow stays on one arc
(`move_id == 0`). `flow_id` gets a `hist_` prefix, so historical and simulated
flows can never collide in one journal.

The loader has no phases, so it stamps `phase_rank` with the timing rule
`phase_rank_by_timing`, and `finalize_flows` derives `step_id` from the
`(period_id, phase_rank, phase_round)` labels (see
[flow_journal.md](flow_journal.md#finalizing-the-journal)).

### The Historical Marginals

The marginals are computed from the historical journal with the read-model
functions from `flows.py`. The same functions later compute the simulated
marginals:

```python
historical_departures_df = flows_to_departures(self.historical_flows_df)
self.historical_arrivals_df = flows_to_arrivals(self.historical_flows_df)
self.historical_od_matrix_df = flows_to_od_matrix(self.historical_flows_df)
```

`historical_demand_df` is the same table as `historical_departures_df`: in
history every wanted trip departed, so demand equals departures.

### Sizing The State Tables

A replay needs two state tables before the simulator starts:

- `initial_inventory_df`: bikes docked at each `(facility, commodity)`.
- `facilities_capacities_df`: dock capacity at each facility.

`ResolvedModelData.__init__` computes `initial_inventory_df` for the base
replay. It leaves `facilities_capacities_df` as the capacity table derived from
the raw data.
A sized run later replaces both tables with the output of
`size_state_for_demand`.

`get_replay_initial_inventory_df` sizes the initial inventory:

```python
moments = inventory_at_moments(historical_flows_df, grid.assign(quantity=0))
low = moments.groupby(["facility_id", "commodity_category"], as_index=False)[
    "inventory_after"
].min()
low["quantity"] = (-low["inventory_after"]).clip(lower=0).astype("int64")
```

Starting from zero bikes everywhere, this function reads the journal step by
step. For each `(facility, commodity)`, it finds the lowest `inventory_after`
value. That value is negative when a station gives out more bikes than it
receives before that step.

The start quantity is the positive amount needed to bring that lowest value up
to zero. Then every historical departure finds a bike, with no extra bikes
added.

Important: the low point is taken per step, not per period. A stockout is checked
inside a period, during the departures phase, before that period's same-period
arrivals dock. The end-of-period value already counts those late arrivals, so
it overstates what is on hand at the moment of departure. Sizing against the
per-period low point would leave real stockouts.

`get_replay_capacities_df` sizes dock capacity for a journal:

```python
moments = inventory_at_moments(historical_flows_df, initial_inventory_df)
facility_total = moments.groupby(["step_id", "facility_id"], as_index=False)[
    "inventory_after"
].sum()
step_peak = facility_total.groupby("facility_id")["inventory_after"].max()
```

With the initial inventory fixed, it finds each facility's peak total
occupancy across every step. The capacity becomes that peak, with
`min_capacity = 10` as a floor for facilities with no replay traffic.

The peak includes the initial occupancy: the moment before the first step. A
station whose inventory only drains has its highest occupancy at the start.

For scaled demand, `run_sized_scenario` calls `size_state_for_demand`. That
function first runs the scenario with saturated inventory and saturated
capacities. Saturated means set far above demand, so no stockout and no
dock-full can happen. It then calls `get_replay_initial_inventory_df` and
`get_replay_capacities_df` on that sizing run's journal
(see [simulator.md](simulator.md#how-a-run-starts)).

`get_saturated_inventory_df` builds the artificial saturated inventory table:
one million bikes per station and commodity. `size_state_for_demand` builds the
matching saturated capacity table itself.

### Riding Speed And Routes

`get_trip_speed_km_per_period` computes the mean riding speed over the
historical trips: total great-circle distance divided by total ride time.

Note: the speed comes from the raw `started_at` / `ended_at` timestamps, not
from the OD matrix. The OD matrix stores durations rounded to whole periods,
and most trips are shorter than one period, so a speed computed from it would
divide by near-zero times.

`routes` is then built once per scenario:

```python
self.routes = Routes(
    self.facilities_geo_df,
    routing_mode,
    trip_speed_km_per_period=self.trip_speed_km_per_period,
    period_len=period_len,
    osrm_url=osrm_url,
)
```

It answers `distance_km(source, target)` and
`duration_periods(source, target)` for facility pairs
([Notations.md §13](../../Notations.md#13-routing-distance-and-travel-time-between-facilities)).
In `osrm` mode the full facility-to-facility table is fetched here, once.

### The Consistency Check

Near its end, `__init__` asserts that the start-of-period inventory at
period 0, summed per commodity, equals the initial inventory. The two are
built by different code paths, so a mistake in either one is caught at load
time, not in the middle of a run.

After the assert, `__init__` ends with `check_engine_tables(self)` — the
schema check described in the next section. The assert stays separate
because it is a cross-table consistency check, which a per-table schema
cannot express.

### The Schema Checks At The Load Boundary

The loaders are the pipeline's fail-fast layer: each table is checked once,
when it is built, so a wrong shape fails at load time instead of as a pandas
error in the middle of a run. Three checks cover the boundary. All three use
pandera schemas and report every violation at once (through
`schema_violations`).

1. The raw trips. `load_trips_raw_df` checks the trip table against
   `TRIPS_SCHEMA` (`dataloader_raw.py`) on every load: required columns and
   dtypes, coordinates inside the service area, and
   `started_at <= ended_at`. A violation raises `ValueError`. The processed
   parquet copy is written only after the check passes, so a bad table is
   never cached.

2. The historical journal. `get_historical_flows_df` runs
   `check_journal_schema(flows)` on the finished journal and raises on
   violations (the snippet above).

3. The engine tables. `ResolvedModelData.__init__` ends with
   `check_engine_tables(self)`: each of the six tables the engine reads
   (`periods_df`, `initial_inventory_df`, `historical_demand_df`,
   `historical_od_matrix_df`, `facilities_capacities_df`,
   `facilities_geo_df`) is checked against its schema in
   `ENGINE_TABLE_SCHEMAS` (`dataloader_graph.py`). The two sized-state
   tables are checked again in `run_sized_scenario`, right after sizing
   replaces them.

## Changing The Truck Fleet

The truck fleet is a run parameter, not part of the loaded data.
`apply_truck_fleet(resolved, truck_homes, truck_capacity_bikes, truck_rate)`
returns a shallow copy of the resolved data with only the three resource
tables rebuilt:

```python
out = copy.copy(resolved)
out.resources_df = get_resources_df(trucks_df)
out.resources_capacities_df = ...
out.resources_rates_df = ...
```

`truck_homes` lists the home depot of each truck, one entry per truck. The
large graph tables are shared with the original object, so changing the fleet
does not rebuild them. The Run page of the UI and the `--truck-homes` runner
flag call this function.

## After A Run: `attach_simulation`

`attach_simulation(resolved, simulated_flows_df, ...)` fills the
`simulated_*` attributes from a finished run's journal:

```python
resolved.simulated_inventory_df = get_inventory_df(
    resolved.simulated_flows_df, resolved.initial_inventory_df
)
simulated_departures_df = flows_to_departures(resolved.simulated_flows_df)
```

Every simulated marginal is derived with the same read-model function as its
historical twin. That makes the two sets directly comparable: in a base replay
they are equal, table by table.

## The Wide Journal

Widening a journal for analysis is done by two model-layer read-models, not by
the loader:

```python
wide = flows_with_inventory(flows_df, initial_inventory_df)
wide = flows_with_measures(wide, routes=..., rates=..., period_len=...)
```

`flows_with_inventory` adds each event's own facility inventory just before
and just after its step (`inventory_before` / `inventory_after`, step-level).
`flows_with_measures` adds the durations, distances, `rate`,
`elapsed_periods`, and `cost` — the same call the artifact builder uses.

The canonical notebook (`notebooks/test_pipeline.ipynb`) builds this table.
The UI does not read it. Its tables are precomputed by `app/artifacts.py`.
An older loader-layer widening (`get_flows_wide`) rebuilt inventory at period
level and disagreed with the step-level read-model; it was deleted in favour
of the two calls above.

## Why It Is Built This Way

### Stations Come From The Trips

There is no station registry in the raw data. Deriving stations from the trip
endpoints guarantees that every station the journal mentions exists in the
facility tables. It also keeps unused stations out of those tables. Depots and
trucks have no source data, so they are synthesized from a seed. The result is
deterministic for a given scenario.

### The Historical Journal Uses The Simulator's Builders

`get_historical_flows_df` could have built its rows by hand. Instead it calls
`departed_events`, `arrived_events` and `finalize_flows` from `flows.py`. This
is what makes the base replay checkable row by row: the historical journal and
a replay run's journal are produced by the same primitives, so any difference
between them is a real behavior difference, never a formatting one.

### The Initial State Is Computed, Not Loaded

Today's real station inventory is a current observation. It is unrelated to
the historical month being replayed. Limiting demand against it would create
stockouts that never happened in history.

The loader instead computes the smallest state under which history replays
cleanly. The computation is a pure function of the journal. It uses the floor
at zero for inventory and the peak occupancy for capacity, not a guessed safety
margin.

### One Read-Model, Two Views

Every `historical_*` marginal and its `simulated_*` twin are produced by one
function from `flows.py`. The container just calls it twice, on two journals.
This supports the base replay check: it compares tables that share one
definition.
