---
title: "Citi Bike in New York City"
weight: 1
---

# Scenario: Citi Bike in New York City

This page describes the first (and so far only) scenario of the framework
end to end: the problem, how the domain maps onto the four entities, where
the data comes from, which runs exist, and how to read the results. The
mechanisms themselves are described once, in
the [Architecture section](../architecture/) — this page links to them
instead of repeating them.

## The problem

Citi Bike is a bike-sharing system: a rider takes a bike from a dock at one
station and returns it at a dock of another station. Demand is uneven in
space and time. On a weekday morning, stations in residential areas drain
and stations near offices fill up; in the evening the flow reverses.

Two physical limits turn this unevenness into failures, and both are events
in the flow journal ([Notations.md §1](../reference/notations.md#1-the-four-flow-outcomes-and-the-two-reasons)):

- A station with no bikes loses its demand: the trip never departs and is
  written as `lost` with `reason = stockout`.
- A station with no free docks bounces an arriving bike: the journal writes
  `redirected`, and the bike rides on to the nearest station with a free
  dock — or `lost` if no station has one.

The scenario replays one month of published trips through the simulator
under these limits, then changes one thing at a time and compares runs:
scale the demand up, add night truck rebalancing, or replace the demand
table with a model's forecast. The current phase of the project is demand
forecasting, and a model is judged by both its forecast error and what
happens when the simulator runs on its forecast (the two-level evaluation,
[Notations.md §17](../reference/notations.md#17-demand-forecasting-the-model-around-the-simulator)).

## Entity mapping

The four entities of the data model (the table in the
[root README](https://github.com/vlzm/GFDRR)) map onto Citi Bike like this:

| Entity | In Citi Bike | In the code |
|---|---|---|
| `commodity` (§8) | a bike | `commodity_category`: `classic_bike` / `electric_bike` |
| `facility` (§4) | a station or a depot | `facility_id`; `facility_category`: `station` / `depot` |
| `resource` (§5b) | a rebalancing truck | `resource_id`; `resource_category`: `truck` |
| `edge` (§4b) | the route between two facilities | `routes` (§13): `distance_km(source, target)` and `duration_periods(source, target)`, measured by `routing_mode` — the `haversine` formula (default) or a local OSRM road-network server |

Two boundaries of the mapping:

- Stations and trips are real — they come from the published CSV. Depots
  and trucks are synthetic: the loader places `n_depots` depots at random
  coordinates inside the city box (`get_depots` in
  `domains/citybike/loaders/dataloader_raw.py`), and the truck fleet is a run parameter
  ([Notations.md §14](../reference/notations.md#14-rebalancing-moving-bikes-by-truck)).
- Citi Bike does not publish station inventories or dock capacities. The
  initial inventory and the capacities are measured by a sizing run instead
  ([decision record](../decisions/sizing-run.md)).

## The data

One month of published trips is the only external input of the scenario.
Citi Bike publishes each month as a zip of CSVs at
`https://s3.amazonaws.com/tripdata`; one month unpacks to 0.3–0.9 GB, split
into files of about one million trips each. The download command is in
[installation.md](../getting-started/installation.md), step 2.

Two loaders turn one CSV into the input tables of the simulator:

- `dataloader_raw.py` reads the trip CSV, checks it against an explicit
  schema, and drops the rows no consumer can use: rows with missing key
  fields, endpoints outside the service area, and trips that end before
  they start. It keeps a processed parquet copy of the CSV in
  `data/processed/` — a cache, so later loads are much faster. It also
  synthesizes the depots and the trucks.
- `dataloader_graph.py` (function `build_resolved`) puts the raw tables into
  the framework's column names and builds the period grid (one row per hour)
  and the historical flow journal. It hands those to `ResolvedModelData`
  (`gbp/model/dataloader_graph.py`), which knows nothing about bikes and
  derives the rest: the demand and the OD matrix computed from the journal,
  the start inventory, and `routes`.

How each table is built, column by column:
[data-model.md](../architecture/data-model.md).

For scale: the first file of January 2026
(`202601-citibike-tripdata_1.csv`) holds 996,388 trips after cleaning,
covering 15 days — a grid of 360 one-hour periods over 2,260 facilities
(2,250 stations plus the 10 synthetic depots).

## The simulator on this domain

One call runs the whole scenario: `run_sized_scenario`
(`gbp/consumers/simulator/scenario.py`). It does three things in order:

1. Sizes the state: a sizing run measures the initial inventory and the
   dock capacities the demand needs (`size_state_for_demand`).
2. Runs the demand against that state, period by period. Each period is
   three canonical phases: dock earlier arrivals → form departures → dock
   same-period arrivals (`canonical_phases()`).
3. Checks the run invariants I1–I5 on the finished journal.

Two scale factors set the run kind
([Notations.md §11](../reference/notations.md#11-run-kinds)):
`sizing_scale_factor` is the demand the state is built to survive with no
loss; `demand_scale_factor` is the demand the run actually faces. Equal
values (the default) give the base replay — departures equal the historical
ones and nothing is lost. A larger run scale makes the limits take effect:
stockout and dock-full events appear.

Rebalancing is opt-in: a run that appends `rebalancing_phases(params)` to
`canonical_phases()` gets two extra phases that plan and execute night
truck routes ([rebalancing.md](../architecture/rebalancing.md)).

The period loop, the phases, and the invariants:
[simulation-engine.md](../architecture/simulation-engine.md). The
smallest real journal of each mechanic — stockout, redirect, truck move —
with row-by-row tables:
[worked-examples.md](../architecture/worked-examples.md).

## Minimal example

The whole pipeline in one block — no forecasting, no rebalancing, nothing
saved to disk. It needs one downloaded month
([installation.md](../getting-started/installation.md), step 2) and runs
from the repository root. The first run parses the CSV and takes a few
minutes; with the processed cache warm the whole script takes about half a
minute.

```python
import pandas as pd

from domains.citybike.loaders import RawModelData, build_resolved
from gbp.consumers.simulator import run_sized_scenario
from gbp.logging import configure_logging

configure_logging()

# 1. Raw data: read the trip CSV, derive the raw entity tables.
raw_data = RawModelData(
    trips_path="data/raw/202601-citibike-tripdata_1.csv",
    seed=42,
    n_depots=10,
    depot_capacity=9000,
    n_trucks=5,
    truck_capacity_bikes=20,
    truck_rate=50.0,
    electric_bike_rate=5,
    classic_bike_rate=3,
)

# 2. Graph tables: the period grid, demand, the OD matrix, geography, routing.
graph_data = build_resolved(raw_data, period_len=pd.Timedelta(hours=1))

# 3. Size the state and run 50 periods. Equal scale factors (the default 1.0)
#    give the base replay: departures equal the historical ones, nothing is lost.
run = run_sized_scenario(
    graph_data,
    scenario_id="citibike_minimal",
    number_of_periods=50,
)

# 4. Read the results: the flow journal and the final inventory.
journal = run.simulated_flows_df
print(f"Invariant violations: {run.violations}")
print(journal["event_type"].value_counts().to_string())

one_flow = journal["flow_id"].iloc[0]
columns = ["flow_id", "event_type", "source_id", "planned_target_id", "period_id"]
print(journal.loc[journal["flow_id"] == one_flow, columns].to_string(index=False))

inventory = run.state.state_inventory_df
print(inventory.sort_values("quantity", ascending=False).head().to_string(index=False))
```

After the progress log lines, the script prints:

```text
Invariant violations: []
event_type
departed    36869
arrived     36506
flow_id event_type source_id planned_target_id  period_id
sim_0_0   departed   6626.01           5703.13          0
sim_0_0    arrived   6626.01           5703.13         17
facility_id commodity_category  quantity
    6004.06      electric_bike      46.0
     6723.1      electric_bike      38.0
    6822.09      electric_bike      34.0
    5470.12      electric_bike      27.0
    6839.04      electric_bike      25.0
```

The run is a base replay, so the journal has only `departed` and `arrived`
rows — no `lost`, no `redirected`. The gap between the two counts (363) is
the bikes still riding when period 50 ends. The first flow shown is one of
the long rentals in the data: it departs in period 0 and docks 17 hours
later; most trips arrive within the same or the next period. The journal
schema and its read-models:
[flow-journal.md](../architecture/flow-journal.md).

## The runs

Three runs cover the scenario today. The two notebooks are the canonical
scenario; the runner saves any of the three as a run artifact the web
interface can show.

| Run | Demand | Phases | Where |
|---|---|---|---|
| base replay | history, scale 1.0 | the canonical three | [test_pipeline.ipynb](test-pipeline/); `python app/runner.py --run-name base` |
| run with rebalancing | history | canonical + the two rebalancing phases | `python app/runner.py --run-name with_trucks --rebalancing` |
| forecast run | a saved forecast | the canonical three | [forecast_pipeline.ipynb](forecast-pipeline/); `python app/runner.py --run-name forecast_demo --demand-source forecast --forecast-name seasonal_naive_w1` |

The notebooks run the same `run_sized_scenario` call as the minimal example
and then verify the run instead of just printing it.
[test_pipeline.ipynb](test-pipeline/) compares the
simulated marginals against the historical ones, reads the inventory at
single moments, explains one redirect station by station, and computes
riding time and cost.
[forecast_pipeline.ipynb](forecast-pipeline/) first
builds and saves a seasonal-naive forecast, runs the simulator on it, and
checks that the simulated departures equal the forecast demand. The
notebooks save no run artifacts; the runner does
([visualization.md](../architecture/visualization.md)).

The recipes: another demand scale or another month —
[change-the-demand.md](../how-to/change-the-demand.md); a run on a saved
forecast, step by step —
[run-on-a-forecast.md](../how-to/run-on-a-forecast.md).

## Demand forecasting

The forecasting task: predict the demand of a future window — `quantity`
per `(period_id, facility_id, commodity_category)`. A model's prediction is
saved as a forecast artifact, and the simulator reads it exactly as it
reads historical demand; nothing else of the run changes
([decision record](../decisions/forecast-replaces-only-demand.md)).

Four model families stand behind one interface (`DemandModel`):
`seasonal_naive`, `sarimax`, `lightgbm`, `graphsage`. The champion — the
version the platform forecasts with — is marked in the MLflow model
registry. The pipeline commands:

```bash
python -m domains.citybike.ml.training --months 202502 202503        # download raw months, build the training table
python -m domains.citybike.ml.ops.backtest                               # rolling-origin backtest of the model families
python -m domains.citybike.ml.ops.pipeline                               # download → build-table → train → backtest → promote
python -m domains.citybike.ml.forecast --champion --forecast-name champion_w1   # forecast with the registry champion
python -m domains.citybike.ml.ops.monitoring --month 202602              # score saved forecasts against the month's actuals
```

How each step works — the training table, the features, the censoring, the
promote rule: [ml-toolkit.md](../architecture/ml-toolkit.md).

A model is judged on this scenario by the two-level evaluation. Level 1
scores the forecast against the held-out month's actual counts. Level 2
runs the simulator on the forecast with the state held fixed — every
forecast runs against the state sized on the actual demand — and compares
the run totals against the reference run:

```bash
python -m domains.citybike.ml.ops.evaluation --month 202601
```

The saved result for January 2026, ending with which model the platform
should use:
[model_evaluation_202601.md](https://github.com/vlzm/GFDRR/blob/city_bike_mvp_accounting/docs/reports/model_evaluation_202601.md).

## Results and visualization

Every saved run is a folder under `data/runs/` — a run artifact
([Notations.md §12](../reference/notations.md#12-run-artifacts-the-files-the-ui-reads)).
The web interface lists them:

```bash
streamlit run app/main.py
```

Pick one run as Scenario A and another as Scenario B in the sidebar — every
page draws the two side by side. The Overview & compare page starts with
the whole-run totals:

![The Overview & compare page of the web interface](images/ui_overview.png)

The evaluation runs (`eval_202601_...`) are normal run artifacts, so a
forecast run and its reference run compare in the same two pickers; the
numeric comparison of one evaluation lands in
`data/ml/evaluation/<month>/comparison.csv`, one row per run.

The first walk through the interface, page by page:
[ui.md](../getting-started/ui.md). How the artifact is built and why the
app only reads: [visualization.md](../architecture/visualization.md).
