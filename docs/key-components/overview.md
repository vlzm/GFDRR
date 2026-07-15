# Overview — the five concepts and the system in diagrams

This page is the starting point of Key Components. It first defines the five
concepts every other page assumes — period, demand, station inventory, flow
journal, run artifact — then shows the system at three zoom levels and ends
with a module depth table. The exact contracts live in
[Notations.md](../../Notations.md) — open it by section when you need a
column list, not before.

## The five concepts

### Period

A period is one step of the simulation clock. Everything in a run — a
departure, a docking, a redirect — happens in some period, identified by
`period_id`: 0, 1, 2, … By default one period is one hour (`period_len`), and
period 0 starts at `t0`, the hour of the earliest historical trip.

Example: with `t0 = 2026-01-01 00:00`, period 5 covers 05:00–06:00 of
January 1. A trip that departs in period 5 and docks in period 6 took one
period.

Exact contract: [Notations.md §6](../../Notations.md#6-time).

### Demand

Demand is the number of trips users wanted to start, per period, station, and
bike type: a table with the columns `period_id`, `facility_id`,
`commodity_category`, `quantity`. The simulator reads this table the same way
whether the quantities come from history or from a forecast model. Wanted does
not mean happened: demand splits exactly into `departed` plus `lost` with
`reason = "stockout"`.

Example: demand of 5 at station `s1` in period 3, but only 3 bikes are docked
there — 3 trips depart and 2 are lost to a stockout.

Exact contract: [Notations.md §2](../../Notations.md#2-core-state) and
[§7](../../Notations.md#7-departures).

### Station inventory

Inventory is how many bikes each station holds at a moment: one row per
station and bike type, columns `facility_id`, `commodity_category`,
`quantity`. It changes only by whole bikes — `-1` when a bike leaves a dock,
`+1` when a bike docks — and every change is recorded as a flow event, so
inventory at any moment can be recomputed from the initial inventory plus the
journal.

Example: `s1` starts with 5 bikes; one trip departs in period 0; from that
step on `s1` holds 4.

Exact contract: [Notations.md §2](../../Notations.md#2-core-state).

### Flow journal

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

Exact contract: [Notations.md §0](../../Notations.md#0-the-flow-event-schema-the-symbol-table)
(the columns) and [§1](../../Notations.md#1-the-four-flow-outcomes-and-the-two-reasons)
(the four outcomes).

### Run artifact

A run artifact is a finished run saved to disk: the folder
`data/runs/<run_name>/`, built once by `app/artifacts.py`. It holds
`meta.json` (the run's parameters, totals, and invariant violations) and five
parquet tables; the two you meet first are `flows.parquet` — the journal —
and `panel.parquet` — per period and station: inventory, demand, departures,
losses. The web interface only reads these files; it never simulates.

Example: `python app/runner.py --run-name demo` writes `data/runs/demo/`, and
`streamlit run app/main.py` lists it.

Exact contract: [Notations.md §12](../../Notations.md#12-run-artifacts-the-files-the-ui-reads).

Scenarios 1–3 of [worked-examples.md](worked-examples.md) show these five
concepts working together on real journal tables, each re-run by a test.

The rest of this page shows the system at three zoom levels, then gives a
module depth table. The diagrams stay high-level on purpose: every arrow
names what one block gives another, such as "passes the flow journal" — not
a function signature. Exact names and table contracts live in
[Notations.md](../../Notations.md) and in the per-module documents linked
below.

## Level 1 — the system and the outside world

```mermaid
flowchart LR
    user["User<br/>terminal + browser"]
    csv["Citi Bike trip CSVs<br/>data/raw/"]
    osrm["OSRM server<br/>optional, local"]
    system["The framework (this repository)<br/>runs scenarios, saves them, shows them"]

    user -->|"starts a run; browses finished runs"| system
    csv -->|"one month of historical trips"| system
    system -.->|"in osrm mode, asks once per scenario for the<br/>facility-to-facility distance table"| osrm
```

The framework takes one month of published Citi Bike trips and replays that
demand period by period, with optional changes (scaled demand, overnight
rebalancing by truck). It can also run on predicted demand: a model trained
on past months saves a forecast demand table, and a forecast run
([Notations.md §11](../../Notations.md#11-run-kinds)) reads that table
through the same run chain. Each run is saved as one folder on disk — a run
artifact ([Notations.md §12](../../Notations.md#12-run-artifacts-the-files-the-ui-reads)) —
and the web interface shows the saved runs. The only optional outside service
is a local OSRM routing server ([set-up-osrm.md](../how-to/set-up-osrm.md));
without it, distances come from the haversine formula (the straight line
between two points on the globe).

## Level 2 — the big blocks

One run passes left to right through the run chain; the demand forecasting
subsystem stands beside the chain and supplies forecast demand:

```mermaid
flowchart LR
    csv["raw trip CSVs<br/>data/raw/"]
    loaders["loaders<br/>gbp/loaders/"]
    sim["simulator<br/>gbp/consumers/simulator/"]
    builder["artifact builder<br/>app/artifacts.py"]
    runs[("run artifacts<br/>data/runs/, one folder per run")]
    ui["web interface<br/>app/main.py + app/views/"]
    api["run-artifact API<br/>app/api.py"]
    ml["demand forecasting<br/>gbp/ml/"]
    forecasts[("forecast artifacts<br/>data/ml/forecasts/")]

    csv -->|"historical trips"| loaders
    csv -->|"past months, as the training table"| ml
    ml -->|"forecast demand table + meta.json"| forecasts
    forecasts -.->|"in a forecast run, takes the place<br/>of the historical demand"| loaders
    loaders -->|"ResolvedModelData, read through<br/>the ScenarioInputs contract"| sim
    sim -->|"flow journal"| builder
    builder -->|"saved tables + meta.json"| runs
    runs -->|"saved tables"| ui
    runs -->|"saved tables"| api
    api -.->|"the same tables over HTTP,<br/>when API_URL is set"| ui
```

One line per block, with its document:

- **loaders** ([data-model.md](data-model.md)) — resolve the raw trip CSV
  into `ResolvedModelData`, the input tables of one scenario. The simulator
  is typed against `ScenarioInputs` (`gbp/consumers/simulator/inputs.py`);
  `ResolvedModelData` is one supplier of that contract.
- **simulator** ([simulation-engine.md](simulation-engine.md)) — plays the scenario period
  by period and produces the flow journal. Overnight rebalancing
  ([rebalancing.md](rebalancing.md)) is an opt-in part of it.
- **artifact builder** ([visualization.md](visualization.md)) — turns a finished run into a run
  artifact: a folder of tables plus `meta.json`.
- **web interface** ([visualization.md](visualization.md)) — saved-run pages load saved tables
  and draw them; the `Run scenario` page starts a run.
- **run-artifact API** ([api.md](../reference/api.md)) — serves the same folders over
  HTTP and can start new runs.
- **demand forecasting** ([ml-toolkit.md](ml-toolkit.md)) — trains demand models on past
  months, keeps versions in a local MLflow store, saves forecasts under
  `data/ml/forecasts/`. A forecast run is the same run chain with one
  substitution: the forecast demand table takes the place of the historical
  demand ([decision record](../decisions/forecast-replaces-only-demand.md)).

Two shared libraries serve several blocks, so they are not separate blocks
here: `gbp/model/` ([flow-journal.md](flow-journal.md)) owns the journal's
event schema, its builders and its read-models; `gbp/routing.py` answers
distance and travel-time questions for facility pairs. `app/runner.py` runs
the full sequence from CSV to saved folder; the terminal, the API and the
"Run scenario" page all go through it.

## Level 3 — every module

The full module map, split in three pictures at the same borders as the code.

### The run chain inside gbp/

```mermaid
flowchart TB
    subgraph loaders["gbp/loaders/"]
        raw["dataloader_raw.py<br/>RawModelData"]
        resolved["dataloader_graph.py<br/>ResolvedModelData"]
    end
    subgraph model["gbp/model/"]
        flows["flows.py<br/>event builders + read-models"]
        schema["journal_schema.py<br/>journal schema checks"]
    end
    routing["routing.py<br/>Routes"]
    subgraph sim["gbp/consumers/simulator/"]
        scenario["scenario.py<br/>run_sized_scenario"]
        sizing["sizing.py<br/>size_state_for_demand"]
        engine["engine.py<br/>Environment"]
        phases["phases.py<br/>DockArrivals, FormDeparturesPhase"]
        rebalancing["rebalancing.py<br/>plan + apply phases, VRP solver"]
        mechanics["mechanics.py<br/>decision functions"]
        state["state.py<br/>SimulationState"]
        validation["validation.py<br/>validate_run"]
    end

    raw -->|"clean source tables"| resolved
    resolved -->|"builds Routes once per scenario"| routing
    resolved -->|"builds the historical journal with"| flows
    resolved -->|"the scenario inputs<br/>(ScenarioInputs)"| scenario
    scenario -->|"step 1: size the state"| sizing
    sizing -->|"sizing run, saturated state"| engine
    scenario -->|"step 2: the real run"| engine
    scenario -->|"step 3: check the run"| validation
    engine -->|"runs the phase list, once per period"| phases
    rebalancing -->|"provides two opt-in phases"| phases
    phases -->|"asks for decisions"| mechanics
    phases -->|"writes events through"| state
    phases -->|"event rows built by"| flows
    validation -->|"reads the journal through read-models"| flows
    mechanics -.->|"travel-time fallback"| routing
```

Not drawn, to keep the picture readable: `journal_schema.py` is called from
many places (loaders, `scenario.py`, `validation.py`, `app/artifacts.py`);
`engine.py` and `state.py` also call small helpers from `flows.py`;
`config.py` (the `EnvironmentConfig` settings record) travels along every
arrow between `scenario.py`, `engine.py` and the phases.

### The forecasting subsystem inside gbp/ml/

```mermaid
flowchart TB
    mldata["data.py<br/>month downloads, month period grid"]
    status["station_status.py<br/>stockout_share from status snapshots"]
    features["features.py<br/>the feature columns"]
    training["training.py<br/>builds the training table"]
    partitions[("data/ml/training/<br/>one parquet partition per month")]
    models["models/<br/>DemandModel families: seasonal_naive.py,<br/>sarimax.py, boosting.py, graph.py"]
    metrics["metrics.py<br/>MAE, Poisson deviance"]
    backtest["backtest.py<br/>rolling-origin backtest"]
    registry["registry.py<br/>MlflowStore: runs + the model registry"]
    pipeline["pipeline.py<br/>download → build-table → train →<br/>backtest → promote"]
    fc["forecast.py<br/>forecast input, rounding,<br/>save and load forecast artifacts"]
    forecasts[("data/ml/forecasts/<br/>one folder per forecast")]
    monitoring["monitoring.py<br/>scores saved forecasts, drift reports"]

    mldata -->|"downloaded months, numbered<br/>on the month period grid"| training
    status -->|"stockout_share per station-hour"| training
    features -->|"the feature columns"| training
    training -->|"monthly partitions"| partitions
    partitions -->|"the training table"| backtest
    models -->|"fit / predict, built through create_model"| backtest
    metrics -->|"scores the aligned rows"| backtest
    backtest -->|"logs every (model, split) run"| registry
    pipeline -->|"runs the steps in order"| training
    pipeline -->|"the promote-or-keep decision"| registry
    registry -->|"resolves the champion for"| fc
    features -->|"the same feature columns"| fc
    fc -->|"demand.parquet + meta.json"| forecasts
    forecasts -->|"scored against the month's actuals"| monitoring
```

Not drawn: `pipeline.py` also calls `data.py` and `backtest.py`;
`metrics.py` also scores monitoring's months; `forecast.py` builds a model
family through the same `create_model` when it is not using the champion.
The run chain reads this picture at one point: a forecast run loads a
forecast artifact by name, and `apply_forecast_demand` in
`dataloader_graph.py` puts its demand table in place of the historical one.

### From journal to browser inside app/

```mermaid
flowchart TB
    runner["runner.py<br/>build_graph_data, run_scenario"]
    evaluate["evaluate.py<br/>the two-level evaluation"]
    artifacts["artifacts.py<br/>save_scenario_run, load_run_*"]
    runs[("data/runs/<br/>one folder per run")]
    api["api.py<br/>six endpoints, one worker thread"]
    client["api_client.py<br/>HTTP calls to the API"]
    backend["backend.py<br/>the one disk-or-API choice"]
    shared["ui_shared.py<br/>typed loaders: load_panel, load_meta, ..."]
    main["main.py<br/>page registry"]
    views["views/<br/>ten Streamlit pages"]

    runner -->|"finished run:<br/>journal + sized state"| artifacts
    evaluate -->|"saves its runs and reads<br/>their panels through"| artifacts
    artifacts -->|"writes and reads"| runs
    api -->|"starts runs through"| runner
    api -->|"loads and saves through"| artifacts
    client -->|"GET / POST over HTTP"| api
    shared -->|"reads through"| backend
    backend -->|"disk backend"| artifacts
    backend -->|"HTTP backend, when API_URL is set"| client
    backend -->|"local runs"| runner
    main -->|"registers"| views
    views -->|"saved-run pages ask for tables"| shared
    views -.->|"Run scenario page starts runs"| backend
```

`runner.py` connects this picture to the previous ones: `build_graph_data`
calls the loaders, and `run_scenario` calls `run_sized_scenario`, then
passes the result to the artifact builder. `evaluate.py` is a second
terminal entry point: it runs the reference demand and one forecast run per
model, saves each as a normal run artifact, and writes
`data/ml/evaluation/<month>/comparison.csv`.

## Module depth — small interface, big module

The design rule this project follows (from John Ousterhout, "A Philosophy of
Software Design"): a module is good when its interface is short and the work
hidden behind it is large. When a change makes an interface column longer,
review the design.

### gbp/

| Module                        | Interface in one line                                                           | What it hides                                                                                                                              |
| ----------------------------- | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `loaders/dataloader_raw.py`   | `RawModelData(trips_path, seed, fleet sizes, rates)`                            | CSV cleaning, the processed-parquet cache (`data/processed/`), the synthetic depots, trucks and price tables                               |
| `loaders/dataloader_graph.py` | `ResolvedModelData(raw, period_len, routing_mode)`                              | the period grid, the historical journal, the OD matrix, trip speeds, the replay sizing helpers, schema checks for the engine-facing tables |
| `routing.py`                  | `routes.distance_km(source, target)`, `routes.duration_periods(source, target)` | haversine vs OSRM, the one-shot `/table` fetch, the fallback for pairs OSRM cannot route                                                   |
| `model/flows.py`              | plain functions: journal-shaped table in, table out                             | `step_id` assignment, phase ordering, inventory reconstruction at any moment, the measure columns                                          |
| `model/journal_schema.py`     | `check_journal_schema(flows) -> list[str]`                                      | the pandera schema and the row-level rules of the event table                                                                              |
| `simulator/inputs.py`         | `ScenarioInputs` — the input tables of one scenario                             | nothing, by design: it is the named field list of the loader–simulator seam, so "what does the simulator read" has one answer              |
| `simulator/scenario.py`       | `run_sized_scenario(resolved, ...) -> ScenarioRun`                              | the fixed order: size, schema-check, copy, run, validate                                                                                   |
| `simulator/sizing.py`         | `size_state_for_demand(resolved, config) -> two tables`                         | the saturated run and why its journal is the right thing to measure                                                                        |
| `simulator/engine.py`         | `Environment(resolved, config).run() -> SimulationState`                        | the period loop, the phase-order guard, finalizing the journal on read                                                                     |
| `simulator/state.py`          | `SimulationState`: append events, advance the clock                             | step bookkeeping, in-transit tracking, inventory updates                                                                                   |
| `simulator/phases.py`         | `Phase.execute(state, resolved, period, config) -> state`                       | which events each phase writes, and in what order inside one period                                                                        |
| `simulator/mechanics.py`      | plain tables in, decision tables out                                            | the redirect rounds, capacity fitting, demand realization                                                                                  |
| `simulator/rebalancing.py`    | `rebalancing_phases(params)` -> the two truck phases                            | target inventory, imbalance, node splitting, the OR-Tools VRP, minute-to-period mapping                                                    |
| `simulator/validation.py`     | `validate_run(...) -> list[str]`                                                | the run invariants I1–I5 and how each is computed from the journal                                                                         |
| `simulator/config.py`         | `EnvironmentConfig` — the run settings                                          | nothing; a plain settings record, and that is fine — not every module needs depth                                                          |
| `ml/` (the forecasting package) | terminal commands `python -m gbp.ml.{training,backtest,pipeline,forecast,monitoring}`; `forecast.load_forecast(name)` for the runner | the feature columns, the training partitions, the four model families behind `DemandModel`, the MLflow store and the promote rule, the monitoring metrics and drift reports |

### app/

| Module               | Interface in one line                                                    | What it hides                                                                                          |
| -------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| `runner.py`          | `RunRequest`, `build_graph_data(...)`, `run_scenario(graph_data, request) -> saved folder` | the one run recipe (`RunRequest`), the stage order, `run_and_save` (the shared run-then-save step), and the progress reporting                                                             |
| `artifacts.py`       | `save_scenario_run(result, data, request, ...)`, `load_run_table`, `load_run_meta` | one build function per saved table, which result field feeds which builder, the artifact's pandera schemas, the `METRICS` registry, run naming |
| `evaluate.py`        | `python app/evaluate.py --month <YYYYMM>`                                 | the reference run, one replay-state forecast run per model, the shared demand cut (`restrict_demand_to_scenario`), `comparison.csv`          |
| `api.py`             | six HTTP endpoints ([api.md](../reference/api.md))                                    | the single worker thread, the run queue, the disk fallback after a restart                             |
| `api_client.py`      | `list_runs`, `load_table`, `start_run`, `run_status`                     | URL building, the API-key header, response decoding                                                    |
| `backend.py`         | `current()` — the chosen backend: reads, and `run_and_wait`              | the disk-or-API choice (`API_URL`), local in-process runs vs POST-and-poll over HTTP                   |
| `ui_shared.py`       | typed loaders: `load_panel(run_name)`, `load_meta(run_name)`, ...        | caching, old-artifact fallbacks                                                                        |
| `main.py` + `views/` | one Streamlit page per view                                              | saved-run pages only draw precomputed artifact values; `Run scenario` starts a run through the backend |

## Keeping this page true

No test catches a stale arrow, so the arrows stay high-level: domain words,
no signatures, no column names. Update this page only when a module appears,
disappears, or changes what it takes or gives. How a module works inside
belongs to the per-module documents ([data-model.md](data-model.md),
[simulation-engine.md](simulation-engine.md), [rebalancing.md](rebalancing.md),
[flow-journal.md](flow-journal.md), [visualization.md](visualization.md), [api.md](../reference/api.md),
[ml-toolkit.md](ml-toolkit.md)).
