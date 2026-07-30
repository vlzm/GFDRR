# Notations

A framework for problems on flow graphs — networks where commodities move
between facilities. The first scenario is the Citi Bike bike-sharing system
in New York City.

This file is the project's dictionary: **one concept, one word**. Code,
docstrings, and chat answers use the word listed here, never a synonym. If a
concept is missing, add it here first, then use it. A name names the
**concept**, never a tag on the data. How the mechanisms work lives in
`docs/site/content/docs/architecture/`, not here; the machine truth for
schemas lives in the code.

## Language

### Flow events

**Trip**:
A journey a user wants to take — the demand-level word. Lives in aggregate
counts and the OD matrix.
_Avoid_: flow (that is the journal record), ride

**Flow**:
One trip recorded in the journal, identified by `flow_id` — the atomic
journal unit. After expansion each flow moves exactly one bike. A flow is a
chain of flow events: it opens with a **Departed**, closes with an
**Arrived** or a **Lost**, and each **Redirected** bounce adds one more arc.
_Avoid_: journal (a role word for the table's append-only nature, never a
variable or column name — the data is always `flows`)

**Flow event**:
One row of the journal: one thing that happened to a flow in one period. The
machine truth for its columns is `FLOW_EVENT_SCHEMA`; this file fixes only
the words.
_Avoid_: record, log entry

**Arc**:
One physical edge of a trip — the `(flow_id, move_id)` pair. A normal trip is
one arc; each redirect bounce closes the current arc and opens a new one.
_Avoid_: leg as a data name (fine in prose), hop

**Departed**:
A bike left its source. On the first arc it is an undocking (−1 to the
source's inventory) and, on a user trip, the demand the OD model learns from;
on a redirect's continuation arc it is pure transport — no inventory change,
not demand.
_Avoid_: dispatched, released

**Arrived**:
A bike docked at the current arc's target (+1 to its inventory). The only
event that docks a bike — when you mean a specific outcome, use its own word,
not "docked".

**Redirected**:
A bike bounced off the full target of its current arc and rides on to a
neighbour on a new arc. The bounce itself docks nowhere.
_Avoid_: placed, rerouted

**Lost**:
A trip that did not happen, or a bike that left the system: `stockout` demand
never departs; a `dock_full` flow found no dock anywhere.
_Avoid_: shortfall, missing, dropped, failed

**Reason**:
The tag on a **Lost** or **Redirected** event saying why: `stockout` (no bike
at the source) or `dock_full` (no free dock at the planned target). A tag,
never a data name: a frame of demand lost to stockouts is `lost_demand`, not
`stockout`. So demand splits exactly into departed + lost(stockout), and
every departed flow closes with arrived or lost(dock_full).
_Avoid_: stockout / dock_full as frame or variable names

### Entities

**Facility**:
A node in the network, identifier `facility_id`; its kind is
`facility_category`: `station` or `depot`. "Station" is the natural domain
word, fine in prose, but never an identifier.

**Source / Target**:
The facility a trip leaves from (`source_id`) and the facility it goes to
(`planned_target_id` / `realized_target_id`).
_Avoid_: origin, destination (reserved for the OD matrix), a bare `target_id`

**Origin / Destination**:
Reserved for the **OD matrix** only. One more allowed spot, in the same O
sense: the attribution rule says a flow's totals belong to its "origin
facility" — in code that is always `source_id`.
_Avoid_: using them anywhere else

**Commodity**:
What moves through the network. The column is `commodity_category`
(`classic_bike` / `electric_bike`); `commodity` is the prose noun. The two
bike types share the same physical docks but are counted per category.

**Resource**:
A vehicle that can carry bikes between facilities, identifier `resource_id`;
its kind is `resource_category`, today only `truck`. Idle in the historical
replay — rebalancing is their first user.

**Edge**:
A pair of facilities with a distance and a travel time between them. There
is no edge table: edges appear as the answers of **Routes**. With facility,
commodity and resource it completes the four entities of a flow graph: what
moves, from where to where, along what, carried by what.

### State

**Inventory**:
Bikes currently docked at facilities — the amount on hand. The same word for
every view: historical, simulated, live-state.
_Avoid_: stock; on-hand as a data name (the prose phrase "bikes on hand" is
fine)

**In transit**:
Bikes that departed but have not yet docked — the working set.
_Avoid_: moving set, moving bikes

**Demand**:
The number of trips users wanted: demand = departed + lost(stockout).

**Supply**:
Inventory in its "available to depart" role. A role view of inventory, not a
second word for the inventory table.

**Occupancy**:
Bikes docked at a facility, summed across commodities — the docks are
shared.
_Avoid_: utilization, per-commodity dock counts

**Free docks**:
Free dock slots per facility: capacity minus the facility's occupancy.
_Avoid_: available docks, slots

**Fits / Overflow**:
The two halves of the one docking rule: within each target the first `free`
flows dock (fits), the rest are overflow.
_Avoid_: spillover, excess

**Step**:
One inventory step: a batch of +1/−1 applied together; between two steps
inventory is constant. `step_id` is the step's run-global ordinal — it
carries only the order, never the inventory, which is always a pure function
of the journal.
_Avoid_: seq, tick, moment_id

**Moment**:
Inventory seen just before or just after a step — a prose word and the
`_before` / `_after` suffix on inventory read-models.
_Avoid_: moment as a column name

### Time and money

**Period**:
One step of the simulation clock, identifier `period_id`; its wall-clock
length is `period_len` (default one hour).

**Start period**:
The period a flow departed. The same on every row of the flow, redirect arcs
included — it records when the *flow* departed, not when an arc started.

**Duration periods**:
A length in whole periods — the one name for a trip's or a facility pair's
duration; the planned/realized prefixes pick the view.
_Avoid_: the retired `planned_duration` / `realized_duration`

**Elapsed periods**:
How many periods a flow has been riding at the moment of an event; cumulative
over redirect arcs.

**PeriodGrid**:
The numbering of periods from a `t0` (the wall-clock start of period 0) with
a fixed `period_len`: period k covers `[t0 + k·len, t0 + (k+1)·len)`. The
one place that rule is written; the forecast horizon, the month period grid,
and a run's metadata all build their grids from it.

**Rate**:
Price per hour of use, in dollars — per commodity for bikes, per resource for
trucks.

**Cost**:
Dollars a flow has accrued at the moment of an event; a trip's total cost is
the value on its final arrived. Cumulative like elapsed periods.

**Measures**:
The money, time and length columns an event row can be widened with: rate,
elapsed periods, cost, and the duration and distance pairs.

### Prefixes and views

**Planned / Realized**:
The journal records both the intent and the outcome: `planned_*` is what was
meant (target, end period), `realized_*` what actually happened (NA if
lost). `realized` as an adjective means "actual outcome vs the plan"; the
verb `realize` means turning wanted demand into departures bounded by
inventory. Never name a departure count `realized`.

**View prefixes**:
The same concept exists as up to five views, told apart by a prefix on the
same canonical word — do not invent new stems: `raw_` (untouched source
data), `historical_` (ground truth from real history), `simulated_` (from a
finished run's journal), `forecast_` (predicted by a model), `state_` (the
live value during a run). `flow_id` carries the same idea at the row level:
`hist_` ids come from history, `sim_` ids from the simulator.

**Raw boundary**:
The raw Citi Bike names (`station_id`, `depot_id`, `truck_id`, `ride_id`,
`rideable_type`) are the external schema, correct only in the raw loader.
The loaders rename at the boundary: station/depot → `facility_id`, truck →
`resource_id`, rideable type → `commodity_category`. Past the loader only
the canonical names exist.

### Read-models

Pure functions of the journal; each marginal has one canonical word, the
same under every view prefix.

**Departures**:
The per-(source, commodity) table of outflow per period. Its columns are
`departed` and `lost` — the same two event types the phase emits.
_Avoid_: `realized` as the table or count name, dispatched

**Lost demand**:
The rows of departures where lost > 0: demand that did not depart.
_Avoid_: stockout (that is the reason tag, not the data)

**Arrivals**:
Inflow per period and target — arrived events only.

**Redirects**:
Bounces per period and facility, counted at the full planned target.

**Losses**:
Lost bikes per period and facility, per reason: a stockout loss at the
trip's source, a dock-full loss at its planned target.

**OD matrix**:
The origin–destination demand model: per (source, target, commodity) a
count, a probability P(target | source, commodity), and a mean duration —
the mean historical trip length in every routing mode.

### Runs

**Scenario inputs**:
The input tables of one scenario — everything the simulator reads during a
run. The type `ScenarioInputs` lists the fields; the simulator is typed
against this contract, not against the loader.
_Avoid_: resolved data, engine tables

**Canonical phases**:
The three-phase list every run of the scenario uses: dock earlier arrivals,
form departures, dock same-period arrivals. Rebalancing opts in by appending
its own phases.

**Base replay**:
A run whose departures equal the historical ones. The limits (stockout,
dock-full) are in the pipeline but never take effect.

**Saturated**:
An initial inventory or a capacity table set far above any demand, so the
limits never take effect.

**Sizing run**:
A run of the same scenario with saturated state, used only to measure what
the scenario needs: the required initial inventory and capacities are read
from its journal.

**Sized run**:
A run whose state was sized first: a sizing run measures the state, then the
demand runs against it, then the run invariants are checked. Equal scale
factors give a base replay.

**Forecast run**:
A sized run whose demand table is a **Forecast demand table** instead of a
historical one. The run's metadata records the forecast's name and the share
of demand dropped when cutting it to the scenario.

**Reference run**:
The run the two-level evaluation compares every forecast against: a sized
run on the actual demand of the held-out month. Its sized state is the
replay state every replay-state forecast run reuses.

**Replay-state forecast run**:
A run of a *forecast* demand table against the state sized on the *actual*
demand of the same period grid. The physical state is the reference run's,
so any difference in run totals comes from the forecast alone.

**Run state**:
The in-memory record of one started run in the API process: run name, status
(`queued` / `running` / `done` / `failed`), progress, error. Lost on a
restart; the status endpoint then falls back to the disk.
_Avoid_: job, task record; pending / in progress / finished

### Run artifacts

**Run artifact**:
One saved run: the folder `data/runs/<run_name>/` — `meta.json` plus the
parquet tables (the journal, the panel, the arcs, the flow totals, the
facilities). The UI and the API read only these files; the contracts live in
`gbp/artifacts.py`.
_Avoid_: output layer, results

**Facility period panel**:
One row per (period, facility, commodity) with that period's values side by
side: inventory at start and end of period, demand, departed, arrived,
redirected, the losses. Every map view and hover box is a slice of this one
table.

**Flow totals**:
One row per flow with its whole-trip values. Not here: a stockout loss (it
has no flow — it lives in the panel as lost demand) and a flow still riding
when the run ends.

**Metric**:
One value the UI can show: a value column of the panel or a whole-run
number. The `METRICS` table describes each metric once; the panel values,
the UI labels and the KPI row are all built from it.

**Attribution rule**:
A flow's cost, distance and duration belong to its **origin facility**
(`source_id`) and its **start period** — the place and period the demand
occurred. A flow's distance is the sum over its arcs.

### Routing

**Routes**:
The one object that answers distance and travel-time queries for facility
pairs, built once per scenario. Every reader of a facility-pair distance
asks it.
_Avoid_: inline distance formulas

**Routing mode**:
How **Routes** measures — `haversine` (straight-line distance, travel time
from the mean historical trip speed; the default) or `osrm` (road-network
distance and riding time from a local OSRM server; an unroutable pair falls
back to the haversine answer). Two things never change with the mode: the OD
matrix's duration stays the mean historical trip length, and truck travel
times stay straight-line at the truck's speed.
_Avoid_: distance mode, travel model

**Neighbor distance**:
The one neighbour-ranking metric for redirects: squared Euclidean distance
on (lat, lng), `neighbor_distance_sq`. Both the redirect mechanics and the
redirect explainer rank stations with it.
_Avoid_: a second inline distance formula

### Rebalancing

**Rebalance**:
The second `flow_type`: one bike moved by a truck. Opens with a departed
(the pickup, −1 at the source), closes with an arrived (the dropoff);
`resource_id` is the truck. Not demand — every demand read-model filters it
out.

**Undocking**:
Any event that takes a bike out of a dock: a first-arc departed, user trip
and truck pickup alike.

**Rebalancing window**:
The wall-clock stretch the trucks work in, at night. Planned once per
window, executed period by period.

**Home depot**:
The depot a truck starts its route from and returns to. A dropoff whose
station is full docks its bikes at the truck's home depot.

**Truck fleet**:
How many trucks run and their home depots — a run parameter, not loaded
data.

**Target inventory**:
How many bikes a station should hold when the window ends, computed from the
expected morning demand.

**Imbalance**:
Inventory minus target, per (facility, commodity). Positive: bikes to give
(pickups). Negative: needs bikes (dropoffs).

**Rebalance plan**:
The bike-level table the phases execute: one row per bike with its pickup
and dropoff facility, period and minute (minutes since the window started —
the solver's time axis). Built from the solver's stops.

### Demand forecasting

**Forecast demand table**:
A table in the shape of the historical demand table whose quantity comes
from a model. The simulator reads it exactly as it reads historical demand —
that is the whole integration contract. Fractional values are rounded once,
by the largest-remainder rule.
_Avoid_: predictions, predicted demand

**Forecast artifact**:
One saved forecast: a named folder under `data/ml/forecasts/` with the
demand table and its metadata, loaded by name.
_Avoid_: model output folder, prediction file

**Forecast horizon**:
The periods a forecast covers, numbered from 0 at the forecast's own `t0` —
a forecast run is its own scenario with its own clock.
_Avoid_: prediction window

**Month period grid**:
The hourly period grid of one calendar month. One grid for every
month-shaped task: training partitions, the backtest, the monitoring
baseline, the two-level evaluation.
_Avoid_: month grid, hourly grid of the month

**Hour of week**:
`weekday * 24 + hour`, 0..167, 0 = Monday 00:00. Carries weekly patterns
onto forecast periods; the OD matrix for a forecast run is the historical
one pooled per hour of week.
_Avoid_: weekly slot, hourofweek bucket

**Seasonal naive**:
The baseline model: the forecast for a station at a given hour is the mean
demand at the same hour of week over the history window. Every later model
must beat it.
_Avoid_: naive baseline (as a data name)

**Training table**:
The table a model learns from: one row per (period, facility, commodity)
with the observed departure count, the feature columns, and the censoring
mark `stockout_share`. Zero rows are kept — no departures is a real
observation.
_Avoid_: dataset, train set

**Feature columns**:
The model's inputs: calendar, daily weather, and history features. A history
value whose source hours are not observed stays NaN — missing, never zero.
_Avoid_: predictors, covariates, X

**History window**:
The weeks of counts the history features read, right before the rows being
built. Features never read the rows they describe or anything after them.
_Avoid_: lookback, context window

**Forecast input**:
The feature table a model predicts from: one row per (period, facility,
commodity) of the forecast horizon, with the feature columns appended.
_Avoid_: inference table, X_test

**Censored demand**:
Observed departures are a lower bound of demand: a stockout hour records
zero no matter how many people wanted a bike. Where station-status snapshots
exist, a training row carries `stockout_share` — the share of its hour the
station had zero bikes. Not a feature; NaN means "no snapshot covers this
hour", never zero.
_Avoid_: truncated demand, demand mask

**Model family**:
One way to forecast demand behind the one `DemandModel` interface: `fit`
learns from a training table, `predict` returns fractional demand. Four
families: seasonal naive, SARIMAX, LightGBM, GraphSAGE.
_Avoid_: algorithm, estimator, model type

**Fractional demand**:
A model's raw prediction: demand-shaped rows whose quantity is a
non-negative float. Becomes a forecast demand table by rounding; the
backtest reads it unrounded.
_Avoid_: raw prediction, y_hat

**Backtest**:
Model validation on held-out months: train on months 1..k, forecast month
k+1, move the split forward. Scores per split, overall and busy vs quiet
stations; logged to MLflow.
_Avoid_: cross-validation

**Two-level evaluation**:
How a model is judged. Level 1: forecast error against the held-out month's
actual counts. Level 2: run the simulator on the forecast with the state
held fixed (the reference run's replay state) and compare the run totals and
the panel against the reference.
_Avoid_: validation, A/B test

**Data version**:
The exact content of the raw and training data, tracked by DVC: git versions
the `.dvc` checksum files, the data lives in the DVC cache. A training run
names its data version by the git commit of the `.dvc` files.
_Avoid_: dataset snapshot, data hash

**Model version**:
One trained model in the registry: a fitted model plus its family, training
months, and data version, carried as tags.
_Avoid_: model artifact, checkpoint

**Champion**:
The model version the platform currently uses for forecasts, marked in the
registry by the alias `champion` and always resolved by that alias, never by
a file path. A version that lost the comparison stays a `challenger`.
_Avoid_: production model, best model

**MLflow store**:
The one folder MLflow keeps everything in: the experiment runs and the model
registry.
_Avoid_: tracking dir (as a concept name), mlflow backend

**Retraining pipeline**:
The steps that turn newly published data into a promote-or-keep decision:
download → build-table → train → backtest → promote. Each step is idempotent
and runnable alone. Promote rule: the candidate becomes champion only when
its backtest score over the same splits is at least as good as the
champion's.
_Avoid_: retraining job, CI for models

**Pipeline log**:
One row per retraining-pipeline run: data version, candidate and champion
scores, promoted yes or no, and the reason. The audit trail of every
promote-or-keep decision.
_Avoid_: audit table, run history

**Monitoring metrics table**:
One row per saved forecast and actual month it covered: MAE, Poisson
deviance, bias, and the month's naive MAE. Scoring the same pair again
replaces its row.
_Avoid_: scoring log, eval history

**Degraded month**:
The monitoring alert: a month where a model version's rolling MAE is worse
than the month's seasonal naive MAE.
_Avoid_: alert month, regression

**Drift report**:
The check that a new month still looks like the champion's training data;
drift means the feature distributions moved.
_Avoid_: data shift report, distribution check

### Data folders

**Data folders**:
`data/raw/` — the downloaded source files exactly as published, never edited
by code; `data/processed/` — a cache of processed trip files, always safe to
delete; `data/runs/` — one folder per saved run artifact; `data/ml/` — the
forecasting data (training tables, forecasts, monitoring, the MLflow store).
_Avoid_: bronze / silver / gold, source data, output layer

## Relationships

- A **Flow** moves one **Commodity** between **Facilities** along **Arcs**;
  a **Resource** carries **Rebalance** flows.
- **Demand** splits exactly into departed + lost(stockout); every departed
  flow closes with arrived or lost(dock_full).
- The **Read-models** are pure functions of the journal; inventory at any
  moment is derivable from it, never stored alongside it.
- A run reads **Scenario inputs** and saves a **Run artifact**; the UI and
  the API read only artifacts.
- A **Forecast demand table** takes the place of the historical demand in a
  **Forecast run**; the **Champion** produces it.

## Flagged ambiguities

- `stock` — banned for inventory: write `inventory` / `inventory_before`,
  never `stock` / `stock_before`. The one exception is the `reason` value
  `stockout` (and the derived training column `stockout_share`, which
  measures time in the stockout condition, not lost demand).
- `origin` / `destination` vs `source` / `target` — resolved: OD matrix
  only; everywhere else `source` and `target`.
- `facility` vs "station" — resolved: `facility` in identifiers and code;
  "station" fine in prose. A local name for rows filtered to
  `facility_category == "station"` states the category value and is allowed.
- `realized` — the adjective and column prefix ("actual outcome vs the
  plan"), never the name of the departure count or table.
- `journal` — the role word for the flows table's append-only nature, never
  a variable or column name; the data is always `flows`.
- "docked" — only an arrived event docks a bike; when you mean one specific
  outcome, use its event word.
