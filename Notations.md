# Notations — the canonical vocabulary

This file is the project's dictionary. **One concept, one word.** When code, a
docstring, or a chat answer needs to name something in this domain, it uses the
word listed here — not a synonym. Different words for one thing make the code
harder to read and impossible for a tool to check.

Every word is anchored to real names in the code: the **flow journal**
(`gbp/model/flows.py`) for everything about movement, and the **graph loader**
(`gbp/loaders/dataloader_graph.py`) for the entities, their attributes, and the
`historical_` / `simulated_` / `state_` prefixes (§10). If a concept is
missing, add it here first, then use it — never coin a synonym in passing.

**The naming rule that catches most drift.** A name names the **concept** (the
data), never a *tag on* the data. `reason` values such as `stockout` and
`dock_full` live **only** in the `reason` field; a frame of demand lost to a
stockout is `lost_demand`, not `stockout` (§1).

How to use it: writing code — use these words for variables, columns,
functions, docstrings. Talking to the user — use these words (translated).
Reviewing code — a listed concept named with a listed "avoid" word is drift to
fix; the `check-notations` skill automates this pass.

This file holds words and their meanings only. How the mechanisms work, and
why they are built that way, lives in `docs/key-components/`.

---

## 0. The flow-event schema (the symbol table)

The literal columns of one flow event (`FLOW_EVENT_COLUMNS` in `flows.py`).
Every other word in this file is one of these columns, one of their values, or
a value computed from them. The machine-checked form is `FLOW_EVENT_SCHEMA`
(`gbp/model/journal_schema.py`), checked by `validate_run` on a finished run
and by `get_historical_flows_df` at load time.

| Column | What it holds |
|---|---|
| `flow_id` | Id of the flow this event belongs to (§3). |
| `move_id` | Arc index inside the trip, `0..m` — one physical edge of the trip; each redirect bounce adds one more arc. |
| `event_id` | Event ordinal inside the trip, `0..n`. Row uniqueness is the pair `(flow_id, event_id)`. |
| `period_id` | The period the event happened in (§6). |
| `flow_type` | The kind of flow: `user_trip` (a rider's trip) or `rebalance` (a bike moved by a truck, §14). |
| `event_type` | One of the four outcomes: `departed`, `arrived`, `redirected`, `lost` (§1). |
| `commodity_category` | The bike type (§8). |
| `source_id` | The facility the flow left (§4). |
| `planned_target_id` | The facility the flow meant to dock at (§4, §5). |
| `realized_target_id` | The facility it actually docked at; NA if lost (§4, §5). |
| `start_period` | The period the flow departed (§6). |
| `planned_end_period` | The period the arc was expected to dock (§6). |
| `realized_end_period` | The period it actually docked; NA if lost (§6). |
| `resource_id` | The truck's id on `rebalance` events (§14); NA on user trips (§5b). |
| `quantity` | Bikes in the event. One per bike after expansion (§3). |
| `reason` | Why a flow did not simply arrive: `stockout` or `dock_full`; NA otherwise (§1). |
| `phase_rank` | Which inventory phase of a period applied the event's change: `0` dock-previous, `1` the period's departures and stockout losses, `2` dock-same; a later phase takes `3`, `4`, … Open-ended. A label, and the historical loader's step-ordering input (§0.1). |
| `phase_round` | The round inside one phase, when a phase applies several ordered batches in a row: `0` for a single batch, `1..` for later rounds (today only the redirect rounds). A label (§0.1). |
| `step_id` | Run-global ordinal of the inventory step the event belongs to (§0.1). |

**Arcs and the two roles of `departed`.** A normal trip is one arc
(`move_id = 0`): a `departed`, then an `arrived`. A redirect adds an arc per
bounce: the bike bounces off a full station (`redirected` closes the arc) and
departs on a new arc — the **continuation leg** (`redirect_leg_events`) — to
the station chosen for it. So a `departed` means one of two things:

- `move_id == 0` — an **undocking**: the bike leaves a dock (`−1` to the
  source's inventory). On a `user_trip` it is a real user departure — the
  outflow and the trip the OD model learns from; on a `rebalance` flow it is a
  truck pickup (§14), which is **not** demand.
- `move_id >= 1` — a redirect continuation leg, pure transport: no inventory
  change, not demand and not outflow.

The predicates in `flows.py`: `is_undocking` (the `−1` side of the inventory
rule), `is_docking` (the `+1` side), `is_user_departure` (an undocking
`departed` with `flow_type == "user_trip"` — every reader that means "a user
departure" filters through it). A stockout `lost` has `move_id = 0`; a
dock-full `lost` closes the flow's current arc, so it carries that arc's
`move_id`.

### 0.1. Moment and step (the inventory time axis)

Inventory changes in discrete **steps**: one batch of `+1`/`-1` applied
together. Between two steps inventory is constant.

| Canonical | Meaning | Avoid |
|---|---|---|
| `step` / `step_id` | One inventory step. `step_id` is its run-global ordinal, monotonic: it orders periods, the phases inside a period, and the rounds inside a phase. Events applied together share one `step_id`. | `seq`, `tick`, `moment_id` |
| `moment` | Inventory seen just **before** or just **after** a step — a prose word and the `_before`/`_after` suffix on inventory read-models. | `moment` as a column name |

`step_id` has one owner per producer. The simulator opens each number from a
run-global counter at apply time (`SimulationState.apply_step_events`). The
historical loader derives it from the `(period_id, phase_rank, phase_round)`
label (`stamp_history_ordering`) — safe because in history one label is always
exactly one batch. `finalize_flows` assigns no number for either producer: it
refuses a journal whose order columns are missing. Why the counter beats the
label: [flow-journal.md](docs/key-components/flow-journal.md).

`step_id` carries only the order, never the inventory: inventory at any moment
is a pure function of the journal. `get_inventory_df` (§9) is the coarse
per-period view; `inventory_at_moments` is the fine view, with
`inventory_before` / `inventory_after` per step — never `stock_before` /
`inventory_snapshot`.

---

## 1. The four flow outcomes and the two reasons

A *flow* is one bike's movement (§3). Each flow ends in exactly one of four
outcomes — the `event_type` values, the most important words here.

| Canonical | Meaning | Builder | Avoid |
|---|---|---|---|
| `departed` | A bike left its source: the flow's opening (move 0), a redirect continuation leg (move ≥ 1), or a truck pickup (§14). | `departed_events` / `redirect_leg_events` / `rebalance_departed_events` | `dispatched`, `released` |
| `arrived` | A bike docked at the current arc's target. | `arrived_events` / `rebalance_arrived_events` | — |
| `redirected` | A bike *bounced* off the full target of its current arc; it rides on on a new leg (`move_id + 1`). The bounce docks nowhere. | `redirected_events` | `placed`, `rerouted` |
| `lost` | A trip that did not happen / a bike that left the system. | `lost_events` | `shortfall`, `missing`, `dropped`, `failed` |

*Dock* is the verb for landing a bike, and only `arrived` docks one
(`DOCKING_EVENT_TYPES = ["arrived"]`). When you mean one specific outcome, use
its event word, not `docked`.

**The `reason` tags** — exactly two values, and they are tags on a `lost` (or
`redirected`) event, never names for the data:

- `stockout` — demand that never departed (no bike at the source). Always `lost`.
- `dock_full` — the planned target had no free dock: `redirected` if another
  station had one, `lost` if none did.

So demand splits exactly into `departed + lost(stockout)`, and every departed
flow ultimately closes with `arrived` or `lost(dock_full)`. The data names are
`lost_demand` (§7) and `lost_dock_full` — never `stockout` or `dock_full` as a
frame or variable name. One allowed derived name: the training table's
`stockout_share` (§17), the share of an hour a station spent with zero bikes —
it measures time in the stockout condition, not lost demand.

The word `stock` is banned for inventory (§2). `stockout` is the one
exception: a single, standard, atomic term and a real `reason` value — not a
license to write `stock` elsewhere.

---

## 2. Core state

| Canonical | Meaning | Avoid |
|---|---|---|
| `inventory` | Bikes currently docked at facilities (the amount on hand). Columns `facility_id`, `commodity_category`, `quantity`. | `stock`; `on-hand` as a data name (the prose phrase "bikes on hand" is fine) |
| `in_transit` | Bikes that departed but have not yet docked (the working set). | "moving set", "moving bikes" |
| `demand` | The number of trips users wanted. `demand = departed + lost(stockout)`. | — |
| `supply` | Inventory in its "available to depart" role (the `available` column inside `realize_departures`). A role view of `inventory`, not a second word for the inventory table. | — |
| `occupancy` | Bikes docked at a facility, summed across commodities — the docks are shared. One function owns this total: `occupancy_per_facility` in `flows.py`. | per-commodity dock counts, `utilization` |
| `free_docks` | Free dock slots per facility: `capacity` minus the facility's `occupancy`. Function `free_docks` in `mechanics.py`. | `available docks`, `slots` |
| `fits` / `overflow` | The two halves of the one docking rule, `dock_up_to_capacity(due, free)`: within each target the first `free` flows dock (`fits`), the rest are `overflow`. | `spillover`, `excess` |

`stock` is the main offender: write `inventory` / `inventory_before`, never
`stock` / `stock_before`. (The one allowed appearance of the letters `stock`
is the `reason` value `stockout`; see §1.)

---

## 3. Flow, trip, bike

Three levels of the same physical movement. Keep them distinct.

| Canonical | Level | Meaning |
|---|---|---|
| `trip` | demand / aggregate | A journey a user wants to take. Lives in counts and the OD matrix (`potential_trips`). |
| `flow` | journal | One trip recorded in the journal, identified by `flow_id`. The atomic journal unit. |
| `bike` | physical | One unit. After expansion each flow moves one bike (`quantity = 1`). |

Rule of thumb: aggregate counts and the OD matrix = `trip`; anything with a
`flow_id` = `flow`; the physical count = `bike`.

### 3.1. The flow-event table and its names

| Canonical | Meaning |
|---|---|
| `flows` | The data name of the flow-event table, taken by the prefix system (§10): `state_flows_df`, `historical_flows_df`, `simulated_flows_df`. |
| `new_flows` | The batch of new flow-event rows a phase just built, not yet appended (the argument of `SimulationState.apply_step_events` / `append_flows`). Same row schema as `flows`. |
| `journal` | Role word only, used in prose for the append-only book / single source of truth. **Never a variable or column name** — the data is always `flows`. |

One stem, one root: row = `flow event`, trip = `flow`, table = `flows`, new
rows = `new_flows`. `journal` names the role, not the data.

---

## 4. Facility and its roles in a trip

| Canonical | Meaning | Avoid |
|---|---|---|
| `facility` | A node in the network. Identifier `facility_id`. | (see decision below) |
| `facility_category` | The kind of facility: `station` or `depot`. | — |
| `source` | The facility a trip leaves from. Column `source_id`. | `origin` (except OD matrix) |
| `target` | The facility a trip goes to. Columns `planned_target_id`, `realized_target_id`. | `destination` (except OD matrix); a bare `target_id` |
| `origin` / `destination` | Reserved for the **OD matrix** (Origin–Destination matrix) only. One more allowed spot, the same O sense: the chart attribution rule (§12) says a flow's totals belong to its "origin facility" — in code that is always `source_id`. | using them anywhere else |
| `neighbor_distance_sq` | The one neighbour-ranking metric: squared Euclidean distance on (lat, lng), in `flows.py`. Both the redirect mechanics and the explainer `redirect_neighbor_table` rank stations with it. | a second inline distance formula |

**Decision — `facility` vs `station`.** `facility` is canonical for
identifiers and code; "station" is the natural domain word and is fine in
plain-English prose, but is never an identifier. A local name for rows
filtered to `facility_category == "station"` (such as `stations` in the
loader's sizing helpers) states the category value, not a facility identifier,
and is allowed.

**The raw → canonical boundary.** The raw Citi Bike names (`station_id`,
`depot_id`, `truck_id`, `ride_id`, `rideable_type`) are the external schema,
correct in `dataloader_raw.py` only. The loaders rename them at the boundary:
`station_id`/`depot_id` → `facility_id`, `truck_id` → `resource_id`,
`rideable_type` → `commodity_category`. Past the loader, only the canonical
names exist.

### 4b. Edge (the pair of facilities a flow moves along)

| Canonical | Meaning |
|---|---|
| `edge` | A pair of facilities with a distance and a travel time between them. There is no edge table in the code today: edges appear as the answers of `routes` (§13) — `distance_km(source, target)` and `duration_periods(source, target)`. |

With `facility` (§4), `resource` (§5b) and `commodity` (§8), `edge` completes
the four entities of a flow graph: what moves (commodity), from where to where
(facility), along what (edge), carried by what (resource).

---

## 5. Planned vs realized

The journal records both what was *intended* and what *actually happened*:

| Canonical | Meaning |
|---|---|
| `planned_*` | What was intended: `planned_target_id`, `planned_end_period`. |
| `realized_*` | What actually happened: `realized_target_id`, `realized_end_period`. |

`realized` as an **adjective** means "actual outcome vs the plan" — use it
only with this meaning. The verb `realize` (as in `realize_departures`) means
"turn wanted demand into actual departures, bounded by inventory". Do **not**
use `realized` as the name of the departure *count* — see §7.

### 5b. Resource (the carrier)

| Canonical | Meaning |
|---|---|
| `resource` | A vehicle that can carry bikes between facilities. Identifier `resource_id`. |
| `resource_category` | The kind of resource. Only value today: `truck`. |

Resources are idle in the historical replay (`resource_id` is NA on every user
trip event). The rebalancing phases (§14) are their first user.

---

## 6. Time

| Canonical | Meaning |
|---|---|
| `period` / `period_id` | One step of the simulation clock. |
| `start_period` | The period a flow departed. The same on every row of the flow, redirect legs included — it records when the *flow* departed, not when an arc started. |
| `planned_end_period` | The period an arc was expected to dock (each redirect leg carries its own). |
| `realized_end_period` | The period a flow actually docked (NA if lost). |
| `duration` | Trip length in whole periods (`planned_end_period - start_period`), carried by the OD matrix. |
| `elapsed_periods` | How many periods a flow has been riding at the moment of an event: `period_id - start_period`. Cumulative over redirect legs. Read-model `flows_with_costs`. |
| `duration_periods` | A length in whole periods — the one name for a trip's or a pair's duration. On `flow_totals.parquet` the trip's realized length; in the wide journal the §5 prefixes pick the view (`planned_duration_periods` / `realized_duration_periods`); per facility pair `routes.duration_periods` (§13). The old names `planned_duration` / `realized_duration` are retired. |
| `leg_end_period` | Planning column of `plan_overflow_redirect`: the period a redirect's new leg will dock. Becomes the leg's `planned_end_period`. |
| `trip_speed_km_per_period` | Mean riding speed over the historical trips, in km per period. `routes` (§13) turns a straight-line distance into a travel time with it. |
| `period_len` | Wall-clock length of one period (default one hour); `start_timestamp` / `end_timestamp` are the period's bounds. |
| `t0` | Wall-clock start of period 0: the earliest historical trip start, floored to the hour. Saved in `meta.json`, so the UI can show times instead of period ids. |

### 6.1. Rate and cost (money)

| Canonical | Meaning |
|---|---|
| `rate` | Price per hour of use, in dollars. Per `commodity_category` for bikes (`commodities_categories_rates_df`); per `resource_id` for trucks (`resources_rates_df`). |
| `cost` | Dollars a flow has accrued at the moment of an event: `rate * elapsed_periods * period_len` hours. Cumulative like `elapsed_periods`; a trip's total cost is the value on its final `arrived`. Read-model `flows_with_costs`. |
| `measures` | The money, time and length columns an event row can be widened with: `rate`, `elapsed_periods`, `cost`, the planned/realized `duration_periods` (§6) and `distance_km` (§13) pairs. One read-model, `flows_with_measures`, adds them all. |

---

## 7. Departures

| Canonical | Meaning | Avoid |
|---|---|---|
| `departed` | The count of bikes that left a source this period (matches the event type). | `realized` (as a column), `dispatched` |
| `departures` | The per-`(source, commodity)` table of departures. Its columns are `departed` and `lost`. | naming the table `realized` |
| `lost_demand` | The rows of `departures` where `lost > 0`: demand that did not depart, bound for `lost` events with `reason="stockout"`. | `stockout` (that is the `reason` tag, not the data — see §1) |

This mirrors the journal: a row of `departures` splits into `departed` +
`lost`, the same two event types the phase emits.

---

## 8. Commodity

| Canonical | Meaning |
|---|---|
| `commodity_category` | The column: the bike type (`classic_bike` / `electric_bike`). |
| `commodity` | The short noun for the same thing in prose. |

Classic and electric bikes share the same physical docks but are counted per
`commodity_category`.

---

## 9. Marginals (the read-models of the journal)

The marginal observations are pure functions of the journal (the `flows_to_*`
and `get_inventory_df` read-models in `flows.py`). Each has one canonical
word, used for the historical, simulated, and live-state views alike (§10).

| Canonical | Meaning |
|---|---|
| `inventory` | Per-period bikes on hand per `(facility, commodity)` (§2). |
| `departures` | Outflow per period and source (§7). |
| `arrivals` | Inflow per period and target (docking events: `arrived` only). |
| `demand` | Realized user demand (= `departures` in an exact replay). |
| `redirects` | Bounces per period and facility: `redirected` events counted at the full `planned_target_id`. Read-model `flows_to_redirects`. |
| `losses` | Lost bikes per period and facility, per loss reason: a `stockout` loss at the trip's `source_id`, a `dock_full` loss at its `planned_target_id`. Read-model `flows_to_losses(flows, reason)`. |
| `od_matrix` | Origin–destination demand model: per `(source, target, commodity)` a `count`, a `probability` `P(target | source, commodity)`, and a mean `duration`. |

---

## 10. The prefix system (one concept, three views)

The same marginal exists as up to five views, told apart by a prefix on the
same canonical word. Do not invent new stems for the views.

| Prefix | Meaning | Example |
|---|---|---|
| `raw_` / `*_raw_df` | Untouched source data, before the canonical schema. | `trips_raw_df` |
| `historical_` | Ground truth derived from real history. | `historical_inventory_df`, `historical_demand_df` |
| `simulated_` | Derived from a finished run's journal. | `simulated_inventory_df`, `simulated_flows_df` |
| `forecast_` | Predicted by a model, for periods that may have no history (§17). | `forecast_demand_df` |
| `state_` | The live value during a run (in `SimulationState`). | `state_inventory_df`, `state_flows_df` |

`flow_id` carries the same idea at the row level: `hist_` ids come from
history, `sim_` ids from the simulator, so the two never collide in one
journal. In the base replay the historical and simulated views are equal by
construction.

---

## 11. Run kinds

A scenario can be run for different purposes. Keep them apart by name.

| Canonical | Meaning |
|---|---|
| `base replay` | A run with `demand_scale_factor = 1` whose departures equal the historical ones. The limits (stockout, dock-full) are in the pipeline but never take effect. |
| `sizing run` | A run of the same scenario with **saturated** initial inventory and capacities, used only to measure what the scenario needs; `size_state_for_demand` reads the required initial inventory and capacities from its journal. |
| `saturated` | An initial inventory or a capacity table set far above any demand, so the limits never take effect (`get_saturated_inventory_df`). |
| `canonical phases` | The three-phase list every run of the scenario uses: dock earlier arrivals, form departures, dock same-period arrivals. Built by `canonical_phases()`; the runner, the tests and the notebook all take the list from there. |
| `sized run` | A run whose state was sized first: a sizing run at `sizing_scale_factor` measures the state, then the demand runs at `demand_scale_factor` against it, then the run invariants are checked. `run_sized_scenario` owns this order and returns a `ScenarioRun`. Equal scale factors give a base replay. |
| `forecast run` | A sized run whose demand table is a forecast demand table (§17) instead of a historical one. `apply_saved_forecast` (in `gbp/loaders/dataloader_graph.py`) owns the step: it loads the named forecast artifact, cuts the demand to what the scenario can run (`restrict_demand_to_scenario`), and puts it in place of the historical demand; the dropped share goes into `meta.json` (`forecast_dropped_share`), and the run's `meta.json` records the forecast's name. |
| `reference run` | The run the two-level evaluation (§17) compares every forecast against: a sized run on the actual demand of the held-out month. Its sized state is the replay state every replay-state forecast run reuses; its `meta.json` says `demand_source: "history"`. |
| `replay-state forecast run` | A run of a *forecast* demand table against the state sized on the *actual* demand of the same period grid (`run_sized_scenario` with `sizing_data` set to the actual scenario data). The physical state is the reference run's, so any difference in run totals comes from the forecast alone. |

### 11.1. Scenario inputs (the tables a run reads)

| Canonical | Meaning | Avoid |
|---|---|---|
| `scenario inputs` | The input tables of one scenario — everything the simulator reads during a run. The type `ScenarioInputs` (`gbp/consumers/simulator/inputs.py`) lists the fields in one place; the simulator is typed against this contract, not against the loader. | "resolved data" as the name of what the simulator needs; "engine tables" |

Two suppliers of the contract exist: `ResolvedModelData` (the loader's
product) and the synthetic scenarios in `tests/scenarios.py`.

---

## 12. Run artifacts (the files the UI reads)

One saved run is a **run artifact**: a folder `data/runs/<run_name>/` built by
`app/artifacts.py` (the folder name is the `run_name`). The UI only reads
those files. Its files:

| Canonical | Meaning |
|---|---|
| `meta.json` | The run's parameters (the scale factors, `period_len`, `t0`, `routing_mode`, `demand_source`, `forecast_name`, `forecast_dropped_share`, the `rebalancing` block), its origin (`inputs` — the raw source files; `code_version` — the git commit, `-dirty` when uncommitted), the invariant `violations` from `validate_run`, whole-run `totals`, and the sized state the run started from (`initial_inventory_bikes`, `station_capacity_docks`). The full field list is the pydantic model `RunMeta` (`app/artifacts.py`). |
| `flows.parquet` | The finalized journal of the run, widened by `flows_with_measures` with the measures (§6.1). |
| `panel.parquet` | The **facility period panel**: one row per `(period_id, facility_id, commodity_category)` with that period's values side by side — `quantity_sop`, `quantity_eop` (§9 inventory), `demand`, `departed`, `arrived`, `redirected`, `lost_demand`, `lost_dock_full`. Every map view and hover box is a slice of this one table. |
| `arcs.parquet` | One row per **arc** — one physical edge of a trip, the `(flow_id, move_id)` pair (§0): `flow_type`, `resource_id`, `source_id`, `target_id` (realized if the arc ended with `arrived`, planned otherwise), `start_period`, `end_period`, the closing `event_type`, `reason`, `distance_km` (§13), and the endpoint coordinates. |
| `flow_totals.parquet` | One row per `flow_id` with the flow's whole-trip values: `flow_type`, origin `source_id`, the target pair, the periods, terminal `event_type`, `reason`, `duration_periods`, `distance_km`, `cost`. Not here: a stockout loss (it has no flow — it lives in the panel as `lost_demand`) and a flow still riding when the run ends. |
| `facilities.parquet` | Facility attributes for the maps: `facility_id`, `facility_category`, `lat`, `lng`, `capacity`. |

The contracts live in `app/artifacts.py`: `RunMeta` for `meta.json`, a pandera
schema per table (`RUN_TABLE_SCHEMAS`), and `save_scenario_run` — the one
operation that saves a finished run.

Chart attribution rule: a flow's `cost`, `distance_km` and `duration_periods`
belong to its **origin facility** (`source_id`) and its **`start_period`** —
the place and period the demand occurred. `distance_km` is the length of an
arc in kilometres, measured by the run's `routing_mode` (§13); a flow's
`distance_km` is the sum over its arcs.

A **metric** is one value the UI can show: a value column of the panel or a
whole-run number (`cost`, `distance_km`). The `METRICS` table in
`app/artifacts.py` describes each metric once; `PANEL_VALUES`, the UI label
dictionaries and the KPI row are all built from it.

---

## 13. Routing (distance and travel time between facilities)

Routing is how an `edge` (§4b) shows up in the code: `routes` answers the
distance and the travel time for any pair of facilities.

| Canonical | Meaning | Instead of |
|---|---|---|
| `routes` | The one object that answers distance and travel-time queries for facility pairs: `distance_km(source, target)` and `duration_periods(source, target)` (class `Routes` in `gbp/routing.py`). Built once per scenario; every reader of a facility-pair distance asks it. | inline `haversine_km` calls |
| `routing_mode` | How `routes` measures: `haversine` or `osrm`. A `ResolvedModelData` parameter; saved in `meta.json`. | "distance mode", "travel model" |
| `haversine` (mode) | The formula mode, and the default: straight (great-circle) distance; travel time is that distance over `trip_speed_km_per_period` (§6). | "formula mode", "straight-line mode" |
| `osrm` (mode) | Road-network mode: distance and riding time come from a local OSRM server (`docs/how-to/set-up-osrm.md`), fetched once when `Routes` is built. An unroutable pair falls back to the `haversine` answer. | — |

Not routing: `duration` on the OD matrix (§6) stays the mean **historical**
trip length in both modes, and the neighbour ranking of a redirect
(`neighbor_distance_sq`, §0) stays as it is in both modes. One recorded
exception: truck travel times (`truck_travel_minutes`, §14) do not ask
`routes` — they use the straight-line distance at the truck's speed; a
car-profile OSRM table is a recorded TODO in `rebalancing.py`.

---

## 14. Rebalancing (moving bikes by truck)

Rebalancing moves bikes between stations by truck at night, planned once per
window and executed period by period. Module:
`gbp/consumers/simulator/rebalancing.py`; how the two phases work:
[rebalancing.md](docs/key-components/rebalancing.md). A run opts in by appending
`rebalancing_phases(params)` to `canonical_phases()`.

| Canonical | Meaning |
|---|---|
| `rebalance` | The second `flow_type`: one bike moved by a truck. Opens with a `departed` (the pickup, `−1` at `source_id`), closes with an `arrived` (the dropoff); `resource_id` is the truck. Not demand: every demand read-model filters it out through `is_user_departure`. |
| `undocking` | Any event that takes a bike out of a dock: a `departed` with `move_id == 0`, user trip and rebalance pickup alike. Predicate `is_undocking` (§0). |
| `rebalancing window` | The wall-clock stretch the trucks work in: `window_start_hour` (default 1) plus `window_minutes` (default 120), both on `RebalancingParams`. |
| `home depot` / `home_facility_id` | The depot one truck starts its route from and returns to (column `home_facility_id` on `resources_df`). A dropoff whose station is full docks its bikes at the truck's home depot. |
| `truck fleet` | How many trucks run and their home depots — a **run parameter**, not loaded data: `apply_truck_fleet` replaces the resource tables on a copy of the resolved data. Default: 5 trucks at `depot_1`. |
| `target inventory` / `target` | How many bikes a station should hold when the window ends, computed from the expected morning demand over the target hours. Function `target_inventory`. |
| `imbalance` | `inventory − target`, per `(facility, commodity)`. Positive: bikes to give (pickups). Negative: needs bikes (dropoffs). Function `station_imbalance`. |
| free-docks clip | Planning-time cut of the dropoff side: a station's planned inflow is reduced to its free docks (§2). Function `clip_dropoffs_to_free_docks`. |
| `truck travel time` | Minutes a truck drives between two facilities: straight-line distance at `truck_speed_km_per_hour`, in both routing modes — the recorded exception to `routes` (§13). Function `truck_travel_minutes`. |
| `node` | One solver visit: at most `portion_size` bikes picked up or dropped at one facility. Built by `build_rebalance_nodes`. |
| `stop` | One row of a truck's route in the solver's answer: `resource_id`, `stop_seq`, `facility_id`, `stop_type` (`pickup` / `dropoff`), `commodity_category`, `quantity`, `minute`. |
| `minute` | Minutes since the window started — the solver's time axis. Applied as `period = window period + minute // minutes-per-period`. |
| `rebalance plan` | The bike-level table the phases execute: one row per bike with the pickup and dropoff facility, period and minute. Built by `assign_bikes_to_stops`; lives on `SimulationState.rebalance_plan` between the window's periods. |
| `REBALANCE_RANK` | 3 — the `phase_rank` of `ApplyRebalancingPhase`, after the three user-trip phases (0/1/2). |

---

## 15. Data folders (raw → processed → runs)

| Canonical | Meaning | Instead of |
|---|---|---|
| `data/raw/` | The downloaded source files, exactly as published: the Citi Bike trip CSVs, the daily Central Park weather, the archived station-status dumps (§17). Code never edits this folder; the one exception is the current year's weather file, downloaded again when it does not yet reach the asked dates. | "source data", "input folder", "bronze" |
| `data/processed/` | The processed copy of each trip CSV, written by `load_trips_raw_df` on the first load and read instead of the CSV afterwards. A cache: deleting the folder is always safe — the next load rebuilds it. | "preprocessed layer", "intermediate data", "silver" |
| `data/runs/` | One folder per saved run — the run artifacts the UI reads (§12). | "output layer", "results", "gold" |
| `data/ml/` | The forecasting data (§17): training tables in `training/`, saved forecasts in `forecasts/`, monitoring in `monitoring/`, the local MLflow store in `mlflow/`, and the pipeline log `pipeline_log.csv`. | "model folder", "ml artifacts" |

---

## 16. The run-artifact API (serving runs over HTTP)

The API (`app/api.py`, described in `docs/reference/api.md`) serves run
artifacts (§12) over HTTP and starts runs through the same
`runner.run_scenario` the Run scenario page calls. The artifact contract (§12)
**is** the API contract.

| Canonical | Meaning | Instead of |
|---|---|---|
| run state | The in-memory record of one started run in the API process: `run_name`, `status`, `progress`, `error`. Lost on a restart; the status endpoint then falls back to the disk. | "job", "task record" |
| `queued` / `running` / `done` / `failed` | The four values of a run state's `status`. One single-thread worker runs one scenario at a time. | "pending", "in progress", "finished" |
| `API_URL` | The backend switch of the Streamlit loader (`ui_shared.py`): unset — read local files; set — fetch the same runs from the API at that URL. | "remote mode flag" |
| `API_KEY` | The one shared access key, checked against the `X-API-Key` header on every endpoint except `/health`; unset (local development) — the check is off. | "token", "credentials" |

---

## 17. Demand forecasting (the model around the simulator)

The demand forecasting phase adds a model that predicts future demand; the
simulator runs on that prediction. Code in `gbp/ml/`, data under `data/ml/`
(§15); how it works: [ml-toolkit.md](docs/key-components/ml-toolkit.md). The words anchor to the
demand schema (`HISTORICAL_DEMAND_SCHEMA`, `gbp/loaders/dataloader_graph.py`).

| Canonical | Meaning | Avoid |
|---|---|---|
| `forecast demand table` | A table in the shape of `HISTORICAL_DEMAND_SCHEMA` (`period_id`, `facility_id`, `commodity_category`, `quantity`) whose `quantity` comes from a model. The simulator reads it exactly as it reads historical demand — that is the whole integration contract. Fractional values are rounded once, by the largest-remainder rule (`round_forecast_demand` in `gbp/ml/forecast.py`). | `predictions`, `predicted demand` |
| `forecast artifact` | One saved forecast: the folder `data/ml/forecasts/<forecast_name>/` with `demand.parquet` and `meta.json` (contract: `ForecastMeta` in `gbp/ml/forecast.py`; loaded by name through `load_forecast`). | `model output folder`, `prediction file` |
| `forecast horizon` | The periods a forecast covers: `horizon_periods` periods starting at the forecast's `t0`, numbered from 0 — a forecast run is its own scenario with its own clock. Grid builder: `get_forecast_periods_df`. | `prediction window` |
| `month period grid` | The hourly period grid of one calendar month (`month_period_grid` in `gbp/ml/data.py`). One grid for every month-shaped task: training partitions, the backtest, the monitoring baseline, the two-level evaluation. | `month grid`, `hourly grid of the month` |
| `hour of week` | `weekday * 24 + hour`, 0..167, 0 = Monday 00:00 (`hour_of_week` in `dataloader_graph.py`). Carries weekly patterns onto forecast periods; the OD matrix for a forecast run is the historical one pooled per hour of week (`map_od_matrix_by_hour_of_week`). | `weekly slot`, `hourofweek bucket` |
| `seasonal naive` | The baseline model: the forecast for a station at a given hour is the mean demand at the same hour of week over the history window (`SeasonalNaiveModel`, reading the feature `facility_hour_of_week_mean`). Every later model must beat it. Its month forecast is built by `naive_month_prediction`. | `naive baseline` (as a data name) |
| `training table` | The table a model learns from: one row per `(period_id, facility_id, commodity_category)` with the observed departure count, the feature columns, and the censoring mark `stockout_share`. Zero rows are kept — no departures is a real observation. Monthly parquet partitions under `data/ml/training/`, built oldest-first. | `dataset`, `train set` |
| `feature columns` | The model's inputs, appended by the one feature module `gbp/ml/features.py`. Calendar: `hour_of_day`, `day_of_week`, `month`, `is_holiday`. Weather (daily Central Park table): `temperature_max_c`, `temperature_min_c`, `precipitation_mm`. History: `quantity_lag_1w`, `quantity_mean_4w`, `facility_mean`, `facility_hour_of_week_mean`. A history value whose source hours are not observed stays NaN — missing, never zero. | `predictors`, `covariates`, `X` |
| `history window` | The counts the history features read: the `HISTORY_WEEKS` (8) weeks right before the rows being built (`clip_history_window`). Features never read the rows they describe or anything after them. | `lookback`, `context window` |
| `forecast input` | The feature table a model predicts from: one row per `(period, facility, commodity)` of the forecast horizon, with the feature columns appended (`forecast_input` in `gbp/ml/forecast.py`). | `inference table`, `X_test` |
| `censored demand` | Observed departures are a lower bound of demand: a stockout hour records zero no matter how many people wanted a bike. Where archived station-status snapshots exist, each training row carries `stockout_share` — the share of its hour the station had zero bikes (`gbp/ml/station_status.py`). Not a feature; NaN means "no snapshot covers this hour", never zero. | `truncated demand`, `demand mask` |
| `model family` | One way to forecast demand behind the one interface `DemandModel` (`gbp/ml/models/`): `fit(training_table)` learns, `predict(feature_table)` returns fractional demand. Four families: `seasonal_naive`, `sarimax`, `lightgbm`, `graphsage`. Constructed only through `create_model`. | `algorithm`, `estimator`, `model type` |
| `fractional demand` | A model's raw prediction: demand-shaped rows whose `quantity` is a non-negative float. Becomes a forecast demand table through `round_forecast_demand`; the backtest reads it unrounded. | `raw prediction`, `y_hat` |
| `backtest` | Model validation on held-out months: train on months `1..k`, forecast month `k+1`, move the split forward (`gbp/ml/backtest.py`). Scores per split: MAE and Poisson deviance (`gbp/ml/metrics.py`), overall and busy vs quiet stations; logged to MLflow. | `cross-validation` |
| `two-level evaluation` | How a model is judged. Level 1: forecast error against the held-out month's actual counts. Level 2: run the simulator on the forecast with the state held fixed (the replay state from the reference run, §11) and compare the run totals and the panel against the reference. Built by `python app/evaluate.py --month <YYYYMM>`. | `validation`, `A/B test` |
| `data version` | The exact content of `data/raw/` and `data/ml/training/`, tracked by DVC: git versions the `.dvc` checksum files, the data lives in the DVC cache and the local remote. A training run names its data version by the git commit of the `.dvc` files. | `dataset snapshot`, `data hash` |
| `champion` | The model version the platform currently uses for forecasts, marked in the model registry by the alias `champion`. The forecast builder resolves the model by this alias, never by a file path. A version that lost the comparison stays a `challenger`. | `production model`, `best model` |
| `model version` | One trained model in the registry (named `demand-model`, `gbp/ml/registry.py`): a fitted model plus its family, training months, and data version, carried as tags. Two pipeline runs over the same three reuse the version. | `model artifact`, `checkpoint` |
| `MLflow store` | The one folder MLflow keeps everything in (default `data/ml/mlflow/`): the experiment runs and the model registry. In code: the object `MlflowStore` (`gbp/ml/registry.py`); every registry operation is a method on it. | `tracking dir` (as a concept name), `mlflow backend` |
| `retraining pipeline` | The steps that turn newly published data into a promote-or-keep decision: `download → build-table → train → backtest → promote` (`gbp/ml/pipeline.py`). Each step is idempotent and runnable alone (`--steps`). Promote rule: the candidate becomes champion only when its backtest score over the same splits is at least as good as the champion's. | `retraining job`, `CI for models` |
| `pipeline log` | One row per retraining-pipeline run, appended to `data/ml/pipeline_log.csv`: the data version, the candidate and champion scores, `promoted` yes or no, and the reason. The audit trail of every promote-or-keep decision. | `audit table`, `run history` |
| `monitoring metrics table` | `data/ml/monitoring/metrics.parquet` — one row per saved forecast and actual month it covered: MAE, Poisson deviance, bias (mean of predicted − actual), and the month's `naive_mae`. Appended by `python -m gbp.ml.monitoring --month <YYYYMM>`; scoring the same pair again replaces its row. | `scoring log`, `eval history` |
| `degraded month` | The monitoring alert: a month where a model version's rolling MAE (mean over its last `ROLLING_MONTHS` (3) scored months) is worse than the month's seasonal naive MAE (`metric_history` in `gbp/ml/monitoring.py`). Marked red on the "Model monitoring" page. | `alert month`, `regression` |
| `drift report` | The check that a new month still looks like the champion's training data; drift means the feature distributions moved. Built by Evidently; files `drift_<month>.html` and `drift_<month>.json` in `data/ml/monitoring/`. | `data shift report`, `distribution check` |

---

## Known drift to fix

The audit (`check-notations`) lists current offenders here so the file does
not rot. Remove an entry once its hits are gone.

- None currently (last sweep: 2026-07-06).
