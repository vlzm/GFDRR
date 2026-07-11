# Notations — the canonical vocabulary

This file is the project's dictionary, the way a textbook lists its symbols
before chapter one. **One concept, one word.** When code, a docstring, or a chat
answer needs to name something in this domain, it uses the word listed here — not
a synonym.

**Why this exists.** The same thing kept getting several names (`stock` and
`inventory`; `shortfall`, `lost`, "the missing part"; `placed` and `redirected`),
and reason tags leaked into data names (`stockout` used as a frame variable for
lost demand). Different words for one thing make the code harder to read and
impossible for a tool to check. Fixing one word per concept lets both people and
Claude rely on it, and lets close-by concepts be recognized by their names.

**The two anchors.** Every word here is anchored to real names in the code, not
chosen by taste:

- **The flow journal** (`gbp/model/flows.py`) is the anchor for everything
  about *movement* — the event schema (§0), the four outcomes, the planned/realized
  axis, the marginals. The journal is the single source of truth for what happened
  in a run, and the layer the whole system speaks.
- **The graph loader** (`gbp/loaders/dataloader_graph.py`) is the anchor for the
  *entities and their attributes* — facilities, resources, commodities, the period
  grid — and for the `historical_` / `simulated_` / `state_` naming system (§10).

If a concept is missing, add it here first (anchored to one of the two), then use
it — never coin a synonym in passing.

**The naming rule that catches most drift.** A name names the **concept** (the
data), never a *tag on* the data and never a near-synonym. The clearest case:
`reason` values such as `stockout` and `dock_full` live **only** in the `reason`
field. A frame of demand lost to a stockout is `lost_demand`, not `stockout`. See
§1.

**How to use it.**
- *Writing code:* use these words for variables, columns, functions, docstrings.
- *Talking to the user:* use these words (translated) instead of inventing new ones.
- *Reviewing code:* a name for a listed concept that uses a listed "avoid" word is
  drift to fix. The `check-notations` skill automates this pass.
- *Missing a word?* Add the concept here first, then use it.

---

## 0. The flow-event schema (the symbol table)

The literal columns of one flow event (`FLOW_EVENT_COLUMNS` in `flows.py`).
Every other word in this file is one of these columns, one of their values, or a
projection of them. This is the anchor; read it first.

The machine-checked form of this table is `FLOW_EVENT_SCHEMA`
(`gbp/model/journal_schema.py`): a pandera schema built from
`FLOW_EVENT_DTYPES`, checked once per journal — by `validate_run` on a
finished run and by `get_historical_flows_df` at load time.
`check_journal_schema(flows)` returns its violations as a list of strings,
empty when the journal fits.

| Column | What it holds |
|---|---|
| `flow_id` | Id of the flow this event belongs to (§3). |
| `move_id` | Arc index inside the trip, `0..m`. One physical edge of the trip; each redirect bounce adds one more arc (`move_id = 1, 2, …`). Set by the builders. |
| `event_id` | Event ordinal inside the trip, `0..n`, set by the builders at emit time. Arc `m` opens with `departed` at event `2m` and ends at event `2m + 1`. Row uniqueness is the pair `(flow_id, event_id)`. |
| `period_id` | The period the event happened in (§8). |
| `flow_type` | The kind of flow: `user_trip` (a rider's trip) or `rebalance` (a bike moved by a truck, §14). |
| `event_type` | One of the four outcomes: `departed`, `arrived`, `redirected`, `lost` (§1). |
| `commodity_category` | The bike type (§9). |
| `source_id` | The facility the flow left (§4). |
| `planned_target_id` | The facility the flow meant to dock at (§4, §5). |
| `realized_target_id` | The facility it actually docked at; NA if lost (§4, §5). |
| `start_period` | The period the flow departed (§8). |
| `planned_end_period` | The period it was expected to dock (§8). |
| `realized_end_period` | The period it actually docked; NA if lost (§8). |
| `resource_id` | The resource that carried it: the truck's id on `rebalance` events (§14); NA on user trips (§5b). |
| `quantity` | Bikes in the event. One per bike after expansion (§3). |
| `reason` | Why a flow did not simply arrive: `stockout` or `dock_full`; NA otherwise (§1). |
| `phase_rank` | Which inventory phase of a period applied the event's change. An **open-ended** integer that orders the phases inside a period, not a fixed set. Today's user trips use `0` dock-previous, `1` the period's own departures and stockout losses, `2` dock-same; a later phase (such as rebalancing) takes `3`, `4`, … Stamped by the emitting phase (the historical loader stamps it by timing, `phase_rank_by_timing`). A **label** that says which phase; the historical loader's step-ordering input (§0.1). |
| `phase_round` | The round inside a single phase, for a phase that applies several ordered inventory batches in a row: `0` when the phase applies one batch, `1..` for each later round. Today only the redirect mechanics use it (a redirect's rounds); a later phase that iterates (such as a rebalancer's rounds) reuses the same column. `0` on every other row. A **label** that says which round; the historical loader's step-ordering input (§0.1). |
| `step_id` | Run-global ordinal of the inventory step the event belongs to; the inventory time axis below the period (§0.1). In the simulator it is **opened at apply time** -- a phase takes the next number from a run-global counter when it begins an ordered change and writes it onto that step's events. The historical loader stamps no number, so `finalize_flows` **derives** it from the `(period_id, phase_rank, phase_round)` label instead. |

**Arcs and the two roles of `departed`.** A normal trip is one arc
(`move_id = 0`): a `departed` then an `arrived`. A redirect adds an arc per
bounce: the bike reaches the arc's full target, bounces (`redirected`, closing
the current arc), then departs on a new arc — the **continuation leg**
(`redirect_leg_events`) — to the station chosen for it. The leg takes the pair's
travel time from the OD matrix (for a pair with no OD entry, the `routes`
estimate — §13); a leg that takes time docks in a
later period, where it can bounce again. So a `departed` means one of two things:

- `departed` with `move_id == 0` — an **undocking**: the bike leaves a dock
  (`−1` to the source's inventory). On a `user_trip` it is a real user
  departure — the outflow and the trip the OD model learns from; on a
  `rebalance` flow it is a truck pickup (§14), which moves inventory the same
  way but is **not** demand.
- `departed` with `move_id >= 1` — a **redirect continuation leg** (pure
  transport). The bike never occupied a dock at the full station it left, so this
  event changes **no** inventory and is **not** demand or outflow.

The `−1` side of the inventory delta rule is the undocking predicate
`is_undocking` in `flows.py` (a `departed` with `move_id == 0`, user trip and
rebalance pickup alike); the `+1` side is the docking predicate `is_docking`.
Every reader that means "a user departure" (demand, outflow, the OD model)
filters through `is_user_departure` — an undocking `departed` with
`flow_type == "user_trip"` (used by `flows_to_departures`,
`flows_to_od_matrix`). A stockout `lost` has `move_id = 0`
(it has no arc); a dock-full `lost` closes the flow's current arc, so it
carries that arc's `move_id`.

### 0.1. Moment and step (the inventory time axis)

The journal also carries time *below* the period. Inventory changes in discrete
**steps**: one batch of `+1`/`-1` applied together (a dock batch, a period's
departures, one redirect round). Between two steps inventory is constant.

| Canonical | Meaning | Avoid |
|---|---|---|
| `step` / `step_id` | One inventory step. `step_id` is its run-global ordinal, monotonic: it orders periods, and inside a period the phases (dock-previous → departures → dock-same, then any later phase) and, inside a phase that batches in rounds, its rounds. Events applied together share one `step_id`. | `seq`, `tick`, `moment_id` |
| `moment` | Inventory seen just **before** or just **after** a step — a prose word and the `_before`/`_after` suffix on inventory read-models. A step has two moments around it; the after-moment of step `s-1` is the before-moment of step `s`. | `moment` as a column name |

`step_id` is filled two ways, depending on who produced the events.

**Simulator: opened at apply time.** Each phase writes its events through one
operation, `SimulationState.apply_step_events`: behind it the state opens a step
per ordered batch (`SimulationState.open_step` — the next number from a single
run-global counter), stamps `phase_rank`, `phase_round` and `step_id` on the
rows, and appends them to the journal. The same call moves the live inventory by
the batch's `+1`/`-1` rule (`inventory_deltas_from_events`) and updates
`in_transit` (`in_transit_after_events`), so a phase cannot write events that
disagree with either value.
The phases run in step order — dock-previous, then departures, then dock-same, then
any later phase, and a redirect's rounds in turn — so the counter hands out
0, 1, 2, … in exactly the order steps must sort. Each phase class declares its
rank once (`Phase.phase_rank`), and the engine refuses a phase list that is not
ordered by rank, so the list order and the stamped ranks cannot disagree. The
number comes from the counter,
never from the event columns, so two separately opened steps always get different
`step_id` values.

**Historical loader: derived from the label.** The loader has no phases and opens
no step, so it stamps no number. `finalize_flows` then numbers the distinct
`(period_id, phase_rank, phase_round)` tuples 0, 1, 2, … in sorted order. This is
safe because history is pure user trips — no redirects, no rebalancing — so one
tuple is always exactly one batch. `phase_rank` orders the phases inside a period
(today: dock-previous = 0, the period's own departures and stockout losses = 1,
dock-same = 2, with later phases taking 3, 4, …) and the loader stamps it by timing
(`phase_rank_by_timing`); `phase_round` orders the rounds inside one phase.

`phase_rank` and `phase_round` stay on every simulator row too, but only as
**labels** — when / which phase / which round. They no longer *define* a step in
the simulator (the opened number does); they remain the historical loader's
step-ordering input.

**Why opening beats deriving.** When a step is opened, two *separately ordered*
inventory changes can never share a `step_id`, because the counter never hands out
a number twice. Deriving from the tuple was correct only while every phase kept an
unwritten contract — one `(period_id, phase_rank, phase_round)` tuple is exactly
one batch — and the journal could not prove the contract held, because the batch
boundary is not stored once events are written. A future rebalancer that emits two
ordered batches under one tuple would have had them silently merged; opening the
step makes that collision impossible to express. The per-step non-negativity check
(`inventory_at_moments`, §2) stays as a cheap end-of-run guard, but it is no longer
the only thing standing between us and a silent merge.

`step_id` carries only the *order*, never the inventory: inventory at any moment
stays a pure function of the journal (initial inventory plus the cumulative
`+1`/`-1` along `step_id`). Per-period inventory (`get_inventory_df`, §9) is the
coarse view — the value at each period's last step; `inventory_at_moments` is the
fine view, with `inventory_before` / `inventory_after` per step (§2 — never
`stock_before` / `inventory_snapshot`).

---

## 1. The four flow outcomes and the two reasons

A *flow* is one bike's movement (§3). Each flow ends in exactly one of four
outcomes — the `event_type` values, the most important words here.

| Canonical | Meaning | Builder | Avoid |
|---|---|---|---|
| `departed` | A bike left its source. Opens the flow (move 0); a redirect's continuation leg is also a `departed` (move ≥ 1, built by `redirect_leg_events`), and so is a truck pickup (`flow_type="rebalance"`, §14). | `departed_events` / `redirect_leg_events` / `rebalance_departed_events` | `dispatched`, `released` |
| `arrived` | A bike docked at the current arc's target (a plain trip's planned target, the station a redirect leg headed to, or a truck dropoff's station). | `arrived_events` / `rebalance_arrived_events` | — |
| `redirected` | A bike *bounced* off the full target of its current arc; it rides on on a new leg (`move_id + 1`). The bounce itself docks nowhere. | `redirected_events` | `placed`, `rerouted` |
| `lost` | A trip that did not happen / a bike that left the system. | `lost_events` | `shortfall`, `missing`, `dropped`, `failed` |

*Dock* is the verb for landing a bike. Only `arrived` docks a bike: it ends a
normal trip, and it also ends a redirect's second arc (so a redirected bike's real
docking is its final `arrived`). `redirected` is now the intermediate *bounce* off
a full station — the bike did not dock there — so it is **not** a docking event
(`DOCKING_EVENT_TYPES = ["arrived"]`). When you mean one specific outcome, use its
event word, not `docked`.

**The `reason` tags.** When a flow does not simply arrive, the `reason` field says
why. There are exactly two values, and they are **tags on a `lost` (or
`redirected`) event, never names for the data**:

- `stockout` — demand that never departed (no bike at the source). Always `lost`.
- `dock_full` — the planned target had no free dock. If another station had one,
  the outcome is `redirected`; if none did, it is `lost`.

So demand splits exactly into `departed + lost(stockout)`, and every departed
flow's arc ends as `arrived`, `redirected`, or `lost(dock_full)`. A
`redirected` flow is not yet finished — each bounce opens a new leg, which can
bounce again where it arrives — so every departed flow ultimately closes with
one terminal: `arrived` or `lost(dock_full)`.

**`stockout` is a `reason` value only — never a data name.** A frame or variable
holding demand lost to a stockout is `lost_demand` (§7). A bike that left and
docked nowhere is a `lost` flow with `reason="dock_full"` — do not name that frame
`dock_full` either; it is `lost_dock_full`. Use the tag in the `reason` field and
in prose ("a stockout loss"); use `lost` / `lost_demand` / `lost_dock_full` for
the data. One allowed derived name: the training table's `stockout_share` (§17),
the share of an hour a real station spent with zero bikes — it measures time in
the stockout condition, not lost demand, so `lost_demand` would be the wrong word
for it.

**Note on the letters `stock`.** The word `stock` is banned for inventory (§2).
`stockout` is the one exception: it is a single, standard, atomic term and a real
`reason` value in the journal. It is **not** a license to write `stock` for
inventory anywhere else.

---

## 2. Core state

| Canonical | Meaning | Avoid |
|---|---|---|
| `inventory` | Bikes currently docked at facilities (the amount on hand). Columns `facility_id`, `commodity_category`, `quantity`. | `stock`; `on-hand` as a data name (the prose phrase "bikes on hand" is fine — this file uses it too) |
| `in_transit` | Bikes that departed but have not yet docked (the working set). | "moving set", "moving bikes" |
| `demand` | The number of trips users wanted. `demand = departed + lost(stockout)`. | — |
| `supply` | Inventory in its "available to depart" role (`state_supply_df`; the `available` column inside `realize_departures`). A *role view* of `inventory`, not a second word for the inventory table in general. | — |
| `free_docks` | Free dock slots per facility: `capacity` minus all bikes docked there, summed across commodities (the docks are shared). Function `free_docks` in `mechanics.py`; the redirect explainer's `free_before` / `free_after` are the same value at the step's two moments. | `available docks`, `slots` |
| `fits` / `overflow` | The two halves of the one docking rule, `dock_up_to_capacity(due, free)`: within each target the first `free` flows dock (`fits`), the rest are `overflow`. A user trip's overflow is redirected (§1); a rebalance dropoff's overflow docks at the truck's home depot (§14). | `spillover`, `excess` |

`stock` is the main offender: the fundamental thing is `inventory`, so never write
`stock` / `stock_before` — write `inventory` / `inventory_before`. (The one
allowed appearance of the letters `stock` is the `reason` value `stockout`; see §1.)

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

A `flow` is made of one or more **flow events** (rows, schema in §0). The table of
those rows has one canonical stem -- `flows` -- and one role word -- `journal`.
Keep the two jobs apart so they never read as two different things:

| Canonical | Meaning |
|---|---|
| `flows` | The data name of the flow-event table, taken by the prefix system (§10): `state_flows_df`, `historical_flows_df`, `simulated_flows_df`. |
| `new_flows` | The batch of new flow-event rows a phase just built, not yet appended (the argument of `SimulationState.apply_step_events` / `append_flows`). Same row schema as `flows`. |
| `journal` | Role word only, used in prose for the append-only book / single source of truth ("the `flows` table is the journal"). **Never a variable or column name** -- the data is always `flows`, never `journal` or `journal_events`. |

One stem, one root: row = `flow event`, trip = `flow`, table = `flows`, new rows =
`new_flows`. `journal` names the role, not the data.

---

## 4. Facility and its roles in a trip

| Canonical | Meaning | Avoid |
|---|---|---|
| `facility` | A node in the network. Identifier `facility_id`. | (see decision below) |
| `facility_category` | The kind of facility: `station` or `depot`. | — |
| `source` | The facility a trip leaves from. Column `source_id`. | `origin` (except OD matrix) |
| `target` | The facility a trip goes to. Columns `planned_target_id`, `realized_target_id`. | `destination` (except OD matrix); a bare `target_id` |
| `origin` / `destination` | Reserved for the **OD matrix** (Origin–Destination matrix) only, where O and D stand for exactly source and target. One more allowed spot, the same O sense: the chart attribution rule (§12) says a flow's totals belong to its "origin facility" — in code that is always the `source_id` column. | using them anywhere else |
| `neighbor_distance_sq` | The one neighbour-ranking metric: squared Euclidean distance on (lat, lng), in `flows.py`. Both the redirect mechanics (deciding where a bounced bike goes) and the explainer `redirect_neighbor_table` rank stations with it, so the explanation always matches the decision. | a second inline distance formula |

**Decision — `facility` vs `station`.** The schema column is `facility_id` and
the journal is the source of truth, so `facility` is canonical for identifiers and
code. "station" is the natural domain word and is fine in plain-English prose, but
is never an identifier. Collapsing the two fully would mean renaming `facility_id`
→ `station_id` across the whole schema — a separate, larger decision. Until then:
`facility` in code, "station" only as prose. A local name for rows filtered to
`facility_category == "station"` (such as `stations` in the loader's sizing
helpers) states the category value, not a facility identifier, and is allowed.

**The raw → canonical boundary.** The raw Citi Bike sources use their own names
(`station_id`, `depot_id`, `truck_id`, `ride_id`, `rideable_type`). These are the
*external* schema and are correct in `dataloader_raw.py`. The loaders rename them
to the canonical schema at the boundary: `station_id`/`depot_id` → `facility_id`,
`truck_id` → `resource_id`, `rideable_type` → `commodity_category`. Past the
loader, only the canonical names exist.

---

## 5. Planned vs realized

The journal records both what was *intended* and what *actually happened*:

| Canonical | Meaning |
|---|---|
| `planned_*` | What was intended: `planned_target_id`, `planned_end_period`. |
| `realized_*` | What actually happened: `realized_target_id`, `realized_end_period`. |

`realized` as an **adjective** means "actual outcome vs the plan" — use it only
with this meaning. The verb `realize` (as in `realize_departures`) means "turn
wanted demand into actual departures, bounded by inventory."

Do **not** use `realized` as the name of the departure *count* — see §7.

### 5b. Resource (the carrier)

| Canonical | Meaning |
|---|---|
| `resource` | A vehicle that can carry bikes between facilities. Identifier `resource_id`. |
| `resource_category` | The kind of resource. Only value today: `truck`. |

Resources are idle in the historical replay (`resource_id` is NA on every user
trip event, and the resource observations are empty). The rebalancing phases
(§14) are their first user: every `rebalance` event carries the truck's
`resource_id`, and the trucks' attributes (`resources_capacities_df`,
`resources_rates_df`, `home_facility_id`) feed the routing solver.

---

## 6. Time

| Canonical | Meaning |
|---|---|
| `period` / `period_id` | One step of the simulation clock. |
| `start_period` | The period a flow departed. The same on every row of the flow, redirect legs included — it records when the *flow* departed, not when an arc started. |
| `planned_end_period` | The period an arc was expected to dock (each redirect leg carries its own). |
| `realized_end_period` | The period a flow actually docked (NA if lost). |
| `duration` | Trip length in whole periods (`planned_end_period - start_period`), carried by the OD matrix. |
| `elapsed_periods` | How many periods a flow has been riding at the moment of an event: `period_id - start_period`. Because `start_period` is the flow's opening period on every row, the value is cumulative over redirect legs: 0 on the opening `departed`, the first leg's length on a `redirected` bounce, the sum of all legs on the final `arrived`. Read-model `flows_with_costs`. |
| `duration_periods` | A length in whole periods — the one name for a trip's or a pair's duration. On `flow_totals.parquet` it is the trip's realized length (the terminal event's `elapsed_periods`). In the wide journal the §5 prefixes pick the view: `planned_duration_periods` (`planned_end_period - start_period`) and `realized_duration_periods` (`realized_end_period - start_period`, NA if lost). Per facility pair it is `routes.duration_periods` (§13). The old names `planned_duration` / `realized_duration` are retired. |
| `leg_end_period` | Planning column of `plan_overflow_redirect`: the period a redirect's new leg will dock (the bounce period plus the leg's travel time). Becomes the leg's `planned_end_period`. |
| `trip_speed_km_per_period` | Mean riding speed over the historical trips (total great-circle distance over total ride time, from the raw timestamps), in km per period. `routes` (§13) turns a straight-line distance into a travel time with it. |
| `period_len` | Wall-clock length of one period (default one hour); `start_timestamp` / `end_timestamp` are the period's bounds. |
| `t0` | Wall-clock start of period 0: the earliest historical trip start, floored to the hour. Period `k` starts at `t0 + k * period_len` (the `periods_df` grid). Saved in `meta.json`, so the UI can show times instead of period ids. |

### 6.1. Rate and cost (money)

| Canonical | Meaning |
|---|---|
| `rate` | Price per hour of use, in dollars. Per `commodity_category` for bikes (`commodities_categories_rates_df` — what a user pays to ride); per `resource_id` for trucks (`resources_rates_df`). |
| `cost` | Dollars a flow has accrued at the moment of an event: `rate * elapsed_periods * hours per period` (`period_len`). Cumulative like `elapsed_periods` (§6); a trip's total cost is the value on its final `arrived`. Read-model `flows_with_costs`. |
| `measures` | The money, time and length columns an event row can be widened with: `rate`, `elapsed_periods`, `cost`, `planned_duration_periods`, `realized_duration_periods` (§6), `planned_distance_km`, `realized_distance_km` (§13). One read-model, `flows_with_measures`, adds them all; the canonical notebook and the artifact builder call it. |

---

## 7. Departures

| Canonical | Meaning | Avoid |
|---|---|---|
| `departed` | The count of bikes that left a source this period (matches the event type). | `realized` (as a column), `dispatched` |
| `departures` | The per-`(source, commodity)` table of departures. Its columns are `departed` and `lost`. | naming the table `realized` |
| `lost_demand` | The rows of `departures` where `lost > 0`: demand that did not depart, bound for `lost` events with `reason="stockout"`. | `stockout` (that is the `reason` tag, not the data — see §1) |

This mirrors the journal: a row of `departures` splits into `departed` + `lost`,
the same two event types the phase emits.

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
and `get_inventory_df` read-models in `flows.py`). Each has one canonical word,
used for the historical, simulated, and live-state views alike (§10).

| Canonical | Meaning |
|---|---|
| `inventory` | Per-period bikes on hand per `(facility, commodity)` (§2). |
| `departures` | Outflow per period and source (§7). |
| `arrivals` | Inflow per period and target (docking events: `arrived` only — a redirected bike's inflow is the `arrived` that ends its second arc). |
| `demand` | Realized user demand (= `departures` in an exact replay). |
| `redirects` | Bounces per period and facility: `redirected` events counted at the full `planned_target_id` the bike bounced off. Read-model `flows_to_redirects`. |
| `losses` | Lost bikes per period and facility, per loss reason. The reason decides where the loss lands: a `stockout` loss at the trip's `source_id`, a `dock_full` loss at its `planned_target_id`. Read-model `flows_to_losses(flows, reason)`. |
| `od_matrix` | Origin–destination demand model: per `(source, target, commodity)` a `count`, a `probability` `P(target | source, commodity)`, and a mean `duration`. |

---

## 10. The prefix system (one concept, three views)

The same marginal exists as up to five views, told apart by a prefix on the same
canonical word. This is how related things are recognized by name — do not invent
new stems for the views.

| Prefix | Meaning | Example |
|---|---|---|
| `raw_` / `*_raw_df` | Untouched source data, before the canonical schema. | `trips_raw_df` |
| `historical_` | Ground truth derived from real history. | `historical_inventory_df`, `historical_demand_df` |
| `simulated_` | Derived from a finished run's journal. | `simulated_inventory_df`, `simulated_flows_df` |
| `forecast_` | Predicted by a model, for periods that may have no history (§17). | `forecast_demand_df` |
| `state_` | The live value during a run (in `SimulationState`). | `state_inventory_df`, `state_flows_df` |

`flow_id` carries the same idea at the row level: `hist_` ids come from history,
`sim_` ids are generated by the simulator, so the two never collide in one journal.

In the base replay the historical and simulated views are equal by construction —
that is the point of deriving both through the same read-model functions.

---

## 11. Run kinds

A scenario can be run for two different purposes. Keep the two apart by name.

| Canonical | Meaning |
|---|---|
| `base replay` | A run with `demand_scale_factor = 1` whose departures equal the historical ones. The limits (stockout, dock-full) are in the pipeline but never take effect. |
| `sizing run` | A run of the same scenario with **saturated** initial inventory and capacities, used only to measure what the scenario needs. Its journal shows what the demand *wants* to do when no limit takes effect; `size_state_for_demand` reads the required initial inventory and capacities from it. |
| `saturated` | An initial inventory or a capacity table set far above any demand, so the limits never take effect (`get_saturated_inventory_df`). |
| `canonical phases` | The three-phase list every run of the scenario uses: dock earlier arrivals, form departures, dock same-period arrivals. Built by `canonical_phases()` in the simulator layer; the runner, the tests and the notebook all take the list from there. |
| `sized run` | A run whose state was sized first: measure the initial inventory and capacities against `sizing_scale_factor` with a sizing run, then run the demand at `demand_scale_factor` against that state, then check the run invariants. `run_sized_scenario` owns this order and returns a `ScenarioRun`: the journal, the final state, the sized state tables, and the invariant violations. Equal scale factors give a base replay; a larger run scale makes the limits take effect. |
| `forecast run` | A sized run whose demand table is a forecast demand table (§17) instead of a historical one. It goes through the same path — `size_state_for_demand` sizes the state, `run_sized_scenario` runs it — and its `meta.json` records the name of the forecast it used. |
| `reference run` | The run the two-level evaluation (§17) compares every forecast against: a sized run on the actual demand of the held-out month. Its sized state (initial inventory and dock capacities, computed from the actual demand) is the replay state that every replay-state forecast run reuses. Its demand comes from history, so its `meta.json` says `demand_source: "history"`. |
| `replay-state forecast run` | A run of a *forecast* demand table against the state sized on the *actual* demand of the same period grid: `run_sized_scenario` with `sizing_data` set to the actual scenario data. The physical state is the reference run's, so any difference in run totals against the reference comes from the forecast alone: overprediction becomes `lost_demand` and `redirected` (the state has no slack beyond what the actual demand needed), underprediction becomes departures below the reference. |

The sizing run works because every phase is deterministic and departures depend on
inventory only through `min(demand, inventory)`: a real run started from the
measured state repeats the sizing run's journal exactly, with zero stockout and
zero dock-full.

---

## 12. Run artifacts (the files the UI reads)

A finished run is saved to disk once, and the UI only reads those files. This
keeps the UI a pure reader: no simulation and no journal-level computation
happens while a page renders. One saved run is a **run artifact**: a folder
`data/runs/<run_name>/` built by `app/artifacts.py` (the folder name is the
`run_name`). Its files:

| Canonical | Meaning |
|---|---|
| `meta.json` | The run's parameters (`scenario_id`, `demand_scale_factor`, `sizing_scale_factor`, `number_of_periods`, `period_len`, `t0` — see §6, `routing_mode` — see §13, `demand_source` — `history` or `forecast`, and `forecast_name` — the forecast a forecast run used, see §11/§17, `rebalancing` — see §14: `enabled`, and when on also `truck_homes` and `truck_capacity_bikes`), the run's origin (`inputs` — file names of the raw source files the run was built from; `code_version` — the git commit of the code, with `-dirty` appended when there were uncommitted changes), the invariant `violations` list from `validate_run` (empty = valid), and `totals` — whole-run sums (demand, departed, arrived, redirected, lost_demand, lost_dock_full, cost, distance_km). Parameters + `inputs` + `code_version` together make a run reproducible: they name what was computed, from which data, by which code. |
| `flows.parquet` | The finalized journal of the run, widened by `flows_with_measures` with the measures (§6.1): `rate`, `elapsed_periods`, `cost`, the planned/realized `duration_periods` and `distance_km` pairs. |
| `panel.parquet` | The **facility period panel**: one row per `(period_id, facility_id, commodity_category)` with that period's values side by side — `quantity_sop`, `quantity_eop` (§9 inventory), `demand`, `departed`, `arrived`, `redirected` (bounces at this facility as the full planned target), `lost_demand`, `lost_dock_full`. Every map view and hover box is a slice of this one table. |
| `arcs.parquet` | One row per **arc** — one physical edge of a trip, the `(flow_id, move_id)` pair (§0). Carries `flow_type` (`user_trip` or `rebalance` — §14) and `resource_id` (the truck on a rebalance arc, NA otherwise), `source_id`, `target_id` (realized if the arc ended with `arrived`, planned otherwise), `start_period`, `end_period`, the closing `event_type`, `reason`, `distance_km` (measured by the run's `routing_mode` — §13), and the endpoint coordinates (`source_lat`, `source_lng`, `target_lat`, `target_lng`), so the trips map draws arcs without joining another table. |
| `flow_totals.parquet` | One row per `flow_id` with the flow's whole-trip values: `flow_type` (`user_trip` or `rebalance` — §14), origin `source_id`, `planned_target_id`, `realized_target_id`, `start_period`, `end_period`, terminal `event_type`, `reason`, `duration_periods`, `distance_km` (sum over its arcs), `cost` (value on the terminal event). The cost and distance/duration charts group this table. Not here: a stockout loss (it has no flow — `flow_id` is NA; it lives in the panel as `lost_demand`) and a flow still riding when the run ends (no terminal event yet). |
| `facilities.parquet` | Facility attributes for the maps: `facility_id`, `facility_category`, `lat`, `lng`, `capacity`. |

The `meta.json` contract is the pydantic model `RunMeta` (`app/artifacts.py`,
with the nested `RebalancingMeta` block): `build_meta` constructs it,
`save_run` writes it, and `load_run_meta` validates it back — an artifact
missing a field fails at load, with the field named. Each parquet table has a
pandera schema (`RUN_TABLE_SCHEMAS`, same file), checked in `save_run` before
writing; loading is not re-checked.

Chart attribution rule: a flow's `cost`, `distance_km` and `duration_periods`
belong to its **origin facility** (`source_id`) and its **`start_period`** — the
place and period the demand occurred. `distance_km` is a new column name: the
length of an arc in kilometres, measured by the run's `routing_mode` (§13);
a flow's `distance_km` is the sum over its arcs.

A **metric** is one value the UI can show: a value column of the panel
(`quantity_sop` … `lost_dock_full`) or a whole-run number (`cost`,
`distance_km`). The `METRICS` table in `app/artifacts.py` describes each metric
once — column name, full label, short label, whether it enters the KPI row and
the `totals` of `meta.json`. `PANEL_VALUES`, the UI label dictionaries and the
KPI row are all built from this one table.

---

## 13. Routing (distance and travel time between facilities)

| Canonical | Meaning | Instead of |
|---|---|---|
| `routes` | The one object that answers distance and travel-time queries for facility pairs: `distance_km(source, target)` and `duration_periods(source, target)` (class `Routes` in `gbp/routing.py`). Built once per scenario, held on `ResolvedModelData.routes`. Every reader of a facility-pair distance (the wide journal, the arcs table, the redirect travel-time fallback) asks it. | inline `haversine_km` calls |
| `routing_mode` | How `routes` measures: `haversine` or `osrm`. A `ResolvedModelData` parameter; saved in `meta.json`. | "distance mode", "travel model" |
| `haversine` (mode) | The formula mode, and the default. Distance is the straight (great-circle) line between the two facilities; travel time is that distance over `trip_speed_km_per_period` (§6). Needs nothing but coordinates. | "formula mode", "straight-line mode" |
| `osrm` (mode) | Road-network mode. Distance and riding time come from a local OSRM server (`docs/guides/osrm_setup.md`): the full facility-to-facility table is fetched once, in one `/table` request, when `Routes` is built. A pair the server cannot route falls back to the `haversine` answer. | — |

Not routing: `duration` on the OD matrix (§6) stays the mean **historical**
trip length in both modes — observed data beats any model. `routes` supplies
distances everywhere, but travel times only where history has no answer (a
redirect pair with no OD entry). The neighbour ranking of a redirect
(`neighbor_distance_sq`, §0) also stays as it is in both modes: it only orders
candidate stations by closeness.

One recorded exception: truck travel times for rebalancing
(`truck_travel_minutes`, §14) do not ask `routes`, because its table is built
with the bike profile and would give riding times. They use the straight-line
distance at the truck's speed in both modes; a car-profile OSRM table is a
recorded TODO in `rebalancing.py`.

---

## 14. Rebalancing (moving bikes by truck)

Rebalancing moves bikes between stations by truck at night, so that the
morning demand finds them. It is planned **once per window** and executed
**period by period**: the plan is computed at one point of simulated time,
and each of its stops is applied in the period its minute falls into. The
solver's clock (minutes) and the simulator's clock (periods) never mix.
Module: `gbp/consumers/simulator/rebalancing.py`.

| Canonical | Meaning |
|---|---|
| `rebalance` | The second `flow_type`: one bike moved by a truck. Opens with a `departed` (the pickup, `−1` at `source_id`), closes with an `arrived` (the dropoff, `+1` at `realized_target_id`); `resource_id` is the truck. Not demand: every demand read-model filters it out through `is_user_departure`. |
| `undocking` | Any event that takes a bike out of a dock: a `departed` with `move_id == 0`, user trip and rebalance pickup alike. Predicate `is_undocking` — the `−1` side of the inventory rule (§0); `is_user_departure` narrows it to `flow_type == "user_trip"` (the demand side). |
| `rebalancing window` | The wall-clock stretch the trucks work in: `window_start_hour` (default 1, i.e. 01:00) plus `window_minutes` (default 120), both on `RebalancingParams`. The planning phase fires in each period whose start hour equals `window_start_hour`. |
| `home depot` / `home_facility_id` | The depot one truck starts its route from and returns to (column `home_facility_id` on `resources_df`). Trucks may have different home depots. A dropoff whose station is full docks its bikes at the truck's home depot. |
| `truck fleet` | How many trucks run and each truck's home depot — a **run parameter**, not part of the loaded data: `apply_truck_fleet` (in `gbp/loaders/dataloader_graph.py`) replaces the three resource tables on a shallow copy of the resolved data. The Run page and the `--truck-homes` runner flag set it; default: 5 trucks at `depot_1`. |
| `target inventory` / `target` | How many bikes a station should hold when the window ends, from the expected morning demand: per `(facility, commodity)`, the running total of expected departures minus expected arrivals over the target hours (`target_start_hour..target_end_hour`), taken at its highest point. The departures and arrivals are the historical marginals scaled by the run's `demand_scale_factor`. Function `target_inventory`. |
| `imbalance` | `inventory − target`, per `(facility, commodity)`. Positive: the station has bikes to give (pickups happen there). Negative: it needs bikes (dropoffs happen there). Function `station_imbalance`. |
| free-docks clip | Planning-time cut of the dropoff side: a station's planned inflow is reduced to its free docks (§2 `free_docks`; every commodity's share is scaled by the same factor and rounded down, so the total fits). Pickups are untouched — `target >= 0` already bounds them by the inventory. Function `clip_dropoffs_to_free_docks`, applied between the imbalance and the nodes. |
| `truck travel time` | Minutes a truck drives between two facilities: straight-line (great-circle) distance at `truck_speed_km_per_hour`, in both routing modes — the recorded exception to `routes` (§13). Function `truck_travel_minutes`; the solver's travel input. |
| `node` | One solver visit: at most `portion_size` bikes picked up or dropped at one facility. A large imbalance is split into several nodes so that one truck does not have to serve it whole. Before the split, pickup and dropoff totals are matched per commodity — only `min(total surplus, total shortage)` bikes can move, because every truck must end its route empty. Built by `build_rebalance_nodes`. |
| `stop` | One row of a truck's route in the solver's answer: `resource_id`, `stop_seq` (visit order), `facility_id`, `stop_type` (`pickup` / `dropoff`), `commodity_category`, `quantity`, `minute`. |
| `minute` | Minutes since the window started — the solver's time axis. Applied as `period = window period + minute // minutes-per-period`; the sub-period detail is kept only for explanation. |
| `rebalance plan` | The bike-level table the phases execute: one row per bike with `flow_id`, `resource_id`, `commodity_category`, `source_id`, `planned_target_id`, `pickup_period` / `dropoff_period` and the two minutes. Built from the stops by `assign_bikes_to_stops` (a dropoff hands over the bikes that were picked up earliest). Lives on the state (`SimulationState.rebalance_plan`) between the window's periods. |
| `REBALANCE_RANK` | 3 — the `phase_rank` of `ApplyRebalancingPhase`, after the three user-trip phases (0/1/2). |

**The two phases.** `PlanRebalancingPhase` (writes no events) computes the
target and the imbalance, clips the dropoff side to each station's free docks
(`clip_dropoffs_to_free_docks`), builds the nodes, calls the routing solver
(`solve_rebalance_vrp` — OR-Tools; each truck starts and ends at its own
home depot (`home_facility_id` on `resources_df`), all stops inside
`window_minutes`), and stores the plan on the state.
`ApplyRebalancingPhase` (rank 3) runs every period in three rounds: dock the
dropoffs due from earlier periods (round 0), execute this period's pickups
(round 1, cut down to the bikes actually on hand), dock the same-period
dropoffs (round 2). Between pickup and dropoff the bikes sit in `in_transit`
like any riding bike; `DockArrivals` skips them (it docks user trips only). A
dropoff that finds the station full docks at the truck's home depot instead —
the `planned_*` / `realized_*` split (§5) records the difference. A run opts
in by appending `rebalancing_phases(params)` to `canonical_phases()`.

---

## 15. Data folders (raw → processed → runs)

The `data/` folder has four subfolders, one per stage of the data on disk:

| Canonical | Meaning | Instead of |
|---|---|---|
| `data/raw/` | The downloaded source files, exactly as published: the Citi Bike trip CSVs, the daily Central Park weather (`weather-central-park-<year>.csv`, one per year), and the archived station-status dumps (`<YYYYMM>-citi-bike-nyc-stats.parquet`) — see §17. Code never edits this folder; the one exception is the current year's weather file, which is downloaded again when it does not yet reach the asked dates. | "source data", "input folder", "bronze" |
| `data/processed/` | The processed copy of each trip CSV, written by `load_trips_raw_df` (`gbp/loaders/dataloader_raw.py`) on the first load: rows with missing key fields dropped, dtypes fixed, the trips schema checked, saved as parquet. Later loads read this copy instead of parsing the CSV, which is much faster. It is a cache: a copy counts as fresh only while it is newer than its CSV, and deleting the folder is always safe — the next load rebuilds it. Delete it after changing the cleaning code in `load_trips_raw_df`. | "preprocessed layer", "intermediate data", "silver" |
| `data/runs/` | One folder per saved run — the run artifacts the UI reads (§12). | "output layer", "results", "gold" |
| `data/ml/` | The forecasting data (§17): training tables in `training/`, one folder per saved forecast in `forecasts/` (the forecast demand table plus its `meta.json`), the monitoring metrics table and the drift reports in `monitoring/` (§17), the local MLflow store in `mlflow/` (experiment runs and the model registry), and the pipeline log `pipeline_log.csv` (§17). Created in the demand forecasting phase. | "model folder", "ml artifacts" |

Everything between `processed` and `runs` — `RawModelData`,
`ResolvedModelData`, the flow journal of a run — lives in memory for one run
and is not saved on its own. Those tables depend on the run's parameters, so
their only form on disk is the run artifact (§12), which records the
parameters, `inputs` and `code_version` next to the tables.

---

## 16. The run-artifact API (serving runs over HTTP)

The API (`app/api.py`, described in `docs/explanation/api.md`) serves run artifacts (§12) over
HTTP and starts runs through the same `runner.run_scenario` the Run scenario
page calls. It is a reader and a saver of run artifacts: it never computes
what `build_run_tables` can precompute, and the artifact contract (§12) **is**
the API contract — `meta.json` travels as JSON (the `RunMeta` model), each
table travels as its saved parquet bytes.

| Canonical | Meaning | Instead of |
|---|---|---|
| run state | The in-memory record of one started run in the API process: `run_name`, `status`, the `progress` lines from `on_progress`, and `error` when it failed. Lost on a restart; the status endpoint then falls back to the disk — an existing `meta.json` answers `done`, anything else `404`. | "job", "task record" |
| `queued` / `running` / `done` / `failed` | The four values of a run state's `status`. One single-thread worker runs one scenario at a time, so a second started run waits as `queued`. | "pending", "in progress", "finished" |
| `API_URL` | The backend switch of the Streamlit loader (`ui_shared.py`): unset — read local files, exactly as before; set — fetch the same runs from the API at that URL. The typed accessors (`load_panel`, `load_meta`, ...) keep their signatures either way. | "remote mode flag" |
| `API_KEY` | The one shared access key: the server checks it against the `X-API-Key` header on every endpoint except `/health`; unset (local development) — the check is off. | "token", "credentials" |

---

## 17. Demand forecasting (the model around the simulator)

The demand forecasting phase (plan: `docs/plans/ml_demand_forecast_plan.md`)
adds a model that predicts future demand; the simulator then runs on that
prediction. New code lives in `gbp/ml/`, new data under `data/ml/` (§15).
These words are fixed here before they appear in code. They anchor to the
demand schema in the graph loader (`HISTORICAL_DEMAND_SCHEMA`,
`gbp/loaders/dataloader_graph.py`).

| Canonical | Meaning | Avoid |
|---|---|---|
| `forecast demand table` | A table in the shape of `HISTORICAL_DEMAND_SCHEMA` (`period_id`, `facility_id`, `commodity_category`, `quantity`) whose `quantity` comes from a model, not from history. The simulator reads it exactly as it reads historical demand — that is the whole integration contract. Quantities are whole bikes: a model's fractional values are rounded once, by the largest-remainder rule per `(period, commodity)` (`round_forecast_demand` in `gbp/ml/forecast.py` — the same rule `form_potential_trips` uses to split departures over targets). | `predictions`, `predicted demand` |
| `forecast artifact` | One saved forecast: the folder `data/ml/forecasts/<forecast_name>/` with `demand.parquet` (the forecast demand table) and `meta.json` (model name and version, the history window, and the horizon — `t0`, `horizon_periods`, `period_len_hours`). The contract is the pydantic model `ForecastMeta` (`gbp/ml/forecast.py`); a forecast run loads a forecast by name through `load_forecast`, and its run `meta.json` records that name (`forecast_name`). | `model output folder`, `prediction file` |
| `forecast horizon` | The periods a forecast covers: `horizon_periods` periods starting at the forecast's `t0` (right after the history ends), numbered from 0 — a forecast run is its own scenario with its own clock. Grid builder: `get_forecast_periods_df`. | `prediction window` |
| `hour of week` | `weekday * 24 + hour`, 0..167, 0 = Monday 00:00 (`hour_of_week` in `dataloader_graph.py`). The key that carries weekly patterns onto forecast periods: the seasonal naive averages demand per hour of week, and the OD matrix for a forecast run is the historical one pooled per hour of week (`map_od_matrix_by_hour_of_week` — counts summed, durations count-weighted, shares recomputed). | `weekly slot`, `hourofweek bucket` |
| `seasonal naive` | The baseline model, and the first model of the platform: the forecast for a station at a given hour is the mean demand at the same hour of week over the history window. Since phase 4 it lives behind the model interface (`SeasonalNaiveModel` in `gbp/ml/models/seasonal_naive.py`) and reads the feature column `facility_hour_of_week_mean` — the same number, computed once by the feature module. Every later model must beat it. | `naive baseline` (as a data name) |
| `training table` | The table a model learns from: one row per `(period_id, facility_id, commodity_category)` with the observed departure count, the feature columns, and the censoring mark `stockout_share` (see `censored demand` below). Zero rows are kept — a station-hour with no departures is a real observation, not a missing one. Monthly parquet partitions under `data/ml/training/`; a month's features read only the partitions before it, so partitions are built oldest-first. | `dataset`, `train set` |
| `feature columns` | The model's inputs, appended to the training table and the forecast input by the one feature module `gbp/ml/features.py` — both builders call it, neither keeps a copy. Calendar: `hour_of_day`, `day_of_week`, `month`, `is_holiday` (US public holidays). Weather, joined by date from the daily Central Park table (`load_weather_daily`): `temperature_max_c`, `temperature_min_c`, `precipitation_mm`. History, from the history window: `quantity_lag_1w`, `quantity_mean_4w`, `facility_mean`, `facility_hour_of_week_mean`. A history value whose source hours are not observed stays missing (NaN) — missing, never zero. | `predictors`, `covariates`, `X` |
| `history window` | The counts the history features read: the `HISTORY_WEEKS` (8) weeks right before the rows being built (`clip_history_window`). Features never read the rows they describe or anything after them; the training table of a month is built as if that month were being forecast. | `lookback`, `context window` |
| `forecast input` | The feature table a model predicts from: one row per `(period, facility, commodity)` of the forecast horizon — every facility × commodity of the history window crossed with every horizon period — with the feature columns appended (`forecast_input` in `gbp/ml/forecast.py`). For the same station-day it holds exactly the training table's feature values; a test proves it. | `inference table`, `X_test` |
| `censored demand` | Observed departures are a lower bound of demand: an hour a station stood with no bikes records zero departures no matter how many people wanted one. The platform's working assumption is observed departures ≈ demand, biased low exactly where the system fails. Where archived station-status snapshots exist (the CityBikes dumps in `data/raw/`, from 2024-11 on), each training row carries the mark `stockout_share` — the share of its hour the station had zero bikes (`gbp/ml/station_status.py`); training can exclude or down-weight marked rows. The mark is not a feature: future stockouts are unknown at prediction time. NaN means "no snapshot covers this hour", never zero. | `truncated demand`, `demand mask` |
| `model family` | One way to forecast demand behind the one interface `DemandModel` (`gbp/ml/models/`): `fit(training_table)` learns, `predict(feature_table)` returns fractional demand. One file per family, four families: `seasonal_naive` (the baseline every other family must beat), `sarimax` (the classical baseline — SARIMAX on the daily city total, split by the hour-of-week means), `lightgbm` (the expected main model — one gradient boosting over all stations, the station as a categorical feature), `graphsage` (the research model — GraphSage on the station graph, edges from OD flows). The forecast builder and the backtest construct families only through `create_model`. | `algorithm`, `estimator`, `model type` |
| `fractional demand` | A model's raw prediction: demand-shaped rows (`period_id`, `facility_id`, `commodity_category`, `quantity`) whose `quantity` is a non-negative float, not yet whole bikes. It becomes a forecast demand table through the one rounding rule (`round_forecast_demand`), and the backtest metrics read it unrounded — rounding is a simulator constraint, not a model property. | `raw prediction`, `y_hat` |
| `backtest` | Model validation on held-out months: train on months `1..k`, forecast month `k+1`, move the split forward, average the scores over at least 3 splits (`gbp/ml/backtest.py`, splits built by `backtest_splits`). Rows from the future never appear in training — no random splits. Scores per split: MAE and Poisson deviance over the aligned rows (`gbp/ml/metrics.py`), overall and split into busy stations (the smallest set that made half of the month's departures) against the quiet rest. Every `(model, split)` run is logged to the local MLflow store `data/ml/mlflow/`; the run named `comparison` holds the table of every family against the seasonal naive baseline. | `cross-validation` |
| `two-level evaluation` | How a model is judged (plan, phase 5). Level 1: forecast error against the held-out month's actual counts — the backtest metrics. Level 2: run the simulator on the forecast with the physics held fixed — the state is the replay state from the reference run, only the demand table changes — and compare the run totals (`lost_demand`, `redirected`, `lost_dock_full`, `cost`) and the facility period panel against the reference. The runs: one reference run (§11) and per model one replay-state forecast run (§11). Built by `python app/evaluate.py --month <YYYYMM>`; the demand tables are first cut to the scenario's stations and OD coverage (`restrict_demand_to_scenario`) so every run faces the same universe. Result: `data/ml/evaluation/<month>/comparison.csv` and a report in `docs/reports/`. | `validation`, `A/B test` |
| `data version` | The exact content of the tracked data folders, `data/raw/` and `data/ml/training/`. DVC (the data versioning tool) writes each folder's checksum into a small text file next to it (`data/raw.dvc`, `data/ml/training.dvc`); git versions those files, while the data itself lives in the DVC cache and the local remote (`~/dvc-store/gfdrr-citibike`). A training run names its data version by the git commit of the `.dvc` files. | `dataset snapshot`, `data hash` |
| `champion` | The model version the platform currently uses for forecasts. In the model registry (the list of trained model versions that MLflow keeps) it is marked by the alias `champion` — a movable name that points at one version. The forecast builder resolves the model by this alias, never by a file path. A version that lost the comparison stays registered as a `challenger` and waits for the next one. | `production model`, `best model` |
| `model version` | One trained model in the registry: a fitted model plus what defines it — the model family, the training months, and the data version. The registered model is named `demand-model` (`gbp/ml/registry.py`); each version carries those three as tags, and its files are the folder `save` wrote, logged to the training run. Two pipeline runs over the same family, months, and data version reuse the same version instead of registering a twin. | `model artifact`, `checkpoint` |
| `retraining pipeline` | The steps that turn newly published data into a promote-or-keep decision: `download → build-table → train → backtest → promote` (`gbp/ml/pipeline.py`, `python -m gbp.ml.pipeline`). Each step is idempotent — rerunning it without new data changes nothing — and runnable alone (`--steps`). The trigger lives in the download step: it asks the bucket for published months missing from disk, so a scheduler (cron, later a cloud job) only has to start the pipeline. The promote rule: the candidate becomes champion only when its backtest score over the same splits is at least as good as the current champion's (an equal score promotes — same recipe, newer data version wins); otherwise it stays a challenger. | `retraining job`, `CI for models` |
| `pipeline log` | One row per retraining-pipeline run, appended to `data/ml/pipeline_log.csv`: when it ran, the data version, the candidate and champion versions with their backtest scores, `promoted` yes or no, and the reason in words. The log is the audit trail of every promote-or-keep decision. | `audit table`, `run history` |
| `monitoring metrics table` | `data/ml/monitoring/metrics.parquet` — one row per saved forecast and actual month it covered: MAE, Poisson deviance, bias (the mean of predicted − actual; positive means the model predicts too much), and the month's seasonal naive MAE next to them (`naive_mae`). Appended by `python -m gbp.ml.monitoring --month <YYYYMM>` once the month's training partition is on disk (`score_month` in `gbp/ml/monitoring.py`); scoring the same pair again replaces its row. Unlike the backtest, what is scored is the saved forecast demand table — whole bikes, the numbers the simulator consumed. | `scoring log`, `eval history` |
| `degraded month` | The monitoring alert: a month where a model version's rolling MAE — the mean of its MAE over the version's last `ROLLING_MONTHS` (3) scored months — is worse than the seasonal naive MAE of that month (`metric_history` in `gbp/ml/monitoring.py`). The "Model monitoring" page marks such months in red. A month with no baseline (no earlier partitions) is never marked. | `alert month`, `regression` |
| `drift report` | The check that a new month still looks like the data the champion was trained on; drift means the feature distributions moved. Built by Evidently over a row sample from both sides: the month's feature columns against the champion's training months (the month itself left out of the reference). Per column the measure is a distance (Wasserstein for number columns, Jensen–Shannon for category columns); the column has drifted when the distance is at or above its threshold. Files in `data/ml/monitoring/`: `drift_<month>.html` (the full report) and `drift_<month>.json` (the summary the monitoring page lists). | `data shift report`, `distribution check` |

---

## Known drift to fix

The audit (`check-notations`) lists current offenders here so the file does not
rot. Remove an entry once its hits are gone.

- None currently. The 2026-07-06 sweep fixed `stock` in docstrings and comments
  (`dataloader_graph.py`, `flows.py`), an `origin_id` parameter and a bare
  `realized` local in `flows.py`, `destinations` used for OD targets
  (`phases.py`, `mechanics.py`), `shortfall` in a rebalancing test name, and
  `dropped` as a variable for dropoff events (`tests/test_rebalancing.py`).
  Allowed uses were written into the sections instead of being re-flagged every
  audit: "bikes on hand" as prose (§2), "origin facility" in the chart
  attribution rule (§4, §12), `stations` as a local name for category-filtered
  rows (§4), and the truck travel-time exception to `routes` (§13).
- Earlier sweeps fixed `stockout`/`sink` used as data names, `target_id`
  aligned to `planned_target_id`, and the figurative words `dormant` / `bites` /
  `gating` / `spine` (the last also renamed `check_spine_closure` →
  `check_flow_closure`).
