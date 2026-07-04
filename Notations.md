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

| Column | What it holds |
|---|---|
| `flow_id` | Id of the flow this event belongs to (§3). |
| `move_id` | Arc index inside the trip, `0..m`. One physical edge of the trip; each redirect bounce adds one more arc (`move_id = 1, 2, …`). Set by the builders. |
| `event_id` | Event ordinal inside the trip, `0..n`, set by the builders at emit time. Arc `m` opens with `departed` at event `2m` and ends at event `2m + 1`. Row uniqueness is the pair `(flow_id, event_id)`. |
| `period_id` | The period the event happened in (§8). |
| `flow_type` | The kind of flow. Only value today: `user_trip`. |
| `event_type` | One of the four outcomes: `departed`, `arrived`, `redirected`, `lost` (§1). |
| `commodity_category` | The bike type (§9). |
| `source_id` | The facility the flow left (§4). |
| `planned_target_id` | The facility the flow meant to dock at (§4, §5). |
| `realized_target_id` | The facility it actually docked at; NA if lost (§4, §5). |
| `start_period` | The period the flow departed (§8). |
| `planned_end_period` | The period it was expected to dock (§8). |
| `realized_end_period` | The period it actually docked; NA if lost (§8). |
| `resource_id` | The resource that carried it; NA for user trips (§5b). |
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
travel time from the OD matrix (for a pair with no OD entry, the great-circle
distance over `trip_speed_km_per_period`); a leg that takes time docks in a
later period, where it can bounce again. So a `departed` means one of two things:

- `departed` with `move_id == 0` — a **real user departure** from a dock (`−1` to
  the source's inventory; it is the outflow and the trip the OD model learns from).
- `departed` with `move_id >= 1` — a **redirect continuation leg** (pure
  transport). The bike never occupied a dock at the full station it left, so this
  event changes **no** inventory and is **not** demand or outflow.

Every reader that means "a user departure" filters through the one named
predicate `is_user_departure` in `flows.py` (used by `flows_to_departures`,
`flows_to_od_matrix`, and the `−1` side of the inventory delta rule); the `+1`
side is the docking predicate `is_docking`. A stockout `lost` has `move_id = 0`
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
rows, and appends them to the journal.
The phases run in step order — dock-previous, then departures, then dock-same, then
any later phase, and a redirect's rounds in turn — so the counter hands out
0, 1, 2, … in exactly the order steps must sort. The number comes from the counter,
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
| `departed` | A bike left its source. Opens the flow (move 0); a redirect's continuation leg is also a `departed` (move ≥ 1, built by `redirect_leg_events`). | `departed_events` / `redirect_leg_events` | `dispatched`, `released` |
| `arrived` | A bike docked at the current arc's target (a plain trip's planned target, or the station a redirect leg headed to). | `arrived_events` | — |
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
the data.

**Note on the letters `stock`.** The word `stock` is banned for inventory (§2).
`stockout` is the one exception: it is a single, standard, atomic term and a real
`reason` value in the journal. It is **not** a license to write `stock` for
inventory anywhere else.

---

## 2. Core state

| Canonical | Meaning | Avoid |
|---|---|---|
| `inventory` | Bikes currently docked at facilities (the amount on hand). Columns `facility_id`, `commodity_category`, `quantity`. | `stock`, `on-hand` |
| `in_transit` | Bikes that departed but have not yet docked (the working set). | "moving set", "moving bikes" |
| `demand` | The number of trips users wanted. `demand = departed + lost(stockout)`. | — |
| `supply` | Inventory in its "available to depart" role (`state_supply_df`; the `available` column inside `realize_departures`). A *role view* of `inventory`, not a second word for the inventory table in general. | — |

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
| `origin` / `destination` | Reserved for the **OD matrix** (Origin–Destination matrix) only, where O and D stand for exactly source and target. | using them anywhere else |
| `neighbor_distance_sq` | The one neighbour-ranking metric: squared Euclidean distance on (lat, lng), in `flows.py`. Both the redirect mechanics (deciding where a bounced bike goes) and the explainer `redirect_neighbor_table` rank stations with it, so the explanation always matches the decision. | a second inline distance formula |

**Decision — `facility` vs `station`.** The schema column is `facility_id` and
the journal is the source of truth, so `facility` is canonical for identifiers and
code. "station" is the natural domain word and is fine in plain-English prose, but
is never an identifier. Collapsing the two fully would mean renaming `facility_id`
→ `station_id` across the whole schema — a separate, larger decision. Until then:
`facility` in code, "station" only as prose.

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
trip event, and the resource observations are empty), but the entity, its
attributes (`resources_capacities_df`, `resources_rates_df`, `home_facility_id`)
and its `resource_id` column are canonical and reserved.

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
| `leg_end_period` | Planning column of `plan_overflow_redirect`: the period a redirect's new leg will dock (the bounce period plus the leg's travel time). Becomes the leg's `planned_end_period`. |
| `trip_speed_km_per_period` | Mean riding speed over the historical trips (total great-circle distance over total ride time, from the raw timestamps), in km per period. The travel-time fallback for a redirect pair with no OD entry: `round(haversine_km / trip_speed_km_per_period)`. |
| `period_len` | Wall-clock length of one period (default one hour); `start_timestamp` / `end_timestamp` are the period's bounds. |
| `t0` | Wall-clock start of period 0: the earliest historical trip start, floored to the hour. Period `k` starts at `t0 + k * period_len` (the `periods_df` grid). Saved in `meta.json`, so the UI can show times instead of period ids. |

### 6.1. Rate and cost (money)

| Canonical | Meaning |
|---|---|
| `rate` | Price per hour of use, in dollars. Per `commodity_category` for bikes (`commodities_categories_rates_df` — what a user pays to ride); per `resource_id` for trucks (`resources_rates_df`). |
| `cost` | Dollars a flow has accrued at the moment of an event: `rate * elapsed_periods * hours per period` (`period_len`). Cumulative like `elapsed_periods` (§6); a trip's total cost is the value on its final `arrived`. Read-model `flows_with_costs`. |

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
| `od_matrix` | Origin–destination demand model: per `(source, target, commodity)` a `count`, a `probability` `P(target | source, commodity)`, and a mean `duration`. |

---

## 10. The prefix system (one concept, three views)

The same marginal exists as up to four views, told apart by a prefix on the same
canonical word. This is how related things are recognized by name — do not invent
new stems for the views.

| Prefix | Meaning | Example |
|---|---|---|
| `raw_` / `*_raw_df` | Untouched source data, before the canonical schema. | `trips_raw_df`, `gbfs_raw_df` |
| `historical_` | Ground truth derived from real history. | `historical_inventory_df`, `historical_demand_df` |
| `simulated_` | Derived from a finished run's journal. | `simulated_inventory_df`, `simulated_flows_df` |
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
| `meta.json` | The run's parameters (`scenario_id`, `demand_scale_factor`, `sizing_scale_factor`, `number_of_periods`, `period_len`, `t0` — see §6), the invariant `violations` list from `validate_run` (empty = valid), and `totals` — whole-run sums (demand, departed, arrived, redirected, lost_demand, lost_dock_full, cost, distance_km). |
| `flows.parquet` | The finalized journal of the run, widened by `flows_with_costs` (`rate`, `elapsed_periods`, `cost`). |
| `panel.parquet` | The **facility period panel**: one row per `(period_id, facility_id, commodity_category)` with that period's values side by side — `quantity_sop`, `quantity_eop` (§9 inventory), `demand`, `departed`, `arrived`, `redirected` (bounces at this facility as the full planned target), `lost_demand`, `lost_dock_full`. Every map view and hover box is a slice of this one table. |
| `arcs.parquet` | One row per **arc** — one physical edge of a trip, the `(flow_id, move_id)` pair (§0). Carries `source_id`, `target_id` (realized if the arc ended with `arrived`, planned otherwise), `start_period`, `end_period`, the closing `event_type`, `reason`, and `distance_km` (great-circle, `haversine_km`). The trips map draws these. |
| `flow_totals.parquet` | One row per `flow_id` with the flow's whole-trip values: origin `source_id`, `planned_target_id`, `realized_target_id`, `start_period`, `end_period`, terminal `event_type`, `reason`, `duration_periods`, `distance_km` (sum over its arcs), `cost` (value on the terminal event). The cost and distance/duration charts group this table. Not here: a stockout loss (it has no flow — `flow_id` is NA; it lives in the panel as `lost_demand`) and a flow still riding when the run ends (no terminal event yet). |
| `facilities.parquet` | Facility attributes for the maps: `facility_id`, `facility_category`, `lat`, `lng`, `capacity`. |

Chart attribution rule: a flow's `cost`, `distance_km` and `duration_periods`
belong to its **origin facility** (`source_id`) and its **`start_period`** — the
place and period the demand occurred. `distance_km` is a new column name: the
great-circle length of an arc in kilometres; a flow's `distance_km` is the sum
over its arcs.

---

## Known drift to fix

The audit (`check-notations`) lists current offenders here so the file does not
rot. Remove an entry once its hits are gone.

- None currently. The last sweep fixed `stockout`/`sink` used as data names,
  `target_id` aligned to `planned_target_id`, and the figurative words `dormant` /
  `bites` / `gating` / `spine` (the last also renamed `check_spine_closure` →
  `check_flow_closure`).
