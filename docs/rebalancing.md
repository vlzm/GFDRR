# The rebalancer

How bikes are moved between stations by truck: why the simulator does it,
how a plan is computed, how the trucks execute it period by period, and what
that leaves in the journal. The vocabulary is
[Notations.md §14](../Notations.md#14-rebalancing-moving-bikes-by-truck);
each term links to its definition on first use. The code is one module,
`gbp/consumers/simulator/rebalancing.py`; how its two phases sit inside the
period is drawn in the phase diagram of
[`simulator.md`](simulator.md#one-period-phase-by-phase) (phases 4 and 5) —
this document does not redraw it. Worked journal examples are scenarios 10–14
in [`scenarios.md`](scenarios.md#rebalancing-scenarios).

## Why rebalancing exists

Demand is one-directional at times: in the morning, bikes leave home-side
stations and pile up at work-side stations. A station that starts the morning
empty turns every trip into a `lost` event with `reason="stockout"`
([Notations.md §1](../Notations.md#1-the-four-flow-outcomes-and-the-two-reasons)).
Rebalancing counters this: at night, when demand is low, trucks move bikes to
the stations the morning will drain. The effect is measurable in the journal:
in the test story of `tests/test_rebalancing.py`, three morning trips at an
empty station are three stockouts without the truck
(`test_without_rebalancing_the_same_story_loses_the_morning_demand`) and
three served departures with it
(`test_rebalancing_moves_bikes_and_serves_the_morning_demand`).

## One window, two clocks

Work happens in a [rebalancing window](../Notations.md#14-rebalancing-moving-bikes-by-truck):
a wall-clock stretch that opens at `window_start_hour` (default 01:00) and
lasts `window_minutes` (default 120). All settings live on one frozen
dataclass, `RebalancingParams` (`rebalancing.py`).

The plan is computed **once per window** and executed **period by period**.
Two time axes are involved and they never mix:

- the solver clock — minutes since the window started; the routing solver
  plans every truck's route on this axis, inside `window_minutes`;
- the simulator clock — periods ([Notations.md §6](../Notations.md#6-time));
  each planned stop is applied in the period its minute falls into.

The trucks never "return to the depot at the end of each period" — the period
edge exists only for accounting. A bike picked up at minute 10 and dropped at
minute 70 simply stays on the truck across the period edge (scenario 11).

## Planning: from morning demand to a bike-level plan

`PlanRebalancingPhase` fires only in a period whose wall-clock start hour is
`window_start_hour`; every other period passes through untouched:

```python
if period.start_timestamp.hour != self.params.window_start_hour:
    return state
```

(`rebalancing.py:648`.) It writes no events and moves no inventory — the plan
is a decision, and `ApplyRebalancingPhase` applies it. The pipeline is five
plain functions, each testable on a small hand-made table.

### 1. `target_inventory` — how many bikes each station needs

```python
peak = matrix.cumsum(axis=1).max(axis=1).clip(lower=0)
```

(`rebalancing.py:229`.) For each `(facility, commodity)` the morning
(wall-clock hours `target_start_hour..target_end_hour`, same calendar day as
the window) is walked period by period: expected departures minus expected
arrivals, added up as a running total. The highest point of that running
total is the most bikes the station is ever short by, so holding that many at
the window's end covers the whole morning. The expected values are the
historical [marginals](../Notations.md#9-marginals-the-read-models-of-the-journal)
scaled by the run's `demand_scale_factor`. A station whose arrivals outrun
its departures never runs short and gets
[target](../Notations.md#14-rebalancing-moving-bikes-by-truck) 0.

### 2. `station_imbalance` — who gives, who needs

```python
out["imbalance"] = (out["quantity"].fillna(0) - out["target"].fillna(0)).astype("int64")
```

(`rebalancing.py:246`.) [`imbalance`](../Notations.md#14-rebalancing-moving-bikes-by-truck)
is `inventory − target` per `(facility, commodity)`. Positive: the station
has bikes to give (pickups happen there). Negative: it needs bikes (dropoffs
happen there). The inventory used here is the state after this period's own
user phases — the freshest picture the simulator has.

### 3. `clip_dropoffs_to_free_docks` — the plan must fit the docks

A station cannot take in more bikes than it has free dock slots
([`free_docks`](../Notations.md#2-core-state); docks are shared across
commodities). When a facility's planned inflow exceeds its free docks, every
commodity's share is scaled down by the same factor and rounded down.
Pickups are untouched: `target >= 0` already implies
`imbalance <= inventory`.

### 4. `build_rebalance_nodes` — visits the solver can route

```python
n_nodes = -(-total // portion_size)  # whole nodes, rounded up
rep = nonzero.loc[nonzero.index.repeat(n_nodes)].copy()
```

(`rebalancing.py:327-328`.) Two things happen here. First the sides are
matched: per commodity, only `min(total surplus, total shortage)` bikes can
actually move, because every truck must end its route empty — the excess is
trimmed away, and facilities with the largest imbalance keep their share
first (`_trim_to_common_total`). Then each facility's remaining share is
split into [nodes](../Notations.md#14-rebalancing-moving-bikes-by-truck) of
at most `portion_size` bikes: a facility 12 bikes over its target with
`portion_size=5` becomes three pickup nodes of 5, 5 and 2 bikes, so the
solver may send different trucks to them or skip the least valuable one.

### 5. `solve_rebalance_vrp` — routing the trucks (OR-Tools)

The routing problem: every truck starts and ends empty at its
[home depot](../Notations.md#14-rebalancing-moving-bikes-by-truck)
(`home_facility_id` on `resources_df`), a pickup node puts its bikes on the
truck, a dropoff node takes bikes off it, the load never goes below zero or
above the truck's capacity, and every route fits into `window_minutes`. The
travel input is [`truck_travel_minutes`](../Notations.md#14-rebalancing-moving-bikes-by-truck):
straight-line distance at `truck_speed_km_per_hour`, in both routing modes —
the recorded exception to
[`routes`](../Notations.md#13-routing-distance-and-travel-time-between-facilities),
whose table is built with the bike profile and would give riding times.

Two model pieces carry the guarantees the rest of the design leans on:

```python
routing.AddDimension(transit_id, 0, horizon, True, "minutes")
```

(`rebalancing.py:480`.) A running-total constraint caps every route at the
window, return to the home depot included. Service time (a fixed
`stop_service_minutes` plus `bike_service_minutes` per bike) is charged at
departure from a stop, so the running total at a node is that node's arrival
minute.

```python
dimension.CumulVar(routing.End(vehicle)).SetRange(0, 0)
```

(`rebalancing.py:493`.) One load dimension per commodity, bounded
`[0, truck capacity]` and forced to 0 at the route's end: a truck can only
drop bikes it picked up, of the same commodity, and never keeps bikes when
the route ends. Every node may be skipped at `drop_penalty_minutes` — a cost
far above any driving, so the solver serves as many nodes as fit in the
window and only then minimizes driving. The answer is the
[stops](../Notations.md#14-rebalancing-moving-bikes-by-truck) table: each
truck's visits in order, with the arrival minute of every stop; skipped nodes
simply do not appear.

### 6. `assign_bikes_to_stops` — from stops to bikes

```python
"pickup_period": window_period_id + int(pickup_minute // minutes_per_period),
"dropoff_period": window_period_id + int(stop.minute // minutes_per_period),
```

(`rebalancing.py:590-591`.) The stops are walked in visit order per truck: a
pickup puts its bikes on the truck, a dropoff hands over the bikes that were
picked up earliest (per truck and commodity). Each handed-over bike becomes
one row of the [rebalance plan](../Notations.md#14-rebalancing-moving-bikes-by-truck)
with a `flow_id` of the form `rb_<window period>_<row>`, its source, its
planned target, and its pickup and dropoff periods — the solver's minutes
become simulator periods on this line and nowhere else. The plan is stored on
the state (`SimulationState.rebalance_plan`) and waits there between the
window's periods.

When the nodes lack a pickup side or a dropoff side, nothing can move: the
planner stores an empty plan and the solver is not called at all
(`rebalancing.py:665`, scenario 14).

## Execution: three rounds per period

`ApplyRebalancingPhase` runs every period at `phase_rank` 3
([`REBALANCE_RANK`](../Notations.md#14-rebalancing-moving-bikes-by-truck)),
after the three user-trip phases. It executes the plan rows whose period has
come, in three ordered rounds — each round is its own
[step](../Notations.md#01-moment-and-step-the-inventory-time-axis):

- round 0 — dock the dropoffs due now for bikes picked up in an earlier
  period; they were waiting in `in_transit` with `flow_type="rebalance"`;
- round 1 — this period's pickups, cut down to the bikes actually on hand;
- round 2 — dock the dropoffs of bikes picked up within this same period.

The plan was built from the inventory at planning time, but the night demand
of the window's own periods keeps running (user departures run at
`phase_rank` 1, before the truck's 3). Execution therefore never trusts the
plan blindly, on either end of a bike's journey:

```python
rank = ordered.groupby(["source_id", "commodity_category"]).cumcount()
...
return ordered[rank < available]
```

(`rebalancing.py:841-844`, `_pickups_up_to_inventory`.) A pickup is cut down
to the bikes on hand at the source: within each `(source, commodity)` the
first plan rows are kept, the rest are removed from the plan — their bikes
stay where the night demand left them, and their dropoffs never happen
(scenario 13).

```python
fits, overflow = dock_up_to_capacity(due, free_docks(inventory, capacities))
fits = fits.assign(realized_target_id=fits["planned_target_id"])
overflow = overflow.assign(realized_target_id=overflow["resource_id"].map(home_by_resource))
```

(`rebalancing.py:819-821`, `_dock_dropoffs`.) A dropoff docks at its planned
station while it has free docks — the same
[`dock_up_to_capacity`](simulator.md#mechanics-the-rules-mechanicspy) rule
the user phases use. Bikes that do not fit dock at the truck's home depot
instead (the truck could not unload and takes them back); the depot's own
capacity is not checked — it is the parking of last resort (scenario 12).
There is no redirect chain and no `lost` for a truck bike: the depot always
takes it.

## What a truck journey writes into the journal

One moved bike is one flow of `flow_type="rebalance"` with exactly two
events, built by `rebalance_departed_events` and `rebalance_arrived_events`
(`gbp/model/flows.py`):

```text
departed (event 0)  — the pickup:  −1 at source_id
arrived  (event 1)  — the dropoff: +1 at realized_target_id
```

`resource_id` is the truck, `quantity` is 1, `move_id` is 0 — a rebalance
flow is a single arc, never redirected. Between the two events the
`departed` row itself sits in `in_transit`, like any riding bike;
`DockArrivals` skips it (it docks user trips only). A dropoff diverted to the
depot keeps `planned_target_id` at the planned station while
`realized_target_id` becomes the depot — the
[planned vs realized](../Notations.md#5-planned-vs-realized) split records
the difference.

The rows reach the journal through the same single write path as every other
phase (`apply_step_events(new_flows, REBALANCE_RANK)`,
`rebalancing.py:792`), so each round gets its own `step_id` and the label
order `(period_id, phase_rank, phase_round)` holds. Rebalance flows are not
demand: every demand read-model filters them out through
[`is_user_departure`](../Notations.md#14-rebalancing-moving-bikes-by-truck),
while the inventory arithmetic counts them through `is_undocking` — a pickup
takes a bike out of a dock exactly like a user departure does.

Row-by-row journals for every shape — a route inside one period, a route
across periods, the depot overflow, the cut pickup, the empty window — are
scenarios 10–14 in [`scenarios.md`](scenarios.md#rebalancing-scenarios), each
reproduced by a test in `tests/test_docs_scenarios.py`.

## Invariants

At planning time:

- per commodity, the plan moves `min(total surplus, total shortage)` bikes —
  every truck ends its route empty, so no bike is created or stranded on a
  truck (`_trim_to_common_total`, the load dimensions of the solver, and the
  closing assert of `assign_bikes_to_stops`);
- a planned inflow never exceeds the station's free docks at planning time
  (`clip_dropoffs_to_free_docks`) — reality may still differ by execution
  time, which is what the depot fallback is for;
- a run that opts into rebalancing must have a usable fleet: at least one
  truck, every truck with a home depot, every home depot present in the
  facility tables — otherwise `SimulatorConfigError` at planning time.

At execution time, the phase checks its own contract every period
(`rebalancing.py:801`):

```python
assert moved == docked_n - picked_n, "rebalance events and inventory moved disagree"
```

Only the executed pickups and dockings moved inventory: `−1` per pickup,
`+1` per docked dropoff, nothing else. A dropoff never vanishes — a full
station sends the bike to the home depot, not out of the system.

At run end, the general run invariants I1–I5
([`simulator.md`](simulator.md#invariants-what-is-checked-and-where)) cover
the truck flows with no special cases: every rebalance `departed` closes with
exactly one `arrived` (I2), bikes on trucks are counted in the conservation
total through `in_transit` (I4), and a pickup never takes a station below
zero (I5). The full-run tests in `tests/test_rebalancing.py` assert
`check_journal_well_formed` and `validate_run` on every story.

## Why it is built this way

Five decisions carry the design. For each: the decision, the alternative,
and why the alternative lost.

### 1. Plan once per window, execute period by period

The plan is computed in the window-opening period and applied across the
following periods; the solver plans in minutes, the phases apply in periods.
The alternative was to keep everything on the period grid: plan and execute
within one period, or re-plan at every period edge.

It lost because a truck route is one physical journey and the period edge is
an accounting line, not a depot visit. A route that crosses the hour edge
(pickup at minute 70 of a two-hour window) would have to be cut or faked if
each period had to stand alone. Re-planning every period would also mean an
OR-Tools search every period — paid in real solver seconds — to re-decide a
journey the truck is already half-way through. Keeping the two clocks
separate costs one conversion line
(`period = window period + minute // minutes-per-period`) and nothing else.

### 2. Execution re-checks reality instead of trusting the plan

Pickups are cut to the bikes on hand; dropoffs overflow to the home depot.
The alternative was to make the plan binding: reserve the planned bikes and
dock slots at planning time, so execution always finds what the plan
promised.

Reservation lost because the night demand keeps running between planning and
execution, and it has priority — a rider at 01:30 should not find a bike
refused because a truck plans to take it at 02:10. A reservation is also a
fact the journal cannot show: nothing departed, nothing arrived, yet the
inventory would behave as if something had. Cutting at execution time keeps
the journal the single source of truth and turns every plan-vs-reality
difference into visible rows (a missing pickup, a depot dropoff) instead of
a hidden lock.

### 3. One plan row per bike, on the same flow machinery as user trips

The plan is bike-level (`PLAN_COLUMNS`, one row per bike), and an executed
move is an ordinary two-event flow. The alternative was aggregate accounting:
keep the stops table (facility, quantity, minute) and apply quantities to the
inventory directly.

Aggregates lost because the bike-level row buys the whole existing machinery
unchanged: `dock_up_to_capacity` decides dropoffs exactly as it decides user
dockings, `in_transit` carries truck bikes exactly as it carries riding
bikes, and I1–I5 check truck moves with no special cases. An aggregate path
would duplicate each of those rules in a second form — and partial execution
(2 of 3 bikes on hand) falls out of row filtering instead of needing
quantity arithmetic.

### 4. The routing solver sits behind a seam

`PlanRebalancingPhase` takes any function of the shape
`(nodes, travel_minutes, trucks, params) -> stops` (`SolverFn`);
`solve_rebalance_vrp` is only the default. The alternative was to call
OR-Tools inline in the phase.

Inline lost because the solver search is not deterministic enough to pin
journal tables on: it runs under a real-time limit, and a slower machine may
find a different (equally valid) route. Behind the seam, the full-run tests
and the scenario tables (10–13) inject a hand-written stops table and get the
same journal every run, while the OR-Tools core keeps its own tests plus one
end-to-end run (`test_full_run_with_the_real_solver`). The seam is also where
a different planner could plug in later without touching the phases.

### 5. Portions and a drop penalty instead of a hard model

The imbalance is split into nodes of at most `portion_size` bikes, and every
node may be skipped at a high cost. The alternative was the literal model:
one mandatory visit per facility, sized to its whole imbalance.

The literal model lost on both counts. A whole-imbalance visit cannot be
split between trucks or partially served, so one 40-bike station would need a
40-bike truck. And with mandatory visits, a window or fleet too small for the
full plan makes the model infeasible — the solver returns nothing, and the
night moves zero bikes. With portions and penalties the solver degrades
instead of failing: it serves as many portions as fit and drops the least
valuable ones, which is the behaviour a planning tool should have when
resources are short.
