# Rebalancing, Step by Step

This document explains how the simulator moves bikes by truck at night. The
code is `gbp/consumers/simulator/rebalancing.py`. The vocabulary is defined in
[`Notations.md` §14](../Notations.md#14-rebalancing-moving-bikes-by-truck).

Rebalancing exists for one reason: a station can be empty before the morning
demand reaches it. If riders want bikes there and none are docked, those trips
become `lost` events with `reason="stockout"`. The rebalancer moves bikes during
a low-demand night window so that more morning trips can depart.

The whole process is:

```text
historical demand and arrivals
-> target inventory
-> imbalance
-> nodes
-> stops
-> rebalance plan
-> journal events
```

## The Two Phases

Rebalancing is added to a run by appending `rebalancing_phases(params)` to
`canonical_phases()`. That adds two phases.

`PlanRebalancingPhase` computes a bike-level plan. It runs only in the period
whose wall-clock start hour is `window_start_hour`. It writes no journal events
and does not change inventory.

```python
if period.start_timestamp.hour != self.params.window_start_hour:
    return state
```

`ApplyRebalancingPhase` executes that plan. It runs every period after the
normal user-trip phases. Its `phase_rank` is `REBALANCE_RANK`, currently `3`.
It writes the actual pickup and dropoff events.

The split is important. Planning is a decision. Execution is what changes the
state and the journal.

## The Two Time Axes

The routing solver uses minutes since the rebalancing window started. The
simulator uses periods.

The conversion happens when the solver's `stops` table becomes the
`rebalance_plan`:

```python
pickup_period = window_period_id + int(pickup_minute // minutes_per_period)
dropoff_period = window_period_id + int(stop.minute // minutes_per_period)
```

After this point the simulator applies the plan by period. A truck route may
cross a period boundary. That boundary does not mean the truck returned to its
home depot. A bike picked up in one period can stay in `in_transit` until a
later period.

## Main Data Objects

The code uses four tables between planning and execution.

`nodes` is the solver input. One row is one pickup or dropoff visit at one
facility. Each row moves at most `portion_size` bikes.

`stops` is the solver output. One row says that one truck visits one facility at
one minute and either picks up or drops off bikes.

`rebalance_plan` is the table stored on `SimulationState`. It has one row per
bike, with `flow_id`, `resource_id`, `commodity_category`, `source_id`,
`planned_target_id`, `pickup_period`, `dropoff_period`, `pickup_minute`, and
`dropoff_minute`.

`flows` is the journal. A moved bike becomes one `rebalance` flow with two
events: `departed` for the pickup and `arrived` for the dropoff.

## Parameters

All rebalancing settings are on `RebalancingParams`.

`window_start_hour` opens the night window. The default is `1`, meaning 01:00.

`window_minutes` is how long trucks may work. The default is `120`.

`target_start_hour` and `target_end_hour` define the morning period used to
compute the target inventory. The defaults are 06:00 to 12:00.

`portion_size` limits one solver visit. The default is `5` bikes.

`truck_speed_km_per_hour`, `stop_service_minutes`, and `bike_service_minutes`
turn a route into minutes.

`drop_penalty_minutes` is the solver cost for skipping one node. It is high, so
the solver tries to serve as many nodes as the window allows.

`solver_time_limit_seconds` limits real solver runtime. It does not change
simulated time.

## Planning

Planning starts in `PlanRebalancingPhase.execute`. It uses the inventory after
this period's user-trip phases have already run. That is the newest inventory
the simulator has at the window start.

### Step 1: Compute `target_inventory`

`target_inventory` asks how many bikes each station should hold for the morning
period.

For each `(facility, commodity)`, the code walks the morning periods and adds:

```text
expected departures - expected arrivals
```

The largest running total is the target. It is the largest number of bikes the
station is expected to be short by during the morning.

If arrivals are enough to cover departures, the target is `0` and the station
does not need a truck dropoff.

The expected departures and arrivals come from historical marginals and are
scaled by the run's `demand_scale_factor`.

### Step 2: Compute `station_imbalance`

`station_imbalance` compares current inventory with the target:

```text
imbalance = inventory - target
```

A positive `imbalance` means the station can give bikes. The truck may pick up
bikes there.

A negative `imbalance` means the station needs bikes. The truck may drop off
bikes there.

### Step 3: Clip Dropoffs to `free_docks`

A station cannot receive more bikes than its free dock slots.

`clip_dropoffs_to_free_docks` reduces the planned dropoff side when a station
does not have enough `free_docks`. Docks are shared across commodities, so the
function scales every commodity at that facility by the same factor and rounds
down.

Pickups are not clipped here. They are already bounded by inventory at planning
time because `target` is never negative.

### Step 4: Build `nodes`

`build_rebalance_nodes` turns the clipped imbalance into solver visits.

Before it creates nodes, it matches the pickup and dropoff totals per
commodity. Only this many bikes can move:

```text
min(total surplus, total shortage)
```

This is required because every truck must end its route empty. A surplus bike
with no shortage cannot be carried anywhere useful. A shortage with no surplus
cannot be filled.

Then the code splits each facility's amount into portions. With
`portion_size = 5`, a station that can give 12 bikes becomes three pickup
nodes: 5, 5, and 2 bikes.

This lets the solver serve part of a large imbalance when the window or truck
capacity is too small for all of it.

### Step 5: Route the Trucks

Before calling the solver, `PlanRebalancingPhase.execute` checks the truck
configuration. It raises `SimulatorConfigError`, which aborts the run, in
three cases:

- there is no truck at all;
- a truck has no `home_facility_id`;
- a truck's home depot is missing from the facility tables.

A broken truck setup is a configuration error, so the run stops instead of
planning around it.

`solve_rebalance_vrp` is the default routing solver. It uses OR-Tools.

The solver receives:

- `nodes`, the pickup and dropoff visits.
- `truck_travel_minutes`, the truck travel time between involved facilities.
- the truck fleet, with `resource_id`, truck capacity, and `home_facility_id`.
- `RebalancingParams`.

The solver must obey these rules:

- Each truck starts at its own `home_facility_id`.
- Each truck returns to that same home depot.
- A pickup puts bikes onto the truck.
- A dropoff takes bikes off the truck.
- A truck never carries less than zero bikes.
- A truck never carries more than its capacity.
- Each truck ends empty.
- Each route fits inside `window_minutes`, including the return to the depot.

`truck_travel_minutes` uses straight-line distance and truck speed. It does not
use `routes`, because `routes` is built for bike travel time.

The output is `stops`. Skipped nodes simply do not appear in this table.

### Step 6: Build the `rebalance_plan`

`assign_bikes_to_stops` turns route stops into one row per bike.

The function walks each truck's stops in order. A pickup adds bikes to a small
per-truck list. A dropoff removes the bikes that were picked up earliest for
that truck and commodity.

Each removed bike becomes one `rebalance_plan` row. This row stores:

- the truck in `resource_id`;
- the pickup station in `source_id`;
- the planned dropoff station in `planned_target_id`;
- the pickup and dropoff periods;
- the pickup and dropoff minutes.

If there is no pickup side or no dropoff side, the planner stores an empty plan
and does not call the solver.

## Execution

`ApplyRebalancingPhase` runs after the user-trip phases in every period. It
executes only plan rows whose period has arrived.

Execution has three ordered rounds. Each round is one inventory step.

### Round 0: Drop Off Bikes Picked Up Earlier

The phase first looks in `in_transit` for `rebalance` flows whose
`planned_end_period` is the current period.

These are bikes that were picked up in an earlier period and are now due to
dock.

### Round 1: Pick Up Bikes Due Now

The phase selects rows from `rebalance_plan` with `pickup_period` equal to the
current period.

The plan may no longer match the current inventory. User demand keeps running
during the night window, and it has already changed inventory before
rebalancing runs.

So `_pickups_up_to_inventory` keeps only the first planned pickups that the
source can actually provide:

```python
rank = ordered.groupby(["source_id", "commodity_category"]).cumcount()
return ordered[rank < available]
```

Rows that do not fit the current inventory are removed from the plan. Their
dropoffs will not happen.

For each executed pickup, the phase writes a `departed` event with
`flow_type="rebalance"`. That event removes one bike from `source_id`.

If the dropoff period is later than the current period, the departed row goes
into `in_transit`. If the dropoff period is the same period, it is handled in
round 2.

### Round 2: Drop Off Bikes Picked Up This Period

The phase docks same-period truck moves after pickups. This is separate from
round 0 because the pickup must happen first.

Dropoffs use the same `dock_up_to_capacity` rule as user trips. If the planned
station has free docks, the bike docks there.

If the planned station is full, the bike docks at the truck's home depot:

```python
fits, overflow = dock_up_to_capacity(due, free_docks(inventory, capacities))
fits = fits.assign(realized_target_id=fits["planned_target_id"])
overflow = overflow.assign(realized_target_id=overflow["resource_id"].map(home_by_resource))
```

The depot is the fallback place for truck bikes. Its capacity is not checked in
this step.

There is no redirect chain for a truck bike. A full planned station changes
`realized_target_id` to the home depot. It does not create `redirected` or
`lost`.

## Journal Events

One moved bike is one `rebalance` flow.

```text
departed  event_id 0  pickup   -1 at source_id
arrived   event_id 1  dropoff  +1 at realized_target_id
```

The `resource_id` is the truck. `quantity` is always `1`. `move_id` is always
`0`.

The `planned_target_id` stays the planned dropoff station. The
`realized_target_id` is where the bike actually docks. These differ when the
planned station is full and the truck returns the bike to its home depot.

Rebalance pickups are not demand. They do remove inventory because they are
`departed` events with `move_id == 0`, but demand read-models filter them out
with `is_user_departure`.

## What Is Checked

Execution itself asserts nothing. `ApplyRebalancingPhase.execute` builds the
pickup and dropoff events and applies them through `apply_step_events`, the
same write path as the user phases. Inventory is derived from the events by
`inventory_deltas_from_events`, so the inventory change always equals the
executed dropoffs minus pickups by construction — there is nothing separate to
compare it against.

Planning has the only two asserts in the module, both in
`assign_bikes_to_stops`:

```python
assert len(loaded) >= int(stop.quantity), "dropoff exceeds the bikes on the truck"
...
assert not any(on_truck.values()), "bikes left on a truck at the end of its route"
```

They reject a malformed `stops` table: a truck cannot drop off more bikes than
it carries, and every route must end with an empty truck.

Correctness of the executed flows is enforced after the run by `validate_run`
(`gbp/consumers/simulator/validation.py`). Its invariants also cover rebalance
flows: every pickup must close with one arrival (I2), bikes on trucks are
counted through `in_transit` until they dock (I4), and no inventory step takes
a station below zero (I5).

A dropoff never leaves the system. If it does not fit at the planned station,
it docks at the truck's home depot.

## Why It Is Built This Way

The plan is computed once per window because a truck route is one continuous
route. Re-planning every period would ask the solver to redo a route that may
already be in progress.

Execution checks current inventory instead of reserving bikes and dock slots.
This keeps user demand first. It also keeps the journal accurate: only actual
departures and arrivals change inventory.

The plan has one row per bike because the simulator already records one flow per
bike. That lets rebalancing use the same inventory rules, `in_transit`, and run
checks as user trips.

The solver is called through a function parameter. Tests can pass a fixed
`stops` table and assert exact journal rows. The default function still uses
OR-Tools for real routing.

Large imbalances are split into portions because the fleet may not be able to
serve a whole station in one visit. With portions and skip penalties, the solver
can still return the best smaller set of visits when time or capacity is short.
