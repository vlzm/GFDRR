# Rebalancing

This document explains how the simulator moves bikes by truck at night. The
code is one file, `gbp/consumers/simulator/rebalancing.py`; the vocabulary is
[`Notations.md` §14](../../Notations.md#14-rebalancing-moving-bikes-by-truck).

Rebalancing exists for one reason: a station can be empty before the morning
demand reaches it, and those trips become `lost(stockout)` events. The
rebalancer moves bikes during a low-demand night window so that more morning
trips can depart. Each function's exact behavior is in its docstring — this
page gives the map and the design.

## Code Map

Everything is in `rebalancing.py`, in pipeline order:

| Name | Role |
|---|---|
| `RebalancingParams` | All settings: the night window, the morning target hours, `portion_size`, truck speed and service times, solver limits. |
| `PlanRebalancingPhase` | Runs once per day at `window_start_hour`; computes the plan, writes no events. |
| `target_inventory` | Bikes each station should hold for the morning: the peak of the running total `expected departures − expected arrivals`. |
| `station_imbalance` | `inventory − target`: positive = pickup side, negative = dropoff side. |
| `clip_dropoffs_to_free_docks` | Cuts planned dropoffs to the docks actually free at planning time. |
| `build_rebalance_nodes` | Matches the two sides to `min(surplus, shortage)` and splits amounts into `portion_size` visits. |
| `solve_rebalance_vrp` | The OR-Tools routing solver; returns `stops`. |
| `assign_bikes_to_stops` | Turns route stops into the bike-level `rebalance_plan`; converts solver minutes into periods. |
| `ApplyRebalancingPhase` | Runs every period; executes the plan rows whose period has come. |

## The Main Idea

The whole process is one pipeline from demand history to journal events:

```text
historical demand and arrivals
-> target inventory
-> imbalance
-> nodes
-> stops
-> rebalance plan
-> journal events
```

Planning and execution are two phases on purpose. Planning is a decision: it
runs once per window, replaces `state.rebalance_plan`, and moves no bike.
Execution is what changes the state: `ApplyRebalancingPhase` runs every
period after the user-trip phases, in three ordered rounds — dock the
dropoffs due from earlier periods, pick up this period's bikes (cut down to
what the station still holds), dock same-period dropoffs.

The two sides use different time axes. The solver thinks in minutes since
the window started; the simulator thinks in periods.
`assign_bikes_to_stops` converts one into the other
(`period = window_period_id + minute // minutes_per_period`). A truck route
may cross a period boundary: a bike picked up in one period can stay in
`in_transit` until a later one.

One moved bike is one `rebalance` flow with two events: `departed`
(`-1` at `source_id`) and `arrived` (`+1` at `realized_target_id`), with the
truck in `resource_id`. Truck dropoffs use the same `dock_up_to_capacity`
rule as user trips; a bike that does not fit docks at the truck's home depot
— there is no redirect chain and no `lost` for a truck bike. Rebalance
pickups do remove inventory, but demand read-models filter them out with
`is_user_departure`, so they never count as demand
([flow_journal.md](flow_journal.md)).

## What Is Checked

Execution asserts nothing of its own: events go through `apply_step_events`,
so the inventory change equals dropoffs minus pickups by construction.
Planning has the module's only two asserts, in `assign_bikes_to_stops`: a
dropoff must not exceed the bikes on the truck, and every route must end
with an empty truck. Correctness of the executed flows is enforced after the
run by `validate_run` — I2, I4, and I5 cover rebalance flows like user trips
([simulator.md](simulator.md#invariants)).

## Why It Is Built This Way

### The Plan Is Computed Once Per Window

A truck route is one continuous route. Re-planning every period would ask
the solver to redo a route that may already be in progress.

### Execution Checks Inventory, It Does Not Reserve

User demand keeps running during the night window. Cutting pickups to the
bikes actually on hand keeps user demand first, and keeps the journal
accurate: only actual departures and arrivals change inventory. The rejected
alternative — reserving bikes and dock slots at planning time — would make
the journal show movements that never happened.

### One Plan Row Per Bike

The simulator already records one flow per bike. A bike-level plan lets
rebalancing reuse the same inventory rules, `in_transit`, and run checks as
user trips, instead of a parallel aggregate bookkeeping.

### The Solver Is A Function Parameter

Tests pass a fixed `stops` table and assert exact journal rows; the default
still routes with OR-Tools. The seam is `SolverFn` in the code.

### Large Imbalances Are Split Into Portions

The fleet may not be able to serve a whole station in one visit. With
portions and skip penalties, the solver can still return the best smaller
set of visits when time or capacity is short.
