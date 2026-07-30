---
title: "The initial state is measured by a sizing run"
weight: 2
---

# The initial state is measured by a sizing run, not loaded

## Decision

A run needs two state tables before it starts: the initial inventory (bikes
docked at each station) and the dock capacities. Neither is loaded from
outside data. `run_sized_scenario` first runs the scenario once with a
saturated state — inventory and capacities set far above demand, so no
stockout and no dock-full can happen — and then measures that sizing run's
journal: the initial inventory is the smallest start under which no station
goes below zero at any step, and the capacity is each station's peak
occupancy (`size_state_for_demand` in `gbp/consumers/simulator/sizing.py`,
using the two helpers in `gbp/model/dataloader_graph.py`).

The sizing run and the real run use two independent demand multipliers:
`sizing_scale_factor` sizes the state, `demand_scale_factor` is what the
run faces. Equal values give a clean run. A higher `demand_scale_factor`
is what makes `stockout` and `dock_full` events appear at all: the run
faces more demand than the state was sized for. The `sizing_data` parameter
separates the two further: the state can be sized on one demand table (the
month's actuals) while the run faces another (a model's forecast) — this is
how the two-level evaluation isolates forecast error.

## Rejected alternative

Load today's real station inventory and capacities. Today's inventory is a
current observation, unrelated to the historical month being replayed;
limiting demand against it would create stockouts that never happened in
history. A guessed safety margin instead of the measured peak would make
losses depend on the guess, not on the scenario.

## Where in code

- `gbp/consumers/simulator/scenario.py` — `run_sized_scenario`, the two
  multipliers, `sizing_data`.
- `gbp/consumers/simulator/sizing.py` — the saturated run.
- `gbp/model/dataloader_graph.py` — `get_replay_initial_inventory_df`,
  `get_replay_capacities_df`.
