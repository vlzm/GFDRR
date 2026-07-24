---
title: "The flow journal"
weight: 3
---

# The flow journal library

This document explains `gbp/model/` — the model layer that owns the flow
journal, the append-only table of flow events and the single source of truth
for what happened in a run ([decision record](../decisions/journal-as-source-of-truth.md)).
Everything else the project shows — inventory, demand, maps, costs — is
computed from it.

The column-by-column schema is
[Notations.md §0](../reference/notations.md#0-the-flow-event-schema-the-symbol-table).
Worked examples with concrete journal rows are in
[worked-examples.md](worked-examples.md). Each function's exact behavior is in its
docstring in `flows.py` — this page gives the map and the design.

## Code Map

The model layer is two files in `gbp/model/`. `journal_schema.py` holds the
shape contract of the event table: the pandera schema (`FLOW_EVENT_SCHEMA`),
the canonical value sets, and `check_journal_schema`. Everything else lives
in `flows.py`, top to bottom:

| Part | Names |
|---|---|
| Schema | `FLOW_EVENT_COLUMNS`, `FLOW_EVENT_DTYPES`, the `*_RANK` constants |
| Inventory predicates | `is_undocking`, `is_user_departure`, `is_docking` |
| Inventory delta rule | `_event_deltas` (the one `+1`/`-1` rule), `inventory_deltas_from_events` (the write-time form) |
| Occupancy | `occupancy_per_facility` — inventory summed across commodities |
| Event builders | `departed_events`, `arrived_events`, `redirected_events`, `redirect_leg_events`, `lost_events`, `rebalance_*_events` |
| In-transit set | `empty_in_transit`, `in_transit_after_events` |
| Finalizing | `phase_rank_by_timing`, `stamp_history_ordering`, `finalize_flows` |
| Marginals | `flows_to_departures`, `flows_to_arrivals`, `flows_to_redirects`, `flows_to_losses`, `flows_to_od_matrix`, `get_inventory_df`, `inventory_at_moments` |
| The panel | `flows_to_panel`, with its column lists `PANEL_KEYS` / `PANEL_VALUES` |
| Wide views | `flows_with_inventory`, `flows_with_costs`, `flows_with_measures` |
| Geometry helpers | `haversine_km`, `neighbor_distance_sq` |
| Redirect explainer | `redirect_neighbor_table` |
| Checks | `check_demand_split` (I1), `check_flow_closure` (I2) |

## The Place In The System

`flows.py` is the model layer: the shared vocabulary both producers of a
journal speak.

```text
gbp/loaders/          gbp/consumers/simulator/
(historical journal)  (simulated journal)
        \                 /
         both import from
        gbp/model/flows.py
         (imports neither)
```

The historical loader builds a journal from real trips in one pass. The
simulator builds a journal phase by phase during a run. Both use the same
builders and the same readers, so a historical value and a simulated value of
the same table have one definition, not two kept in sync by hand.

## The Main Idea

One row of the journal is one flow event. A plain trip is one arc:
`departed` (move 0, event 0) then `arrived` (move 0, event 1); each redirect
bounce closes the current arc and opens the next. The builders set `move_id`
and `event_id` at emit time; the three order columns (`phase_rank`,
`phase_round`, `step_id`) are stamped by the producer — the simulator in
`SimulationState.apply_step_events`, the historical loader in
`stamp_history_ordering`.

Three masks are the single definition of which events move inventory.
`is_undocking` (`departed` with `move_id == 0`) is the `-1` side — a user
departure or a truck pickup; a redirect's continuation leg has
`move_id >= 1` and moves nothing. `is_docking` (`arrived`) is the `+1` side,
at `realized_target_id`. `is_user_departure` narrows the undockings to user
trips, so a rebalance pickup never counts as demand. One helper,
`_event_deltas`, turns the masks into `+1`/`-1` rows; the read side and the
write side both delegate to it.

Reading the journal back is done by read-models: pure functions that compute
a table from the journal and add no new facts. The marginals count one event
kind per `(period_id, facility_id, commodity_category)`; `flows_to_panel`
merges them into the one table every map view slices (saved as
`panel.parquet`). Inventory is never stored per period — `get_inventory_df`
(per period) and `inventory_at_moments` (per step) both add up the same
`_event_deltas`, only along different time axes. The wide views add columns
to the journal itself; `flows_with_measures` is the one place a journal
gains its duration, distance, and cost columns, used by both the canonical
notebook and the artifact builder.

Before any reading, `finalize_flows` casts the dtypes, sorts by
`(step_id, flow_id, event_id)`, and cuts to `FLOW_EVENT_COLUMNS`. It assigns
nothing and refuses a journal whose order columns are missing — filling them
there would be a second definition of `step_id`.

Two whole-journal invariants live here, next to the schema they check:
`check_demand_split` (I1) and `check_flow_closure` (I2). They return
violation lists instead of raising, so the simulator-level `validate_run`
can collect everything and report once. The shape rules of a single row —
columns, value sets, `move_id == event_id // 2`, which fields each event
type fills — are `check_journal_schema` in `journal_schema.py`. The other
invariants (I3–I5) need the live simulator state, so they live in
`gbp/consumers/simulator/validation.py`
([simulation-engine.md](simulation-engine.md#invariants)).

## Who Calls What

| Caller | Uses |
|---|---|
| `gbp/loaders/dataloader_graph.py` | builders, `stamp_history_ordering`, `finalize_flows` for the historical journal; the marginals for the historical tables and, in `attach_simulation`, their simulated twins; `inventory_at_moments` for the replay sizing |
| `gbp/consumers/simulator/` | builders inside the phases; `inventory_deltas_from_events` and `in_transit_after_events` in `apply_step_events`; `neighbor_distance_sq` in the redirect mechanics; `finalize_flows` at run end |
| `gbp/consumers/simulator/validation.py` | `check_demand_split`, `check_flow_closure`, `get_inventory_df`, `inventory_at_moments` |
| `gbp/artifacts.py` | `flows_with_measures` for `flows.parquet`; `flows_to_panel` for `panel.parquet` |

## Why It Is Built This Way

### Write And Read Live In One Module

The module owns one internal detail on both sides: the shape of a flow
event. Splitting builders and read-models into two files would spread the
column layout across two places, and every schema change would have to be
made twice.

### The Model Layer Has No Loader Or Simulator Imports

Both of them import from it, never the other way. This direction is what
makes the historical and simulated views comparable: `get_inventory_df` is
one function, so "historical inventory" and "simulated inventory" share one
definition. The geometry helpers live here for the same reason — the loaders
(trip distances) and the simulator (redirect decisions) both need them, and
the import direction only allows sharing at the bottom.

### One Inventory Delta Rule

`_event_deltas` is the only place that says which event moves inventory and
by how much. The per-period view and the per-step view add up the same
deltas, so the coarse and the fine view cannot disagree; the write-time form
moves the live inventory by exactly what the journal says.

### `step_id`: One Owner Per Producer

Two separately ordered inventory batches must never share a `step_id`. The
simulator proves that with a counter; the historical loader derives the
number from the `(period_id, phase_rank, phase_round)` label, which is safe
only because history is pure user trips — one label is one batch.
`finalize_flows` stays out of it: it sorts by the stamped number and refuses
a journal without one. The full reasoning is
[Notations.md §0.1](../reference/notations.md#01-moment-and-step-the-inventory-time-axis).

### The Redirect Explainer Shares The Decision's Metric

`redirect_neighbor_table` answers "was this bike's redirect the right
choice" after a run. It ranks neighbours with `neighbor_distance_sq` — the
same metric the redirect mechanics use — so the order the explainer shows is
always the order the simulator walked.
