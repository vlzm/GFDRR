# The flow journal library

This document explains `gbp/model/flows.py` — the module that owns the flow
journal. The flow journal is the append-only table of flow events and the
single source of truth for what happened in a run. Everything else the project
shows (inventory, demand, maps, costs) is computed from it.

The module does four things:

1. It defines the shape of a flow event: the columns, the types, and the ids
   that place an event inside its trip.
2. It builds event rows for the writers: one builder function per event kind.
3. It reads a finished journal back into tables: departures, arrivals,
   inventory, losses, the OD matrix.
4. It checks two whole-journal invariants: I1 (demand split) and I2 (flow
   closure).

The column-by-column schema is [Notations.md §0](../../Notations.md#0-the-flow-event-schema-the-symbol-table).
How the simulator calls these functions period by period is
[simulator.md](simulator.md). Worked examples with concrete journal rows are
in [scenarios.md](scenarios.md).

## Code Map

Everything lives in one file, `gbp/model/flows.py`. Its parts, top to bottom:

| Part | Names |
|---|---|
| Schema | `FLOW_EVENT_COLUMNS`, `FLOW_EVENT_DTYPES`, the `*_RANK` constants, `DOCKING_EVENT_TYPES` |
| Inventory predicates | `is_undocking`, `is_user_departure`, `is_docking` |
| Inventory delta rule | `_event_deltas` (the one `+1`/`-1` rule), `inventory_deltas_from_events` (the write-time batch form, used by `SimulationState.apply_step_events`) |
| Event builders | `departed_events`, `arrived_events`, `redirected_events`, `redirect_leg_events`, `lost_events`, `rebalance_departed_events`, `rebalance_arrived_events` |
| Empty frames | `empty_in_transit`, `empty_flows_journal` |
| In-transit set | `in_transit_after_events` (the working set after one event batch) |
| Finalizing | `phase_rank_by_timing`, `stamp_history_ordering` (the history rule for the order columns), `finalize_flows` |
| Marginals | `flows_to_departures`, `flows_to_arrivals`, `flows_to_redirects`, `flows_to_losses`, `flows_to_od_matrix`, `flows_to_panel`, `get_inventory_df`, `inventory_at_moments` |
| Wide views | `flows_with_inventory`, `flows_with_costs`, `flows_with_measures` |
| Geometry helpers | `haversine_km`, `neighbor_distance_sq` |
| Redirect explainer | `redirect_neighbor_table` |
| Checks | `check_demand_split` (I1), `check_flow_closure` (I2) |

## The Place In The System

`flows.py` is the model layer: the shared vocabulary that both producers of a
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

## The Event Schema

One row of the journal is one flow event. The full column table is
[Notations.md §0](../../Notations.md#0-the-flow-event-schema-the-symbol-table);
grouped by purpose:

| Group | Columns |
|---|---|
| Identity inside the trip | `flow_id`, `move_id`, `event_id` |
| What happened | `flow_type`, `event_type`, `reason`, `quantity`, `commodity_category`, `resource_id` |
| Where | `source_id`, `planned_target_id`, `realized_target_id` |
| When | `period_id`, `start_period`, `planned_end_period`, `realized_end_period` |
| Order of inventory changes | `phase_rank`, `phase_round`, `step_id` |

The identity ids follow one rule, fixed in the schema comment:

```python
# ``move_id`` is the arc index (0..m -- one physical edge; each redirect bounce
# adds one more arc) and ``event_id`` is the event ordinal (0..n). Arc ``m``
# opens with ``departed`` at event ``2m`` and ends at event ``2m + 1``.
```

A plain trip is one arc: `departed` (move 0, event 0) then `arrived` (move 0,
event 1). Each redirect bounce closes the current arc and opens the next one.
Row uniqueness is the pair `(flow_id, event_id)`.

The builders set `move_id` and `event_id` at emit time. The three order
columns are set after builder output. The simulator stamps all three in
`SimulationState.apply_step_events`; the historical loader stamps all three
with `stamp_history_ordering`.

## Which Events Move Inventory

Three masks are the single definition of "which events move inventory". Every
reader in the module uses them; nothing else re-derives the rule.

```python
def is_undocking(flows):
    return (flows["event_type"] == "departed") & (flows["move_id"] == 0)

def is_user_departure(flows):
    return is_undocking(flows) & (flows["flow_type"] == "user_trip")

def is_docking(flows):
    return flows["event_type"].isin(DOCKING_EVENT_TYPES)  # ["arrived"]
```

`is_undocking` is the `-1` side: a `departed` with `move_id == 0` takes a bike
out of a dock at `source_id`. This covers a user departure and a truck pickup
alike. A redirect's continuation leg is also a `departed`, but with
`move_id >= 1` — the bike never held a dock at the full station it bounced
off, so that row moves no inventory.

`is_docking` is the `+1` side: only an `arrived` lands a bike, at its
`realized_target_id`. A `redirected` bounce docks nothing; a `lost` docks
nothing.

`is_user_departure` narrows the undockings to user trips. Every reader that
means "what riders wanted" (demand, outflow, the OD model) filters through it,
so a rebalance pickup never counts as demand.

## The Builders (Writing Events)

Each builder takes a plain table of trips or in-transit rows and returns
flow-event rows with the canonical columns and types. Builders only build
rows; applying them to inventory is the caller's job.

| Builder | Event it writes | `move_id`, `event_id` | Inventory effect when read |
|---|---|---|---|
| `departed_events` | `departed` — a user trip leaves | 0, 0 | `-1` at `source_id` |
| `arrived_events` | `arrived` — the current arc docks | m, 2m+1 | `+1` at `realized_target_id` |
| `redirected_events` | `redirected` — the bounce off a full target | m, 2m+1 | none |
| `redirect_leg_events` | `departed` — the leg after a bounce | m+1, 2m+2 | none (`move_id >= 1`) |
| `lost_events` | `lost` — a trip that did not happen | see below | none |
| `rebalance_departed_events` | `departed` — a truck pickup | 0, 0 | `-1` at `source_id` |
| `rebalance_arrived_events` | `arrived` — a truck dropoff | 0, 1 | `+1` at `realized_target_id` |

One builder as an example — the continuation leg after a bounce:

```python
def redirect_leg_events(redirects, period_id):
    move = redirects["move_id"] + 1
    ...
    "source_id": redirects["planned_target_id"],
    "planned_target_id": redirects["realized_target_id"],
    "start_period": redirects["start_period"],
    "planned_end_period": redirects["leg_end_period"],
```

The new leg departs from the full station the bike bounced off, heads to the
station the redirect chose, and is due to dock at `leg_end_period`.
`start_period` stays the flow's opening period on every row — it records when
the flow departed, not when this arc started.

Note: `lost_events` serves two different losses with one shape.
A stockout loss is demand that never became a flow: aggregated per
`(source_id, commodity_category)`, no `flow_id`, `move_id` 0 and `event_id` 0.
A dock-full loss closes a real flow that found no dock anywhere: one row per
`flow_id`, closing the current arc (`event_id` = 2m+1). Both kinds touch no
inventory: a stockout bike never left, and a dock-full bike already left at
its `departed`.

Two helper constructors give the empty starting frames: `empty_flows_journal`
(a journal with no rows, all canonical columns) and `empty_in_transit` (an
empty `departed`-event frame — the in-transit table has the same shape as the
events that created it).

## Finalizing The Journal

Before a journal is read, `finalize_flows` turns the appended event batches
into the finished table:

```python
def finalize_flows(journal):
    flows = journal.copy()
    ...  # cast the dtypes; refuse rows without the order columns
    flows = flows.sort_values(["step_id", "flow_id", "event_id"], kind="stable")
    return flows.reset_index(drop=True)[FLOW_EVENT_COLUMNS]
```

It casts the types, sorts by `(step_id, flow_id, event_id)`, and cuts the
frame to exactly `FLOW_EVENT_COLUMNS`. It assigns nothing. The builders
already set `move_id` and `event_id`, and the three order columns are stamped
before the journal gets here. A journal with a missing or NA order column is
refused with a `ValueError` — filling it here would be a second definition of
`step_id`.

A step is one batch of `+1`/`-1` inventory changes applied together; `step_id`
is its run-global ordinal
([Notations.md §0.1](../../Notations.md#01-moment-and-step-the-inventory-time-axis)).
Each producer stamps it its own way, before finalizing:

- The simulator. Each phase writes through
  `SimulationState.apply_step_events`, which opens a step from the run-global
  counter per ordered batch and stamps `phase_rank`, `phase_round` and
  `step_id` on the rows.
- The historical loader. It has no phases, so it calls
  `stamp_history_ordering`: `phase_rank` by the timing rule below,
  `phase_round` 0 on every row, and `step_id` numbering the distinct
  `(period_id, phase_rank, phase_round)` labels 0, 1, 2, … in sorted order.
  This is safe because history is pure user trips: no redirects and no
  rebalancing, so one label is always exactly one batch.

The timing rule is `phase_rank_by_timing` — it reads the phase off the
event's own columns:

```python
is_period_own = is_user_departure(flows) | is_stockout
rank = pd.Series(DOCK_PREVIOUS_RANK, index=flows.index, dtype="int64")
rank = rank.mask(is_period_own, PERIOD_OWN_RANK)
rank = rank.mask(~is_period_own & (flows["period_id"] == flows["start_period"]), DOCK_SAME_RANK)
```

A real departure or a stockout loss is the period's own activity (rank 1). Any
other event is a docking-phase event: rank 0 when the flow opened in an
earlier period, rank 2 when it opened in this one. The rule matches the
simulator's phase order by construction, and the scenario test
`test_stamped_step_id_matches_tuple_order` uses it to check that the
historical order and the simulator order agree.

## The Marginals (Reading The Journal Back)

A read-model is a table computed from the journal by a pure function. It adds
no new facts; running it twice on the same journal gives the same answer. The
marginals are the per-period read-models
([Notations.md §9](../../Notations.md#9-marginals-the-read-models-of-the-journal)):

| Function | Counts | At which facility |
|---|---|---|
| `flows_to_departures` | user departures (`is_user_departure`) | `source_id` |
| `flows_to_arrivals` | docking events (`is_docking`) | `realized_target_id` |
| `flows_to_redirects` | `redirected` bounces | `planned_target_id` — the full station bounced off |
| `flows_to_losses(flows, reason)` | `lost` events with that `reason` | `source_id` for `stockout`, `planned_target_id` for `dock_full` |
| `flows_to_od_matrix` | user departures, into `count`, `probability`, mean `duration` per (source, target) | — |

Each of the first four returns one row per
`(period_id, facility_id, commodity_category)`. `flows_to_od_matrix` compares
pairs of facilities, so its key is
`(source_id, planned_target_id, period_id, commodity_category)` instead.
Because both the historical and the simulated journal go through these same
functions, an exact replay produces equal historical and simulated marginals
by construction.

### Inventory

Inventory is not stored per period anywhere. It is computed from the journal
plus the initial inventory. One helper, `_event_deltas`, defines the delta of
every inventory-moving event:

```python
def _event_deltas(events, extra_cols=()):
    cols = [*extra_cols, "facility_id", "commodity_category", "delta"]
    dock = events[is_docking(events)].copy()
    dock["facility_id"] = dock["realized_target_id"]
    dock["delta"] = dock["quantity"].astype("int64")
    undock = events[is_undocking(events)].copy()
    undock["facility_id"] = undock["source_id"]
    undock["delta"] = -undock["quantity"].astype("int64")
    return pd.concat([dock[cols], undock[cols]], ignore_index=True)
```

Every other event is dropped: it moves no inventory. The read side reaches
this rule through a one-line delegator:

```python
def _inventory_deltas(flows):
    return _event_deltas(flows, ("step_id", "period_id"))
```

Each delta row keeps its `step_id` and `period_id`, so the same deltas can be
added up on either time axis:

- `get_inventory_df(flows, initial_inventory)` — the coarse view: one row per
  `(period_id, facility_id, commodity_category)` with `quantity_sop`
  (start-of-period) and `quantity_eop` (end-of-period). End-of-period is the
  initial inventory plus the running total of deltas up to and including the
  period.
- `inventory_at_moments(flows, initial_inventory)` — the fine view: one row
  per `(step_id, facility, commodity)` with `inventory_before` and
  `inventory_after`. A facility with no event in a step keeps its value, so
  the table answers "what did any station hold at the moment of step s".

Both need a finalized journal: the deltas are ordered by `step_id`.

The write side reaches the same rule through `inventory_deltas_from_events`:
the deltas of one event batch, summed per `(facility_id, commodity_category)`.
`SimulationState.apply_step_events` uses it to move the live inventory by
exactly what the events it writes imply, so the live inventory and the journal
cannot state the rule differently.

## The Wide Views

Three functions widen the journal — same rows, more columns:

| Function | Adds | Needs |
|---|---|---|
| `flows_with_inventory` | `facility_id` (the event's own facility), `inventory_before`, `inventory_after` | initial inventory |
| `flows_with_costs` | `rate`, `elapsed_periods`, `cost` | rates, `period_len` |
| `flows_with_measures` | the money columns from `flows_with_costs` plus `planned_duration_periods`, `realized_duration_periods`, `planned_distance_km`, `realized_distance_km` | `routes`, rates, `period_len` |

`flows_with_measures` is the one place a journal gains its measure columns
([Notations.md §6.1](../../Notations.md#61-rate-and-cost-money)). The wide
journal (the canonical notebook) and the artifact builder
(`app/artifacts.py`) both call it, so `flows.parquet` and a notebook's wide
journal price a trip identically.

Note: `elapsed_periods` and `cost` are cumulative over redirect legs, because
`start_period` is the flow's opening period on every row. The value on the
final `arrived` is the whole trip's duration and cost.

## The Redirect Explainer

`redirect_neighbor_table(flows, initial_inventory, geo, flow_id, ...)` answers
one question after a run: was this bike's redirect the right choice? A bike
bounced off a full station and was sent to another station. The function
returns the full station's neighbours in distance order, up to the station the
bike was sent to. Each row shows dock occupancy just before and just after the
redirect step. The nearer neighbours should show no free dock, and the chosen
station should be the first one with room.

It ranks neighbours with `neighbor_distance_sq` — the same squared-distance
metric the redirect mechanics use to make the decision — so the order the
explainer shows is always the order the simulator walked.

## The Checks

Two whole-journal invariants live here, next to the schema they check. They
return a list of human-readable violations (empty list = the invariant holds)
instead of raising, so the simulator-level `validate_run` can collect
everything and report once.

`check_demand_split` is I1 — demand splits exactly into served departures and
stockout losses:

```python
bad = merged[merged["demand"] != merged["departed"] + merged["lost_demand"]]
```

It is checkable because `demand` is a scenario input, not derived from the
journal.

`check_flow_closure` is I2 — every flow due by the run's horizon closes with
exactly one terminal event, `arrived` or `lost(dock_full)`:

```python
is_terminal = (flows["event_type"] == "arrived") | (
    (flows["event_type"] == "lost") & (flows["reason"] == "dock_full")
)
```

A `redirected` is not terminal — the flow rides on. A flow whose latest leg is
due past the last period is legitimately still in transit. Zero terminals for
a due flow means a bike vanished; more than one means a double close.

The other run invariants (I3–I5) need the live simulator state, so they live
in `gbp/consumers/simulator/validation.py` and are described in
[simulator.md](simulator.md#invariants).

## Who Calls What

| Caller | Uses |
|---|---|
| `gbp/loaders/dataloader_graph.py` | builders and `finalize_flows` to build the historical journal; the marginals for the historical tables; `inventory_at_moments` for the replay sizing |
| `gbp/consumers/simulator/` | builders inside the phases; `flows_to_departures` and friends as the live read-models on the state; `neighbor_distance_sq` in the redirect mechanics; `finalize_flows` at run end |
| `gbp/consumers/simulator/validation.py` | `check_demand_split`, `check_flow_closure`, `get_inventory_df`, `inventory_at_moments` |
| `app/artifacts.py` | `flows_with_measures`, `get_inventory_df`, the marginals — to build the saved run tables |

## Why It Is Built This Way

### Write And Read Live In One Module

The module owns one internal detail on both sides: the shape of a flow event.
The builders write events, and the read-models read them back. Splitting them
into a "writers" module and a "readers" module would spread the column layout
across two files, and every schema change would have to be made twice.

### The Model Layer Has No Loader Or Simulator Imports

`flows.py` imports nothing from the loaders or the simulator (the one `Routes`
import is type-checking only). Both of them import from it. This direction is
what makes the historical and simulated views comparable: `get_inventory_df`
is one function, so "historical inventory" and "simulated inventory" use the
same definition.

### One Inventory Delta Rule

`_event_deltas` is the only place that says which event moves inventory and
by how much. The read-time `_inventory_deltas` and the write-time
`inventory_deltas_from_events` (used by `SimulationState.apply_step_events`)
both delegate to it. The per-period view (`get_inventory_df`) and the per-step
view (`inventory_at_moments`) both add up the same deltas, only along
different time axes. The per-period deltas are literally the per-step deltas
aggregated by period, so the coarse and the fine view cannot disagree.

### `step_id`: One Owner Per Producer

Two separately ordered inventory batches must never share a `step_id`. The
simulator proves that with a counter: a number handed out once is never handed
out again, and `SimulationState.apply_step_events` is the only place that
stamps it. The historical loader has no counter, but its history is pure user
trips — one `(period_id, phase_rank, phase_round)` label is always exactly one
batch — so it derives the number from the label, in `stamp_history_ordering`.
`finalize_flows` stays out of it: it sorts by the stamped number and refuses a
journal without one, so "where does an event's `step_id` come from" has one
answer per producer. The full reasoning is in
[Notations.md §0.1](../../Notations.md#01-moment-and-step-the-inventory-time-axis).

### The Geometry Helpers Live Here

`haversine_km` and `neighbor_distance_sq` are in the model layer because both
sides need them: the loaders (trip distances) and the simulator (redirect
decisions). The import direction only allows sharing at the bottom.
`neighbor_distance_sq` in particular is shared between the redirect decision
and the redirect explainer on purpose: the explanation must rank stations
exactly the way the decision did, or the explanation would be wrong.
