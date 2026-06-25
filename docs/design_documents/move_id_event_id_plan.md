# Plan: per-trip `event_id`, new `move_id`, redirect as two arcs

Status: **approved, not yet implemented.** This document is a self-contained
hand-off so the work can be done in a fresh chat, step by step. Read it together
with [`Notations.md`](../../Notations.md) (the canonical vocabulary) and
[`gbp/model/flows.py`](../../gbp/model/flows.py) (the event schema).

---

## 1. Why we are doing this

### The current model

One row of the flow journal is one **event**. Today there are three ids in the
schema (`FLOW_EVENT_COLUMNS` in `flows.py`):

- `flow_id` — string id of one bike's whole journey (`hist_5`, `sim_7_2`). One
  flow = one bike's movement. This is the journal's atomic unit (`Notations.md`
  §3).
- `event_id` — a **global** monotonic row number, assigned once in
  `finalize_flows` after sorting the whole journal. It is *not* scoped to a trip
  and numbers nothing inside a trip. Nothing in the code filters on it.
- `period_id` — the period the event happened in (stays unchanged).

There is also a hidden internal field `event_order` (0 for `departed`, 1 for the
terminal event) that exists only to order the two events of a flow during the
sort in `finalize_flows`. It is not part of `FLOW_EVENT_COLUMNS` (it is projected
away).

Today every flow has at most **two** events: `departed` plus one of `arrived` /
`redirected` / `lost`. A redirect is therefore modeled as a **single** arc
`source → realized_target` (the bike appears to go straight to the other
station); the intermediate full station is implicit, there is no second arc in
the data.

### What we want and why

- `event_id` should be a **per-trip ordinal (0..n)** — the order of events
  *inside one trip* — not a global counter. (This was the original complaint:
  the global `event_id` is "not what I need".)
- a new `move_id` — a **per-trip arc index (0..m)** — one physical edge of the
  trip. A normal trip is one arc (`move_id = 0`); a redirect produces a real
  **second** arc (`move_id = 1`).
- Redirect is modeled as **two arcs**, made explicit in the data, not collapsed
  into one row.

**The driving reason for `move_id` is the next big step: the rebalancer
(trucks/resources).** There, a bike's journey is genuinely multi-leg
(station → depot → station), and `move_id` is the natural index of the leg.
Redirect is the first place this branching already appears, so we represent it
explicitly now rather than lose that transparency. (The `resource_id` /
`resources_capacities` machinery is already reserved in the code but idle.)

### Decisions already made (do not re-open)

1. **Keep `flow_id`.** Do **not** rename it to `trip_id`. Reason: the vocabulary
   is a 4-rung granularity ladder — `demand` (facility level) → `trip` (OD,
   source+target aggregate, lives in `potential_trips`) → `flow` (journal,
   per-bike) → `bike` (physical). The word `trip` already names the OD-aggregate
   rung; moving it down to the journal rung would collide with `flow` and leave
   the OD rung with no good name (`demand` is the facility rung, not a synonym).
   So `flow_id` stays.
2. **Redirect = two arcs** (the variant where `move_id` reaches 1).
3. **Stockout loss stays aggregated with no `flow_id`** (as today): one `lost`
   row per `(source, commodity)` with `quantity` possibly > 1.

---

## 2. Target model

### Columns

`FLOW_EVENT_COLUMNS` becomes (id of trip, then arc, then event, then the rest):

```
flow_id, move_id, event_id, period_id, flow_type, event_type,
commodity_category, source_id, planned_target_id, realized_target_id,
start_period, planned_end_period, realized_end_period,
resource_id, quantity, reason
```

- `flow_id` — string, **unchanged**.
- `move_id` — Int64, arc index inside the trip (0..m).
- `event_id` — Int64, event ordinal inside the trip (0..n).
- The **old global `event_id` is removed.** Row uniqueness is now the pair
  `(flow_id, event_id)`.
- `event_order` is **removed** (its job is taken by `move_id` + `event_id`).

### Event shape per outcome (the heart of the design)

`(event_type, move_id, event_id)` for each outcome:

| Outcome | Events |
|---|---|
| Normal | `departed`(0,0) → `arrived`(0,1) |
| Lost — stockout | `lost`(0,0) — no `flow_id`, aggregated |
| Lost — dock_full | `departed`(0,0) → `lost`(0,1) |
| **Redirect** | `departed`(0,0) → `redirected`(0,1) → `departed`(1,2) → `arrived`(1,3) |

Reading the redirect row by row for a bike that left A, aimed at B (full),
parked at C:

| flow_id | move_id | event_id | period | event_type | source | planned_target | realized_target |
|---|---|---|---|---|---|---|---|
| sim_7_2 | 0 | 0 | 5 | departed | A | B | NA |
| sim_7_2 | 0 | 1 | 7 | redirected | A | B | NA |
| sim_7_2 | 1 | 2 | 7 | departed | B | C | NA |
| sim_7_2 | 1 | 3 | 7 | arrived | B | C | C |

Arc 0 (`A→B`) ends with `redirected` (reached B, did not fit). Arc 1 (`B→C`) is
an ordinary `departed`+`arrived` pair. `move_id` increases on the **second**
`departed`.

**For `lost` we confirmed `move_id` is never needed:** a stockout has **0** arcs
(the bike never moved — there is nothing to index); a dock-full loss has exactly
**1** arc (`move_id = 0`), the same one-arc shape as a normal trip. What tells
the outcomes apart is `event_type` + `reason`, never `move_id`. Only redirect can
have more than one arc, and at most two (the redirect mechanics dock a bike at
exactly one free station or lose it — never a third leg).

---

## 3. Key design decision: assign ids at emit time

**Set `move_id` and `event_id` as constants in the event builders, at the moment
each event is created — do NOT compute them later in `finalize_flows`.**

Why: the live state's derived marginals (`state_departures_df`,
`state_arrivals_df`, `state_od_matrix_df` in
[`state.py`](../../gbp/consumers/simulator/state.py)) read the **un-finalized**
journal (`state_flows_df`). They will need `move_id` for the `move_id == 0`
filter (see §4). If the ids are assigned only in `finalize_flows`, the live
journal lacks them. Assigning at emit time means both the live and finalized
journals carry the columns, with no special "finalize-only" fields.

This is possible because every event's position inside its trip is **fixed by
its outcome** (see the table in §2) — it does not need a runtime `cumcount`. Each
builder/emission site knows its `(move_id, event_id)` statically, exactly the way
it hard-codes `event_order` today.

Consequence: `event_order` disappears, and `finalize_flows` no longer assigns any
id. It only sorts (`period_id`, `flow_id`, `event_id`) and projects the columns.

---

## 4. Consequence: `departed` has two roles

After this change a `departed` event means one of two different things:

- `departed` with `move_id == 0` — a **real user departure** from a dock
  (`−1` to the source's inventory).
- `departed` with `move_id >= 1` — a **redirect continuation leg** (`B→C`), pure
  transport. The bike never occupied a dock at B (B was full), so this event must
  **not** change B's inventory and must **not** count as demand/outflow.

Therefore every reader that treats "`departed` = a user departure" must add the
filter **`move_id == 0`**:

- `flows_to_departures` — count `departed & move_id == 0`.
- `flows_to_od_matrix` — build the matrix from `departed & move_id == 0` only
  (the redirect legs are not intended trips and would pollute the model; their
  duration is 0 anyway).
- `get_inventory_df` — `−1` at the source only for `departed & move_id == 0`.
- `build_potential_trips` (loader) — filter `move_id == 0` defensively (history
  never redirects, so all historical `departed` are already `move_id == 0`).

And **`redirected` stops being a docking event.** It becomes a "bounce" (the bike
did not dock at B). The actual docking is now the final `arrived` at C. So:

- `DOCKING_EVENT_TYPES` shrinks from `["arrived", "redirected"]` to `["arrived"]`.
- `flows_to_arrivals` counts `arrived` only (its `realized_target_id` is C for a
  redirect, so inflows still land at the right station).
- `get_inventory_df` adds `+1` only for `arrived`.

Net inventory effect of a redirect is unchanged: `−1` at A (the move-0
`departed`), `+1` at C (the final `arrived`). The `redirected` bounce and the
move-1 `departed` carry **zero** inventory effect. The live inventory in the
phases already nets `−1 A / +1 C`; we only add journal rows.

---

## 5. File-by-file changes

### 5.1 `gbp/model/flows.py` (the core — most of the work)

- `FLOW_EVENT_COLUMNS` / `FLOW_EVENT_DTYPES`: add `move_id` and `event_id`
  (`Int64`); remove `event_order`; remove the global `event_id` assignment.
- `_typed_events`: stop casting `event_order`; cast `move_id` / `event_id`.
- Event builders — set `move_id` / `event_id` as constants per the §2 table:
  - `departed_events` → `move_id=0, event_id=0`.
  - `arrived_events` → `move_id=0, event_id=1` (the normal-trip arrival).
  - `lost_events` → stockout: `move_id=0, event_id=0`; dock_full:
    `move_id=0, event_id=1`. (The builder serves both; the caller's columns
    decide which. Pick `event_id` from context — dock-full carries a `flow_id`
    and a `departed` precedes it, stockout does not. Simplest: a small parameter
    or two thin wrappers. Keep stockout `move_id=0` as a convention; it has no
    arc but the value is never read by any `move_id==0` departed filter.)
  - `redirected_events` → **redefine as a bounce**: `realized_target_id = NA`,
    `reason = "dock_full"`, `move_id=0, event_id=1`. No longer a docking.
  - **New** `redirect_continuation_events(redirected, period)` → emits the
    move-1 pair for each redirected bike: a `departed`(1,2) and an `arrived`(1,3)
    with `source_id ← planned_target_id` (the full station B),
    `planned_target_id ← realized_target_id` (C), `realized_target_id = C`,
    `start_period = planned_end_period = realized_end_period = period`, same
    `flow_id` / `commodity_category` as the original flow.
- `empty_flows_journal`, `empty_in_transit`: new columns, no `event_order`.
- `finalize_flows`: sort by `(period_id, flow_id, event_id)`, `reset_index`,
  return `flows[FLOW_EVENT_COLUMNS]`. **No** id computation anymore.
- `DOCKING_EVENT_TYPES = ["arrived"]`.
- `flows_to_departures`: filter `event_type == "departed"` **and**
  `move_id == 0`.
- `flows_to_arrivals`: `arrived` only (via `DOCKING_EVENT_TYPES`).
- `flows_to_od_matrix`: `departed & move_id == 0`.
- `get_inventory_df`: `−1` per `departed & move_id == 0`; `+1` per `arrived`.
- `check_demand_split` (I1): uses `flows_to_departures`, so it follows the filter
  automatically; double-check the stockout `lost` grouping still matches.
- `check_flow_closure` (I2): the terminal event is now `arrived` or
  `lost(dock_full)` — **not** `redirected` (it is intermediate now). Rework the
  count to be **per `flow_id`**: each flow that has a move-0 `departed` (i.e. not
  a stockout) and is due by the horizon must have exactly one terminal
  (`arrived` or `lost` with `reason == "dock_full"`). A redirect flow now has two
  `departed` rows, so do not count "departed rows" as "flows" — group by
  `flow_id`.

### 5.2 `gbp/loaders/dataloader_graph.py`

- `build_potential_trips`: add `move_id == 0` to the `departed` filter.
- Fix docstrings that mention `event_order` or the global monotonic `event_id`
  (`get_historical_flows_df`, the module header). Historical flows still emit
  only `departed`(0,0) + `arrived`(0,1); history never redirects.

### 5.3 `gbp/consumers/simulator/mechanics.py`

- `plan_overflow_redirect`: make sure the returned `redirected` frame carries
  both ends of the redirect — `source_id = A`, `planned_target_id = B`,
  `realized_target_id = C` — so the phase can build both the bounce and the
  continuation leg. (It already adds `realized_target_id`; confirm `source_id`
  and `planned_target_id` survive.)

### 5.4 `gbp/consumers/simulator/phases.py`

- `DockArrivals.execute`: for the redirected set, emit **three** event groups
  instead of one:
  1. `redirected_events(redirected, t)` — the bounce (no docking).
  2. `redirect_continuation_events(redirected, t)` — the move-1
     `departed` + `arrived`.
  Keep the single `+1` inventory update at C (via `dock_deltas(redirected,
  "realized_target_id")`, unchanged net effect). **Do not** add the move-1
  `departed` to `in_transit` — the bike docks in this same phase, so it must not
  re-enter the working set. The existing `assert`s about conservation may need
  their counts re-checked against the new event count.

### 5.5 `gbp/consumers/simulator/validation.py`

- Re-verify I2/I3/I4 on an overflow scenario:
  - I2 (`check_flow_closure`) — see §5.1 rework.
  - I3 (`_check_projection_consistency`) — depends on the new `get_inventory_df`;
    live vs journal-projected inventory must still match.
  - I4 (`_check_conservation`) — a redirect keeps the bike in the system (it
    `arrived` at C → counted in final inventory), so
    `Σ initial == Σ final + Σ lost(dock_full) + Σ in_transit` must still hold.

### 5.6 `Notations.md`

- §0 symbol table: rewrite the `event_id` row ("event ordinal inside the trip,
  0..n, set by the builders"); add a `move_id` row ("arc index inside the trip,
  0..m; a redirect makes a second arc"). Remove any mention of `event_order` and
  of the global monotonic `event_id`.
- §1: note that `redirected` is now a non-docking bounce and the docking of a
  redirected bike is its final `arrived` (update "Dock is the verb…"/
  `DOCKING_EVENT_TYPES` wording).
- Add the `move_id == 0` rule for `departed` somewhere visible (the "departed has
  two roles" point) so future readers know to filter.

### 5.7 Tests / notebook

- `tests/` has no real coverage (only `__init__.py`) — nothing to break, but it
  would be a good moment to add a small test for a redirect-producing scenario.
- `notebooks/test_pipeline.ipynb` needs **no code change**; `flow_id` / `event_id`
  appear only in saved table output and refresh on re-run.

---

## 6. Correctness anchors

- **The canonical scenario must not change in substance.** The base replay
  (`saturate_stock=True`, `demand_scale_factor=1.0` in
  `notebooks/test_pipeline.ipynb`) has **no** redirects and **no** stockouts, so
  every flow has `move_id = 0`, `event_id ∈ {0, 1}`, and the new `move_id` column
  is all zeros. The invariant `simulated_flows_df == historical_flows_df` must
  still hold — this is the main check that the ordinary path is intact. (The only
  visible difference: `event_id` is now per-trip `0/1` instead of a global
  counter, plus the all-zero `move_id` column.)
- **An overflow scenario is the first place `move_id` reaches 1.** Build one by
  raising `demand_scale_factor` above 1 or shrinking dock capacity (so docks fill
  and redirect fires), then check: redirect flows have the four-event shape,
  `move_id` reaches 1, inflows/inventory still balance, and I1–I4 hold.

---

## 7. Suggested order of work

1. **Journal schema + builders + `finalize_flows`** (including
   `redirect_continuation_events`). Get the four event shapes right.
2. **Journal readers**: `move_id == 0` filters, `DOCKING_EVENT_TYPES`,
   `check_flow_closure` rework.
3. **Simulator**: `plan_overflow_redirect` ends + `DockArrivals` three-event
   emission + `in_transit` handling.
4. **Run the canonical notebook** — confirm `simulated == historical` still holds
   — then an overflow scenario — confirm redirect shape and I1–I4.
5. **`Notations.md`** updates.

After each step run: `ruff check gbp/`, `ruff format gbp/`, `mypy gbp/`.
