# Plan: make the `step_id` ordering one rule, and fix two smaller id issues

A fresh-eyes review of the id / period / step design. The identifiers
themselves (`flow_id`, `move_id`, `event_id`, `period_id`, `step_id`) are
correct and lean. The debt is in **how `step_id` is assigned**, plus two smaller
issues. This document states each problem and a concrete implementation plan.

Anchors: `gbp/model/flows.py` (the journal), `gbp/consumers/simulator/engine.py`
(the run loop), `gbp/consumers/simulator/mechanics.py` /
`gbp/consumers/simulator/phases.py` (the redirect), Notations.md §0 / §0.1.

---

## Problem 1 (main): `step_id` has two definitions and scaffolding through the engine

### What is wrong now

`step_id` is the run-global ordinal of an inventory step (Notations.md §0.1). It
is assigned in `finalize_flows` from **two** inputs by **two** code paths:

- a **simulated** journal carries scaffolding the run stamped — `phase_tick`
  (one value per phase invocation, stamped by `Environment.step`) and
  `redirect_round` (the round a redirected flow docked in, stamped by the
  redirect) — and `finalize_flows` densifies `(phase_tick, redirect_round)`;
- a **historical** journal carries neither, so `finalize_flows` derives the
  order from the event semantics (`_phase_rank`, today keyed off `start_period`).

Two problems follow:

1. **Two definitions of one order.** The simulated path and the historical path
   must agree, and they only agree "by coincidence" in a base replay. That is a
   standing correctness burden — a subtle change to either side can split them.
2. **Scaffolding threaded through the engine.** `phase_tick` is a counter the
   engine maintains and stamps onto every phase's events (`engine.py`), and both
   `phase_tick` and `redirect_round` are dropped again by `finalize_flows`
   (`SCAFFOLDING_COLUMNS`). The engine has to know about step ordering at all,
   and two throwaway columns ride through `SimulationState`.

### The fix: one rule, derived from the journal

`step_id` can be a pure function of the journal. The order is

```
(period_id, phase_rank, redirect_round)
```

where:

- **`phase_rank`** is read from the event semantics and the flow's opening period
  `t` (the `period_id` of the flow's move-0 `departed`):
  - `1` — the period's departures (`departed` with `move_id == 0`) and stockout
    losses (`lost` with `reason == "stockout"`); this is the middle phase;
  - `0` — a docking-phase event whose flow opened **earlier** (`period_id > t`):
    the `DockArrivals("previous")` batch;
  - `2` — a docking-phase event whose flow opened **this** period
    (`period_id == t`): the `DockArrivals("same")` batch.
- **`redirect_round`** is the round a redirected flow docked in (`0` for a normal
  dock batch, `1..` for redirect rounds). This is the one piece of order that is
  **not** recoverable from the other columns — a redirect's rounds are the
  mechanics' internal iteration — so it must be stored.

`step_id` is then the dense rank of the distinct `(period_id, phase_rank,
redirect_round)` tuples in that sorted order.

**Verified.** A prototype of this single rule produced a `step_id` identical,
row for row, to the current scaffolding-based one on `canonical`, `overflow`, a
three-round nested redirect (`multiround`), and a mixed cur/prev redirect
scenario. The migration is behaviour-preserving.

### What this removes

- `phase_tick` entirely, and its threading in `engine.py` — the engine goes back
  to a plain phase loop and knows nothing about step ordering (cleaner layering:
  ordering lives only in the model layer);
- the two-path branch in `finalize_flows`;
- the `SCAFFOLDING_COLUMNS` concept.

`step_id` becomes a provable pure function of the journal — history and
simulation get the same `step_id` from the **same formula**, not by coincidence,
matching the module's stated philosophy ("inventory is a projection of the
journal"; now ordering is too).

### Trade-off (the chosen variant)

`redirect_round` becomes a **canonical column** (it was scaffolding). It is `0`
on almost every row — non-zero only on a redirect's continuation events above
the historical baseline — so it is mostly-zero noise on the schema. The exchange
is: **two dropped scaffolding columns → one honest stored column**, minus the
finalize branch, minus the engine threading.

Considered alternative (not chosen): do not store `step_id` at all and compute
it inside the read-models on demand. The schema would not grow (only
`redirect_round` stays), but the raw journal would no longer *show* `step_id`,
which is useful when inspecting the journal by eye. Decision: **store `step_id`**
(this plan).

### Implementation steps

1. **`gbp/model/flows.py` — schema.**
   - Add `redirect_round` to `FLOW_EVENT_COLUMNS` and `FLOW_EVENT_DTYPES`
     (`Int64`).
   - Remove the `SCAFFOLDING_COLUMNS` constant and its docstring note.
   - Keep `_typed_events` tolerant of a missing column (builders still do not set
     `redirect_round`; `finalize_flows` fills it).

2. **`gbp/model/flows.py` — `finalize_flows` / helpers.**
   - Replace `_assign_step_id` with the single rule above (one path, no
     `phase_tick` branch).
   - Rewrite `_phase_rank` to take the opening period `t` (mapped per `flow_id`
     from the move-0 `departed`) instead of `start_period`. This is also what
     makes it correct for redirect continuation rows (see Problem 2).
   - Fill `redirect_round` with `0` where absent/NA, then project it as a
     canonical column (no longer dropped).
   - Sort key: `(period_id, phase_rank, redirect_round, flow_id, event_id)`;
     `step_id` = dense rank of `(period_id, phase_rank, redirect_round)`.

3. **`gbp/consumers/simulator/engine.py`.**
   - Remove `self._phase_tick` and the per-phase stamping in `step`; revert to
     the plain `for phase in phases: execute → append_flows` loop.

4. **`gbp/consumers/simulator/mechanics.py` / `phases.py`.**
   - Keep `redirect_round` as produced today (mechanics numbers the rounds,
     `DockArrivals` carries it onto the bounce + continuation events). Update the
     comments that call it "scaffolding" — it is now a canonical column.

5. **Notations.md.**
   - Add `redirect_round` to the §0 schema table.
   - Update §0.1: `step_id` is derived by one rule from `(period_id, phase_rank,
     redirect_round)`; drop any "scaffolding" wording.

6. **Tests.**
   - Existing tests must stay green (the `step_id` values do not change).
   - Add one test that `step_id` is a pure function of the finalized journal:
     recomputing it from `(period_id, phase_rank, redirect_round)` equals the
     stored `step_id`. This locks the "single rule" property.

---

## Problem 2 (separate pass): `start_period` means two different things

### What is wrong now

`redirect_continuation_events` sets `start_period = period_id` (the bounce
period) on the move-1 rows. So within one redirected flow, `start_period` is
**not** consistent: the move-0 rows carry the flow's opening period, the move-1
rows carry the bounce period.

This is the root of two "gotchas":

- in `_phase_rank` you cannot use a continuation row's own `start_period` to find
  the opening period (it would equal `period_id` and mislabel the phase);
- in a `classify_departed`-style split (cur vs prev redirect departures) the
  opening period `t` must be read from the move-0 row, not the continuation row.

### Fix

Make `start_period` **always the flow's opening period** on every row of the
flow, including the continuation. If a redirect's second arc later needs its own
timing (the multi-period second-leg scenarios 5 and 7 in
`docs/scenario_event_tables.md`, not implemented today), add a separate column
for the arc's own start (e.g. `arc_start_period`) rather than overloading
`start_period`.

### Why this is low-risk but still separate

- The read-models that use `start_period` for timing are OD-side and filter
  `move_id == 0` (`flows_to_od_matrix`, the duration), so they read only move-0
  rows and are **unaffected** by changing the continuation rows.
- `get_inventory_df`, `flows_to_arrivals`, `inventory_at_moments` do not use
  `start_period` at all.

So the change is mostly mechanical, but it does touch `redirect_continuation_events`
and the meaning of a schema column, and it interacts with the unimplemented
multi-period second arc — hence a separate, deliberate pass.

### Bonus interaction with Problem 1

Once `start_period` is the opening period on **all** rows, the Problem 1 rule can
read `phase_rank` straight off each row's `start_period` and drop the per-flow
opening-period join entirely. Order the passes so Problem 1 lands first (it maps
`t` from the move-0 rows, robust to the current inconsistency); Problem 2 can
then simplify the rule afterwards.

---

## Problem 3 (note only): `move_id` is derivable from `event_id`

`move_id` (0,0 normal; 0,0,1,1 redirect) can be derived from `event_id` (0,1 /
0,1,2,3). Mild redundancy. **Do not change.** `move_id` carries the clear "which
arc" semantics and is what every reader filters on (`move_id == 0` for a real
user departure); deriving it from `event_id` ranges at each read site would be
less transparent, not more. Listed only for completeness.

---

## Suggested order of work

1. Problem 1 — unify `step_id` (canonical `redirect_round`, one rule, drop
   `phase_tick` + branch). Behaviour-preserving, verified.
2. Problem 2 — make `start_period` the opening period on all rows; then simplify
   the Problem 1 rule to read `start_period` directly.
3. Problem 3 — no action.
