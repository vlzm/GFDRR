# Plan: stamp `step_id` at the moment a step is opened (option 3)

## One-paragraph summary

Today `step_id` is **computed after the run** from the tuple
`(period_id, phase_rank, phase_round)`: `finalize_flows` numbers the distinct
tuples 0, 1, 2, … This works only while every phase keeps an unwritten rule —
"one tuple = one inventory batch." This plan replaces the computed `step_id` with
a **stamped** one: a single run-global counter hands out the next integer each
time a phase opens an inventory step, and that integer is written onto the events
of that step at the moment they are produced. After the change, "two ordered
batches that share one step" can no longer be expressed, because each opened step
gets its own fresh number. The historical loader, which has no phases and no
batches, keeps computing `step_id` the old way.

---

## 1. Background: how `step_id` works today

- A flow event carries the tuple `(period_id, phase_rank, phase_round)`.
  `phase_rank` is stamped by the emitting phase; `phase_round` is stamped by the
  redirect mechanics (0 on every non-redirect row).
- `step_id` is **not** set during the run. It is assigned once, at the very end,
  by `_assign_step_id` inside `finalize_flows` (`gbp/model/flows.py`): sort the
  distinct tuples, number them 0, 1, 2, …, and give every event with the same
  tuple the same number.
- The same `finalize_flows` is used by both the simulator
  (`Environment.simulated_flows_df`, `gbp/consumers/simulator/engine.py`) and the
  historical loader (`get_historical_flows_df`,
  `gbp/loaders/dataloader_graph.py`). That is why history and simulation number
  their steps the same way — one rule, used twice.

## 2. The problem

The rule that makes the computed `step_id` correct is:

> One `(period_id, phase_rank, phase_round)` tuple is exactly one inventory step
> (one batch of `+1`/`-1` applied together). A phase that needs two *separately
> ordered* inventory changes must give them different `phase_rank` or different
> `phase_round`. Two batches must never share a tuple.

This rule is real and currently holds, but it is only **written in prose**
(`Notations.md` §0.1). Nothing in the code stops a future phase from breaking it.
When the work grows — for example a rebalancing phase that moves bikes between
stations in several ordered moves within one period — it is easy to emit two
batches that must be ordered but accidentally carry the same tuple. The computed
`step_id` would then **merge them into one step**, silently losing the order
between them.

Worse, this mistake **cannot be detected from the finished journal**: once events
are written, the boundary between the two batches is gone, so no after-the-fact
check can see that two batches were merged. (The run-level check I5,
`_check_step_nonnegativity` in `validation.py`, only catches the *harmful
consequence* — a station driven below zero — not the merge itself.)

The fix is to stop *computing* the step number from the tuple and instead
**assign it when the step is opened**, so the number is a fact created by the
producer, not a guess reconstructed afterward. With a fresh number per opened
step, two ordered batches simply cannot end up with the same `step_id`.

## 3. Goal

Make `step_id` in the simulator a value **stamped at creation**: a single
run-global counter gives out the next integer each time a phase opens an
inventory step, and that integer is written onto the events of that step. The
tuple `(period_id, phase_rank, phase_round)` stays in the schema as a **label**
(it still says when, which phase, which round), but it stops being the
**definition** of a step.

Non-goal: do **not** build the rebalancer or any new phase here. This plan only
changes how `step_id` is assigned for the phases that exist today, so the new
mechanism is in place before the next phase is added.

## 4. The complication found in the code (read this before designing)

Two facts about the current phases make "just stamp at each `adjust_inventory`
call" wrong:

1. **One logical step spans two phases — the departures step.**
   - `FormDeparturesPhase` applies the `-1` to live inventory (one
     `adjust_inventory` call) and builds the **stockout** `lost` events
     (`phase_rank = 1`).
   - `FormPotentialTripsPhase`, which runs **after** it, builds the actual
     `departed` events (`phase_rank = 1`). It calls `adjust_inventory` **not at
     all** — in the journal the `-1` is read from the `departed` move-0 event,
     not from a stored delta.
   - Both sets of events are the **same** step today (tuple `(t, 1, 0)`). So the
     step is opened by one phase and finished by a later phase. The stamped
     number must be **shared** across the two phases.

2. **One phase opens several steps — the redirect rounds.**
   - `DockArrivals` first docks bikes at their planned station (one
     `adjust_inventory`, this is round 0), then the redirect mechanics
     (`plan_overflow_redirect`) docks the overflow in rounds, each round its own
     `adjust_inventory` and its own `phase_round` (1, 2, …).
   - So `DockArrivals` opens one step for the planned dock and one more per
     redirect round.

A correct design must therefore let a producer (a) **open a fresh step and get
its number** when it begins an ordered change, and (b) **re-use an already-opened
step's number** when a later phase finishes the same step (the departures case).

A tempting shortcut — let the engine assign a fresh number per phase append, keyed
by the tuple — is **not** option 3. Keying by the tuple is the same derivation we
are trying to leave behind: a future rebalancer that emits two ordered batches
under one tuple would still have them merged. The number must come from an
explicit "open a step" call, not from the tuple.

## 5. Design

### 5.1. A run-global step counter on the state

`SimulationState` (`gbp/consumers/simulator/state.py`) gains one field:

```python
next_step_id: int = 0      # the next step number to hand out
```

and one method:

```python
def open_step(self) -> tuple[int, "SimulationState"]:
    """Hand out the next step number and return the advanced state."""
    return self.next_step_id, dataclasses.replace(self, next_step_id=self.next_step_id + 1)
```

Because the engine threads `self._state` through the phases in order, and the
phases run in step order already
(`DockArrivals("previous")` → `FormDeparturesPhase` → `FormPotentialTripsPhase`
→ `DockArrivals("same")`, i.e. ranks 0, 1, 1, 2), the counter advances in exactly
the order steps must sort. So the stamped `step_id` is already run-global,
contiguous, and monotonic — no after-the-fact numbering is needed for the
simulator.

> Why a counter threaded through immutable state, not a mutable allocator object:
> the rest of the simulator state is immutable (`dataclasses.replace`
> everywhere). Threading keeps that property and keeps the run deterministic.

### 5.2. Where each phase opens and stamps steps

- **`DockArrivals`** (`phases.py`):
  - Open one step for the planned dock (round 0); stamp its number on the
    `arrived` events of that batch.
  - For each redirect round (ascending `phase_round`), open one more step and
    stamp its number on that round's events (the `redirected` bounce, the move-1
    `departed`, the `arrived` at C, and a dock-full `lost` if any belongs to that
    round).
  - Thread the advancing state through these opens; return the final state in the
    `PhaseResult`.
  - The redirect mechanics (`plan_overflow_redirect`, `mechanics.py`) stays a pure
    function returning dataframes with `phase_round`; `DockArrivals` is what opens
    one step per distinct `phase_round`, so the counter is not threaded into the
    mechanics.

- **`FormDeparturesPhase`** (`phases.py`):
  - Open the departures step; get its number `d`.
  - Stamp `d` on the stockout `lost` events.
  - Pass `d` forward to the next phase through `intermediates` (the existing
    per-period hand-off, already used to pass `departures`), e.g.
    `state.with_intermediates(departures=..., departures_step_id=d)`.

- **`FormPotentialTripsPhase`** (`phases.py`):
  - Read `departures_step_id` from `intermediates` and stamp it on the `departed`
    events. (It does **not** open a new step — the departures step is already
    open.)

Every event the simulator emits now carries a `step_id` at creation. Events that
change no inventory (the redirect bounce, the move-1 `departed`, a `lost`) get the
`step_id` of the batch they belong to, exactly as they share a tuple today.

### 5.3. `finalize_flows` after the change

`finalize_flows` becomes: **if events already carry `step_id`, trust it** (sort by
it; it is already correct for the simulator); **if they do not, derive it the old
way** (the historical loader path — see 5.4). Concretely, `_assign_step_id` keeps
its current tuple derivation but runs only when `step_id` is absent or all-NA.

The tuple sort key and `phase_round` filling stay, because the historical loader
still needs them.

### 5.4. The historical loader keeps the old path

`get_historical_flows_df` has no phases and applies no batches — it turns observed
trips into a journal in one pass. It has nothing to open a step with, so it keeps
stamping `phase_rank` by timing (`phase_rank_by_timing`) and lets `finalize_flows`
**derive** `step_id` from the tuple, exactly as today. This is safe: history is
pure user trips (no redirects, no rebalancing), so one tuple is always one batch —
the rule we could not guarantee for the simulator is automatic for history.

So option 3 applies **where batches exist** (the simulator); the computed rule
stays **where it is provably safe** (history).

### 5.5. What stays the same

- `phase_rank` and `phase_round` stay in `FLOW_EVENT_COLUMNS` and keep being
  stamped. They are now **labels** (when / which phase / which round) and the
  historical loader's step-ordering input — no longer the simulator's definition
  of a step.
- The journal schema, the four event outcomes, the inventory read-models
  (`get_inventory_df`, `inventory_at_moments`) — unchanged.
- Exact historical replay must still produce identical numbers (see 7).

## 6. Migration — small, always-green steps

Each step leaves the test suite green and is shippable on its own.

1. **Add the counter, unused.** Add `next_step_id` and `open_step` to
   `SimulationState`. No phase calls it yet. No behavior change.
2. **Stamp in the simulator, in parallel.** Make the phases open steps and stamp
   `step_id` as in 5.2, but keep `_assign_step_id` still computing the tuple-based
   number into a temporary column. Add a bridge assertion/test: the stamped
   `step_id` equals the tuple-derived one on all seven scenarios and on the
   historical replay. This proves the new mechanism reproduces today's numbers
   before anything depends on it.
3. **Trust the stamp.** Flip `finalize_flows` to keep a present `step_id` and only
   derive when it is absent (5.3). The simulator now uses the stamped number; the
   historical loader still derives.
4. **Demote the tuple in the docs.** Update `Notations.md` §0.1 and
   `docs/scenario_step_id_tables_ru.md`: `step_id` is *opened at apply time* in the
   simulator (and *derived* for history); the tuple is a label. Remove the
   "the tuple defines the step" wording for the simulator; keep it for history.
5. **Keep the bridge as the oracle.** Keep the step-2 equality test
   (rename `test_step_id_is_a_pure_function_of_the_journal` to reflect that, for the
   simulator, the stamped number must match the tuple order on user-trip
   scenarios). This is now the guard that the labels stay consistent with the
   stamped order for the phases that exist today; a future rebalancer is allowed to
   break tuple-equality and will not be covered by it — which is the whole point.

## 7. Tests

- **Bridge / oracle (updated).** On every scenario and on the historical replay,
  the stamped `step_id` equals the old tuple-derived `step_id`. This is the
  acceptance test for the migration: same numbers, new mechanism.
- **Distinct steps are distinct numbers.** A test that two ordered changes which
  share a tuple but are opened separately get **different** `step_id`s. (Today no
  phase does this; the test can drive `open_step` directly or use a small fake
  phase, to lock the guarantee for the future rebalancer.)
- **Unchanged.** Conservation (I4), projection consistency (I3), per-step
  non-negativity (I5), and the per-period vs per-moment inventory tests must stay
  green — they do not depend on how `step_id` is assigned, only on its order.
- **Optional, stronger.** A property-based test (Hypothesis) over random phase
  orders that checks `step_id` is monotonic with the true apply order and never
  repeats.

## 8. How this changes the invariants

- The collision the prose rule warned about becomes **impossible to express** in
  the simulator: `open_step` never returns a number twice, so two ordered batches
  cannot share a `step_id`. The rule moves from "documented and hopefully kept" to
  "true by construction."
- I5 (`_check_step_nonnegativity`) stays as a cheap end-of-run guard, but it is no
  longer the only thing standing between us and a silent merge.

## 9. Risks and notes

- **Threading the counter.** The main cost is passing the advancing state through
  `DockArrivals` (planned dock + one open per redirect round) and through the
  departures hand-off. Keep it functional (return the advanced state) to preserve
  the immutable-state design; do not reach for a mutable global.
- **Replay must match exactly.** The base run is an exact historical replay, so
  the stamped numbers must equal the historical numbers on replay. Step 2's bridge
  test is what locks this; do not skip it.
- **Determinism.** The counter depends only on phase order and the redirect-round
  order, both already deterministic, so runs stay reproducible.
- **No new column.** `step_id` keeps its name and place in the schema; only *how*
  it is filled changes. No `Notations.md` schema row is added — only the §0.1
  definition is reworded.

## 10. Touch-point checklist

- `gbp/consumers/simulator/state.py` — add `next_step_id` field and `open_step`.
- `gbp/consumers/simulator/phases.py` — `DockArrivals` opens per dock/round;
  `FormDeparturesPhase` opens the departures step and passes its id forward;
  `FormPotentialTripsPhase` stamps that id on `departed` events.
- `gbp/model/flows.py` — `finalize_flows` / `_assign_step_id`: keep a present
  `step_id`, derive only when absent.
- `gbp/loaders/dataloader_graph.py` — no change (still derives via `finalize_flows`).
- `tests/test_scenarios.py` — update the oracle test to the bridge form; add the
  "distinct opens → distinct numbers" test.
- `Notations.md` §0.1 and `docs/scenario_step_id_tables_ru.md` — reword
  `step_id` from "derived from the tuple" to "opened at apply time (simulator),
  derived (history)"; the tuple is a label.
