# Design Doc: Loss Logging (lost trips in the flow journal)

> Status: IMPLEMENTED (steps 1-4 + 5a/5b/5c done; verified end-to-end -- see §10)
> Vertical: Citi Bike historical replay + above-baseline demand
> Source of truth: `notebooks/test_pipeline.ipynb`

---

## 1. Problem

A trip can fail to happen for two reasons: no bike at the origin (stockout) or no
free dock anywhere at the destination (dock full). Today neither is recorded in
the journal. The losses are even *computed* and then dropped:

- `realize_departures` returns a `lost` column (mechanics.py) — `FormDeparturesPhase`
  consumes only `realized` and emits no event.
- `plan_overflow_redirect` returns `remaining` (the un-docked overflow) —
  `DockArrivals` captures it as `_lost` and discards it.

Consequence: after a run there is no way to answer "why was this demand not
served?", and the books do not balance (see §6).

## 2. Principle

A loss is a first-class event, equal to `departed` and `arrived`. It belongs in
the journal — the single source of truth. Once losses are journaled, the question
"what was lost and why" is a filter: `event_type == "lost"`, grouped by `reason`.

The schema already supports this: `event_type` includes `lost`, and `reason`
carries `stockout | dock_full | ...`. No schema change is required.

## 3. The two losses have different shapes (on purpose)

### 3.1 Stockout (origin side, `FormDeparturesPhase`)
Demand that never became a trip. No target, no `flow_id` (the flow was never
born). Logged **aggregated** per origin:

```
period_id=t, source_id, commodity_category,
quantity=lost,            # the shortfall, not 1
event_type="lost", reason="stockout",
planned_target_id=NA, realized_target_id=NA, flow_id=NA,
start_period=NA, planned_end_period=NA, realized_end_period=NA
```

### 3.2 Dock full (destination side, `DockArrivals`)
A trip that died in transit. It **has** a `flow_id` (its spine), a `source_id`
and a `planned_target_id`. Logged **one row per flow**, closing the spine:

```
period_id=t, flow_id, source_id, planned_target_id,
quantity=1,
event_type="lost", reason="dock_full",
realized_target_id=NA, realized_end_period=NA
```

The asymmetry is the meaning: a stockout is *demand that never became a flow*
(aggregate, no spine); a dock-full is *a flow that ended without docking*
(per-flow, terminal event on its spine).

## 4. New builder

Add `lost_events(...)` to `gbp/model/journal.py`, next to `redirected_events`,
same schema, with `reason` passed in. One builder serves both call sites; the
caller supplies the columns it has (aggregate stockout rows vs per-flow dock-full
rows) and leaves the rest NA.

## 5. Emission sites

- `FormDeparturesPhase`: it already holds `realized` with a `lost` column. Build
  `lost_events` from the rows where `lost > 0` and return them as the phase's
  events. The phase stops being event-less; its event is the stockout loss.
  (`departed` events are still emitted later in `FormPotentialTripsPhase` — the
  split "how many leave" vs "where they go" is intentional.)
- `DockArrivals`: turn the discarded `remaining` (`_lost`) into `lost_events`
  with `reason="dock_full"` and append them. See the bug in §6.1 for the
  inventory side.

## 6. Two bugs that must be fixed alongside (they cause the imbalance)

### 6.1 Dock-full bikes silently vanish
`DockArrivals` does `in_transit = state.in_transit.drop(due.index)`, dropping the
lost flows too, but never docks them anywhere — their quantity leaves the system
with no record. Today this is an *unlogged, unintended* sink. After the fix it
becomes a *logged* event; whether the bike truly leaves the system or is retried
is an open decision (§9).

### 6.2 `redirected` is not counted as a docking in the projections
`get_inventory_df` and `flows_to_arrivals` filter `event_type == "arrived"` only,
but a redirect docks a bike at `realized_target_id`. The live state counts it
(`adjust_inventory(... dock_deltas(placed, "realized_target_id"))`); the journal
projection does not. Above baseline the live inventory and the inventory
recomputed from the journal diverge.

Rule: **every projection reads the full event taxonomy that moves a bike.**
- docking (+1 at `realized_target_id`): `arrived` ∪ `redirected`
- undocking (−1 at `source_id`): `departed`
- `lost` and the demand side contribute **nothing** to inventory.

## 7. Invariants (executable, placed by scope)

Invariants are code that runs, not notebook prose. Where each lives depends on
its scope.

### Tier 1 — local contracts (in the phase / mechanics)
Cheap properties about a single phase's output, checked every step as `assert`s
(design by contract): `realized <= stock`, `lost >= 0`, docked count never
exceeds free docks, a `lost` event never changes inventory. They fire at the
exact point a bug is introduced.

### Tier 2 — run invariants (a `validate_run` function)
Properties about the whole journal at run end. They cannot live in a phase: a
phase sees one period (mid-run `in_transit` is legitimately non-empty) and
recomputing the journal every step is wasteful. They live in one `validate_run`
function the engine calls at end of run behind a `validate` flag (off the hot
path, always available, exercised by the notebook). I1/I2 are pure functions of
the journal (home: `journal.py`); I3/I4 also need the live inventory,
`in_transit` and initial stock (home: the simulator layer).

- **I1 — demand split** (per `period_id, source_id, commodity`):
  `demand == Σ departed + Σ lost(reason=stockout)`. The journal faithfully
  records the split; checkable because `demand` is an input.
- **I2 — spine closure**: every `flow_id` with a `departed` whose
  `planned_end_period` falls by the last run period has exactly one terminal event
  ∈ {`arrived`, `redirected`, `lost`}. A flow due past the horizon is legitimately
  still in transit (the run window ended mid-trip), so closure is required only up
  to the horizon; more than one terminal is always a double close.
- **I3 — projection consistency**: the live `state_inventory_df` at run end equals
  `get_inventory_df(finalize_flows(journal), initial)`. Catches §6.2.
- **I4 — conservation**: `Σ initial == Σ final_inventory + Σ lost(dock_full) +
  Σ in_transit`. At run end a bike is docked (inventory), gone (the explicit
  dock-full sink), or still riding because the window ended mid-trip (in transit);
  those three account for every bike.

### Tier 3 — scenario checks (the notebook)
Properties of a specific scenario, not of the engine: in the exact replay,
`simulated_flows == historical_flows`. The notebook owns these and *calls*
`validate_run(env)` on the canonical run -- so the invariant logic is code and the
notebook is its consumer, not its owner.

All of I1-I4 are dormant in the exact replay (no loss or redirect fires) and only
bite above the baseline -- same posture as the rest of the constraint logic.

## 8. Out of scope

Time-aware redirect. The redirect is instantaneous today because duration lives
only in the OD matrix and is applied only in `FormPotentialTripsPhase`. Making a
redirect take time means modelling the hop to the neighbour as a new leg with its
own duration (a new `departed`, not an instant `arrived`), which also depends on
the unsolved "duration for non-historical pairs" problem. Tracked separately;
loss logging does not need it.

## 9. Decision: dock-full bike is a sink (MVP)

A dock-full-lost bike **leaves the system**. It is logged as a `lost` event and
is not re-docked, re-queued, or returned to its source; I4 carries it as a sink
term. This is the chosen MVP behaviour.

Deferred alternative: **(b) retry / continuation** — the bike becomes a new leg
next period. Realistic, but pulls in the §8 time work; revisit then.

## 10. Implementation status (resume point)

Done (each smoke-tested in isolation; canonical replay not yet run end-to-end):

- **§4 builder** — `lost_events(losses, period_id, reason)` in
  `gbp/model/journal.py`, exported from `gbp/model/__init__.py`.
- **§6.2 projections** — `DOCKING_EVENT_TYPES = ["arrived", "redirected"]` near
  the schema; `flows_to_arrivals` and `get_inventory_df` now count `arrived` ∪
  `redirected` as dockings (lint-wrapped the touched groupby lines).
- **§5 stockout emission** — `FormDeparturesPhase.execute` emits `lost`
  (`reason="stockout"`) from the `realized.lost > 0` rows; no longer event-less.
- **§6.1 + dock-full emission** — `DockArrivals.execute` turns the former
  discarded `_lost` into `lost` (`reason="dock_full"`) events as a sink.

Done — **5a Tier-1 contracts** (cheap `assert`s, smoke-tested on loss-triggering
inputs; dormant in exact replay by construction):

- `mechanics.realize_departures` — `realized <= available`, `lost >= 0`.
- `mechanics.dock_up_to_capacity` — no target docks over its free slots.
- `mechanics.plan_overflow_redirect` — `placed + remaining == overflow`.
- `phases.FormDeparturesPhase` — stock falls by exactly the realized count
  (stockout shortfall moves no inventory).
- `phases.DockArrivals` — every due flow docks/redirects/loses exactly once and
  stock rises only by `docked + redirected` (lost moves no inventory).

Done — **5b Tier-2 `validate_run`** (each invariant smoke-tested on synthetic
journals: clean run returns no violations, planted faults are caught):

- I1/I2 as pure journal functions in `gbp/model/journal.py`
  (`check_demand_split`, `check_spine_closure`), exported from `gbp/model`.
- I3/I4 in `gbp/consumers/simulator/validation.py` (`validate_run`, returning a
  list of violations; `_check_projection_consistency`, `_check_conservation`).
- Engine wiring: `EnvironmentConfig.validate` (default `False`); `Environment.run`
  calls `validate_run` at run end and raises `RunInvariantError` on violations.

Done — **5c Tier-3 notebook + end-to-end verification**. `test_pipeline.ipynb`
gained a validation cell after the canonical run: it calls
`validate_run(env_canonical.state, graph_data)`, prints losses by reason and the
in-transit count, and asserts no violations; the `simulated_departures ==
historical_departures` replay check was already there.

First end-to-end run on the real February 2026 trip data (saturated replay)
confirmed:

- I1 and I3 clean -- in particular I3 validates the §6.2 redirect-projection fix
  against live inventory on real data; `dock_full` losses are zero under
  saturation.
- 22 trips depart late enough that their mean-duration arrival lands past the last
  period; they are legitimately still in transit at run end. This drove the I2/I4
  revision above (closure required only up to the horizon; conservation carries an
  `in_transit` term) -- the canonical run then validates with **zero** violations.

The horizon tail is the same edge as §8 (a trip needs time the window does not
have); loss logging does not depend on resolving it.

Pre-existing lint/type debt left untouched: `D205` and one `E501`
(`empty_flows_journal`) in `journal.py`; `D101`/`D102` and pandas-stub `mypy`
errors across the phases (all present before this work).
