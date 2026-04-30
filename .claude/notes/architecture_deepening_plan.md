# Architecture deepening — handoff plan

> Session in progress. Survived `/compact` / `/clear`. Read this first before continuing.

## Context

Running the `/improve-codebase-architecture` skill. Surfaced four deepening
candidates and committing to all four in order, with `pytest` + `ruff` after
each. Vocabulary: module / interface / depth / seam / adapter / deletion test
(see `.claude/skills/improve-codebase-architecture/LANGUAGE.md`).

User language preference: code/comments/docstrings — English; conversation —
Russian, plain-language style (see CLAUDE.md AI Collaboration Rules).

## Status of the four candidates

1. **Event-bus replacement for `PhaseResult` × `SimulationLog`** — DONE.
   - `PhaseResult.events: dict[str, pd.DataFrame]` replaces six DataFrame fields.
   - `LogTableSchema` registry in `gbp/consumers/simulator/log.py` drives one
     unified `record_events` loop.

2. **`DispatchPhase` lifecycle extraction** — DONE.
   - `gbp/consumers/simulator/dispatch_lifecycle.py` (new): public
     `run_dispatch_lifecycle(...) -> DispatchOutcome`. Five rejection rules
     + assign + apply are private.
   - `dispatch_phase.py` slimmed to ~65 lines.
   - `tests/unit/consumers/simulator/test_dispatch_lifecycle.py` (new) tests
     the lifecycle directly without a Task fake.

3. **Inventory delta helpers** — DONE.
   - `gbp/consumers/simulator/inventory.py` (new): `to_inventory_delta`,
     `apply_delta(op="subtract"|"add"|"add_clip_zero")`, `merge_with_inventory`.
   - 4 of 7 inventory-merge sites collapsed to two lines (organic dep/arr,
     ArrivalsPhase, dispatch lifecycle apply).
   - `DemandPhase`, `DeparturePhysicsPhase` use `merge_with_inventory`.
   - `DockCapacityPhase` left inline on purpose (NaN in `capacity` means
     "unbounded"; `fillna(0.0)` would break that semantics).

4. **`ResolvedModelData` defensive None/empty checks** — DONE.
   - `ResolvedModelData.__post_init__` (in `gbp/core/model.py`) replaces
     `None`/empty consumer-facing tables with column-aware empty DataFrames.
     The set is `_CONSUMER_NORMALIZED_TABLES` (13 fields: demand, supply,
     observed_flow, observed_inventory, inventory_initial,
     inventory_in_transit, resources, resource_fleet, resource_categories,
     resource_commodity_compatibility, resource_modal_compatibility, edges,
     edge_commodities). Time-resolved tables substitute `date → period_id`
     via `_TIME_RESOLVED_TABLES`.
   - `validate()` now skips empty optional tables (resolved tables have
     `period_id` columns that diverge from raw schemas — column validation
     would false-positive on the normalized empties).
   - Consumer code (`built_in_phases.py`, `dispatch_lifecycle.py`,
     `state.py`, `engine.py`) collapses `is None or .empty` to a single
     `.empty` check. `_is_nonempty` helper deleted.
   - `test_attributes_none_returns_empty` removed: it tested
     `attributes is None`, which the registry's `default_factory` makes
     impossible.
   - New tests in `tests/unit/core/test_model.py`:
     `test_resolved_consumer_tables_normalized_to_empty` and
     `test_resolved_normalization_preserves_user_provided_data`.
   - Move B (capability predicates) deferred — Move A removed enough
     friction that the predicates are not yet justified.

Test baseline after #4: 340 tests pass (was 339; +2 new, -1 removed),
ruff clean on changed files. Pre-existing `I001` in `gbp/core/model.py`
imports and `D205` in `_state_with_latent` are unrelated and remain as
upstream debt.

## Candidate #4 — what to do

### Friction (re-locate with `grep -n "is None" gbp/consumers/simulator/`)

`ResolvedModelData` (`gbp/core/model.py`) is a flat dataclass with ~53
optional DataFrame fields. Consumers repeat the same defensive dance over
and over:

- Pattern 1: `if resolved.X is None: return PhaseResult.empty(state)` — used
  by `DemandPhase`, `DispatchPhase` validators, `DockCapacityPhase`, etc.
- Pattern 2: `if resolved.X is None or resolved.X.empty:` — same idea, more
  defensive. Two phrasings of the same question.
- Pattern 3: Compatibility tables — `_assign_resources` in
  `dispatch_lifecycle.py` checks `resolved.resource_commodity_compatibility`
  AND `resolved.resource_modal_compatibility` separately; both are part of
  one concept ("resource compatibility").

Specific call sites to inspect (line numbers may shift; grep first):
- `gbp/consumers/simulator/dispatch_lifecycle.py` — assign step
  (compatibility tables), validation step (edges, resource_categories).
- `gbp/consumers/simulator/built_in_phases.py` — `DemandPhase.execute`,
  `DockCapacityPhase.execute`, `_period_flows` helper, `HistoricalLatentDemandPhase`.

### Constraints (DO NOT VIOLATE)

From CLAUDE.md "Data Model Invariants":
- **Invariant #6**: One `ResolvedModelData` for all consumers (Environment,
  Optimizer, Analytics). The deepening must not split the model into
  consumer-specific containers.
- **Invariant #1**: Nullable = LP-compatible. Discrete parameters are
  intentionally nullable. The fix is not "make everything required."
- Build pipeline is stateless and deterministic
  (`gbp/build/pipeline.py:build_model`).

### Suggested deepening shape (open for grilling)

Two related moves, smallest viable first:

**Move A — Normalise empties at build time.** In `build_model`, replace
`Optional[pd.DataFrame]` fields with non-None empty DataFrames (with the
right column schema) for tables that the consumer cares about. Then call
sites switch from `if X is None or X.empty` to a single `if not X.empty`
form. No interface change to `ResolvedModelData`'s public attribute names.

- Pro: minimal API churn, big locality win.
- Con: `None` vs empty currently encodes "table not declared" vs "table
  declared but no rows". Worth confirming none of the build code actually
  relies on that distinction.
- Risk: schemas need to be canonicalised — probably already exist in
  `gbp/core/schemas/`.

**Move B — Capability predicates on the model.** Add small `has_X` /
`is_present` predicates (or one accessor) for the "do I have this knowledge"
question, especially for compatibility tables. Example:

```python
resolved.resources.commodity_compat_set()  # set[(category, commodity)] or None
```

centralises the "load compatibility, fall back to None" logic. Each phase
no longer rebuilds the set.

- Pro: shared logic + tested in one place.
- Con: adds surface area; only worth it when ≥3 callers benefit.

Start with A (normalise empties). Defer B until A reveals which predicates
actually duplicate. Don't preemptively design containers — that violates
invariant #6.

### Test plan for #4

- After Move A: existing unit + integration tests should still pass. If a
  test relied on `is None` semantics, surface and discuss before patching.
- Add a small test in `tests/unit/build/` that asserts the new "empty by
  default" invariant for the canonicalised tables.
- Optionally: a test that captures the dispatch lifecycle's compatibility
  fallback ("no compatibility tables → assign anyway").

## Process reminders for next session

- Run after each move: `pytest --tb=short` and `ruff check <changed-files>`.
- mypy errors about `pandas-stubs` are pre-existing — ignore.
- Pre-existing lint issues (`E501` in old code, `D205` in
  `_state_with_latent`) are upstream — only fix if I touch the line.
- After completing #4, update `PROJECT_STATE.md` if the structural change
  is meaningful (per CLAUDE.md guidance).
- Do not delete this plan file when done; the user can decide.
