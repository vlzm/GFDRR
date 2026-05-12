# Architecture Deepening Candidates

Architectural review of the codebase as of 2026-05-12.
Each candidate is self-contained: copy the relevant section into a new chat
to work on it independently.

Pipeline reminder:
`Raw Citi Bike data -> DataLoaderGraph -> RawModelData -> build_model() -> ResolvedModelData -> Environment -> SimulationLog`

Source of truth: `notebooks/canonical_scenario.ipynb`.

---

## Candidate 1: Extract `OverflowRedirectPhase` from `built_in_phases.py`

### Files

- `gbp/consumers/simulator/built_in_phases.py` (1538 LOC, 14 phases)
- Specifically lines 944-1204: `OverflowRedirectPhase`

### Problem

`OverflowRedirectPhase` is 260 lines with its own internal logic: distance
matrix construction, vectorized argmin over capacities, proportional
redistribution of overflow. It lives in the same file as 13 other phases,
most of which are 30-80 lines of "filter period, update inventory, emit
events."

The file forces you to hold all 14 phases in your head, even though they
only depend on each other through `state.intermediates` (a message bus).

Deletion test: removing `OverflowRedirectPhase` from the file changes zero
lines in the remaining 13 phases. They are coupled only through the `Phase`
protocol, not through shared implementation.

Additionally, the phase rebuilds the full N*N distance matrix on every
`execute()` call, even though it depends only on `resolved` (immutable),
not on `state`. This is both a performance bug and a signal that the module
needs its own `__init__`-time setup.

### Solution

Extract `OverflowRedirectPhase` into its own module
(e.g. `gbp/consumers/simulator/overflow_redirect_phase.py`).
Cache the distance matrix at `__init__` time instead of rebuilding every
period.

### Benefits

- Locality: bugs in redirect logic (proportional distribution, tie-breaking,
  commodity isolation) are in one file, not buried in a 1538-line file.
- Leverage: distance matrix cached once at init. Tests on redirect logic can
  be written in isolation from other phases.

### Validation

After extraction, run `notebooks/canonical_scenario.ipynb` end to end.
`OverflowRedirectPhase` should be a no-op on the canonical historical replay
(no capacity violations by construction), so the simulation log should be
identical.

---

## Candidate 2: Deduplicate distance matrix construction

### Files

- `gbp/consumers/simulator/built_in_phases.py` lines 1033-1065
  (`OverflowRedirectPhase._build_distance_matrix` logic)
- `gbp/consumers/simulator/tasks/rebalancer.py`
  (builds its own distance matrix for OR-Tools)
- Cross-module import: `built_in_phases.py` imports the private function
  `_haversine_distance_m` from `rebalancer.py`

### Problem

Two modules independently build the same N*N distance matrix using the same
algorithm: "if `resolved.distance_matrix` exists, use it; otherwise compute
Haversine from facility coordinates." The formula, fallback logic, and
facility sort order are duplicated. A bug in the distance formula needs
fixing in two places.

The private import (`from ...tasks.rebalancer import _haversine_distance_m`)
crosses the seam in the wrong direction: a built-in phase reaches into a
task's internals.

### Solution

Extract a `distance` module (e.g. `gbp/consumers/simulator/distance.py` or
`gbp/core/distance.py`) with interface:

```python
def build_facility_distance_matrix(
    facilities: pd.DataFrame,
    distance_matrix: pd.DataFrame | None,
) -> tuple[list[str], np.ndarray]:
    """Return (sorted facility_ids, N*N distance array).

    Uses resolved distance_matrix when available, falls back to Haversine
    from facility lat/lon coordinates.
    """
```

Both `OverflowRedirectPhase` and `RebalancerTask` call this one function.

### Benefits

- Locality: distance formula, fallback logic, facility sort order in one
  place.
- Leverage: both consumers get a correct matrix through one call. Tested
  once.
- Removes the private cross-module import.

### Validation

Run `notebooks/canonical_scenario.ipynb`. Distance matrix values should be
identical (same formula, same sort order).

### Note

This candidate pairs naturally with Candidate 1. If you extract
`OverflowRedirectPhase` first (Candidate 1), do the distance extraction
(Candidate 2) in the same session.

---

## Candidate 3: Vectorize `EndOfPeriodDeficitPhase`

### Files

- `gbp/consumers/simulator/built_in_phases.py` lines 1207-1385
  (`EndOfPeriodDeficitPhase` and `_reduce_in_transit`)

### Problem

`EndOfPeriodDeficitPhase.execute()` iterates over deficit rows with
`for _, row in deficits.iterrows()` and for each calls `_reduce_in_transit`,
which also iterates row-by-row (`for idx in candidates`). This is O(D * S)
over deficits and shipments. All other simulator code is strictly vectorized
-- this module violates the "vectorization first" codebase contract.

At scale (thousands of stations, hundreds of thousands of trips per period),
this becomes a bottleneck.

### Solution

Rewrite `_reduce_in_transit` using `groupby` + `cumsum` for batch quantity
reduction. The outer loop over deficits can also be collapsed into a merge +
clip operation.

Key constraint to preserve: shipment cancellation order must remain
deterministic (DataFrame order). The vectorized version should produce
identical results.

### Benefits

- Leverage: consistent with "vectorization first" rule across the codebase.
  Scales to real data volumes.
- Locality: deficit code becomes declarative pandas, like all other phases.

### Validation

Run `notebooks/canonical_scenario.ipynb`. On canonical historical replay
(no inflator), `EndOfPeriodDeficitPhase` is a no-op (no negative inventory
by construction), so the test is that the simulation log stays identical.

To test the actual vectorized path: use `DataLoaderMock` with
`LatentDemandInflatorPhase(multiplier=2.0)` and `DeparturePhysicsPhase(
mode="permissive")` to create real deficits, then compare output before
and after the rewrite.

---

## Candidate 4: Vectorize `dispatch_lifecycle.py` hot paths

### Files

- `gbp/consumers/simulator/dispatch_lifecycle.py` lines 120-182
  (`_assign_resources`)
- `gbp/consumers/simulator/dispatch_lifecycle.py` lines 362-398
  (`_reject_insufficient_inventory`)

### Problem

`_assign_resources` iterates row-by-row over dispatches needing a resource
(`for idx in dispatches.index[needs_resource]`) and uses `.apply(lambda)`
for compatibility filtering. This is the only non-vectorized code in
`dispatch_lifecycle`.

`_reject_insufficient_inventory` uses a manual `dict` to track remaining
inventory and iterates row-by-row. The sequential semantics are intentional
(earlier dispatches consume inventory for later ones), but the implementation
can be expressed as `cumsum` + comparison.

### Solution

For `_assign_resources`: replace `.apply(lambda)` with a merge against
pre-filtered compatibility tables. The sequential assignment (each chosen
resource excluded from pool) can use `pd.merge` + deduplication in a loop
over resource slots, or `groupby("source_id").first()` when compatibility
is pre-filtered.

For `_reject_insufficient_inventory`: group dispatches by
`(source_id, commodity_category)`, compute `cumsum` of quantities, compare
against inventory limit. Rows where cumsum exceeds the limit are rejected.

### Benefits

- Leverage: dispatch lifecycle runs every period with dispatches (every 6th
  period in canonical scenario). Hot path at scale.
- Locality: removes mutable `remaining` dict, replaces with pandas idiom.

### Validation

Run `notebooks/canonical_scenario.ipynb`. The rebalancer dispatches should
produce identical `simulation_rejected_dispatches_log` and
`simulation_flow_log`.

### Note

Sequential assignment semantics in `_assign_resources` is load-bearing:
two dispatches from the same facility must not be assigned the same truck.
Any vectorized version must preserve this invariant. If it cannot be
expressed cleanly, the row-by-row loop is acceptable -- document it with
a comment explaining why.

---

## Candidate 5: Port canonical scenario to pytest

### Files

- `notebooks/canonical_scenario.ipynb` (source of truth)
- `tests/` (nearly empty)

### Problem

The only verification of the full pipeline is running the notebook manually.
Module interfaces (`Phase.execute`, `run_dispatch_lifecycle`, `build_model`)
are tested only through a single integration run. If `DockCapacityPhase`
breaks, the notebook either silently produces wrong numbers or fails with
an opaque error in `InvariantCheckPhase`.

A module is only as deep as the trust in its interface. Without tests, that
trust rests on one integration run.

### Solution

Create `tests/test_canonical_scenario.py` that:

1. Runs the full pipeline: `DataLoaderMock -> DataLoaderGraph -> build_model
   -> Environment.run()`.
2. Asserts key invariants:
   - No `InvariantViolationError` raised.
   - `simulation_rejected_dispatches_log` is empty (baseline has no
     rejections).
   - Total inventory + in_transit is conserved per commodity across all
     periods.
   - `simulation_flow_log` is non-empty (trips actually happened).
   - Final inventory is non-negative everywhere.
3. Optionally: snapshot key aggregates (total flow, total unmet demand) and
   assert they match expected values within tolerance.

Additionally, create focused unit tests for:
- `tests/test_dispatch_lifecycle.py`: test `run_dispatch_lifecycle` with
  small hand-crafted DataFrames covering each rejection reason.
- `tests/test_build_pipeline.py`: test `build_model` with `DataLoaderMock`
  output, verify resolved tables have `period_id` instead of `date`.

### Benefits

- Leverage: each test verifies an interface once, protecting all consumers.
- Locality: a break in a specific phase is caught by a specific test, not
  by a silent number shift in a notebook.

### Validation

`pytest tests/` should pass. Then modify a phase (e.g. break
`ArrivalsPhase` by commenting out resource status update) and verify the
test catches it.

---

## Execution order

Candidates 1 and 2 are coupled (both touch `OverflowRedirectPhase` and
distance matrix). Do them together.

Candidates 3 and 4 are independent vectorization tasks. Can be done in
any order.

Candidate 5 (tests) is best done last: it locks in the correct behavior
after the other refactors are complete, preventing regressions.

Suggested order: **1+2 -> 3 -> 4 -> 5**.
