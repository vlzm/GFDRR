# Plan: Historical Bike Replay Pipeline

**Source spec:** `.omc/specs/deep-interview-historical-replay.md`
**Form-factor anchor:** `phases = [...]; env = Environment(resolved, EnvironmentConfig(phases=...)); env.run()`
**Status:** APPROVED via consensus (Planner / Architect / Critic — 2 iterations).
**Mode:** SHORT (deliberate not requested; risk profile moderate — schema bump on a domain entity, two new phases, parity acceptance gate is binary).

Consensus result: **APPROVE-via-ADR**. Iteration-2 plan absorbed all six iteration-1 must-folds. Four contract-completeness items (capture log shape, ordering-test bite, argmin determinism, empty-commodity baseline) are bound as ADR §7.4–§7.7 below.

---

## §1. Principles

1. **Extend, do not duplicate.** Every existing phase that already matches Algorithm 1a stays in place. New code is a delta, not a parallel stack.
2. **One canonical source for derived simulator constants.** When the same arithmetic appears in multiple phases (period duration in hours), it lives in exactly one helper module and is imported. No third copy.
3. **Binary parity is the acceptance gate.** Reference-data run must satisfy `simulation_inventory_log == observed_inventory` and `simulation_flow_log == observed_flow` exactly after key alignment. Non-binary outcomes are not acceptance.
4. **Each lifecycle event has its own log table.** Redirect events do not share a table with normal flow because their ontology is different (intent-vs-outcome divergence, not flow).
5. **State invariants are first-class.** Conservation baselines live as fields where they must persist, never inside `intermediates` (wiped on `advance_period`).
6. **Vectorized first.** Every operation that scales with `n_facilities × n_commodities` uses pandas/NumPy. Python `for` loops over data rows are forbidden, including in skeletons — TODO comments must specify a vectorized algorithm shape.
7. **Skeletons for core algorithms, full implementation for plumbing.** The redirect heuristic (nearest-with-capacity arg-min) ships as a documented vectorized skeleton with TODO. Schema migration, build pipeline plumbing, and tests are implemented in full.

---

## §2. Decision Drivers

1. **Parity must be exact and verifiable.** Defining acceptance test is two equality checks against `resolved.observed_*`. Every design decision keeps parity directly testable.
2. **Brownfield form-factor preservation.** No new entry point, no `ReplayEnvironment`. The user composes `phases = [...]` and calls `Environment(...).run()`.
3. **Module-depth discipline (Ousterhout).** Each new abstraction has non-trivial behavior and a small interface. Three near-identical period-duration helpers fail this; redirect ontology leaking into a generic flow log fails this.

---

## §3. Viable Options Considered

### Fork A — Schema field on `ObservedFlow`

| Option | Pros | Cons |
|---|---|---|
| **A1. `duration_hours: float \| None` (chosen)** | Uniform unit (hours); matches existing edge `lead_time` semantics and `period_duration_hours`; survives period-grain rebucketing without arithmetic on dates. | Loses literal "I docked at this timestamp" trace; nullable adds a code path. |
| A2. `arrival_date: dt.date` | Mirrors existing `date` field; intuitive. | Date-arithmetic at every consumer site; sub-day awkward; no aggregation rule when trips collapse into one period bucket. |

**Decision:** A1. Aggregation rule for the build pipeline becomes `mean(duration_hours)` (continuous quantity, averaging is principled).

### Fork B — Build pipeline aggregation

| Option | Pros | Cons |
|---|---|---|
| **B1. Widen `resolve_to_periods` signature: `agg_func: str \| Mapping[str, str] = "mean"` (chosen)** | One pass, one merge_asof, one groupby; pandas `groupby(...).agg(mapping)` is native and vectorized; no merge afterwards. | Touches a public function used by the registry resolver — must verify all call sites still pass `str` (verified). |
| B2. Invoke `resolve_to_periods` twice + merge | Zero change to `resolve_to_periods`. | Two `merge_asof`s and two `groupby`s where one suffices; merge afterwards adds index-mismatch risk; flagged as architectural debt. |
| B3. Compute `duration_hours` mean in registry resolver | Keeps `resolve_to_periods` untouched. | `observed_flow` is structural (resolved by `resolve_all_time_varying`), not a registry attribute — fork doesn't apply. *Invalidated.* |

**Decision:** B1. Pandas semantics: when `agg_func` is `str`, behavior unchanged; when `Mapping[str, str]`, pandas applies per-column rule. All existing call sites continue to pass `str`.

### Fork C — Redirect event log routing

| Option | Pros | Cons |
|---|---|---|
| **C1. New `LogTableSchema("redirected_flow", "simulation_redirected_flow_log", [...])` (chosen)** | Redirect ontology has its own columns (`original_target_id`, `redirected_target_id`); empty on canonical replay → trivial parity assertion (`len(...) == 0`); module depth — flow log stays focused on flows, new table on divergences. | One more `LogTableSchema` row; one more `events[...]` short-name. |
| C2. Route via `simulation_flow_log` with `phase_name` marker | No new table. | Forces consumers to filter flow log by phase_name to recover semantics; ontology leakage. *Invalidated.* |
| C3. Side-channel parquet outside log abstraction | Decoupled. | Breaks `to_dataframes()` single-source contract. *Invalidated.* |

**Decision:** C1. Final schema:

```python
LogTableSchema(
    "redirected_flow",
    "simulation_redirected_flow_log",
    ["period_index", "period_id", "phase_name",
     "source_id", "original_target_id", "redirected_target_id",
     "commodity_category", "quantity"],
)
```

### Fork D — Conservation baseline location

| Option | Pros | Cons |
|---|---|---|
| 8a. Add `conservation_baseline: dict[str, float]` to `SimulationState` | First-class, immutable, survives `advance_period`. | Touches frozen dataclass; mixes "model state" with "audit baseline"; blast radius on existing tests. |
| **8b. Compute baseline at call site, pass into `InvariantCheckPhase(baseline=...)` (chosen)** | No `SimulationState` change; phase self-contained; mirrors existing `RebalancerTask(period_duration_hours=...)` form-factor. | Two lines of pre-amble in user code; `baseline=None` triggers auto-capture (documented fallback — see ADR §7.4). |

**Decision:** 8b. User-facing API:

```python
state = init_state(resolved)
baseline_per_commodity = (
    state.inventory.groupby("commodity_category")["quantity"].sum()
    + state.in_transit.groupby("commodity_category")["quantity"].sum()
).to_dict()

phases = [
    HistoricalLatentDemandPhase(),
    HistoricalODStructurePhase(),
    DeparturePhysicsPhase(mode="strict"),
    HistoricalTripSamplingPhase(use_durations=True),
    ArrivalsPhase(),
    OverflowRedirectPhase(),
    InvariantCheckPhase(baseline=baseline_per_commodity, fail_on_violation=True),
]
```

---

## §4. Implementation Plan

### Step 1 — Schema migration

- File: `gbp/core/schemas/observations.py:10-21` (`ObservedFlow`).
- Add `duration_hours: Annotated[float | None, Field(ge=0.0)] = None`.
- Update class docstring: "trip duration in absolute hours; `None` means same-period delivery (legacy fallback)".

**Acceptance:** new `tests/unit/core/test_schemas.py::test_observed_flow_duration_hours` passes; mypy + ruff clean.

### Step 2 — `DataLoaderMock` populates `duration_hours`

- File: `gbp/loaders/dataloader_mock.py` (`_generate_initial_telemetry_trips`).
- Compute `duration_hours = (ended_at - started_at).total_seconds() / 3600.0` per trip; emit on the trips DataFrame.

**Acceptance:** `tests/unit/loaders/test_dataloader_mock.py` updated to assert column presence and dtype `float64`.

### Step 3 — `DataLoaderGraph` populates `duration_hours`

- File: `gbp/loaders/dataloader_graph.py:642`.
- Current keep_cols: `["start_station_id", "end_station_id", "started_at"]`.
- Resolution: extend keep_cols to include `ended_at`; compute `duration_hours = (pd.to_datetime(ended_at, utc=True).dt.tz_convert(None) - pd.to_datetime(started_at, utc=True).dt.tz_convert(None)).dt.total_seconds() / 3600.0`; drop `ended_at` before rename.
- When `ended_at` missing on source: `duration_hours = None` (Pydantic accepts via Step 1).
- Aggregate with `mean` inside the existing `(source, target, commodity, date)` groupby.

**Acceptance:** `tests/test_graph_loader.py` updated to assert column presence + dtype.

### Step 4 — `time_resolution.resolve_to_periods` signature widening

- File: `gbp/build/time_resolution.py:106-155`.
- Current signature: `agg_func: str = "mean"`.
- Widen to `agg_func: str | Mapping[str, str] = "mean"`.
- Body line `:154` (`out = merged.groupby(gb_cols, as_index=False)[value_columns].agg(agg_func)`) — pandas accepts both forms natively, no branching.
- `resolve_all_time_varying` at `:215, :231, :245` keeps `str` for other tables (sum/min/mean/last) — behavior unchanged.
- Update entry for `observed_flow` (currently around `:238-:244`) to:
  ```python
  ("observed_flow",
   raw.observed_flow,
   ["source_id", "target_id", "commodity_category"],
   ["quantity", "duration_hours"],
   {"quantity": "sum", "duration_hours": "mean"})
  ```
- Registry caller at `:195-:197` keeps passing `attr.aggregation` (str) — behavior unchanged.
- Update docstring at `:111`/`:125`.

**Acceptance:**
- new `tests/unit/build/test_time_resolution.py::test_resolve_to_periods_per_column_mapping`.
- new `tests/unit/build/test_observed_flow_duration_resolution.py` — feeds raw `observed_flow` with `duration_hours`; asserts resolved table has both `quantity` (summed) and `duration_hours` (mean) at `(source_id, target_id, commodity_category, period_id)` grain.
- existing `test_time_resolution.py` stays green.

### Step 5 — IO round-trip

- No production change in `gbp/io/`. Float passes through dict ↔ frame ↔ parquet natively.

**Acceptance test (new):** `tests/unit/test_io/test_observed_flow_round_trip.py::test_observed_flow_parquet_null_float_dtype` — round-trips an `observed_flow` with **all-null** `duration_hours` through parquet; asserts `dtype == float64` after load.

### Step 6 — Shared period-duration helper + `HistoricalTripSamplingPhase` extension

- New module: `gbp/consumers/simulator/_period_helpers.py` exposing `period_duration_hours(resolved: ResolvedModelData) -> float`.
- Implementation lifts from `gbp/consumers/simulator/tasks/rebalancer.py:225-245`. Falls back to `1.0` when `periods is None or len(periods) < 2` or date subtraction raises.
- Module docstring documents the boundary with `gbp/build/_helpers.py:13` (`get_duration_hours(period_type)`):
  - Build helper is **grain-based** (`"day"` → 24.0, `"week"` → 168.0, `"month"` → 720.0).
  - Simulator helper is **data-based** (subtracts adjacent `start_date` values from actual `resolved.periods`).
  - They serve different layers and **must not be unified** — unifying would couple simulator runtime to the `PeriodType` enum (build-time concept).
- `RebalancerTask._resolve_period_duration` becomes a thin wrapper preserving the `self.period_duration_hours` override:
  ```python
  def _resolve_period_duration(self, resolved):
      if self.period_duration_hours is not None:
          return float(self.period_duration_hours)
      return period_duration_hours(resolved)
  ```
- `HistoricalTripSamplingPhase` (`built_in_phases.py:517-590`) gains:
  - Constructor flag `use_durations: bool = True`.
  - In `execute()`, after per-period flows loaded:
    ```python
    period_dur_h = period_duration_hours(resolved)
    duration_h = flows["duration_hours"]
    arrival_period = state.period_index + np.ceil(
        np.where(
            duration_h.notna(),
            duration_h.to_numpy(dtype=float, na_value=0.0) / period_dur_h,
            0.0,
        )
    ).astype(int)
    ```
  - When `use_durations=False`: legacy zero-τ path preserved.
- **Null-handling rule:** `duration_hours == NaN` → same-period delivery (`τ = 0`).
- **Empty-flow rule:** when per-period observed-flow slice is `None` or empty, return `PhaseResult(events={}, ...)` — current behavior preserved.

**Acceptance:**
- `tests/unit/consumers/simulator/test_period_helpers.py::test_period_duration_hours_two_periods`
- `...::test_period_duration_hours_fallback`
- `tests/unit/consumers/simulator/test_historical_trip_sampling_with_durations.py::test_arrival_period_math`
- `...::test_null_duration_same_period`
- `...::test_use_durations_false_legacy_path`
- `...::test_historical_phase_empty_flow_noop`
- existing rebalancer tests stay green (wrapper preserves behavior).

### Step 7 — `OverflowRedirectPhase` (skeleton + dedicated log table)

- New class `OverflowRedirectPhase` in `gbp/consumers/simulator/built_in_phases.py` after `ArrivalsPhase`.
- Add `LogTableSchema("redirected_flow", "simulation_redirected_flow_log", [...])` to `gbp/consumers/simulator/log.py:154` per Fork C1.

**Class docstring (mandatory):**
```
ORDERING CONTRACT: Must run immediately after ArrivalsPhase. Inserting other
phases between ArrivalsPhase and this one breaks the redirect accounting.

On the canonical historical replay (target_id is post-redirect by spec
Constraint 3), this phase is a no-op — no overflow exists by construction.
```

**Implementation boundary:** redirect physics is **fully implemented**:
1. Build per-`(facility_id, commodity_category)` capacity from `resolved.attributes["operation_capacity"]` filtered by `operation_type == "storage"`. Facilities **without** a storage row → **unbounded** (no overflow recorded).
2. Compute overflow vector: `overflow = max(0, state.inventory.quantity - capacity)`, vectorized.
3. Per-iteration nearest-with-capacity search → **skeleton + TODO** (only this part).
4. Apply redirect: subtract from source, add to target, update target's remaining capacity (full implementation).
5. Emit one `redirected_flow` row per redirect.

**TODO body (mandatory text):**
```python
# TODO: vectorized nearest-with-capacity selection.
#   Precondition: D = D.reindex(columns=sorted(D.columns)) before any argmin.
#   1. Build distance matrix D of shape (n_overflow, n_facilities).
#   2. Build availability mask M[i, j] = (free_capacity[j, commodity_i] > 0)
#      & (j != source_i).
#   3. masked_D = np.where(M, D, np.inf)
#   4. winner = masked_D.argmin(axis=1)
#   5. Tie-break is lexicographic on facility_id thanks to the sort
#      precondition above (np.argmin returns first minimum on ties).
#   No Python `for` loops over overflow rows.
```

Distance source: `resolved.distance_matrix` if populated; else Haversine over `facilities[lat, lon]` mirroring `tasks/rebalancer.py:495-534`.

**Acceptance:**
- `tests/unit/consumers/simulator/test_overflow_redirect_phase.py::test_no_overflow_noop`
- `...::test_redirect_per_commodity`
- `...::test_facility_without_storage_unbounded`
- `...::test_overflow_redirect_pipeline_ordering_contract` (see ADR §7.5 for assertion)
- `...::test_redirected_flow_log_schema`

### Step 8 — `InvariantCheckPhase`

- New class in `gbp/consumers/simulator/built_in_phases.py` at end of phase list.

**Class docstring:**
```
ORDERING CONTRACT: Must run last in the period.

The invariant is per commodity_category — bikes do not turn into docks.
A baseline dict[str, float] is captured at construction time (or on first
execute when omitted; see ADR §7.4) and asserted against on every subsequent
execute.

Per ADR §7.7: per-commodity baseline tracks ONLY commodities present in the
initial inventory snapshot. Commodities appearing in observed_flow or demand
but absent from inventory_initial are EXCLUDED from invariant tracking.
```

**Constructor:**
```python
def __init__(
    self,
    *,
    baseline: Mapping[str, float] | None = None,
    fail_on_violation: bool = True,
    tolerance: float = 1e-9,
) -> None: ...
```

**Behavior:**
- `baseline=None` → on first `execute()`, capture per-commodity baseline and write to `state.intermediates["invariant_baseline"]` (no log row — see ADR §7.4).
- On every subsequent execute: per-commodity diff vs baseline; on violation either raise `InvariantViolationError(period_id, commodity, baseline, current, delta)` (when `fail_on_violation=True`) or emit row to `simulation_invariant_violation_log`.
- Default `fail_on_violation=True` — canonical replay treats parity as binary acceptance gate.

**New log table** in `gbp/consumers/simulator/log.py:154`:
```python
LogTableSchema(
    "invariant_violation",
    "simulation_invariant_violation_log",
    ["period_index", "period_id", "commodity_category",
     "baseline", "current", "delta"],
)
```

**Acceptance:**
- `tests/unit/consumers/simulator/test_invariant_check_phase.py::test_invariant_pass_canonical`
- `...::test_invariant_per_commodity`
- `...::test_invariant_fail_on_violation_false`
- `...::test_invariant_first_execute_capture`
- `...::test_invariant_check_skips_inventory_absent_commodity` (see ADR §7.7)

### Step 9 — Test fixtures

- Reuse `resolved_model_with_obs` in `tests/unit/consumers/simulator/conftest.py:39-60`.
- Extend the `minimal_raw_model` helper to populate `duration_hours` on observed_flow rows (deterministic small floats, e.g. `0.1`).

### Step 10 — Verification notebook `notebooks/verify/12_historical_replay.ipynb`

Single notebook, `# %% N. Section` markers, leading markdown map (CLAUDE.md notebook style). No `print`/`display`, full descriptive variable names, domain terms.

- **Cell A — baseline mock parity:**
  ```python
  # %% A. Baseline mock parity
  mock_loader = DataLoaderMock(...)
  raw = mock_loader.build_raw()
  resolved = build_model(raw)

  state = init_state(resolved)
  baseline_per_commodity = (
      state.inventory.groupby("commodity_category")["quantity"].sum()
      + state.in_transit.groupby("commodity_category")["quantity"].sum()
  ).to_dict()

  phases = [
      HistoricalLatentDemandPhase(),
      HistoricalODStructurePhase(),
      DeparturePhysicsPhase(mode="strict"),
      HistoricalTripSamplingPhase(use_durations=True),
      ArrivalsPhase(),
      OverflowRedirectPhase(),
      InvariantCheckPhase(baseline=baseline_per_commodity, fail_on_violation=True),
  ]
  env = Environment(resolved, EnvironmentConfig(phases=phases, seed=42, scenario_id="historical_replay"))
  env.run()
  logs = env.log.to_dataframes()

  inventory_parity_assert = pd.testing.assert_frame_equal(
      logs["simulation_inventory_log"].sort_values([...]).reset_index(drop=True),
      resolved.observed_inventory.sort_values([...]).reset_index(drop=True),
  )
  flow_parity_assert = pd.testing.assert_frame_equal(...)
  redirect_empty_assert = logs["simulation_redirected_flow_log"].empty
  ```
- **Cell B — invariant constancy** (per-commodity total per period).
- **Cell C — overflow safety net** with intentionally undersized capacity. Asserts `simulation_redirected_flow_log` non-empty.
- **Cell D — same-period vs cross-period τ_k** cases.

### Step 11 — Pipeline-assembly compatibility smoke test

- File: `tests/integration/test_canonical_replay_pipeline.py::test_canonical_replay_runs_clean`.
- Assembles canonical pipeline, runs `env.run()`, asserts no exception.
- Then assembles treatment-style pipeline (insert `LatentDemandInflatorPhase` between B1 and B3, `DispatchPhase(RebalancerTask, ...)` after `ArrivalsPhase`); asserts `env.run()` completes.

---

## §5. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Step 4: pandas may treat `groupby(...).agg({"a": "sum", "b": "mean"})` differently across versions for numeric NaNs in `b`. | Test pins behavior; `pyproject.toml` pins pandas major. NaN handling matches default (`skipna=True`); documented. |
| Step 6: lifting helper changes import paths; downstream RebalancerTask consumers may break. | Wrapper preserves the method on `RebalancerTask` with identical signature; existing rebalancer tests stay green. |
| Step 7: facility without `operation_capacity[storage]` row could be silently dropped, missing real overflow. | Explicit "no row → unbounded" rule in docstring; tested via `test_facility_without_storage_unbounded`. |
| Step 7: TODO ships, executor writes Python `for` loop. | TODO body specifies vectorized algorithm shape (mask + `np.where(...inf)` + `argmin`). CLAUDE.md vectorization rule cited inside TODO. |
| Step 7: ordering-contract violation breaks accounting silently. | `test_overflow_redirect_pipeline_ordering_contract` per ADR §7.5; class docstring states contract. |
| Step 7: `np.argmin` tie-break platform-dependent. | ADR §7.6 mandates `D = D.reindex(columns=sorted(D.columns))` before any argmin. |
| Step 8: aggregate-sum invariant masks per-commodity bugs. | Per-commodity baseline + `test_invariant_per_commodity`. |
| Step 8: `fail_on_violation=True` aborts deep into period 50 with no diagnostic context. | Exception message includes `(period_index, period_id, commodity_category, baseline, current, delta)`; treatment scenarios opt into `fail_on_violation=False`. |
| Step 8: spurious violation when commodity present in observed_flow but absent from inventory_initial. | ADR §7.7 — exclude absent commodities from baseline; `test_invariant_check_skips_inventory_absent_commodity`. |
| Step 8: capture-mode log row schema undefined. | ADR §7.4 — write to `state.intermediates["invariant_baseline"]`; no log row on capture. |
| Step 5/Step 1: parquet stores all-null `duration_hours` as `object` dtype. | `test_observed_flow_parquet_null_float_dtype` covers; failure surfaces as IO follow-up. |
| Step 3: `df_trips["ended_at"]` may be tz-aware while `started_at` is tz-naive in some Citi Bike vintages. | `pd.to_datetime(..., utc=True).dt.tz_convert(None)` before subtraction; documented in loader docstring. |

---

## §6. Acceptance Criteria (pass-through from spec)

Each spec acceptance bullet is satisfied by the step number indicated in §4. Specific test/notebook artifacts are named in each step's "Acceptance" block.

**Spec section coverage:**
- *Schema and loaders* → Steps 1, 2, 3.
- *Phases* → Steps 6, 7, 8.
- *Pipeline assembly* → Step 11.
- *Verification notebook* → Step 10.
- *Tests* → Step 9 + per-step "Acceptance" blocks (17 named tests total).

**Canonical pipeline (verbatim from spec):**
```python
phases = [
    HistoricalLatentDemandPhase(),                       # B1
    HistoricalODStructurePhase(),                        # B2
    DeparturePhysicsPhase(mode="strict"),                # B3
    HistoricalTripSamplingPhase(use_durations=True),     # B4
    ArrivalsPhase(),                                     # Phase A (transfer)
    OverflowRedirectPhase(),                             # Phase A (redirect) — NEW
    InvariantCheckPhase(baseline=..., fail_on_violation=True),  # Phase C — NEW
]
```

---

## §7. ADR

**Decision:** Implement Algorithm 1a (Historical Replay) by extending `ObservedFlow` with `duration_hours: float | None`, widening the build pipeline's `resolve_to_periods` aggregator signature, lifting the period-duration helper into a shared simulator module, adding a dedicated `simulation_redirected_flow_log` table for redirect events, and constructing `InvariantCheckPhase` with a per-commodity baseline passed at the call site.

**Drivers:**
1. Parity must be exactly testable as `simulation_inventory_log == observed_inventory` and `simulation_flow_log == observed_flow` after key alignment.
2. Brownfield form-factor preservation — no `ReplayEnvironment`, only phase additions.
3. Module-depth discipline — no third copy of period-duration helper, no ontology leakage into the flow log.

**Alternatives considered:**
- Schema fork: `arrival_date: dt.date` (A2). Rejected: forces date arithmetic; awkward sub-day; no aggregation rule on collapse.
- Build pipeline fork: invoke `resolve_to_periods` twice and merge (B2). Rejected: two `merge_asof`s + two groupbys + post-merge index risk.
- Build pipeline fork: compute mean inside registry resolver (B3). Invalidated: `observed_flow` is structural, not registry-attribute.
- Redirect log fork: route via `simulation_flow_log` with phase_name marker (C2). Rejected: ontology leakage; Ousterhout-shallow.
- Redirect log fork: side-channel parquet (C3). Invalidated: breaks `to_dataframes()` contract.
- Baseline fork: first-class field on `SimulationState` (8a). Rejected: blast radius on frozen dataclass; mixes audit data with model state.

**Why chosen:**
- `duration_hours` matches existing time-unit convention (hours everywhere in `lead_time` and `period_duration_hours`).
- Signature widening is one line in `resolve_to_periods`, zero lines at six of seven call sites; native pandas semantics.
- Lifted helper resolves third-copy violation without unifying with the build-time helper (different layer, different concept).
- Dedicated redirect log table makes parity assertion trivial (`empty == True`) and treatment detection trivial (`empty == False`).
- Call-site baseline keeps `SimulationState` untouched and lets users construct multiple invariant phases with different baselines.

**Consequences:**
- `resolve_to_periods` public function gains a new accepted type for `agg_func`.
- `gbp/consumers/simulator/_period_helpers.py` is new module; future simulator phases can import for derived constants.
- `simulation_redirected_flow_log` is a new public log table; users see one more key in `to_dataframes()`. Empty on canonical replay.
- `simulation_invariant_violation_log` is a new public log table; empty unless `fail_on_violation=False` AND a violation occurred.
- `OverflowRedirectPhase` and `InvariantCheckPhase` have hard ordering contracts enforced by class docstring + regression tests.

**Follow-ups:**
- Unified trip-status column across flow/lost-demand/redirected logs — out of scope per spec Non-Goals.
- IO schema-aware parquet load (if null-float-dtype test ever fails on a future pandas/pyarrow combination).
- Real GBFS feed integration — out of scope per spec Non-Goals.
- Integration of `OverflowRedirectPhase` with `DispatchPhase` ordering audit for treatment scenarios.

---

### §7.4 Invariant baseline capture log shape (resolves Critic §2.4)

When `InvariantCheckPhase` is constructed with `baseline=None`, the first `execute()` call computes the per-commodity baseline as

```python
baseline = (
    state.inventory.groupby("commodity_category")["quantity"].sum()
    + state.in_transit.groupby("commodity_category")["quantity"].sum()
).to_dict()
```

and writes it to `state.intermediates["invariant_baseline"]` as a `dict[str, float]`. **NO log row is written on capture.** Subsequent `execute()` calls read from `state.intermediates["invariant_baseline"]` if present, otherwise from the constructor-provided baseline. Violations are written to `simulation_invariant_violation_log` with columns `[period_id, commodity_category, baseline, current, delta]`.

Note: `intermediates` is wiped on `advance_period()`. On the first `execute()` of period N>0, the captured baseline must therefore be re-loaded from `self._captured_baseline` (memoised on the phase instance) — `intermediates` is for transient hand-off between phases within a single period only. The phase memoises the captured baseline on `self` to survive across periods.

### §7.5 OverflowRedirect ordering-contract test bite (resolves Critic §2.5)

`test_overflow_redirect_pipeline_ordering_contract` MUST construct a scenario where `OverflowRedirectPhase` placed BEFORE `ArrivalsPhase` produces an observable downstream violation: the resulting `simulation_inventory_log` MUST contain at least one row where post-resolution `quantity > capacity` for some `(facility_id, commodity_category)`. The canonical-order pipeline on the same fixture MUST produce zero such rows. Assertion form:

```python
assert (wrong_order_inv["quantity"] > wrong_order_inv["capacity"]).any()
assert not (canonical_inv["quantity"] > canonical_inv["capacity"]).any()
```

### §7.6 OverflowRedirect argmin determinism precondition (resolves Critic §2.6)

Before any `np.argmin` call in `OverflowRedirectPhase`, the distance frame `D` MUST be reindexed to sorted facility_id columns:

```python
D = D.reindex(columns=sorted(D.columns))
```

This precondition applies BOTH to the `resolved.distance_matrix` path AND to the Haversine fallback (which by default inherits `facilities.index` insertion order — see `gbp/consumers/simulator/tasks/rebalancer.py:507-533` for the existing pattern that DOES NOT sort, and which this phase MUST NOT replicate without the sort). Tie-breaking is therefore lexicographic on `facility_id`.

### §7.7 Empty-commodity baseline exclusion (resolves Critic §2.7)

The `InvariantCheckPhase` per-commodity baseline tracks ONLY commodities present in the initial inventory snapshot. Commodities appearing in `observed_flow` or `demand` but absent from `inventory_initial` are EXCLUDED from invariant tracking (documented one-line in the `InvariantCheckPhase` docstring). Test `test_invariant_check_skips_inventory_absent_commodity` MUST construct such a commodity and assert no violation is raised.

---

**Plan ready for execution.** Hand-off to `oh-my-claudecode:autopilot` with this file as the consensus plan; autopilot skips Phase 0 (Expansion) and Phase 1 (Planning) and starts at Phase 2 (Execution).
