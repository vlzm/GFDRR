# Plan: Rebalancer as PDP Task inside Environment

- Plan ID: rebalancer-pdp-task
- Mode: RALPLAN-DR (short)
- Source spec: `.omc/specs/deep-interview-rebalancer.md` (status PASSED, ambiguity 15%)
- Plan generated: 2026-05-01
- Owner: planner agent (handoff to executor on approval)

## 1. Requirements Summary

Implement `RebalancerTask` (a `Task` Protocol implementation per
`gbp/consumers/simulator/task.py:32`) that runs an OR-Tools Pickup-and-Delivery
solver on the current `state.inventory` snapshot and emits multi-stop truck
routes as `DISPATCH_COLUMNS`-shaped DataFrames. The task is wrapped in
`DispatchPhase(task, schedule=Schedule.every_n(12))` and inserted after
`ArrivalsPhase`. Multi-stop routes are mapped one row per pickup-delivery pair
with shared `arrival_period = period_index + ceil(total_route_hours /
period_duration_hours)`, so `ArrivalsPhase` releases the truck once when the
whole route lands together (`gbp/consumers/simulator/built_in_phases.py:632-646`).

A new `LatentDemandInflatorPhase` is added to `built_in_phases.py` between
`HistoricalLatentDemandPhase` and `DeparturePhysicsPhase`. It mutates
`state.intermediates["latent_demand"]` in place, so a multiplier > 1.0 forces
`DeparturePhysicsPhase(mode="strict")` to record `lost_demand > 0`
(`built_in_phases.py:483-485`) — without this, `permissive` mode produces
`lost = 0` identically (`built_in_phases.py:480`) and the experiment cannot
detect rebalancing benefit. The deliverable is finished by
`notebooks/verify/10_rebalancer_experiment.ipynb`, which runs baseline vs
treatment Environments, produces three summary tables and two matplotlib
charts, and demonstrates `total_lost_demand_treatment <
total_lost_demand_baseline`.

## 2. RALPLAN-DR Summary

### Principles

- **P1.** Reuse existing dispatch lifecycle and phase contracts; do not extend
  `DISPATCH_COLUMNS`, `Task` Protocol, `DispatchPhase`, or `ArrivalsPhase` —
  **modulo a minimal validator fix in `dispatch_lifecycle._reject_unavailable_resource`
  to accept route dispatches that legitimately reuse a single `resource_id`
  across multiple `(resource_id, source_id)` pairs (R8 resolution; see §5).**
  This fix preserves the public Task interface and keeps the
  `DISPATCH_COLUMNS` contract untouched. Spec: "No extension of the Task
  interface, of `DispatchPhase`, or of `ArrivalsPhase`"
  (`.omc/specs/deep-interview-rebalancer.md:42`).
- **P2.** Salvage the OR-Tools PDP model (`gbp/rebalancer/routing/vrp.py`,
  `gbp/rebalancer/routing/postprocessing.py`,
  `gbp/rebalancer/demand.py:25-52`); delete the broken loader/pipeline in
  this PR (`gbp/rebalancer/dataloader.py`, `gbp/rebalancer/pipeline.py` —
  depend on removed `df_inventory_ts` per spec line 53-54), salvaging
  necessary parts into `gbp/consumers/simulator/tasks/rebalancer.py` first.
- **P3.** Vectorized pandas, strict typing, Google docstrings, English in code
  (CLAUDE.md). Notebook markdown in plain Russian per project convention.
- **P4.** Notebook owns analytics. No new public modules under `gbp/` for
  summary tables, charts, or comparison logic. Spec line 78-79.
- **P5.** Strict-mode DeparturePhysics in both runs. The whole experiment
  observability hinges on `lost_demand > 0`; permissive mode zeroes it
  (`built_in_phases.py:480`).

### Decision Drivers (top 3)

- **D1. Ship a working experiment fast.** User said "Мне это нужно уже сейчас"
  (round 3). The simplest path that satisfies the spec wins.
- **D2. Preserve existing phase contracts.** Spec forbids touching
  `DISPATCH_COLUMNS`, `Task`, `DispatchPhase`, `ArrivalsPhase`. Multi-stop
  routes must fit into the existing 7-column schema.
- **D3. Make the broken-prototype salvage explicit.** The user accepted that
  `gbp/rebalancer/` is partially broken (round 3). The plan must mark
  precisely which files are salvaged, which are deprecated, and which are
  rewritten.

### Viable Options for non-trivial decisions

#### O1. Truck infrastructure: parameterize mock vs extend production loader (Risk #1, #2, #3 from spec line 222-235)

> **Codebase fact (Architect-confirmed):** `gbp/loaders/dataloader_mock.py:30-35`
> already includes truck-related DataFrames (`df_depots`, `df_resources`,
> `df_resource_capacities`, `df_truck_rates`) in its `_GROUPS` constant and
> emits them unconditionally. The decision below is therefore between
> *parameterizing what is already emitted* vs *teaching the production
> loader to synthesize trucks*.

**Option A — Parameterize the existing `DataLoaderMock` truck fixture** *(chosen)*
- The truck DataFrames are already part of the mock loader's emission set.
  This step adds tunable knobs (`n_trucks: int`, `truck_capacity_bikes: int`)
  so tests and the notebook can dial truck count and capacity without
  touching loader-protocol contracts. No `include_trucks` flag is needed —
  trucks default to ON in the mock. Setting `n_trucks=0` is the way to
  exercise "no trucks" code paths.
- **Pros.** Smallest blast radius. Existing mock stays the source of trucks
  for tests. Driver D1 satisfied — no infrastructure refactor in the
  critical path. Knob-only change keeps the public protocol intact.
- **Cons.** Production `DataLoaderGraph` still does not synthesize trucks
  for real Citi-Bike data; that promotion is a follow-up.

**Option B — Extend `gbp/loaders/dataloader_graph.py`**
- Add `df_depots`, `df_resources`, `df_resource_capacities`,
  `df_truck_rates` synthesis in `DataLoaderGraph` when the source omits
  them.
- **Pros.** One canonical truck-seeding path. Real Citi-Bike data wakes up
  with trucks automatically.
- **Cons.** Higher blast radius — touches a production loader that is
  validated by 9 verification notebooks. Driver D1 loses; risk #1 grows. Also
  makes "truck count and capacity" a hidden default rather than an explicit
  experimenter knob.

**Decision:** Option A — verify that `DataLoaderMock` already emits truck
DataFrames (per `dataloader_mock.py:30-35`) and add `n_trucks` /
`truck_capacity_bikes` parameters. Notebook 10 instantiates
`DataLoaderMock(n_trucks=3, truck_capacity_bikes=20)` and runs
`build_model(loader.load())` — no other infrastructure work. We promote
the helper to the production loader only after the experiment proves the
configuration is right.

#### O2. Inventory capacity source for utilization computation

**Option A — `resolved.attributes["operation_capacity"]` filtered by
`operation_type == "storage"`** *(chosen)*
- Same source `DockCapacityPhase` uses (`built_in_phases.py:723-729`).
- **Pros.** Already part of build pipeline. One source of truth. Risk #3 (no
  separate capacity feed to maintain) avoided.
- **Cons.** Requires the attribute to exist; we need a defensive fallback (use
  `quantity.max() * 2` per facility) for fixtures that omit capacity.

**Option B — Read inventory_capacity from a separate `resolved.attributes["station_capacity"]` registered by the loader.**
- **Pros.** Decoupled from operation grain.
- **Cons.** Duplicates data already in `operation_capacity`. Invalidated.

**Decision:** Option A.

#### O3. Empty-imbalance early return

**Option A — Return empty `DataFrame` with correct columns when no station
crosses min/max thresholds** *(chosen)*
- `DispatchPhase` already handles `dispatches.empty` cleanly
  (`dispatch_lifecycle.py:85-90`).
- **Pros.** Zero overhead, matches existing contract.
- **Cons.** None.

**Option B — Skip the `should_run` predicate dynamically based on imbalance.**
- Invalidated: `Schedule` predicate is by design period-based, not
  state-based; the spec mandates `Schedule.every_n(12)` (round 4 decision C).

**Decision:** Option A.

## 3. Acceptance Criteria

> Citation format: `[file:line]` for code refs, `[test path::test_name]` for
> tests this plan introduces.

### A. RebalancerTask schema and contract

- [test] `tests/unit/consumers/simulator/test_rebalancer_task.py::test_returns_dispatch_columns_schema`
  asserts that `RebalancerTask(...).run(state, resolved, period)` returns a
  `pd.DataFrame` whose `list(df.columns) == DISPATCH_COLUMNS` (per
  `gbp/consumers/simulator/task.py:20`).
- [test] `test_rebalancer_task.py::test_empty_when_no_imbalance` asserts the
  returned DataFrame is empty (`len == 0`) when every station's
  `quantity / inventory_capacity` is within `[min_threshold, max_threshold]`.
- [test] `test_rebalancer_task.py::test_empty_when_no_truck_available`
  asserts empty DataFrame when `state.resources` has no row with
  `resource_category == "rebalancing_truck"` and
  `status == ResourceStatus.AVAILABLE.value`. **This is the FIRST guard at
  the top of `RebalancerTask.run()`, before any solver work** (see Step 3).
- [test] `test_rebalancer_task.py::test_inventory_capacity_fallback_when_no_operation_capacity`
  asserts that when `resolved.attributes` does not contain
  `"operation_capacity"` (or the storage slice is empty), the task falls
  back to `inventory_capacity = state.inventory.groupby("facility_id")["quantity"].max() * 2`
  per facility, and still produces a valid `DISPATCH_COLUMNS` DataFrame
  (or empty if no imbalance survives the fallback).
- [test] `test_rebalancer_task.py::test_quantities_respect_truck_capacity`
  asserts `df.groupby("resource_id")["quantity"].sum() <= base_capacity` for
  every truck (capacity from `resolved.resource_categories.base_capacity`,
  per `dispatch_lifecycle.py:296-298`).
- [test] `test_rebalancer_task.py::test_arrival_period_shared_within_route`
  asserts that for any single `resource_id` group, every row has the same
  `arrival_period`. This enforces the multi-stop mapping decision
  (spec round 5 decision A).
- [test] `test_rebalancer_task.py::test_arrival_period_formula` asserts
  `arrival_period == period.period_index + ceil(total_route_hours /
  period_duration_hours)` for a fixture where `total_route_hours` is known.
- [test] `test_rebalancer_task.py::test_resource_id_pre_assigned` asserts
  every output row has a non-null `resource_id` matching a truck in
  `state.resources` (avoids `_assign_resources` collision in
  `dispatch_lifecycle.py:111-173`).
- [test] `test_rebalancer_task.py::test_modal_type_road` asserts every row's
  `modal_type == ModalType.ROAD.value` ("road" per
  `gbp/core/enums.py:24`; spec uses "truck" colloquially but the materialized
  edges in `dataloader_graph.py:379` are `ModalType.ROAD.value`). This is
  an interpretation flagged in §8.
- [test] `tests/unit/consumers/simulator/test_dispatch_lifecycle.py::test_route_dispatch_passes_validator`
  (regression test for R8 fix) constructs a `dispatches` DataFrame with
  two rows sharing the same `resource_id` whose `source_id` chain starts
  at the truck's `current_facility_id` (row 1: depot→S1; row 2: S1→S2).
  Asserts neither row is rejected by `_reject_unavailable_resource`.
  The truck is treated as engaged for the whole route.
- [test] `test_dispatch_lifecycle.py::test_single_row_unavailable_resource_still_rejects`
  (regression test for R8 fix) constructs a single-row dispatch with a
  `(resource_id, source_id)` pair NOT matching any available truck;
  asserts the row is rejected with `RejectReason.NO_AVAILABLE_RESOURCE`.
  Confirms the fix did not weaken the validator for the single-row case.
- [test] `test_dispatch_lifecycle.py::test_route_dispatch_rejected_when_no_row_matches_current_facility`
  (false-positive guard for the chain validator) constructs a multi-row
  dispatch sharing the same `resource_id` whose `source_id` chain does
  NOT start at the truck's `current_facility_id` (truck is at depot, but
  row 1 has `source_id="S1"`, row 2 has `source_id="S2"`). Asserts every
  row in the group is rejected with reason `NO_AVAILABLE_RESOURCE`. This
  guards against the chain validator rubber-stamping arbitrary
  co-grouped batches as routes.
- [test] `test_dispatch_lifecycle.py::test_route_dispatch_rejected_when_chain_breaks`
  (chain-break guard) constructs a multi-row dispatch where row 1 starts
  at the truck's `current_facility_id` (chain root OK) but a later row's
  `source_id` is not present in `visited` (e.g. row 1: depot→S1;
  row 2: S2→S3, where `S2` was never visited). Asserts every row in the
  group is rejected with reason `NO_AVAILABLE_RESOURCE` (fall-back
  per-row check applies after chain failure).
- [test] `test_dispatch_lifecycle.py::test_null_resource_id_falls_back_to_per_row_check`
  (null-resource-id semantics preserved) constructs a multi-row dispatch
  with `resource_id == NaN` for all rows. Asserts the rows are not
  touched by `_reject_unavailable_resource` (the function's existing
  `has_resource` mask filters them out); auto-assignment in
  `_assign_resources` (`dispatch_lifecycle.py:111-173`) handles them
  upstream. Confirms the chain logic is gated behind a non-null
  `resource_id`.

### B. LatentDemandInflatorPhase

- [test] `tests/unit/consumers/simulator/test_latent_demand_inflator.py::test_multiplier_one_is_identity`
  asserts that with `multiplier=1.0`, the resulting
  `state.intermediates["latent_demand"]` is bitwise equal (to 1e-9 tolerance)
  to the input.
- [test] `test_latent_demand_inflator.py::test_scalar_multiplier_scales_departures`
  asserts `out.latent_departures == in.latent_departures * 2.5` row-wise
  for `multiplier=2.5`.
- [test] `test_latent_demand_inflator.py::test_per_facility_dict_targets_correct_rows`
  with `multiplier={"S1": 2.0, "S2": 0.5}`, asserts only rows with
  `facility_id in {"S1", "S2"}` are scaled and remaining rows unchanged.
- [test] `test_latent_demand_inflator.py::test_arrivals_inflated_when_flag_set`
  with `inflate_arrivals=True` asserts `latent_arrivals` is also scaled.
  Default `inflate_arrivals=False` leaves arrivals untouched.
- [test] `test_latent_demand_inflator.py::test_no_op_when_intermediate_missing`
  asserts the phase returns `PhaseResult.empty(state)` when
  `state.intermediates.get("latent_demand")` is `None`.
- [test] `test_latent_demand_inflator.py::test_emits_latent_demand_event`
  asserts `result.events["latent_demand"]` exists and equals the post-mutation
  table — so `simulation_latent_demand_log` records the inflated values.

### C. Phase ordering and Environment integration

- [test] `tests/integration/test_rebalancer_phase_chain.py::test_smoke_single_pair_dispatch`
  **(runs FIRST, isolates wiring failures from multi-stop failures)** builds
  the simplest treatment fixture: 1 truck, 2 stations (one source, one
  destination), 1 depot, exactly one source-destination pair. Asserts the
  task emits exactly one row, `DispatchPhase` accepts it (no validator
  rejection), and `ArrivalsPhase` releases the truck once. If this passes
  but the multi-stop test fails, the failure is in route-mapping logic, not
  basic Task→DispatchPhase wiring.
- [test] `test_rebalancer_phase_chain.py::test_baseline_runs_strict_no_rebalancer`
  builds a baseline phase chain (HistoricalLatentDemand →
  LatentDemandInflator(multiplier=2.0) → HistoricalODStructure →
  DeparturePhysics(strict) → HistoricalTripSampling → Arrivals), runs
  `Environment.run()` on a 3-station fixture for 24 periods, and asserts
  `simulation_lost_demand_log` is non-empty.
- [test] `test_rebalancer_phase_chain.py::test_treatment_runs_with_rebalancer`
  builds the treatment chain (baseline + DispatchPhase(RebalancerTask,
  Schedule.every_n(12))), runs the same 24 periods, asserts no exception, and
  asserts `simulation_flow_log` contains at least one row with
  `resource_id` matching a truck. **This is the multi-stop integration
  test**; it relies on the R8 validator fix (Step 3) to accept routes with
  more than one pickup.
- [test] `test_rebalancer_phase_chain.py::test_treatment_lost_demand_lt_baseline`
  asserts `treatment.simulation_lost_demand_log["lost"].sum() <
  baseline.simulation_lost_demand_log["lost"].sum()`. This is the key
  quantitative success criterion (spec line 126-128).

### D. Notebook 10 — experiment artifacts

- The notebook file exists at
  `notebooks/verify/10_rebalancer_experiment.ipynb`.
- `jupyter execute notebooks/verify/10_rebalancer_experiment.ipynb` runs to
  completion with non-zero exit only on assertion failure.
- The notebook exposes three named DataFrames as cell outputs (assignments,
  per CLAUDE.md notebook style):
  - `trips_per_date_per_station` with columns `[date, period_id, source_id,
    target_id, trips, commodity_category]`.
  - `summary_per_date` with columns `[date, total_trips, total_distance,
    total_lost_demand, revenue]`. `revenue == total_distance` v1; distance
    joined from `resolved.distance_matrix`.
  - `comparison` with index = metric names, columns = `[baseline,
    with_rebalancer, delta, delta_pct]`.
- The notebook produces two matplotlib figures saved to memory (no `display()`
  per CLAUDE.md notebook style):
  - `bar_trips_per_date` (side-by-side bars).
  - `heatmap_delta_per_station` (lost-demand reduction by station × date).
- Notebook last cell asserts
  `comparison.loc["total_lost_demand", "delta"] < 0` and
  `comparison.loc["total_fulfilled_demand", "delta"] > 0`.

### E. Truck infrastructure (Option O1.A — parameterized mock)

> Codebase fact: `dataloader_mock.py:30-35` already emits the four truck
> DataFrames (`df_depots`, `df_resources`, `df_resource_capacities`,
> `df_truck_rates`) unconditionally — Step 1 *parameterizes* the existing
> emission; it does not gate it.

- `DataLoaderMock` accepts `n_trucks: int` (default e.g. 2 for typical
  test usage) and `truck_capacity_bikes: int` (default 30, matching
  `RESOURCE_CATEGORY = "rebalancing_truck"` semantics in
  `dataloader_graph.py:47`). The four truck DataFrames are still emitted by
  default; these knobs only tune *how many* trucks and *what capacity*.
- (i) When `n_trucks > 0`, `resolved.resource_fleet` has exactly `n_trucks`
  rows with `resource_category == "rebalancing_truck"` (sum of `count`
  across rows == `n_trucks`).
- (ii) When `n_trucks == 0`, the four truck DataFrames are emitted but
  empty (or with 0-count rows), and `init_state(resolved).resources`
  contains zero rows with `resource_category == "rebalancing_truck"`.
- (iii) After `init_state(resolved)`, `state.resources` contains exactly
  `n_trucks` rows with `resource_category == "rebalancing_truck"` and
  `status == "available"`.
- `resolved.edges` contains rows with `modal_type == "road"` linking
  station↔station, station↔depot, depot↔station for the depot+stations
  set. Already produced by `_build_behavior` in
  `dataloader_graph.py:362-395`.

### F. Documentation and project state

- `PROJECT_STATE.md` updated: "Multi-stop routes" promoted from "Future
  extension" to delivered scope; "Rebalancer phase" status moved from "next
  phase" to "in progress" then "done" on completion.

## 4. Implementation Steps

> Each step is sized so an executor can pick it and ship a PR-shaped change
> without re-asking architecture questions.

### Step 1 — Verify and parameterize `DataLoaderMock` truck fixture (effort: S, ~1 h)

> **Reframing:** the four truck DataFrames are ALREADY emitted by
> `DataLoaderMock` at `dataloader_mock.py:30-35`. This step is *verification
> + parameterization*, not "opt-in extension". No `include_trucks` flag.

- **Files modified:**
  - `gbp/loaders/dataloader_mock.py` (add `n_trucks` and
    `truck_capacity_bikes` parameters; flow them into the existing
    `df_resources` / `df_resource_capacities` emitter so the rows scale
    with `n_trucks`).
- **Files created:**
  - `tests/unit/loaders/test_dataloader_mock_trucks.py`.
- **Public surface:**
  ```python
  class DataLoaderMock:
      def __init__(
          self,
          n_trucks: int = 2,
          truck_capacity_bikes: int = 30,
          # ... existing params unchanged
      ) -> None: ...
  ```
  No new keyword breaks existing call sites — both new parameters are
  defaulted. The protocol-level emission set in `_GROUPS` is unchanged.
- **Spec ref:** Acceptance criterion E. Risks #1, #3 (spec line 222-228).
- **Tests:**
  - `test_default_emits_trucks` — calling `DataLoaderMock().load()` returns a
    `RawModelData` whose truck DataFrames are non-empty and after
    `build_model(...)` the resolved model has trucks. (Codifies the
    pre-existing default-on behavior.)
  - `test_n_trucks_zero_yields_no_trucks` — calling
    `DataLoaderMock(n_trucks=0).load()` produces a resolved model whose
    `state.resources["resource_category"].eq("rebalancing_truck").sum() == 0`.
  - `test_n_trucks_three` — `DataLoaderMock(n_trucks=3, truck_capacity_bikes=20)`
    yields exactly 3 truck rows in `state.resources`, each with the
    expected capacity per `resolved.resource_categories.base_capacity`.

### Step 2 — LatentDemandInflatorPhase (effort: S, ~1.5 h)

- **Files modified:**
  - `gbp/consumers/simulator/built_in_phases.py` (add class after
    `HistoricalLatentDemandPhase` near `built_in_phases.py:330`).
- **Files created:**
  - `tests/unit/consumers/simulator/test_latent_demand_inflator.py`.
- **Public surface:**
  ```python
  class LatentDemandInflatorPhase:
      name: str = "LATENT_DEMAND_INFLATOR"

      def __init__(
          self,
          multiplier: float | dict[str, float] = 1.0,
          inflate_arrivals: bool = False,
          schedule: Schedule | None = None,
      ) -> None: ...

      def should_run(self, period: PeriodRow) -> bool: ...

      def execute(
          self,
          state: SimulationState,
          resolved: ResolvedModelData,
          period: PeriodRow,
      ) -> PhaseResult: ...
  ```
- **Behaviour:**
  1. Read `state.intermediates.get("latent_demand")`. If None or empty →
     `PhaseResult.empty(state)`.
  2. Make a copy. Multiply `latent_departures` (and `latent_arrivals` iff
     `inflate_arrivals=True`) — vectorized: scalar broadcast OR
     `df.facility_id.map(multiplier_dict).fillna(1.0)`.
  3. `state.with_intermediates(latent_demand=mutated)`.
  4. Emit `events={"latent_demand": mutated.copy()}` so the table reaches
     `simulation_latent_demand_log`.
- **Spec ref:** Acceptance criterion B. Spec round 6 decision B.
- **Tests:** Section B above (six tests).

### Step 3 — RebalancerTask salvage + rewrite + R8 validator fix (effort: M, ~5 h)

- **Files created:**
  - `gbp/consumers/simulator/tasks/rebalancer.py` (new).
  - `tests/unit/consumers/simulator/test_rebalancer_task.py`.
- **Files modified:**
  - `gbp/consumers/simulator/dispatch_lifecycle.py` — **R8 fix**: extend
    `_reject_unavailable_resource` (currently lines 268-282) so that when a
    `resource_id` appears in *multiple rows* in the same dispatch batch
    (route case), the validator treats the resource as available for the
    full route as long as at least one row's `source_id` matches the
    truck's `current_facility_id`. The single-row check is preserved
    unchanged. See "R8 fix details" below.
  - `tests/unit/consumers/simulator/test_dispatch_lifecycle.py` — add
    `test_route_dispatch_passes_validator` and
    `test_single_row_unavailable_resource_still_rejects` (Section A).
  - `gbp/rebalancer/routing/vrp.py` — pin `random_seed` argument flow into
    `RoutingSearchParameters` for determinism (R5 mitigation; SHOULD #6).
    No algorithmic change.
  - `gbp/rebalancer/routing/postprocessing.py` — verify
    `extract_pdp_solution` and `format_pdp_route_output` still work; they
    are pure functions over the OR-Tools solution object.
  - `gbp/rebalancer/__init__.py` — drop the broken
    `df_inventory_ts`-dependent re-export; export only `solve_pdp`,
    `compute_utilization_and_balance`.
- **Files DELETED in this PR (per SHOULD #8):**
  - `gbp/rebalancer/dataloader.py` and `gbp/rebalancer/pipeline.py`.
    Salvage parts that the Task needs by *copying into*
    `gbp/consumers/simulator/tasks/rebalancer.py` first, then delete.
    Spec lines 53-58 mark them obsolete and `gbp/rebalancer/__init__.py:11`
    is already broken.
- **Public surface:**
  ```python
  class RebalancerTask:
      name: str = "rebalancer"

      def __init__(
          self,
          min_threshold: float = 0.3,
          max_threshold: float = 0.7,
          time_limit_seconds: int = 30,
          period_duration_hours: float | None = None,
          truck_speed_kmh: float = 30.0,
          pdp_random_seed: int = 42,
      ) -> None: ...

      def run(
          self,
          state: SimulationState,
          resolved: ResolvedModelData,
          period: PeriodRow,
      ) -> pd.DataFrame: ...
  ```
  - `period_duration_hours=None` (the default) means: read from
    `resolved.periods` at first call, e.g.
    `(periods.iloc[1].period_id - periods.iloc[0].period_id).total_seconds() / 3600.0`.
    A hardcoded 1.0 default would silently mask unit mismatches; making
    `None` the default forces the lookup. (SHOULD #7 applied.)
  - `pdp_random_seed=42` is forwarded to OR-Tools
    `RoutingSearchParameters` so baseline-vs-treatment comparisons are
    reproducible. The notebook uses the same seed in both runs to keep
    the comparison fair. (SHOULD #6 applied.)

- **Algorithm (the "salvage"):**
  0. **Early exit (R8/R9 mitigation; first guard, before any solver work):**
     ```python
     available_trucks = state.resources.query(
         "resource_category == 'rebalancing_truck' "
         "and status == 'available'"
     )
     if available_trucks.empty:
         return pd.DataFrame(columns=DISPATCH_COLUMNS)
     ```
  1. Build `df_nodes` from `state.inventory` joined to per-facility
     `inventory_capacity`. **Capacity-source fallback (MUST #2):**
     ```python
     storage_slice = (
         resolved.attributes.get("operation_capacity")
         if "operation_capacity" in resolved.attributes
         else None
     )
     if storage_slice is None or storage_slice.empty:
         capacity = (
             state.inventory.groupby("facility_id")["quantity"].max() * 2
         )
     else:
         capacity = (
             storage_slice
             .query("operation_type == 'storage'")
             .set_index("facility_id")["operation_capacity"]
         )
     ```
     Filter `df_nodes` to `facility_type == "station"`. Add lat/lon from
     `resolved.facilities`.
  2. Pick the depot: first row in `resolved.facilities` with
     `facility_type == "depot"`. Pre-assign available trucks (already
     filtered in step 0; further constrain to
     `current_facility_id == depot_id`).
  3. Call `compute_utilization_and_balance(df_nodes, min_threshold,
     max_threshold)` (`gbp/rebalancer/demand.py:25`). If `sources.empty` or
     `destinations.empty` → return empty `DISPATCH_COLUMNS` DataFrame.
  4. Build `PdpModel` (`gbp/rebalancer/contracts.py:93`):
     - `node_ids = ["depot", *expand_pickups(sources, _pickup),
       *expand_deliveries(destinations, _delivery)]`.
     - `pickups_deliveries`: greedy match excess→deficit; quantities clamped
       at min(excess_i, deficit_j, truck_capacity).
     - `distance_matrix`: NxN matrix from `resolved.distance_matrix` between
       (depot, all pickup-station copies, all delivery-station copies). Self
       distance = 0; missing pairs filled by Haversine fallback (matches
       loader convention).
     - `resource_capacities = [base_capacity] * n_trucks` from
       `resolved.resource_categories`.
  5. `solve_pdp(pdp, time_limit_seconds=self.time_limit_seconds,
     random_seed=self.pdp_random_seed)` →
     `route_df` via `format_pdp_route_output`.
  6. Translate `route_df` to `DISPATCH_COLUMNS`:
     - For each truck, walk the ordered route, pair every pickup with its
       matching delivery (indices in the same `pickups_deliveries` tuple),
       compute one row per pair: `source_id = pickup_node`,
       `target_id = delivery_node`,
       `commodity_category` (dominant commodity at the source by
       `quantity`, tie-break alphabetic — flagged in §8),
       `quantity = paired_quantity`,
       `resource_id = truck_id`,
       `modal_type = ModalType.ROAD.value`,
       `arrival_period = period.period_index +
       ceil(total_route_hours / period_duration_hours)`.
     - `total_route_hours` derived from `route_df` cumulative distance /
       `truck_speed_kmh` (consistent with `dataloader_graph.py:415`
       `dur = dkm / speed`).
  7. **Belt-and-braces chain assertion (before emit).** Before returning
     the DataFrame, `RebalancerTask` asserts that for every truck group
     the emitted route rows form a chain rooted at the truck's
     `current_facility_id` (row 1's `source_id` equals the truck's
     start, and each subsequent row's `source_id` equals the previous
     row's `target_id` OR is in `visited`). Raise on violation. This
     is redundant with the validator (which would reject), but surfaces
     solver bugs at the Task level rather than at the dispatch
     lifecycle level — much cleaner failure mode.

- **R8 fix details (`dispatch_lifecycle._reject_unavailable_resource`):**
  Today (`dispatch_lifecycle.py:261-282`) builds
  `avail_at_source = {(resource_id, current_facility_id)}` and rejects
  every row whose `(resource_id, source_id)` is not in that set. For a
  multi-stop route, the same `resource_id` is paired with multiple
  `source_id`s (different pickup stations); only the row whose
  `source_id == truck.current_facility_id` would pass.

  **Revised logic — chain validation rooted at `current_facility_id`:**
  ```python
  # Pseudocode for the revised _reject_unavailable_resource body.
  # Group only the not-yet-rejected rows that have a non-null resource_id.
  # Null-resource_id rows are NOT touched here (auto-assignment runs
  # upstream in _assign_resources at dispatch_lifecycle.py:111-173).
  avail = state.resources[state.resources["status"] == AVAILABLE]
  avail_at_source = {(r, s) for r, s in zip(avail["resource_id"],
                                            avail["current_facility_id"])}

  for resource_id, group_rows in groupby_non_null_resource_id(dispatches):
      if len(group_rows) == 1:
          # Single-row case: existing per-row (resource_id, source_id) check.
          row = group_rows[0]
          if (resource_id, row.source_id) not in avail_at_source:
              mark NO_AVAILABLE_RESOURCE on row
      else:
          # Route case (len > 1): require connected chain.
          # The chain MUST be rooted at the truck's current_facility_id.
          if resource_id not in state.resources.index:
              mark NO_AVAILABLE_RESOURCE on every row of the group
              continue
          truck_loc = state.resources.loc[resource_id, "current_facility_id"]
          visited = {truck_loc}
          chain_valid = True
          for row in group_rows in dispatch order:
              if row.source_id not in visited:
                  chain_valid = False
                  break
              visited.add(row.target_id)
          if chain_valid:
              accept all rows in the group  # whole route engages the truck
          else:
              # Fall back to per-row check; each row rejected individually
              # if (resource_id, row.source_id) not in avail_at_source.
              for row in group_rows:
                  if (resource_id, row.source_id) not in avail_at_source:
                      mark NO_AVAILABLE_RESOURCE on row
  # Null-resource_id rows: untouched here; per-row check still applies via
  # the existing has_resource mask early in the function.
  ```

  Commentary: the chain rooted at `current_facility_id` is the property
  that distinguishes a real route from an arbitrary co-grouped dispatch
  batch. Single-row semantics unchanged. Null-`resource_id` semantics
  unchanged. The change is fully internal to
  `_reject_unavailable_resource`; the public validator interface is
  untouched. Three failure modes the chain validator catches:
  (i) **cross-task collision** — two unrelated tasks happen to emit
  rows sharing the same `resource_id` but with sources that do not form
  a chain → fall-back per-row check rejects each row individually;
  (ii) **two-stop degenerate** — a 2-row group whose first row's
  `source_id` is not the truck's `current_facility_id` → both rows
  rejected; (iii) **null `resource_id`** — auto-assignment runs
  upstream and per-row semantics apply unchanged.

  `_reject_over_capacity` (`dispatch_lifecycle.py:285-311`) already
  groups by `resource_id` and remains correct under the new
  semantics — no change needed there.

- **Spec ref:** Acceptance criteria A. Risks R2, R4, R8 (§5).
- **Tests:** Section A above (now thirteen tests including the five
  `test_dispatch_lifecycle.py` regression tests for the chain
  validator). Use a 3-station + 1-depot fixture; preset
  `state.inventory` such that S1 has 18/20, S2 has 1/20, S3 has 10/20;
  with thresholds 0.3/0.7, S1 is source, S2 is destination, S3 is
  neutral.

### Step 4 — Phase-chain integration test, smoke first (effort: S, ~2 h)

- **Files created:**
  - `tests/integration/test_rebalancer_phase_chain.py`.
  - `tests/integration/fixtures/rebalancer_mini.py` (two helpers:
    `build_smoke_fixture()` — 1 truck, 2 stations, 1 depot, hand-shaped
    inventory for exactly one source-destination pair; and
    `build_full_fixture()` — 5 stations + 1 depot + 2 trucks for the
    multi-stop scenario).
- **Spec ref:** Acceptance criteria C.
- **Tests:** Section C above (now four tests). Order matters:
  `test_smoke_single_pair_dispatch` runs FIRST and isolates basic Task →
  DispatchPhase wiring. The multi-stop tests then run on top of the R8
  validator fix.

### Step 5 — Verification notebook 10 (effort: M, ~3 h)

- **Files created:**
  - `notebooks/verify/10_rebalancer_experiment.ipynb`.
- **Structure (single cell, `# %% N. Section`-divided per CLAUDE.md
  notebook style):**
  - `# %% [markdown]` Map of sections (1–9).
  - `# %% 1. Imports`.
  - `# %% 2. Build resolved model from DataLoaderMock with trucks`.
  - `# %% 3. Define phase chains (baseline_phases, treatment_phases)`.
  - `# %% 4. Run baseline (env_baseline = Environment(config_baseline);
    log_baseline = env_baseline.run())`.
  - `# %% 5. Run treatment (env_treatment, log_treatment)`.
  - `# %% 6. Build trips_per_date_per_station, summary_per_date, comparison
    DataFrames` from `log.simulation_flow_log`,
    `log.simulation_lost_demand_log`, `resolved.distance_matrix`.
  - `# %% 7. Charts` (assignments only — `fig_bar`, `fig_heatmap`).
  - `# %% 8. Assertions` (`assert comparison.loc["total_lost_demand",
    "delta"] < 0`).
  - `# %% 9. Russian-language summary in markdown` per CLAUDE.md
    plain-language rule.
- **Spec ref:** Acceptance criterion D.
- **Verification:** `jupyter execute notebooks/verify/10_rebalancer_experiment.ipynb`.

### Step 6 — PROJECT_STATE.md and rebalancer dead-code cleanup (effort: XS, ~30 min)

- **Files modified:**
  - `PROJECT_STATE.md` — flip status entries per acceptance criterion F.
  - `gbp/rebalancer/__init__.py` — remove the broken re-export that
    references `df_inventory_ts` (spec line 53). After deletion of
    `dataloader.py` / `pipeline.py`, this file should re-export only
    `solve_pdp`, `compute_utilization_and_balance`, `format_pdp_route_output`.
- **Files deleted (per SHOULD #8, executed inside Step 3 to avoid leaving
  dangling imports):**
  - `gbp/rebalancer/dataloader.py`
  - `gbp/rebalancer/pipeline.py`
  Salvage parts the Task needs by copying them into
  `gbp/consumers/simulator/tasks/rebalancer.py` first; then delete. Spec
  lines 53-58 and the broken `gbp/rebalancer/__init__.py:11` import
  justify deletion in this PR rather than in a follow-up.
- **Spec ref:** Acceptance criterion F. Risk R4 (§5).

## 5. Risks and Mitigations

> Each risk has probability, impact, mitigation, fallback.

### R1 — Truck `resource_category` not in current resolved model (spec risk #1)

- **Probability:** Low (Architect-confirmed: `dataloader_mock.py:30-35`
  already includes the four truck DataFrames in `_GROUPS` and emits them
  unconditionally; only `n_trucks` and capacity need parameterization).
- **Impact:** High — without trucks, `state.resources` is empty and the
  task returns an empty DataFrame for every period. Acceptance criteria
  A, C, D all fail.
- **Mitigation:** Step 1 verifies the existing emission and adds tunable
  knobs. Tests in `tests/unit/loaders/test_dataloader_mock_trucks.py`
  lock both the default-on behavior and the `n_trucks=0` corner case.
- **Fallback:** If the mock parameterization breaks an existing test, fall
  back to default values that match the legacy fixture (e.g. `n_trucks=2,
  truck_capacity_bikes=30`) and patch only the new tests.

### R2 — Truck-modal edges between stations and depot not materialized (spec risk #2)

- **Probability:** Low. `_build_behavior` in `dataloader_graph.py:362-395`
  already emits `("depot","station")`, `("station","depot")`,
  `("station","station")`, `("depot","depot")` rules with
  `modal_type=ModalType.ROAD.value`. `build_edges`
  (`gbp/build/edge_builder.py:8`) materializes them via the type-pair
  cross-join + `distance_matrix` merge.
- **Impact:** Medium — `_reject_invalid_edge`
  (`dispatch_lifecycle.py:224-258`) would silently drop every dispatch.
- **Mitigation:** Step 1 sets `include_trucks=True` AND `n_depots=1`, which
  triggers the depot-edge branch automatically. Step 4's integration test
  asserts at least one applied dispatch (non-empty
  `simulation_flow_log["resource_id"]`).
- **Fallback:** If the edge table comes up short, add an explicit
  `manual_edges` seed in the test fixture using `resolved.distance_matrix`
  rows verbatim.

### R3 — `resource_modal_compatibility` / `resource_commodity_compatibility` missing (spec risk #3)

- **Probability:** Low. `_build_resources` in `dataloader_graph.py:570-583`
  emits both compatibility tables when `df_resources` and `df_depots` are
  present. Step 1 ensures both are present for the mock.
- **Impact:** Medium — `_assign_resources` in `dispatch_lifecycle.py:131-143`
  would skip every truck. We pre-assign `resource_id` in `RebalancerTask`,
  so this only bites if validation also uses these tables — and it doesn't
  (`_reject_unavailable_resource` at `dispatch_lifecycle.py:261-282` only
  checks `(resource_id, source_id)` against the AVAILABLE set).
- **Mitigation:** Pre-assigned resource IDs make this risk almost moot. Step
  1 still emits both tables for the mock.
- **Fallback:** If a downstream user runs `RebalancerTask` with `resource_id`
  set to None and compatibility tables missing, document in the docstring
  that pre-assignment is mandatory. Tests in section A enforce this
  (`test_resource_id_pre_assigned`).

### R4 — OR-Tools PDP solver in `gbp/rebalancer/routing/` is broken (spec risk #4 / round 3)

- **Probability:** Medium. The user said the package is broken; we verified
  that `gbp/rebalancer/__init__.py:11` imports a removed `df_inventory_ts`,
  but `vrp.py`, `postprocessing.py`, and `demand.py` are pure functions
  over their own contracts and do not transitively depend on the broken
  loader. (Verified by reading the files — see plan §1 paragraph 2.)
- **Impact:** Medium — would block Step 3.
- **Mitigation:** Step 3 explicitly bypasses `gbp/rebalancer/dataloader.py`
  and `gbp/rebalancer/pipeline.py`. Tests for `vrp.solve_pdp` are added in
  Step 3 if the existing call surface is unstable
  (`tests/unit/consumers/simulator/test_rebalancer_task.py::test_solver_smoke`).
- **Fallback:** If `vrp.solve_pdp` is itself broken in a way the spec
  underestimated, replace with a minimal `OR-Tools`-based reimplementation
  inside `tasks/rebalancer.py` (still <80 lines; the existing code already
  proves the model-build pattern works). Defer integration-test step until
  after.

### R5 — OR-Tools determinism across runs (new — not in spec)

- **Probability:** Medium. OR-Tools `GUIDED_LOCAL_SEARCH` is randomized; with
  a 30 s time limit the result may differ run-to-run, breaking notebook
  reproducibility.
- **Impact:** Low (correctness) / Medium (notebook reproducibility for
  storytelling).
- **Mitigation:** Set `search_parameters.solution_limit` and a fixed
  `random_seed` in `solve_pdp`; document in `vrp.py` docstring. Notebook
  fixes a seed before each call.
- **Fallback:** If determinism cannot be guaranteed, add tolerance in
  notebook assertions (`abs(delta) > 5%` rather than exact equality).

### R6 — No imbalance triggers empty DataFrame on all periods (new — edge case)

- **Probability:** Low (the experiment uses `multiplier > 1.0` in strict
  mode, which actively drains stations).
- **Impact:** High if it happens — primary success metric (criterion C
  test 3) fails silently.
- **Mitigation:** Step 4 integration test asserts at least one non-empty
  flow_events row from the rebalancer in 24 periods. Step 5 notebook
  asserts `comparison.loc["total_lost_demand", "delta"] < 0`.
- **Fallback:** If the fixture's natural inflow keeps stations balanced,
  bump `multiplier` to 3.0 in the notebook and rerun. The multiplier is a
  knob, not a contract.

### R7 — `arrival_period` collision with already-in-transit shipments (new)

- **Probability:** Low.
  `dispatch_lifecycle._reject_invalid_arrival` (line 214-221) only rejects
  when `arrival_period < period.period_index`. A shared `arrival_period`
  across multiple rows is fine; `ArrivalsPhase` aggregates by `target_id`
  via `to_inventory_delta` (`built_in_phases.py:630`).
- **Impact:** Low.
- **Mitigation:** None needed — verified at the cited file:lines.
- **Fallback:** If a multi-row aggregation bug surfaces, write a regression
  test in `test_rebalancer_phase_chain.py` and patch `to_inventory_delta`.

### R8 — Multi-stop validator collision in `_reject_unavailable_resource` (Architect-confirmed runtime blocker)

- **Probability:** HIGH (occurs on every multi-stop run; deterministic).
- **Impact:** HIGH — `dispatch_lifecycle.py:268-282` builds
  `avail_at_source = {(resource_id, current_facility_id)}` and rejects
  every row whose `(resource_id, source_id)` is not in that set. For a
  multi-stop route, the same `resource_id` is paired with multiple
  `source_id`s; only the row with `source_id == truck.current_facility_id`
  passes, the rest are rejected as `NO_AVAILABLE_RESOURCE`. The treatment
  experiment then degenerates to single-stop dispatches and lost-demand
  reduction collapses.
- **Mitigation:** Step 3 modifies `_reject_unavailable_resource` to use
  **chain validation rooted at `current_facility_id`**. For multi-row
  groups the validator walks rows in dispatch order maintaining a
  `visited` set seeded with the truck's `current_facility_id`; each
  row's `source_id` must already be in `visited` (then `target_id` is
  added). If the whole chain is valid, the entire route is accepted;
  otherwise the group falls back to per-row `(resource_id, source_id) ∈
  avail_at_source` checks so each broken row is rejected individually.
  Single-row and null-`resource_id` semantics are preserved. Four
  regression tests are added in
  `tests/unit/consumers/simulator/test_dispatch_lifecycle.py`
  (route-passes, route-fails-no-root, route-fails-broken-chain,
  null-resource-fallback) — see Section A.
- **Fallback:** If the validator change introduces unforeseen breakage in
  other tests, revert and downgrade to **Option (a)**: restrict v1 to
  single-pickup-single-delivery per truck per fire and add a follow-up to
  re-introduce true multi-stop in v2. Plan flags this fallback explicitly
  so the executor can choose if needed.

### R9 — `RebalancerTask.run` does no-truck early exit late (encoded in Step 3.0)

- **Probability:** Medium (defensive coding; not a runtime bug today but
  trivial to forget).
- **Impact:** Medium — building `df_nodes` and constructing a `PdpModel`
  before checking truck availability wastes work and adds noise to logs
  in periods where no trucks are available.
- **Mitigation:** Step 3 algorithm step 0 makes the AVAILABLE-truck check
  the FIRST guard, before any pandas/solver work. Test
  `test_empty_when_no_truck_available` enforces it.
- **Fallback:** If the early exit ever short-circuits a legitimate run,
  log a debug event with the truck-availability snapshot for diagnosis.

### R10 — Missing `operation_capacity` attribute crashes Task (encoded in Step 3.1)

- **Probability:** Medium (test fixtures may omit `operation_capacity`).
- **Impact:** High — `KeyError` aborts the period; `Environment.run()`
  fails.
- **Mitigation:** Step 3 algorithm step 1 includes the explicit
  `inventory_capacity` fallback (max(quantity) * 2 per facility). Test
  `test_inventory_capacity_fallback_when_no_operation_capacity` (Section
  A) locks the fallback in. Pattern matches `built_in_phases.py:720-723`.
- **Fallback:** If the fallback is too lax (e.g. always permits sourcing),
  surface a warning and refuse to dispatch from the affected facility.

### R11 — OR-Tools nondeterminism breaks notebook reproducibility (extension of R5)

- **Probability:** Medium with default settings; Low after `pdp_random_seed`
  is pinned.
- **Impact:** Medium — the `comparison` table in notebook 10 may report
  different `delta_pct` values across runs, undermining storytelling.
- **Mitigation:** SHOULD #6 applied: `RebalancerTask.__init__` exposes
  `pdp_random_seed: int = 42`, threaded into `solve_pdp` →
  `RoutingSearchParameters`. Both baseline and treatment use the same
  seed in notebook 10 to keep the comparison fair.
- **Fallback:** If determinism still wavers, pin `solution_limit` instead
  of `time_limit_seconds` for the notebook run; document tolerance in the
  notebook assertion (e.g., `delta < -0.05` rather than equality).

## 6. Verification Steps

A developer (or the executor agent) confirms the plan was implemented by
running, in order:

```bash
# 1. Lint and type-check the new and modified modules.
ruff check gbp/consumers/simulator/tasks/rebalancer.py \
           gbp/consumers/simulator/built_in_phases.py \
           gbp/consumers/simulator/dispatch_lifecycle.py \
           gbp/loaders/dataloader_mock.py \
           tests/unit/consumers/simulator/test_rebalancer_task.py \
           tests/unit/consumers/simulator/test_dispatch_lifecycle.py \
           tests/unit/consumers/simulator/test_latent_demand_inflator.py
ruff format --check gbp/consumers/simulator/tasks/rebalancer.py
mypy gbp/consumers/simulator/tasks/rebalancer.py \
     gbp/consumers/simulator/built_in_phases.py \
     gbp/consumers/simulator/dispatch_lifecycle.py

# 2. Unit tests (R8 regression first, then task and inflator).
pytest tests/unit/consumers/simulator/test_dispatch_lifecycle.py -v
pytest tests/unit/consumers/simulator/test_rebalancer_task.py \
       tests/unit/consumers/simulator/test_latent_demand_inflator.py \
       tests/unit/loaders/test_dataloader_mock_trucks.py -v

# 3. Integration tests.
pytest tests/integration/test_rebalancer_phase_chain.py -v

# 4. Full test suite (regression check).
pytest

# 5. Notebook execution.
jupyter execute notebooks/verify/10_rebalancer_experiment.ipynb

# 6. Manual inspection (optional).
jupyter lab notebooks/verify/10_rebalancer_experiment.ipynb
# Inspect comparison table, bar chart, heatmap.
```

Pass criterion: every command exits 0; the notebook's final cell asserts
`comparison.loc["total_lost_demand", "delta"] < 0`.

## 7. ADR — Decision, Drivers, Alternatives, Consequences

- **Decision.** Implement the rebalancer as a `RebalancerTask` that runs an
  OR-Tools PDP solver, wrapped in `DispatchPhase(task, schedule=Schedule.every_n(12))`,
  inserted after `ArrivalsPhase`. Multi-stop routes are encoded as
  one `DISPATCH_COLUMNS` row per pickup-delivery pair with shared
  `arrival_period`. Truck infrastructure is supplied by parameterizing the
  existing `DataLoaderMock` truck DataFrames (no new `include_trucks` flag,
  trucks stay default-on). The runtime blocker R8 is resolved by
  patching `dispatch_lifecycle._reject_unavailable_resource` to accept
  multi-row route dispatches.

- **Drivers (top 3 from RALPLAN-DR §2).**
  - **D1.** Ship a working experiment fast (user said "Мне это нужно уже сейчас").
  - **D2.** Preserve existing phase contracts (`DISPATCH_COLUMNS`, `Task`,
    `DispatchPhase`, `ArrivalsPhase` untouched at the public-interface
    level; only one private validator function is patched).
  - **D3.** Make the broken-prototype salvage explicit (delete what's
    broken, salvage what works).

- **Alternatives considered.**
  - **O1 (truck infra):** A) parameterize mock *(chosen)*, B) extend
    production loader *(deferred)*.
  - **O2 (capacity source):** A) `operation_capacity` *(chosen)*, B)
    separate `station_capacity` attribute *(invalidated)*.
  - **O3 (empty-imbalance behavior):** A) return empty DataFrame
    *(chosen)*, B) skip via `should_run` *(invalidated, contradicts spec)*.
  - **R8 resolution options:** (a) drop multi-stop in v1, (b) extend
    `_reject_unavailable_resource` *(chosen)*, (c) add a `route_id` column
    to the Task contract *(rejected — bloats public schema)*.

- **Why chosen.**
  - **R8 option (b).** Smallest scope (one validator function), preserves
    the spec's Round-5 multi-stop mapping, requires only one new
    regression test plus one preservation test, leaves the public Task
    contract intact. Option (a) deviates from the spec; option (c) bloats
    the contract. Trade-off: we accept that the dispatch-lifecycle
    semantics now distinguish "single-row dispatch" from "route dispatch",
    a real but well-localized branch.
  - **O1 option (A).** The mock already emits truck DataFrames; only knobs
    are missing. Production-loader extension can be a follow-up after the
    experiment validates the configuration.
  - **O2 option (A).** Avoids duplicating capacity data; aligns with
    `DockCapacityPhase` precedent.
  - **O3 option (A).** Matches `DispatchPhase`'s existing handling of
    `dispatches.empty` — zero overhead.

- **Consequences.**
  - *Positive:* fast path to a working experiment; no public contract
    changes; OR-Tools determinism pinned via `pdp_random_seed`;
    capacity-fallback prevents fixture-related crashes; route validator
    fix unblocks every multi-stop run.
  - *Negative:* `_reject_unavailable_resource` now has two code paths
    (single-row vs route); production `DataLoaderGraph` still does not
    synthesize trucks (mock-only for now); deletion of
    `gbp/rebalancer/dataloader.py` and `pipeline.py` removes diff-history
    breadcrumbs (mitigated by salvage-then-delete inside Step 3);
    hard-coded `dominant commodity by quantity` choice for multi-commodity
    stations is a v1 simplification.

- **Follow-ups.**
  - Promote `n_trucks` / `truck_capacity_bikes` parameterization from
    `DataLoaderMock` to `DataLoaderGraph` so real Citi-Bike data wakes up
    with trucks automatically.
  - Multi-stop v2: revisit the validator semantics if/when the Task
    contract grows a richer route-id concept; consider whether
    `_reject_invalid_edge` needs a route-aware mode.
  - Resolve "one row per category" vs "dominant category" for stations
    with multiple commodity categories (open-question item).
  - Consider promoting summary metrics from notebook 10 to a public
    helper module under `gbp/` after the experiment proves them durable
    (spec acceptance: defer to v2).

## 8. Plan-Time Open Questions and Spec Interpretations

> The spec was tagged "PASSED" at 15% ambiguity, but the following points
> were interpreted by the planner — flagged here for the executor and the
> Critic to confirm or override.

1. **`modal_type` value for trucks.** Spec uses `"truck"` (line 232). The
   only existing enum value is `ModalType.ROAD = "road"`
   (`gbp/core/enums.py:24`). The plan resolves this by using
   `ModalType.ROAD.value` everywhere — matches what the loader actually
   emits at `dataloader_graph.py:379` and `:579`. If the user wants a literal
   `"truck"` modal type, add it to the enum and update the loader; this
   would be a cross-cutting change deferred to a follow-up.
2. **`commodity_category` selection per multi-stop pair.** Spec does not
   specify how to pick the commodity category for a pickup-delivery row
   when a station holds multiple categories (e.g. `electric_bike` and
   `classic_bike`). The plan picks the dominant category by `quantity`
   at the source; tie-break alphabetic. Open for executor to confirm or
   replace with one row per category.
3. **`period_duration_hours`.** *Resolved in iteration 2.* Spec assumes
   1-hour periods (line 49). `RebalancerTask.__init__` defaults the
   parameter to `None`, which causes the constructor to read the period
   length from `resolved.periods` at the first call —
   `(periods.iloc[1].period_id - periods.iloc[0].period_id).total_seconds() / 3600.0`.
   This avoids silently masking unit mismatches behind a hardcoded 1.0.
4. **`truck_speed_kmh`.** Used to convert `route_df` distance to
   `arrival_period`. Plan defaults to 30 km/h, matches
   `GraphLoaderConfig.default_speed_kmh` heuristic. Documented in the
   `RebalancerTask` docstring.
5. **`gbp/rebalancer/{dataloader,pipeline}.py` deletion.** *Resolved in
   iteration 2.* These files are deleted in this PR (Step 3 + Step 6),
   not deferred. Salvage parts the Task needs by copying into
   `gbp/consumers/simulator/tasks/rebalancer.py` first.
6. **`pdp_random_seed`.** *Added in iteration 2.* `RebalancerTask`
   exposes `pdp_random_seed: int = 42`; both baseline and treatment
   notebook runs use the same seed for fair comparison.

(Open questions also written to `.omc/plans/open-questions.md` per the
planner protocol.)

## 9. Iteration 2 Changelog

This iteration applies the Architect/Critic verdict revisions in place.

- **Applied MUST #1 (R8 resolution):** Picked **option (b)** — extend
  `dispatch_lifecycle._reject_unavailable_resource` so multi-row
  (route-dispatch) groups treat the resource as engaged for the full
  route once one row's `source_id` matches the truck's
  `current_facility_id`. Single-row behavior preserved. Added
  `dispatch_lifecycle.py` to Step 3 modify-list, plus two regression
  tests (`test_route_dispatch_passes_validator`,
  `test_single_row_unavailable_resource_still_rejects`) in §3.A.
  Rationale: smallest scope, preserves Round-5 multi-stop mapping, no
  public contract change. Documented trade-off and option (a) fallback
  in R8.
- **Applied MUST #2 (capacity fallback):** Step 3.1 now encodes the
  `inventory_capacity = max(quantity) * 2` fallback when
  `operation_capacity` is missing or empty. Added test
  `test_inventory_capacity_fallback_when_no_operation_capacity` in §3.A.
  Pattern matches `built_in_phases.py:720-723`.
- **Applied MUST #3 (early-exit guard):** Step 3 algorithm step 0 makes
  the AVAILABLE-truck check the FIRST guard before any solver work.
  `test_empty_when_no_truck_available` flagged as the first guard test.
- **Applied MUST #4 (Step 1 reframe):** Step 1 now reads "verify and
  parameterize `DataLoaderMock` truck fixture" — drops the
  `include_trucks` flag idea, adds `n_trucks` / `truck_capacity_bikes`
  parameters, references the codebase fact that `dataloader_mock.py:30-35`
  already emits the four truck DataFrames. Tests updated to cover the
  three required cases (default-on, `n_trucks=0`, `n_trucks=3`).
- **Applied MUST #5 (open-questions append + R8 risk register):** R8,
  R9, R10, R11 added to §5 Risk Register with probability/impact/
  mitigation/fallback. R8/R9/R10/R11 also appended to
  `.omc/plans/open-questions.md`.
- **Applied SHOULD #6 (`pdp_random_seed`):** Added to
  `RebalancerTask.__init__` with default 42; threaded into `solve_pdp` →
  `RoutingSearchParameters`. Documented in §3 and §7 ADR.
- **Applied SHOULD #7 (`period_duration_hours`):** Default flipped from
  `1.0` to `None`; constructor reads from `resolved.periods` when
  `None`. Updated §8 open question 3 to reflect resolution.
- **Applied SHOULD #8 (delete obsolete files in this PR):** Step 6 now
  prescribes deleting `gbp/rebalancer/dataloader.py` and
  `gbp/rebalancer/pipeline.py` in this PR, with salvage-into-tasks-first
  flow. Updated §8 open question 5 to reflect resolution.
- **Applied SHOULD #9 (smoke test before multi-stop):** Section C and
  Step 4 now order `test_smoke_single_pair_dispatch` first, with two
  fixtures (`build_smoke_fixture`, `build_full_fixture`) in
  `tests/integration/fixtures/rebalancer_mini.py`.
- **Principle/option consistency fix:** §2 P1 amended to acknowledge the
  `_reject_unavailable_resource` validator fix (option (b)). The
  amendment notes the public Task / `DispatchPhase` / `ArrivalsPhase` /
  `DISPATCH_COLUMNS` interfaces remain untouched.
- **ADR §7 filled:** Decision, Drivers (D1/D2/D3), Alternatives
  (O1/O2/O3 + R8 (a)/(b)/(c)), Why chosen, Consequences (positive +
  negative), Follow-ups (loader promotion, multi-stop v2, commodity
  selection, summary-metric promotion).
- **Tension noted:** SHOULD #8 (delete obsolete files in this PR) creates
  mild tension with D2 (preserve existing contracts and reviewable
  scope). Resolved by performing the salvage inside Step 3 before
  deletion, so the diff still shows where each piece migrated.

## 10. Iteration 3 Changelog

This iteration applies the Architect (SOUND_WITH_CONCERNS) and Critic
(ITERATE) revisions on top of iteration 2.

- **Applied MUST #1 (R8 chain-validation algorithm in §3):** The R8 fix
  sketch in Step 3's "R8 fix details" block is replaced with the full
  chain-validation pseudocode rooted at the truck's
  `current_facility_id`. Single-row semantics unchanged.
  Null-`resource_id` semantics unchanged. The change is fully internal
  to `_reject_unavailable_resource`; the public validator interface is
  untouched. Commentary added explaining the three failure modes the
  validator catches: (i) cross-task collision, (ii) two-stop degenerate
  (no row matches `current_facility_id`), (iii) null-`resource_id`
  fallback to per-row check.
- **Applied MUST #1 (R8 risk-register tightening in §5):** R8
  Mitigation field rewritten in 3 sentences to describe the
  chain-validation algorithm (visited set seeded with
  `current_facility_id`, walk in dispatch order, accept whole route on
  success or fall back to per-row rejection on failure). Lists the four
  regression tests that lock the semantic.
- **Applied MUST #2 (regression tests in §3.A):** Added three new
  `test_dispatch_lifecycle.py` tests:
  `test_route_dispatch_rejected_when_no_row_matches_current_facility`
  (false-positive guard for chain validator),
  `test_route_dispatch_rejected_when_chain_breaks` (chain-break
  fall-back to per-row rejection), and
  `test_null_resource_id_falls_back_to_per_row_check` (null-resource_id
  semantics preserved). Pre-existing
  `test_route_dispatch_passes_validator` and
  `test_single_row_unavailable_resource_still_rejects` cover the other
  two cases. Step 3 test count updated from "ten tests" to "thirteen
  tests" (five `test_dispatch_lifecycle.py` regression tests).
- **Applied MUST #3 (open-questions.md contradictions):** Lines 15-17
  (`period_duration_hours` defaulting to 1.0) and lines 20-22
  (`dataloader.py` / `pipeline.py` deletion deferred) marked
  `[SUPERSEDED]` with strike-through markdown, plus a one-line pointer
  to plan §8 item 3 / item 5 as authoritative. Historical reasoning
  preserved for the trail.
- **Applied SHOULD #4 (belt-and-braces assertion):** Step 3 algorithm
  step 7 added — `RebalancerTask` asserts the emitted route rows form
  a chain rooted at the truck's `current_facility_id` before returning,
  raising on violation. Redundant with the validator but surfaces
  solver bugs at the Task level instead of the dispatch lifecycle.
- **Applied SHOULD #5 (`_reject_over_capacity` note):** The R8
  fix-details block now explicitly notes that
  `_reject_over_capacity` (`dispatch_lifecycle.py:285-311`) already
  groups by `resource_id` and remains correct under the new
  chain-validation semantics — no change needed there. Eliminates an
  obvious executor question.
- **Tension noted:** None new. The chain validator's "fall back to
  per-row check on chain failure" was a mild design choice between
  (a) reject the whole group on any chain break, vs (b) fall back to
  per-row rejection. Picked (b) because it preserves single-row
  semantics for the rows that *would* have passed the per-row check,
  meaning a partial co-grouping accident does not over-reject. The
  Architect's three failure modes (cross-task collision, two-stop
  degenerate, null `resource_id`) are all explicitly captured by the
  pseudocode and by the new regression tests.

