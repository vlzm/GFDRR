# Deep Interview Spec: Rebalancer (multi-stop PDP Task inside Environment)

## Metadata
- Interview ID: rebalancer-2026-05-01
- Rounds: 7
- Final Ambiguity Score: 15%
- Type: brownfield
- Generated: 2026-05-01
- Threshold: 20%
- Initial Context Summarized: no
- Status: PASSED

## Clarity Breakdown
| Dimension | Score | Weight | Weighted |
|-----------|-------|--------|----------|
| Goal Clarity | 0.90 | 0.35 | 0.315 |
| Constraint Clarity | 0.85 | 0.25 | 0.213 |
| Success Criteria | 0.80 | 0.25 | 0.200 |
| Context Clarity | 0.85 | 0.15 | 0.128 |
| **Total Clarity** | | | **0.855** |
| **Ambiguity** | | | **0.145** |

## Goal

Implement a `RebalancerTask` that runs an OR-Tools Pickup-and-Delivery Problem (PDP) solver
on the current `state.inventory` snapshot, and produces dispatches that move bikes between
stations using trucks as resources. The task is wired into `Environment` via the existing
`DispatchPhase(task)` adapter, scheduled to fire every N periods (default N=12), and inserted
into the phase list after `ArrivalsPhase` so it operates on the finalized end-of-period
inventory.

The deliverable also includes a controlled-demand experiment in
`notebooks/verify/10_rebalancer_experiment.ipynb` that demonstrates rebalancing increases
fulfilled demand on a pipeline where artificial latent-demand inflation forces lost trips.

## Constraints

- **Phase contract.** `RebalancerTask` implements the `Task` Protocol
  (`gbp/consumers/simulator/task.py:32`) and returns a DataFrame with the canonical
  `DISPATCH_COLUMNS = [source_id, target_id, commodity_category, quantity, resource_id,
  modal_type, arrival_period]`. No extension of the Task interface, of `DispatchPhase`,
  or of `ArrivalsPhase`.
- **Multi-stop mapping (Round 5 decision A).** A multi-stop PDP route on a single truck
  (`depot → pickup_a → pickup_b → ... → delivery_x → delivery_y → depot`) is emitted as
  one DISPATCH_COLUMNS row per pickup-delivery pair. All rows belonging to the same truck
  share the same `arrival_period = current_period_index + ceil(total_route_time_hours /
  period_duration_hours)`. Bikes appear at delivery stations simultaneously when the truck
  returns to the depot.
- **Schedule (Round 4 decision C).** `RebalancerPhase` uses `Schedule.every_n(N)` with N
  configurable; default N=12 (twice per day at 1-hour periods).
- **Algorithm (Round 3 decision B).** Reuse the OR-Tools PDP idea from `gbp/rebalancer/`
  but rewrite as a Task. The existing prototype is broken (depends on removed
  `df_inventory_ts`, `gbp/rebalancer/__init__.py:11`) and must be repaired or salvaged for
  parts. Specifically:
  - `gbp/rebalancer/demand.py` (utilization thresholds, source/destination derivation)
    can be reused with a new dataloader.
  - `gbp/rebalancer/routing/solver.py` (OR-Tools PDP model build + solve) can be reused.
  - `gbp/rebalancer/dataloader.py` and `gbp/rebalancer/pipeline.py` are obsolete in the
    new flow — replaced by the Task wrapper that reads `state` and `resolved` directly.
- **Demand inflation (Round 6 decision B).** New phase `LatentDemandInflatorPhase`
  inserted between `HistoricalLatentDemandPhase` and `DeparturePhysicsPhase`. Reads
  `state.intermediates["latent_demand"]`, multiplies `latent_departures` (and optionally
  `latent_arrivals`) by a configurable scalar or per-facility dict, writes back. No
  modification of `resolved.observed_flow`.
- **Departure-physics mode.** Both baseline and treatment scenarios MUST use
  `DeparturePhysicsPhase(mode="strict")`. In permissive mode `lost_demand` is identically
  zero (`built_in_phases.py:480`), so the experiment cannot detect any rebalancing
  benefit.
- **Truck resource lifecycle.** Each truck is a resource in `state.resources` with status
  cycling AVAILABLE → IN_TRANSIT → AVAILABLE. With multiple dispatch rows sharing the same
  `resource_id` and the same `arrival_period`, `ArrivalsPhase` releases the truck once
  when all its rows arrive together (`built_in_phases.py:632-646`).
- **No multi-stop across periods.** A single PDP solve produces a single round-trip route
  starting and ending at the depot within at most one rebalance interval. The truck is
  not re-routed mid-route. (The `total_route_time` may legitimately span multiple periods;
  bikes simply arrive at the period that contains the truck's depot return.)
- **Output location.** All summary tables and comparison logic live in
  `notebooks/verify/10_rebalancer_experiment.ipynb`. No new public modules in `gbp/` for
  analytics.
- **Code style.** Vectorized pandas where possible. Strict typing. Google-style
  docstrings. English only in code/comments. Plain-language Russian for user
  communication in the notebook markdown cells.

## Non-Goals

- **No UI.** Streamlit/web UI is deferred to Phase 5 per `PROJECT_STATE.md:52` and
  `CLAUDE.md`. Visual comparison happens via matplotlib charts inside the notebook.
- **No new public API in `gbp/` for summary metrics.** Aggregations stay in the notebook
  until they prove durable.
- **No extension of `DISPATCH_COLUMNS` / `Task` / `DispatchPhase` / `ArrivalsPhase`.**
- **No real-time / multi-period rerouting.** Trucks commit to a route at solve time.
- **No synthetic demand generator (Trip Generator, Phase 4).** Demand inflation is
  multiplicative on top of historical `observed_flow`.
- **No new `LogTableSchema` entry.** Existing 5-table log is sufficient; summaries are
  derived post-hoc.
- **No strategic LP/MILP optimizer integration.** Optimizer is Phase 6.

## Acceptance Criteria

- [ ] `gbp/consumers/simulator/tasks/rebalancer.py` exists and implements the `Task`
  Protocol. `RebalancerTask.run(state, resolved, period)` returns a DataFrame with the
  exact `DISPATCH_COLUMNS` columns.
- [ ] When wrapped in `DispatchPhase(RebalancerTask(...))` and added to `phases`, the
  full Environment loop runs end-to-end on the bike-share mock fixture without errors.
- [ ] `gbp/consumers/simulator/built_in_phases.py` exposes a new
  `LatentDemandInflatorPhase` class with constructor signature
  `LatentDemandInflatorPhase(multiplier: float | dict[str, float] = 1.0,
  schedule: Schedule | None = None)`. The phase reads
  `state.intermediates["latent_demand"]`, multiplies `latent_departures` (and optionally
  `latent_arrivals`) by the multiplier, writes back to intermediates. Multiplier=1.0 is a
  no-op pass-through.
- [ ] `notebooks/verify/10_rebalancer_experiment.ipynb` exists and runs to completion. It
  builds two `Environment` configs (baseline = no rebalancer, treatment = with
  rebalancer), runs each, and produces three output tables:
    - `trips_per_date_per_station`: columns
      `[date, period_id, source_id, target_id, trips, commodity_category]`,
      derived from `simulation_flow_log`.
    - `summary_per_date`: columns
      `[date, total_trips, total_distance, total_lost_demand, revenue]`, where
      `revenue = total_distance` for v1, joined from `resolved.distance_matrix`.
    - `comparison`: rows = metrics, columns =
      `[baseline, with_rebalancer, delta, delta_pct]`.
- [ ] The notebook also produces two matplotlib charts:
    - Bar chart `trips_per_date` for baseline vs treatment side-by-side.
    - Heatmap `delta_per_station` (lost demand reduction by station × date).
- [ ] On the chosen experiment configuration (multiplier > 1.0, strict mode), the
  treatment run shows `total_fulfilled_demand_treatment > total_fulfilled_demand_baseline`
  AND `total_lost_demand_treatment < total_lost_demand_baseline` for at least one date.
  This is the primary success metric.
- [ ] OR-Tools PDP solver code from `gbp/rebalancer/routing/` is repaired (or replaced by
  a fresh implementation that draws on the same idea) and integrated into `RebalancerTask`.
- [ ] Truck-resource infrastructure exists in the bike-share loader output (or in test
  fixtures): a truck resource_category in `resource_fleet`, station↔station edges with
  `modal_type="truck"` and `lead_time_hours`, depot facility with starting trucks,
  resource_modal_compatibility and resource_commodity_compatibility entries. Specific
  approach (extend `dataloader_graph.py` vs. fixture-only) is an implementation choice
  but the outcome must be a fully resolved model where `state.resources` contains at
  least one truck at simulation start.
- [ ] Unit tests for `LatentDemandInflatorPhase` (multiplier=1.0 is identity; scalar
  multiplier scales correctly; per-facility dict targets correct rows).
- [ ] Unit tests for `RebalancerTask` (returns valid DISPATCH_COLUMNS schema; respects
  truck capacity; respects available resources; returns empty DataFrame when no
  imbalance).
- [ ] `PROJECT_STATE.md` updated to reflect that "Multi-stop routes" was promoted from
  "Future extension" to delivered scope, and that Rebalancer phase moves from "next
  phase" to "in progress / done".

## Assumptions Exposed & Resolved

| Assumption | Challenge | Resolution |
|------------|-----------|------------|
| User said "новая фаза" — interpreted as bare `Phase`, not `DispatchPhase(Task)`. | The codebase, project state, and the deliberate `OrganicFlowPhase` split all point at Task. Round 1 framed the trade-off. | Round 3 decision B confirmed: Task wrapped in DispatchPhase. |
| Rebalancing is instantaneous (teleport between stations). | Round 2 surfaced the alternative: through-time via truck resource and `state.in_transit`. | Round 2 decision B: through-time, trucks consume time and lead-time edges. |
| Algorithm is a simple utilization-threshold heuristic. | Round 3 surfaced reuse of the existing OR-Tools PDP prototype despite its broken state. | Round 3 decision B: reuse OR-Tools PDP, accept multi-stop scope expansion. |
| Rebalancing fires every period. | Round 4 surfaced cost: PDP per period × hundreds of periods is expensive. | Round 4 decision C: every N periods, default N=12. |
| Multi-stop routes can extend `DISPATCH_COLUMNS` with extra columns. | Round 5 surfaced contract impact and `ArrivalsPhase` resource-release behavior. | Round 5 decision A: one row per pickup-delivery pair, shared `arrival_period` at depot return. No contract change. |
| Demand can be artificially inflated by editing `resolved.observed_flow`. | Round 6 surfaced cleaner phase-based mechanism. | Round 6 decision B: `LatentDemandInflatorPhase` between latent-demand and physics phases. |
| `DeparturePhysicsPhase(mode="permissive")` is fine for the experiment. | `permissive` makes `lost_demand` always zero — experiment cannot detect benefit. | Both scenarios must run with `mode="strict"`. |
| Output tables live in `gbp/` as a public helper module. | Premature API surface. Aggregations may not stabilize until first experiment. | Round 7 decision A: notebook-only for v1. |
| UI is required (per the original brief). | `CLAUDE.md` and `PROJECT_STATE.md:52` forbid UI work in the current phase. | Default I: UI deferred to Phase 5. Visual comparison via matplotlib in the notebook. |

## Technical Context

### Existing baseline pipeline (from notebook 09)

```python
phases=[
    HistoricalLatentDemandPhase(),
    HistoricalODStructurePhase(),
    DeparturePhysicsPhase(mode="permissive"),  # MUST switch to "strict" for the experiment
    HistoricalTripSamplingPhase(),
    ArrivalsPhase(),
]
```

### Target baseline (no rebalancer)

```python
phases=[
    HistoricalLatentDemandPhase(),
    LatentDemandInflatorPhase(multiplier=2.0),
    HistoricalODStructurePhase(),
    DeparturePhysicsPhase(mode="strict"),
    HistoricalTripSamplingPhase(),
    ArrivalsPhase(),
]
```

### Target treatment (with rebalancer)

```python
phases=[
    HistoricalLatentDemandPhase(),
    LatentDemandInflatorPhase(multiplier=2.0),
    HistoricalODStructurePhase(),
    DeparturePhysicsPhase(mode="strict"),
    HistoricalTripSamplingPhase(),
    ArrivalsPhase(),
    DispatchPhase(
        RebalancerTask(
            min_threshold=0.3,
            max_threshold=0.7,
            time_limit_seconds=30,
        ),
        schedule=Schedule.every_n(12),
    ),
]
```

### Key code references

- Task contract: `gbp/consumers/simulator/task.py:20` (DISPATCH_COLUMNS), `:32` (Protocol).
- DispatchPhase adapter: `gbp/consumers/simulator/dispatch_phase.py:54`.
- Schedule helpers: `gbp/consumers/simulator/phases.py:104`.
- DeparturePhysics modes: `gbp/consumers/simulator/built_in_phases.py:479-485`.
- Resource release on arrival: `gbp/consumers/simulator/built_in_phases.py:632-646`.
- OR-Tools PDP prototype (broken): `gbp/rebalancer/routing/solver.py`, contracts at
  `gbp/rebalancer/contracts.py:58`.
- Demand inflation target: `state.intermediates["latent_demand"]` set by
  `HistoricalLatentDemandPhase` (`built_in_phases.py:328`).

### Implementation risks (must be triaged in Phase 1 of execution)

1. **Truck resource not in current `resolved.*` for bike-share.** Verify
   `gbp/loaders/dataloader_graph.py` produces a `resource_fleet` row for trucks. If not,
   either extend the loader (proper fix) or seed via test fixtures (faster). Determines
   whether `state.resources` has any truck at all.
2. **Truck-modal edges.** `distance_matrix` exists with raw distance/duration, but the
   build pipeline must materialize edges of `modal_type="truck"` between stations and
   between depot and stations, with `lead_time_hours` derived from
   `distance/avg_truck_speed_kmh`. Check `gbp/build/edge_builder.py` rules.
3. **OR-Tools call performance.** With 50-100 stations × N=12 schedule × 30-day
   simulation, that's ~60 PDP solves. `time_limit_seconds=30` should be enough; verify.
4. **`gbp/rebalancer/dataloader.py` is broken.** Its replacement reads from
   `state.inventory` + `resolved.distance_matrix` directly inside `RebalancerTask.run()`.
   The old `gbp/rebalancer/pipeline.py` is not reused as-is.

## Ontology (Key Entities)

| Entity | Type | Fields | Relationships |
|--------|------|--------|---------------|
| RebalancerTask | core domain | name, min_threshold, max_threshold, time_limit_seconds | implements Task; produced by `gbp/consumers/simulator/tasks/rebalancer.py` |
| LatentDemandInflatorPhase | core domain | multiplier (scalar or dict), schedule | reads/writes `state.intermediates["latent_demand"]`; lives in `built_in_phases.py` |
| DispatchPhase wrapping RebalancerTask | core domain | name, task, schedule | the `Phase` actually placed in `EnvironmentConfig.phases` |
| Truck | supporting | resource_category, capacity, current_facility_id, status | a row in `state.resources`; bound to a `resource_category="truck"` from `resolved.resource_fleet` |
| Depot | supporting | facility_id, facility_type="depot" | starting and ending node of every PDP route |
| Station | supporting | facility_id, capacity, current inventory | source or destination in PDP pairs |
| PDP Route | supporting | resource_id, ordered list of (node, action, qty) | output of OR-Tools solver, translated into DISPATCH_COLUMNS rows |
| LatentDemand | supporting | facility_id, commodity_category, latent_departures, latent_arrivals | published by `HistoricalLatentDemandPhase`, mutated by `LatentDemandInflatorPhase`, consumed by `DeparturePhysicsPhase` |
| LostDemand | supporting | facility_id, latent, realized, lost | emitted by `DeparturePhysicsPhase(mode="strict")`; primary success-criterion metric |
| ExperimentNotebook | external | notebooks/verify/10_rebalancer_experiment.ipynb | hosts baseline vs treatment runs, summary tables, charts |

## Ontology Convergence

| Round | Entity Count | New | Changed | Stable | Stability Ratio |
|-------|-------------|-----|---------|--------|----------------|
| 1 | 4 (RebalancerTask, RebalancerPhase, OrganicDeparture/ArrivalPhase, DispatchPhase) | 4 | - | - | - |
| 2 | 5 (+ Truck, in_transit) | 2 | 0 | 3 | 60% |
| 3 | 6 (+ PDP Route, ORTools solver) | 2 | 0 | 4 | 67% |
| 4 | 6 (+ Schedule.every_n) | 1 | 0 | 5 | 83% |
| 5 | 7 (+ DISPATCH_COLUMNS row, multi-stop pair mapping) | 1 | 0 | 6 | 86% |
| 6 | 9 (+ LatentDemandInflatorPhase, LatentDemand) | 2 | 0 | 7 | 78% |
| 7 | 10 (+ ExperimentNotebook) | 1 | 0 | 9 | 90% |

Ontology converged: same core entity set across last two rounds, only the experiment
shell was added at the end.

## Interview Transcript

<details>
<summary>Full Q&A (7 rounds)</summary>

### Round 1 — Phase vs Task abstraction
**Q:** A) RebalancerTask in DispatchPhase / B) standalone RebalancerPhase / C) hybrid?
Plus: instant teleport vs through-time-via-truck?
**A:** "Вот базовый сценарий [Historical* phase chain]. Вот после этих фаз надо создать
новую фазу" — placement specified (after `ArrivalsPhase`), abstraction not directly chosen.
**Ambiguity:** 49% (Goal 0.55, Constraints 0.40, Criteria 0.45, Context 0.70).

### Round 2 — Time semantics
**Q:** A) Instant teleport (RebalancerPhase, direct inventory mutation) / B) Through-time
truck (RebalancerTask + DispatchPhase + in_transit) / C) Hybrid (instant + distance log)?
**A:** B.
**Ambiguity:** 37% (Goal 0.85, Constraints 0.45, Criteria 0.45, Context 0.75).

### Round 3 — Algorithm
**Q:** A) Threshold heuristic / B) OR-Tools PDP from prototype / C) Other?
**A:** B. "Мне это нужно уже сейчас. Но да, код сломан. Но общую суть и идею можно взять
оттуда." Acknowledged the prototype is broken; accepted scope expansion despite
`PROJECT_STATE.md:57` calling multi-stop a future extension.
**Ambiguity:** 33% (Goal 0.85, Constraints 0.55, Criteria 0.45, Context 0.80).

### Round 4 — Schedule
**Q:** A) Every period / B) Daily fixed / C) Every N periods / D) Threshold-triggered?
**A:** C. N kept configurable, default N=12 proposed.
**Ambiguity:** 31% (Goal 0.85, Constraints 0.65, Criteria 0.45, Context 0.80).

### Round 5 — Multi-stop → DISPATCH_COLUMNS mapping
**Q:** A) One row per pickup-delivery pair, shared `arrival_period` at depot return /
B) Per-leg arrival_period + custom resource-release / C) Extend DISPATCH_COLUMNS /
D) Super-dispatch?
**A:** A.
**Ambiguity:** 25% (Goal 0.90, Constraints 0.80, Criteria 0.45, Context 0.85).

### Round 6 — Demand inflation mechanism
**Q:** A) Pre-process `resolved.observed_flow` / B) `LatentDemandInflatorPhase` /
C) `SyntheticDemandPhase` / D) Loader hook?
**A:** B. Strict-mode confirmation deferred (declared as required by spec).
**Ambiguity:** 18% (Goal 0.90, Constraints 0.85, Criteria 0.65, Context 0.85).

### Round 7 — Output tables location
**Q:** A) Notebook only / B) Helper module in `gbp/` / C) New LogTableSchema?
Side: UI defer? strict mode? date granularity?
**A:** A. Side defaults applied: UI = defer to Phase 5; strict mode = required; date
granularity = both period_id and date columns.
**Ambiguity:** 15% (Goal 0.90, Constraints 0.85, Criteria 0.80, Context 0.85).

</details>
