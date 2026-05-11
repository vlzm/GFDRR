# Project State

> Last updated: 2026-04-10

## Vision

A platform for modeling and optimizing logistics networks. The core is **Environment**: a space where commodities (bikes, goods, money) move through a network of facilities across time periods. Inside the Environment, tasks run (rebalancing, repair, dispatch), decisions are made, and the world state is updated.

The data model is domain-agnostic, built on a multi-commodity flow formulation (Williamson). Tabular structures for pandas/PySpark. The first domain is bike-sharing (Citi Bike-style).

### Two Levels of Tasks

**Operational (Environment)** — step-by-step simulation. The Environment advances through periods: period 0 → 1 → 2... At each step: trips occur, inventory is updated, tasks run (rebalancer at night, repair in the morning). The world state changes after each step. This is a digital twin.

**Strategic (Optimizer)** — a one-shot solve. Takes a year of data, formulates an LP/MILP, and the solver minimizes the cost function. All periods are visible at once — no step-by-step process. A separate consumer, not part of the Environment.

Both levels use the same `ResolvedModelData` but process it differently.

---

## Roadmap

1. ~~**Foundation** — data model, build pipeline, loader~~ ✓
2. ~~**Environment** — step-by-step engine, state management~~ ✓
3. ~~**Rebalancer** — first task inside Environment (multi-stop PDP)~~ ✓
4. **Trip Generator** — synthetic trip stream for simulation ← NEXT PHASE
5. **UI** — Environment visualization (Streamlit)
6. **Strategic Optimizer** — LP/MILP over the planning horizon (separate consumer)
7. **Infrastructure** — DB, API, Docker, CI/CD
8. **Cloud** — Azure deployment

Each phase starts with a design doc.

---

## Current Phase: Trip Generator (not started)

The next step is a synthetic trip stream on top of the existing historical replay (`HistoricalLatentDemandPhase`/`HistoricalODStructurePhase`). Starts with a design doc.

---

## Not Now

These components are planned but **NOT being worked on** in the current phase. Do not create files, write code, or set up infrastructure for these.

- **Trip Generator** — synthetic trip stream for simulation. Phase 4.
- **Strategic Optimizer** — LP/MILP over full planning horizon. Separate consumer, phase 6.
- **ML / forecasting** — demand forecasting, GNN for trip duration. See `docs/IDEAS.md`.
- **API** — FastAPI. Not needed until UI.
- **UI** — Streamlit. Phase 5.
- **Database** — PostgreSQL. Currently CSV/Parquet.
- **DevOps** — Docker, CI/CD. Running locally.
- **Cloud** — Azure, Terraform. Separate phase.
- **Observability** — OpenTelemetry. Not needed without production.
- **Multi-stop routes** — Task creating future dispatches across periods. Future extension.

---

## Data Model Design Decisions (completed)

These decisions were made during collaborative design sessions and should not be revisited without strong justification:

| # | Topic | Key Decision |
|---|-------|-------------|
| 1 | Temporal axis | Multi-resolution via segments; `PlanningHorizon` decoupled from scenario |
| 2 | Commodity transformation | N→M with 3 tables; `conversion_ratio` + `loss_rate` |
| 3 | Edge attributes | Multi-modal PK (`source × target × modal_type`); `lead_time_hours` in absolute units |
| 4 | Discrete constraints | Nullable = LP-compatible; `solver_config.solver_type` for LP vs MILP |
| 5 | Resource as full entity | `resource_fleet` (L2, aggregated) + `resource` (L3, optional instances) |
| 6 | Hierarchy and aggregation | Facility + commodity hierarchies; scenario-level aggregation intent |
| 7 | Attribute system | `AttributeRegistry` with grain, grain groups, spine assembly |
| 8 | Consumers model | One ResolvedModelData → Environment (step-by-step) / Optimizer (all-at-once) / Analytics (post-hoc) |

## Key Architecture Decisions

1. **Table-based data model** — all data in DataFrames for vectorized operations. Pandas now, PySpark-ready.
2. **L2/L3 entity split** — L2 categories always required, L3 instances optional (for individual tracking).
3. **Build pipeline as pure transformation** — `build_model(raw) → resolved` is stateless and deterministic.
4. **Attribute system over hardcoded columns** — costs, capacities via `AttributeRegistry` with explicit grain.
5. **Edge rules for declarative graph construction** — rules like "all stations ↔ all depots via road" instead of listing N² edges.
6. **Spine assembly** — pre-joined attribute DataFrames grouped by grain compatibility for consumer-ready data.
7. **Environment as core** — the platform is a digital twin / simulation environment. Optimizer is a separate, later consumer.
8. **Design doc before code** — each subsystem starts with a design doc, discussion, then implementation.

---

## Completed Phases

### Foundation

Core library `gbp` with data model, build pipeline, bike-sharing loader. All refactoring steps completed.

**What was built:**
- `gbp/core` — RawModelData (~48 tables), ResolvedModelData (~54 tables), Pydantic schemas, grouped table access, `table_summary()`
- `gbp/core/attributes` — AttributeRegistry with grain-aware registration, kind validation, grain groups, spine assembly
- `gbp/build` — `build_model()` pipeline: validation → time resolution → edge building → lead times → transformations → fleet capacity → spines
- `gbp/loaders` — DataLoaderMock, DataLoaderGraph, BikeShareSourceProtocol, GenericSourceProtocol, CsvLoader
- `gbp/core/factory` — `make_raw_model()` quick-start helper
- `gbp/io` — Parquet + JSON serialization with AttributeRegistry support
- `gbp/build/validation` — unit consistency, referential integrity, resource completeness, graph connectivity (BFS)
- `gbp/core/schemas/observations.py` — ObservedFlow, ObservedInventory schemas
- Observations (`observed_flow`, `observed_inventory`) integrated into model, build pipeline (time resolution, validation), and loader (trips → observed_flow, telemetry → observed_inventory, demand derivation)
- Refactoring: model.py grouping, `_build_raw_model()` decomposition, protocol separation, factory function — all done
- Tests: unit + integration, full pipeline coverage

**Architecture cleanup (2026-04-01):**
- `_ModelDataMixin` extracts shared properties/display/validation from RawModelData and ResolvedModelData (DRY)
- `gbp/core/columns.py` — centralized column-name constants
- `gbp/__init__.py` — public API surface (`build_model`, `Environment`, `make_raw_model`, enums)
- `gbp/loading/` removed, consolidated into `gbp/loaders/`
- `BuildError` wraps pipeline step exceptions with step name context
- `dispatch_phase.py` — 5 standalone validator functions, sequential inventory allocation (bugfix)
- `PeriodRow` construction fixed (keyword args instead of positional)
- `validation.py` — vectorized with set ops instead of iterrows
- `factory.py` — vectorized role/operation generation
- `time_resolution.py` — `pd.merge_asof` instead of period-loop (O(N log N) vs O(N×P))
- `lead_time.py` — `np.searchsorted` instead of triple loop (O(E×P×log P) vs O(E×P²))
- `pyproject.toml` — unused future deps moved to optional groups (api, db, observability, storage)
- `rebalancer/` marked as early prototype (will be redesigned as Task)

**Dead field cleanup (2026-04-10):**
- Removed all `*_unit` fields from schemas (demand, supply, inventory, edges, observations, parameters, pricing, output). Only `commodity_categories.unit` retained as single source of truth for measurement units.
- Removed `capacity_consumption` from `EdgeCommodity` (never read by any consumer).
- Removed `_check_unit_consistency` validation (no longer needed without per-table unit fields).
- Updated loaders, factory, build pipeline, test fixtures accordingly.

**Edge materialization boundary refactor (2026-04-12):**
- Moved edge materialization from loader to `build_model()`. Principle: Raw = declaration, Resolved = materialization.
- `DataLoaderGraph._build_edges()` replaced with `_build_distance_matrix()` — loader now produces `distance_matrix` (source_id, target_id, distance, duration) instead of `edges` + `edge_commodities`.
- New `DistanceMatrix` Pydantic schema; `distance_matrix` added as optional field to both `RawModelData` and `ResolvedModelData`.
- `_ensure_edges_and_commodities()` in pipeline now passes `raw.distance_matrix` to `build_edges()` — this was the existing fallback path, now promoted to primary.
- `edges`/`edge_commodities` remain as optional override fields in Raw (escape hatch for external pre-computed edge data).

**Mock surface cleanup — `df_inventory_ts` removal (2026-04-26):**
- Removed `df_inventory_ts` (the hourly MultiIndex ground-truth matrix) from `BikeShareSourceProtocol` and from both mocks. It was a mock-only fixture with no real-world counterpart in GBFS feeds.
- `DataLoaderMock` now exposes `inventory_initial` (long-format DataFrame: `facility_id, commodity_category, quantity`) directly, covering stations and depots. The hourly inventory matrix is still computed internally but only to seed the GBFS-like telemetry.
- `DataLoaderGraph._build_node_parameters()` now reads `source.inventory_initial` directly instead of slicing `df_inventory_ts.iloc[0]`.
- `gbp/rebalancer/dataloader.py` was the only consumer of the hourly matrix beyond `inventory_initial` seeding. It is now broken (`AttributeError` at `load_data`) and `tests/test_rebalancer.py` was deleted. The rebalancer was already marked as early prototype awaiting Task rewrite — this just makes the deprecation honest.
- Verification notebook: `notebooks/verify/07_inventory_initial_refactor.ipynb`.

### Environment

Step-by-step simulation engine on top of `ResolvedModelData`. Design doc: `docs/design/environment_design.md`.

**What was built:**
- `gbp/consumers/simulator/state.py` — `SimulationState` (frozen dataclass), `PeriodRow`, `init_state()`, vectorized resource generation from fleet
- `gbp/consumers/simulator/phases.py` — `Phase` Protocol, `PhaseResult`, `Schedule` (every, every_n, custom)
- `gbp/consumers/simulator/log.py` — `SimulationLog` (5 log tables), `RejectReason` enum
- `gbp/consumers/simulator/built_in_phases.py` — `DemandPhase` (demand → inventory consumption + unmet demand), `ArrivalsPhase` (in-transit → inventory + resource release), `OrganicDeparturePhase` + `OrganicArrivalPhase` (atomic split of `OrganicFlowPhase`: departure subtracts from source, arrival adds to target with clip; enables inserting rebalancer between them)
- `gbp/consumers/simulator/task.py` — `Task` Protocol, `DISPATCH_COLUMNS`
- `gbp/consumers/simulator/dispatch_phase.py` — `DispatchPhase` (auto-assign resources, 5-step validation, apply dispatches)
- `gbp/consumers/simulator/engine.py` — `Environment` class (run, step, step_phase)
- `gbp/consumers/simulator/config.py` — `EnvironmentConfig`
- `gbp/consumers/simulator/tasks/noop.py` — `NoopTask`
- Tests: unit (state, phases, log, built-in phases, dispatch, engine) + integration (full pipeline)
- Verification notebooks: `notebooks/verify/02_environment_skeleton.ipynb`, `notebooks/verify/08_organic_phase_split.ipynb`

**Key design decisions:**
- Immutable state via `with_*` methods
- Logical phases (DEMAND → ARRIVALS → DISPATCH), not temporal
- Three-layer: Phase → Task → Solver
- Instance-level resource tracking (position, status, available_at_period)
- Reject + log (no dispatch queue)
- MVP: current period only

### Rebalancer (multi-stop PDP Task)

Multi-stop pickup-and-delivery rebalancer wired into Environment via the existing `DispatchPhase(task)` adapter. Spec produced via `/deep-interview`, plan produced via ralplan consensus (3 iterations to APPROVE), executed via autopilot.

**What was built:**
- `gbp/consumers/simulator/tasks/rebalancer.py` — `RebalancerTask`, OR-Tools PDP solver, salvaged from the deprecated `gbp/rebalancer/` prototype.
- `gbp/consumers/simulator/built_in_phases.py::LatentDemandInflatorPhase` — multiplicative demand inflation between `HistoricalLatentDemandPhase` and `DeparturePhysicsPhase` for controlled-shortage experiments.
- `gbp/loaders/dataloader_mock.py` — added `n_trucks` and `truck_capacity_bikes` constructor parameters (default `n_trucks=0`, opt-in trucks).
- `gbp/consumers/simulator/dispatch_lifecycle.py::_reject_unavailable_resource` — extended to support multi-row route dispatches via **loose route validation**: a multi-row group sharing a non-null `resource_id` is accepted iff the resource is in the AVAILABLE pool. Single-row dispatches keep the strict per-row check.
- Tests: 4 mock-truck tests, 8 inflator tests, 5 lifecycle route-validation tests, 7 RebalancerTask tests, plus a fixture fix for `tests/conftest.py::loaded_graph_loader` (now passes `n_trucks=3` so legacy graph-loader tests still see truck rows).
- Verification notebook: `notebooks/verify/10_rebalancer_experiment.ipynb` — baseline vs treatment comparison with five tables (trips per date, trips per source station, lost demand, total distance / revenue proxy, combined summary).
- Removed: `gbp/rebalancer/` package (broken since the `df_inventory_ts` cleanup), `notebooks/03_test_rebalancer.ipynb`, `tests/test_rebalancer.py`.

**Key design decisions:**
- One DISPATCH_COLUMNS row per pickup-delivery pair; all rows for one truck share `arrival_period = current + ceil(total_route_time_hours / period_duration_hours)`.
- Schedule: `Schedule.every_n(N)` (default N=12 at hourly periods).
- Pickup-delivery rows do NOT form a physical traversal chain — a row's `source_id` is a pickup station, not the previous row's target. The validator therefore uses **loose route validation** (`resource_id ∈ AVAILABLE`) rather than chain-rooted validation. Documented in `.omc/plans/rebalancer-pdp-task.md` §11 (Iteration 4 pivot).
- Belt-and-braces in `RebalancerTask` asserts every emitted `resource_id` is non-null and known in the available truck pool.
- `pdp_random_seed` (default 42) is forwarded to OR-Tools `RoutingSearchParameters.random_seed` for deterministic baseline-vs-treatment comparisons.

---

## File Map

| Need to understand... | Read this |
|----------------------|-----------|
| Table schemas and structure | `gbp/core/model.py` + `gbp/core/schemas/` |
| Data model design rationale | `docs/design/graph_data_model.md` |
| Build pipeline flow | `gbp/build/pipeline.py` + `docs/diagrams/08_build_pipeline.mermaid` |
| Observations design | `docs/design/observations_design.md` |
| Attribute system design | `docs/design/attribute_system.md` |
| Environment design | `docs/design/environment_design.md` |
| Environment code | `gbp/consumers/simulator/` (state, phases, engine, built_in_phases, dispatch_phase) |
| Architecture overview (visual) | `docs/architecture_diagrams.md` (diagrams 13-14 for Environment) |
| Storytelling guides (onboarding) | `docs/story_telling/` (01_graph_data_model, 02_attribute_system, 03_environment) |
| Data journey end-to-end | `docs/DATA_JOURNEY.md` |
| Refactoring plan | `docs/design/refactoring.md` |
| Repository structure | `docs/repo_struct.md` |
| Future ideas (ML, GNN, etc.) | `docs/IDEAS.md` |
| Project vision and roadmap | `PROJECT.md` |
| This file | `PROJECT_STATE.md` |
