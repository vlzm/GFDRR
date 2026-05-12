# Cleanup Plan: Vertical Citi Bike Minimum

> Date: 2026-05-12
>
> Criterion: everything not required by `notebooks/canonical_scenario.ipynb` must be removed.

---

## 1. Canonical scenario dependency tree

The notebook `canonical_scenario.ipynb` imports and calls:

```
DataLoaderMock → DataLoaderGraph → RawModelData
                                      ↓
                                  build_model()
                                      ↓
                                ResolvedModelData
                                      ↓
                              init_state() → SimulationState
                                      ↓
                              Environment(config, phases).run()
                                      ↓
                              SimulationLog.to_dataframes()
```

### Phases used in canonical scenario

| Phase | Module |
|---|---|
| `HistoricalLatentDemandPhase` | `built_in_phases.py` |
| `HistoricalODStructurePhase` | `built_in_phases.py` |
| `DeparturePhysicsPhase` | `built_in_phases.py` |
| `HistoricalTripSamplingPhase` | `built_in_phases.py` |
| `ArrivalsPhase` | `built_in_phases.py` |
| `OverflowRedirectPhase` | `built_in_phases.py` |
| `DispatchPhase` + `RebalancerTask` | `dispatch_phase.py`, `tasks/rebalancer.py` |
| `InvariantCheckPhase` | `built_in_phases.py` |

### Phases defined but NOT used

| Phase | Status |
|---|---|
| `LatentDemandInflatorPhase` | Imported in notebook but not added to `phases_canonical` — dead import |
| `EndOfPeriodDeficitPhase` | Imported in notebook but not added to `phases_canonical` — dead import |

---

## 2. Files: reachable vs unreachable

### 2.1 Reachable from canonical scenario (keep)

**Loaders:**
- `gbp/loaders/contracts.py` — `GraphLoaderConfig`
- `gbp/loaders/dataloader_graph.py` — `DataLoaderGraph`
- `gbp/loaders/dataloader_mock.py` — `DataLoaderMock`
- `gbp/loaders/protocols.py` — `BikeShareSourceProtocol` (used in `dataloader_graph.py`)

**Build pipeline:**
- `gbp/build/pipeline.py` — `build_model()`
- `gbp/build/_helpers.py`
- `gbp/build/defaults.py`
- `gbp/build/edge_builder.py`
- `gbp/build/fleet_capacity.py`
- `gbp/build/lead_time.py`
- `gbp/build/report.py`
- `gbp/build/spine.py`
- `gbp/build/time_resolution.py`
- `gbp/build/transformation.py`
- `gbp/build/validation.py`

**Core:**
- `gbp/core/model.py` — `RawModelData`, `ResolvedModelData`
- `gbp/core/enums.py`
- `gbp/core/roles.py`
- `gbp/core/attributes/registry.py`
- `gbp/core/attributes/spec.py`
- `gbp/core/attributes/builder.py`
- `gbp/core/attributes/defaults.py`
- `gbp/core/attributes/grain_groups.py`
- `gbp/core/attributes/merge_plan.py`

**Schemas** (used in the `_SCHEMAS` dict of `model.py` for column validation):
- `gbp/core/schemas/entity.py`
- `gbp/core/schemas/temporal.py`
- `gbp/core/schemas/behavior.py`
- `gbp/core/schemas/edge.py`
- `gbp/core/schemas/demand_supply.py`
- `gbp/core/schemas/observations.py`
- `gbp/core/schemas/transformation.py`
- `gbp/core/schemas/resource.py`

**Simulator:**
- `gbp/consumers/simulator/engine.py` — `Environment`
- `gbp/consumers/simulator/config.py` — `EnvironmentConfig`
- `gbp/consumers/simulator/state.py` — `init_state`, `SimulationState`
- `gbp/consumers/simulator/built_in_phases.py`
- `gbp/consumers/simulator/phases.py` — `Schedule`, `Phase`
- `gbp/consumers/simulator/dispatch_phase.py` — `DispatchPhase`
- `gbp/consumers/simulator/dispatch_lifecycle.py`
- `gbp/consumers/simulator/inventory.py`
- `gbp/consumers/simulator/log.py` — `SimulationLog`
- `gbp/consumers/simulator/task.py`
- `gbp/consumers/simulator/_period_helpers.py`
- `gbp/consumers/simulator/exceptions.py`
- `gbp/consumers/simulator/tasks/rebalancer.py` — `RebalancerTask`
- `gbp/consumers/simulator/tasks/rebalancer_planner.py`

### 2.2 Unreachable (removal candidates)

| File | Reason |
|---|---|
| `gbp/core/columns.py` | Not imported anywhere except its own docstring example |
| `gbp/core/factory.py` | Only in `__init__.py` re-exports; canonical scenario does not use `make_raw_model` |
| `gbp/core/schemas/hierarchy.py` | 8 classes for hierarchies — canonical scenario does not use hierarchies |
| `gbp/core/schemas/scenario.py` | `Scenario`, `ScenarioEdgeRules`, `ScenarioParameterOverrides` unused; `ScenarioManualEdges` is used in `pipeline.py` |
| `gbp/core/schemas/parameters.py` | Attribute schemas — not used in model |
| `gbp/core/schemas/pricing.py` | Pricing schemas — not used in model |
| `gbp/core/schemas/output.py` | Output schemas — not used in model |
| `gbp/io/__init__.py` | Serialization package not in canonical path |
| `gbp/io/dict_io.py` | Dict serialization — not in canonical path |
| `gbp/io/parquet.py` | Parquet serialization — not in canonical path |
| `gbp/loaders/csv_loader.py` | CSV loader — canonical scenario uses mock |
| `gbp/loaders/dataloader_mock_minimal.py` | Legacy minimal mock — canonical scenario uses `DataLoaderMock` |
| `gbp/loaders/validators.py` | Only used by `csv_loader.py` — removed together |
| `gbp/consumers/simulator/random.py` | Not imported by any reachable module |
| `gbp/consumers/simulator/tasks/noop.py` | `NoOpTask` not used in canonical scenario |

---

## 3. Files for full removal

| # | File | Rationale |
|---|---|---|
| 1 | `gbp/core/columns.py` | Dead code: no module imports it |
| 2 | `gbp/core/factory.py` | `make_raw_model()` not in canonical path |
| 3 | `gbp/core/schemas/hierarchy.py` | 8 hierarchy schemas unused by canonical scenario |
| 4 | `gbp/core/schemas/parameters.py` | Parameter schemas not referenced in model |
| 5 | `gbp/core/schemas/pricing.py` | Pricing schemas not referenced in model |
| 6 | `gbp/core/schemas/output.py` | Output schemas not referenced in model |
| 7 | `gbp/io/__init__.py` | Entire `gbp/io/` package unused |
| 8 | `gbp/io/dict_io.py` | See above |
| 9 | `gbp/io/parquet.py` | See above |
| 10 | `gbp/loaders/csv_loader.py` | CSV loader not in canonical path |
| 11 | `gbp/loaders/dataloader_mock_minimal.py` | Legacy mock loader |
| 12 | `gbp/loaders/validators.py` | Only used by `csv_loader.py` |
| 13 | `gbp/consumers/simulator/random.py` | Not imported by reachable code |
| 14 | `gbp/consumers/simulator/tasks/noop.py` | `NoOpTask` unused |

**Total: 14 files for removal.**

---

## 4. Files to simplify

| # | File | What to remove |
|---|---|---|
| 1 | `gbp/core/model.py` | Remove 14 fields (see section 5). Remove corresponding entries from `_GROUPS`, `_SCHEMAS`, `_CONSUMER_NORMALIZED_TABLES`. Remove imports of deleted schemas. Remove `hierarchy_tables` and `scenario_tables` properties from `_ModelDataMixin`. Remove passing of deleted fields in `ResolvedModelData.from_raw()` |
| 2 | `gbp/core/schemas/__init__.py` | Remove re-exports of deleted schemas: all hierarchy classes, `Scenario`, `ScenarioEdgeRules`, `ScenarioParameterOverrides`, all output/parameters/pricing classes |
| 3 | `gbp/core/schemas/scenario.py` | Keep only `ScenarioManualEdges` (used in `pipeline.py:70`). Remove `Scenario`, `ScenarioEdgeRules`, `ScenarioParameterOverrides` |
| 4 | `gbp/loaders/protocols.py` | Remove `GenericSourceProtocol` (marked "aspirational — for future domain-agnostic loaders"). Keep `BikeShareSourceProtocol` and `GraphLoaderProtocol` |
| 5 | `gbp/__init__.py` | Remove `make_raw_model` import from `factory`. Remove `gbp.io` mention from docstring |
| 6 | `gbp/core/__init__.py` | Remove `make_raw_model` re-export |
| 7 | `gbp/loaders/__init__.py` | Remove re-exports of `csv_loader`, `dataloader_mock_minimal`, `validators` |
| 8 | `gbp/consumers/simulator/tasks/__init__.py` | Remove `NoOpTask` re-export if present |
| 9 | `gbp/consumers/simulator/built_in_phases.py` | Check: if `LatentDemandInflatorPhase` and `EndOfPeriodDeficitPhase` are not used in canonical scenario or tests — consider removal. Keep for now, but remove dead imports from notebook |
| 10 | `notebooks/canonical_scenario.ipynb` | Remove dead imports: `LatentDemandInflatorPhase`, `EndOfPeriodDeficitPhase` |

---

## 5. Schema fields to remove from `_TabularModelBase`

### Hierarchies (8 fields) — canonical scenario does not use hierarchy navigation

| Field | Defined at | Read in canonical path |
|---|---|---|
| `facility_hierarchy_types` | `model.py:533` | nowhere |
| `facility_hierarchy_levels` | `model.py:534` | nowhere |
| `facility_hierarchy_nodes` | `model.py:535` | nowhere |
| `facility_hierarchy_memberships` | `model.py:536` | nowhere |
| `commodity_hierarchy_types` | `model.py:537` | nowhere |
| `commodity_hierarchy_levels` | `model.py:538` | nowhere |
| `commodity_hierarchy_nodes` | `model.py:539` | nowhere |
| `commodity_hierarchy_memberships` | `model.py:540` | nowhere |

### Scenarios (3 fields) — keep `scenario_manual_edges`, remove the rest

| Field | Defined at | Read in canonical path |
|---|---|---|
| `scenarios` | `model.py:543` | nowhere |
| `scenario_edge_rules` | `model.py:544` | nowhere |
| `scenario_parameter_overrides` | `model.py:546` | nowhere |

### Other unused (3 fields)

| Field | Defined at | Read in canonical path |
|---|---|---|
| `commodities` | `model.py:489` | nowhere (L3 instance-level; `commodity_categories` covers the needs) |
| `edge_vehicles` | `model.py:508` | nowhere |
| `resource_availability` | `model.py:530` | nowhere |

### Write-only artifacts in `ResolvedModelData` — consider removal

These fields are generated by `build_model()` but no consumer (simulator, analytics) reads them:

| Field | Generated in | Read by consumer |
|---|---|---|
| `edge_lead_time_resolved` | `build/lead_time.py` | nowhere in `consumers/` |
| `transformation_resolved` | `build/transformation.py` | nowhere in `consumers/` |
| `fleet_capacity` | `build/fleet_capacity.py` | nowhere in `consumers/` |
| `facility_spines` | `build/spine.py` | nowhere in `consumers/` |
| `edge_spines` | `build/spine.py` | nowhere in `consumers/` |
| `resource_spines` | `build/spine.py` | nowhere in `consumers/` |
| `build_report` | `build/report.py` | nowhere in `consumers/` |

> **Decision:** these fields are tied to the build pipeline, which currently works.
> Removing them means removing the corresponding steps from `build_model()`:
> `resolve_lead_times()`, `resolve_transformations()`, `compute_fleet_capacity()`,
> `assemble_spines()`. This is a separate task — first verify that the canonical
> scenario runs without these steps. Marked as **Phase 2**.

**Phase 1 total: 14 fields for removal.**

---

## 6. Tests for removal

The `tests/` directory contains only an empty `__init__.py` — no tests exist. Nothing to remove.

---

## 7. Documents and notebooks

| File | Action | Rationale |
|---|---|---|
| `docs/design_documents/environment.md` | **Keep** | Describes the Environment architecture used by canonical scenario |
| `docs/deprecated/graph_data_model.md` | **Already in deprecated** | Describes domain-agnostic model — correctly tagged |
| `docs/deprecated/architecture/уровни_понимания_кода.md` | **Already in deprecated** | Outdated approach |
| `docs/deprecated/architecture/уровни_понимания_кода_диаграммы.md` | **Already in deprecated** | Outdated approach |
| `notebooks/workbook_mock.ipynb` | **Consider removal** | Duplicates canonical_scenario.ipynb; if it differs — consolidate into one notebook |

---

## 8. Execution order

### Phase 1 — Dead code removal (safe, does not break canonical scenario)

1. Remove 14 files from section 3
2. Remove 14 fields from `model.py` (section 5, excluding write-only artifacts)
3. Update `_GROUPS`, `_SCHEMAS`, imports in `model.py`
4. Simplify `schemas/__init__.py`, `schemas/scenario.py`
5. Simplify `__init__.py` files (section 4, items 5-8)
6. Simplify `protocols.py` — remove `GenericSourceProtocol`
7. Remove dead imports from `canonical_scenario.ipynb`
8. Run canonical scenario — verify it works
9. Run `ruff check`, `mypy` — verify clean

### Phase 2 — Write-only artifact removal (requires verification)

1. Check: can `resolve_lead_times()` be removed from pipeline
2. Check: can `resolve_transformations()` be removed from pipeline
3. Check: can `compute_fleet_capacity()` be removed from pipeline
4. Check: can `assemble_spines()` be removed from pipeline
5. For each — if removing the step from pipeline, remove field and build module file
6. Run canonical scenario after each removal

### Phase 3 — Notebooks and documentation

1. Compare `workbook_mock.ipynb` with `canonical_scenario.ipynb` — decide its fate
2. Verify `docs/design_documents/environment.md` is still accurate

---

## 9. Summary

| Category | Count |
|---|---|
| Files for full removal | 14 |
| Files to simplify | 10 |
| Model fields to remove (Phase 1) | 14 |
| Write-only artifacts (Phase 2) | 7 fields + up to 4 build modules |
| Tests to remove | 0 (no tests exist) |
| Documents to remove | 0 (already in deprecated) |
| Notebooks to review | 1 (`workbook_mock.ipynb`) |
