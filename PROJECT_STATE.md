# Project State

> Last updated: 2026-05-12

## Current Phase: Codebase Cleanup to Canonical Scenario

The repository was previously built as a "domain-agnostic" platform with many abstractions that were never validated against a real scenario. This led to dead code, broken paths, and untested logic.

**Goal:** strip the codebase to the absolute minimum required to run `notebooks/canonical_scenario.ipynb`. If a module, class, function, schema field, or table is not exercised by the canonical scenario — it should be removed.

### What the canonical scenario uses

The canonical scenario (`notebooks/canonical_scenario.ipynb`) exercises this chain:

1. **Loader:** `DataLoaderMock` → `DataLoaderGraph` → `RawModelData`
2. **Build:** `build_model(raw)` → `ResolvedModelData`
3. **Environment phases (in order):**
   - `HistoricalLatentDemandPhase`
   - `HistoricalODStructurePhase`
   - `DeparturePhysicsPhase(mode="permissive")`
   - `HistoricalTripSamplingPhase(use_durations=True)`
   - `ArrivalsPhase`
   - `OverflowRedirectPhase`
   - `DispatchPhase(RebalancerTask, schedule=Schedule.every_n(...))`
   - `InvariantCheckPhase`
4. **Environment:** `Environment(resolved, config).run()` → `log.to_dataframes()`

Everything outside this chain is a candidate for removal.

### Cleanup progress

**Phase 1 — Dead code removal (completed 2026-05-12):**
- Removed 14 dead files: `columns.py`, `factory.py`, `hierarchy.py`, `parameters.py`, `pricing.py`, `output.py`, `gbp/io/` (3 files), `csv_loader.py`, `dataloader_mock_minimal.py`, `validators.py`, `random.py`, `noop.py`
- Removed 14 unused fields from `_TabularModelBase`: 8 hierarchy fields, 3 scenario fields (`scenarios`, `scenario_edge_rules`, `scenario_parameter_overrides`), `commodities`, `edge_vehicles`, `resource_availability`
- Simplified `schemas/__init__.py`, `schemas/scenario.py` (kept only `ScenarioManualEdges`), all `__init__.py` files, `protocols.py` (removed `GenericSourceProtocol`)
- Removed dead imports from `canonical_scenario.ipynb` (`LatentDemandInflatorPhase`, `EndOfPeriodDeficitPhase`)

**Phase 2 — Write-only artifact removal (completed 2026-05-12):**
- Removed 5 build modules: `lead_time.py`, `transformation.py`, `fleet_capacity.py`, `spine.py`, `report.py`
- Removed 7 write-only fields from `ResolvedModelData`: `edge_lead_time_resolved`, `transformation_resolved`, `fleet_capacity`, `facility_spines`, `edge_spines`, `resource_spines`, `build_report`
- Removed `EdgeLeadTimeResolved` schema from `edge.py` and all re-exports
- Removed `generated_tables` and `spine_tables` properties from `ResolvedModelData`
- Removed `BuildReport` usage from `pipeline.py` (derivation tracking was diagnostic-only)
- Removed `assemble_spines` from `build/__init__.py` re-exports

**Phase 3 — Notebooks and documentation (completed 2026-05-12):**
- Removed `workbook_mock.ipynb` (duplicated canonical_scenario with dead imports and ad-hoc analysis cells)
- Rewrote `docs/design_documents/environment.md` to reflect implemented vertical Citi Bike state (removed gas logistics examples, NoopTask, make_raw_model references, outdated pseudocode; updated status to IMPLEMENTED)

### Cleanup rules

- **No "future-proofing."** Do not keep code because "it might be useful later." If the canonical scenario doesn't call it, remove it.
- **No dead schema fields.** If `RawModelData` or `ResolvedModelData` has fields that the canonical scenario never populates or reads — remove them.
- **No unused loaders.** If a loader is not used by the canonical scenario — remove it.
- **No unused phases, tasks, or solvers.** Same rule.
- **No orphan tests.** Tests for removed code should also be removed.
- **No orphan docs.** Design docs, storytelling guides, and architecture diagrams for removed subsystems should be moved to `docs/deprecated/` or deleted.

---

## Not Now

Do not build, extend, or plan for any of these:

- Other domains (gas logistics, block logistics, etc.)
- Strategic Optimizer / LP / MILP
- ML / forecasting / GNN
- API / UI / Database
- Docker / CI/CD / Cloud
- Multi-stop routes across periods
