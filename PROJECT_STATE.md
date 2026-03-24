# Project State

> Last updated: 2026-03-24

## Vision

Платформа для моделирования и оптимизации логистических сетей. Ядро — **Environment**: пространство, в котором commodity (велосипеды, товары, деньги) перемещаются через сеть объектов по периодам времени. Внутри Environment работают задачи (ребалансировка, ремонт, диспатч), принимаются решения, обновляется состояние мира.

Модель данных — domain-agnostic, построена на multi-commodity flow формулировке (Williamson). Табличные структуры для pandas/PySpark. Первый домен — велошеринг (Citi Bike-style).

### Два уровня задач

**Операционный (Environment)** — пошаговая симуляция. Environment идёт по периодам: period 0 → 1 → 2... На каждом шаге: происходят поездки, обновляется inventory, запускаются задачи (ребалансировщик ночью, ремонт утром). Состояние мира меняется после каждого шага. Это digital twin.

**Стратегический (Optimizer)** — решение "за один раз". Берёт данные за год, формулирует LP/MILP, солвер минимизирует cost function. Все периоды видны сразу, нет пошагового процесса. Отдельный потребитель, не часть Environment.

Оба уровня используют один и тот же `ResolvedModelData`, но обрабатывают его по-разному.

---

## Roadmap

1. **Foundation** — модель данных, build pipeline, loader ← ТЕКУЩАЯ ФАЗА
2. **Environment** — step-by-step engine, state management (следующая фаза)
3. **Rebalancer** — первая задача внутри Environment (VRP)
4. **Trip Generator** — синтетический поток поездок для симуляции
5. **UI** — визуализация Environment (Streamlit)
6. **Strategic Optimizer** — LP/MILP на горизонт (отдельный потребитель)
7. **Infrastructure** — DB, API, Docker, CI/CD
8. **Cloud** — Azure deployment

Каждая фаза начинается с design doc.

---

## Current Phase: Foundation (стабилизация)

Core library `gbp` с моделью данных, build pipeline, mock данные для велошеринга. Всё локально, Python, без инфраструктуры.

### What Works Today

- **Data model** (`gbp/core/model.py`): RawModelData (~46 tables) and ResolvedModelData (~52 tables) with Pydantic row schemas, grouped table access properties, and `table_summary()` introspection.
- **Build pipeline** (`gbp/build/pipeline.py`): `build_model(raw)` runs validation → time resolution → edge building → lead time resolution → transformation resolution → fleet capacity → spine assembly. All steps tested.
- **Attribute system** (`gbp/core/attributes/`): `AttributeRegistry` with grain-aware registration, kind validation (COST ≥ 0, CAPACITY > 0), grain groups, and spine assembly.
- **Bike-sharing loader** (`gbp/loaders/dataloader_graph.py`): Transforms bike-sharing source data (stations, depots, trips, telemetry) into a complete `RawModelData`.
- **Validation** (`gbp/build/validation.py`): Unit consistency, referential integrity, resource completeness, temporal coverage, graph connectivity (BFS).
- **Serialization** (`gbp/io/`): `raw_to_dict` / `dict_to_raw` for persistence.
- **Rebalancer prototype** (`gbp/rebalancer/`): PDP solver using OR-Tools. Early prototype, not connected to attribute system. Will be redesigned as a task inside Environment.
- **Tests**: Unit tests for core, build, loading modules. Integration test for full pipeline.

### What Remains in This Phase

- ~~Stabilize AttributeRegistry edge cases~~ ✅ (aggregation validation, duplicate detection, empty+non-nullable, all-NaN rejection, improved error messages)
- Update `docs/graph_data_model.md` to match current code
- ~~Notebook with full walkthrough~~ ✅ (`notebooks/05_pipeline_walkthrough.ipynb`)

### Refactoring Progress (see `docs/design/refactoring.md`)

| Step | Description | Status |
|------|-------------|--------|
| 1 | `model.py` field grouping + group properties + `table_summary()` | **Done** |
| 2 | `_build_raw_model()` decomposition into methods | **Done** |
| 3 | Protocol separation (`BikeShareSourceProtocol` vs `GenericSourceProtocol`) | **Done** |
| 4 | `make_raw_model()` factory function (`gbp/core/factory.py`) | **Done** |

---

## Not Now

These components are planned but **NOT being worked on** in the current phase. Do not create files, write code, or set up infrastructure for these.

- **Environment** — step-by-step engine. Will have its own design doc (next phase after Foundation).
- **Rebalancer redesign** — VRP task inside Environment. Current `gbp/rebalancer/` is an early prototype; do not extend it until design doc.
- **Trip Generator** — synthetic trip stream for simulation.
- **Strategic Optimizer** — LP/MILP over full planning horizon. Separate consumer, separate design doc. Phase 6.
- **ML / forecasting** — demand forecasting, GNN for trip duration. See `docs/IDEAS.md`.
- **API** — FastAPI. Not needed until UI.
- **UI** — Streamlit. Not needed until Environment works.
- **Database** — PostgreSQL. Currently CSV/Parquet.
- **DevOps** — Docker, CI/CD. Running locally.
- **Cloud** — Azure, Terraform. Separate phase.
- **Observability** — OpenTelemetry. Not needed without production.

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

(пока пусто — Foundation ещё в процессе)

---

## File Map

| Need to understand... | Read this |
|----------------------|-----------|
| Table schemas and structure | `gbp/core/model.py` + `gbp/core/schemas/` |
| Data model design rationale | `docs/graph_data_model.md` |
| Build pipeline flow | `gbp/build/pipeline.py` + `docs/diagrams/08_build_pipeline.mermaid` |
| Attribute system design | `docs/ATTRIBUTE_SYSTEM_DESIGN.md` |
| Architecture overview (visual) | `docs/architecture_diagrams.md` |
| Data journey end-to-end | `docs/DATA_JOURNEY.md` |
| Refactoring plan | `docs/REFACTORING_SPEC.md` |
| Repository structure | `docs/repo_struct.md` |
| Future ideas (ML, GNN, etc.) | `docs/IDEAS.md` |
| Project vision and roadmap | `PROJECT.md` |
| This file | `PROJECT_STATE.md` |
