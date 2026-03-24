# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Graph-Based Logistics Platform (`gbp`) — a universal graph-based logistics platform for network flow problems built on multi-commodity flow formulation. Domain-agnostic data model validated against bike-sharing (Citi Bike-style). Current phase: **Foundation** (stabilizing core library).

The core concept is **Environment** — a step-by-step simulation/digital twin. The Strategic Optimizer (LP/MILP) is a separate, later consumer. Both use the same `ResolvedModelData`.

**Pipeline:** Raw Data → RawModelData (~46 DataFrames) → `build_model()` → ResolvedModelData (~52 DataFrames) → Consumer (Environment / Optimizer / Analytics).

## Commands

```bash
# Install (uses uv)
uv pip install -e ".[dev]"

# Run all tests
pytest

# Run a single test file
pytest tests/unit/build/test_pipeline.py

# Run a single test by name
pytest -k "test_name"

# Lint
ruff check gbp/ tests/

# Type check
mypy gbp/

# Format
ruff format gbp/ tests/
```

pytest is configured with `asyncio_mode = "auto"`, `testpaths = ["tests"]`, and `--cov=src --cov-report=term-missing`.

## Architecture

```
gbp/
├── core/              # Data model + schemas + attribute system
│   ├── model.py       # RawModelData & ResolvedModelData (central contract, ~46/~52 DataFrame fields)
│   ├── schemas/       # Pydantic row schemas per table (entity, edge, temporal, etc.)
│   ├── attributes/    # AttributeRegistry, AttributeSpec, grain groups, spine assembly
│   ├── enums.py       # All enumerations (FacilityType, ModalType, etc.)
│   ├── roles.py       # FacilityType → FacilityRole derivation
│   └── factory.py     # Model factory utilities
├── build/             # Build pipeline: raw → resolved (stateless, deterministic)
│   ├── pipeline.py    # Orchestrator: build_model(raw) runs all steps
│   ├── validation.py  # Unit consistency, referential integrity, graph connectivity (BFS)
│   ├── time_resolution.py  # date → period_id with aggregation
│   ├── edge_builder.py     # Edge materialization from rules + manual pairs
│   ├── lead_time.py        # hours → periods per edge × period
│   ├── transformation.py   # N→M commodity conversion
│   ├── fleet_capacity.py   # count × base_capacity per facility × resource_category
│   └── spine.py            # Grain-grouped attribute DataFrames for vectorized lookups
├── loaders/           # Source data → RawModelData
│   ├── dataloader_graph.py  # Bike-sharing loader (main loader)
│   ├── dataloader_mock.py   # Mock data for tests
│   └── protocols.py         # BikeShareSourceProtocol
├── io/                # Serialization (raw_to_dict / dict_to_raw, parquet)
├── loading/           # CSV loading utilities
└── rebalancer/        # EARLY PROTOTYPE — PDP solver using OR-Tools (will be redesigned)
```

**Test structure** mirrors source: `tests/unit/core/`, `tests/unit/build/`, `tests/unit/test_io/`, `tests/integration/`, plus top-level `tests/test_graph_loader.py` and `tests/test_rebalancer.py`.

## Data Model Invariants (NEVER VIOLATE)

1. **Nullable = LP-compatible.** Discrete parameters (min_shipment, batch_size) are nullable. Null → LP mode (continuous relaxation). Set → MILP mode. `solver_config.solver_type` controls the mode.
2. **Absolute units + resolution pattern.** Time values stored in absolute units (hours, dates). Resolution to period_id happens in build pipeline via `ceil(hours / period_duration)`.
3. **Orthogonal dimensions.** FacilityType, OperationType, FacilityRole are independent axes. Roles derived from type + operations.
4. **Expanded PK for edges.** Edge identity = `source_id × target_id × modal_type`.
5. **AttributeRegistry with explicit grain.** Parametric data registered with grain tuple. Grain determines grouping into spines during build.
6. **One ResolvedModelData for all consumers.** Environment, Optimizer, Analytics all consume the same resolved model.

## Code Style

- **Vectorization first.** No `for` loops over data — use pandas/NumPy operations.
- **Flat is better.** No AbstractFactoryBuilder or deep nesting.
- **Explicit dependencies.** Pass in `__init__`, no DI containers.
- **Strict typing.** Type hints on all public functions. Pydantic V2 strict mode for schemas.
- **Google-style docstrings** on all public classes and functions.
- **English only** in code, comments, and docstrings.
- Ruff config: line-length 100, target Python 3.11, rules: E, W, F, I, B, C4, UP, D (Google convention).
- Mypy: strict mode.

## AI Collaboration Rules

- Provide skeletons with detailed TODO comments, NOT full implementations of core algorithms (solver formulation, simulation engine, VRP).
- Refactoring, docstrings, boilerplate, and test scaffolds are OK to generate fully.
- Always validate changes against the data model invariants above.
- **Do NOT build or extend:** Environment engine, optimizer/solver, API, UI, Docker, database, cloud — these have their own future phases.
- **After completing a task**, check if `PROJECT_STATE.md` should be updated (e.g. marking items as done, adding new findings). Update it if relevant.
- **Verification notebook:** after completing a code task, create or update a notebook in `notebooks/verify/` that lets the user interactively test what changed. Keep cells minimal and focused — one cell per behavior. Name pattern: `NN_short_description.ipynb`. The user runs these by hand to build intuition. Notebooks must be in English (markdown cells, comments, print messages) — same rule as code.
- **Language:** code, comments, docstrings — English only. Communication with the user — Russian.

## Key Reference Documents

- **Data model:** `gbp/core/model.py` + `gbp/core/schemas/`
- **Build pipeline:** `gbp/build/pipeline.py`
- **Attribute system:** `docs/design/attribute_system.md` + `gbp/core/attributes/`
- **Architecture diagrams:** `docs/architecture_diagrams.md`
- **Current project state:** `PROJECT_STATE.md`
- **Design rationale:** `docs/design/graph_data_model.md`
