# Graph Optimization Platform - Technical Specification

## **1. Overview**

End-to-end pipeline: from raw data ingestion to rebalancing route visualization, using a modular monolith architecture with a focus on minimalism and performance.

### **Key Objectives**

- Build a **reusable Python library** (`gop-core`) for graph-based optimization.
- Provide **production-ready services** (API, UI) as separate deployable units.
- Implement **full local infrastructure** for deep understanding of components.
- Deploy to **Azure** as a learning exercise for cloud-native architecture.
- Create a **portfolio project** demonstrating end-to-end engineering skills.

## **2. Architectural Strategy**

### **2.1 Core Philosophy (The "Nano" Way)**

We follow the engineering principles of **Andrej Karpathy (Nano-style)**: code must be simple, understandable, and fast.

- **Minimalism:** Code should be "hackable". No model factories, heavy DI containers, or hidden magic.
- **Vectorization First:** All mathematics must be done via NumPy/Pandas. No `for` loops in hot paths.
- **Modular Monolith:** Clear separation into modules (Vertical Slices) within a single repository.

### **2.2 Monorepo Multi-Package Architecture**

The project uses a monorepo structure with clearly separated packages:

```
graph-optimization-platform/
│
├── packages/
│   ├── gop-core/          # Python library (the brain)
│   ├── gop-api/           # FastAPI service (REST interface)
│   └── gop-ui/            # Streamlit application (visual interface)
│
├── infrastructure/
│   ├── local/             # Docker Compose stack
│   └── azure/             # Terraform configurations
│
├── tests/                 # Cross-package integration tests
├── docs/                  # Sphinx documentation
├── scripts/               # Development utilities
└── README.md
```

### **2.3 Package Responsibilities**

| Package | Type | Purpose | Can Import |
|---------|------|---------|------------|
| `gop-core` | Library | Optimization algorithms, graph models | External libs only |
| `gop-api` | Service | REST API, async processing | `gop-core` |
| `gop-ui` | Application | Visualization, user interaction | `gop-core` or `gop-api` |

### **2.4 Target Production Architecture (Azure)**

- **Compute:** Azure Container Apps (Serverless Containers).
- **Database:** Azure Database for PostgreSQL (Flexible Server).
- **Object Storage:** Azure Data Lake Storage Gen2 (ADLS) — for Bronze/Silver layers (Parquet).
- **Secrets:** Azure Key Vault.
- **Observability:** Azure Monitor + Managed Grafana (via OpenTelemetry Collector).

### **2.5 Local Development Architecture**

Full cloud emulation via Docker Compose:

- **Compute:** Local containers (Python 3.11).
- **Database:** PostgreSQL 16 (with PostGIS extension if geo-indexes are needed).
- **Object Storage:** MinIO (S3-compatible API).
- **Observability:** Prometheus + Grafana + Loki + Tempo (LGTM Stack) via OpenTelemetry.

## **3. Technology Stack**

### **Core**

- **Language:** Python 3.11+
- **Package Manager:** `uv` for dependency management, `hatchling` as build backend.
- **Data Processing:** Pandas 2.0+ / NumPy (strictly vector operations).
- **Validation:** Pydantic V2 (strict typing for all inputs/outputs).
- **Abstractions:** Python ABC (Abstract Base Classes) for dependency inversion.

### **Components**

- **API:** FastAPI (Async).
- **UI:** Streamlit (Multi-page Apps).
- **ORM:** SQLAlchemy 2.0 + Alembic (async migrations).
- **Math Engine:** Google OR-Tools (for VRP).

### **DevOps & Docs**

- **Infrastructure:** Terraform.
- **Containerization:** Docker (Multi-stage builds).
- **Documentation:** Sphinx + MyST Parser + GitHub Pages.
- **CI/CD:** GitHub Actions.

### **Observability**

- **Standard:** OpenTelemetry (OTel) for Tracing, Metrics, and Logs.
- **Instrumentation:** `opentelemetry-instrumentation-fastapi`, `opentelemetry-instrumentation-sqlalchemy`.
- **Collector:** OpenTelemetry Collector (sidecar/agent).

### **Optional Dependencies (if PostGIS needed)**

- `geoalchemy2` — SQLAlchemy extension for PostGIS.
- `shapely` — geometric operations.

## **4. Project Structure (Modular Monolith)**

```
project_root/
├── gop/                            # Source Code (package name)
│   ├── __init__.py                 # Package root
│   ├── py.typed                    # PEP 561 marker for strict typing
│   │
│   ├── config/                     # ═══ Configuration ═══
│   │   ├── __init__.py
│   │   ├── settings.py             # pydantic-settings (env loading)
│   │   ├── otel_collector.yaml     # OTel Collector config
│   │   └── prometheus.yml          # Prometheus scrape config
│   │
│   ├── shared/                     # ═══ Shared Kernel (Platform Core) ═══
│   │   ├── __init__.py             # Public exports
│   │   ├── base.py                 # ABC: AbstractStorage, AbstractSolver, AbstractDataLoader
│   │   ├── database.py             # Async Postgres engine (DatabaseManager)
│   │   ├── core/                   # L2 logistics model: RawModelData, schemas, enums
│   │   ├── observability.py        # OpenTelemetry + structlog setup
│   │   ├── storage.py              # MinIOStorage implementation
│   │   ├── exceptions.py           # Domain exception hierarchy
│   │   └── types.py                # Type aliases (DistanceMatrix, NodeId, etc.)
│   │
│   ├── etl/                        # ═══ Ingestion Module ═══
│   │   ├── __init__.py
│   │   ├── loader.py               # DataIngestionOrchestrator
│   │   └── adapters/
│   │       ├── __init__.py
│   │       ├── base.py             # BaseAdapter ABC
│   │       ├── csv_adapter.py      # CSV file loader
│   │       └── s3_adapter.py       # MinIO/S3 Parquet loader
│   │
│   ├── rebalancer/                 # ═══ Feature Module: Rebalancer ═══
│   │   ├── __init__.py
│   │   ├── domain/                 # Pure Business Logic (no external deps)
│   │   │   ├── __init__.py
│   │   │   ├── models.py           # VehicleConfig, RebalancingProblem, Route, Result
│   │   │   └── vrp_solver.py       # VRPSolver (OR-Tools wrapper)
│   │   ├── adapters/
│   │   │   ├── __init__.py
│   │   │   └── data_loader.py      # Silver → Gold transformation
│   │   └── service.py              # RebalancerService (Orchestrator)
│   │
│   ├── simulator/                  # ═══ Feature Module: Simulator (Future) ═══
│   │   └── __init__.py
│   │
│   ├── optimizer/                  # ═══ Feature Module: Optimizer (Future) ═══
│   │   └── __init__.py
│   │
│   └── entrypoints/                # ═══ Delivery Mechanisms ═══
│       ├── __init__.py
│       ├── api/                    # FastAPI Application
│       │   ├── __init__.py
│       │   ├── main.py             # App factory, lifespan, middleware
│       │   ├── schemas/            # Request/Response DTOs
│       │   │   ├── __init__.py
│       │   │   ├── common.py       # ErrorResponse, HealthResponse, Pagination
│       │   │   └── rebalancer.py   # OptimizeRequest, OptimizeResponse
│       │   └── routers/            # Endpoint handlers
│       │       ├── __init__.py
│       │       ├── health.py       # /health, /ready
│       │       └── rebalancer.py   # /api/v1/rebalancer/*
│       │
│       └── ui/                     # Streamlit Application
│           ├── __init__.py
│           ├── app.py              # Entry point (Home page)
│           ├── pages/              # Multi-page navigation
│           │   ├── 1_rebalancer.py
│           │   ├── 2_simulator.py
│           │   └── 3_optimizer.py
│           └── components/         # Reusable UI widgets
│               ├── __init__.py
│               ├── map_view.py     # Network visualization
│               └── metrics_panel.py # KPI display
│
├── tests/                          # Pytest test suite
│   ├── conftest.py                 # Shared fixtures
│   ├── unit/                       # Domain logic tests
│   │   ├── __init__.py
│   │   ├── test_graph_model.py
│   │   └── test_rebalancer_models.py
│   ├── integration/                # Adapter tests (with real DB/MinIO)
│   │   ├── __init__.py
│   │   └── test_etl_adapters.py
│   └── e2e/                        # API endpoint tests
│       ├── __init__.py
│       └── test_api.py
│
├── migrations/                     # Alembic DB migrations
│   ├── env.py                      # Async migration setup
│   ├── script.py.mako
│   └── versions/
│
├── infrastructure/                 # Terraform code
│   └── ...
│
├── scripts/                        # Utilities
│   └── setup_local.py              # MinIO bucket setup, test data seeding
│
├── docs/                           # Sphinx documentation source
│   ├── conf.py
│   ├── index.md
│   └── ...
│
├── docker-compose.yml              # Local LGTM stack
├── Dockerfile                      # Multi-stage build
├── pyproject.toml                  # Dependencies + Ruff/Mypy config
├── uv.lock                         # Locked dependencies (generated)
├── alembic.ini                     # Alembic config
└── .gitignore
```

### **4.1. Module Boundaries**

| Module | Can Import From | Cannot Import From |
|--------|-----------------|-------------------|
| `gop.shared` | stdlib, external libs | any `gop.*` module |
| `gop.config` | `gop.shared`, stdlib, external libs | feature modules |
| `gop.etl` | `gop.shared`, `gop.config` | `rebalancer`, `simulator`, `optimizer` |
| `gop.rebalancer` | `gop.shared`, `gop.config` | `etl`, `simulator`, `optimizer` |
| `gop.simulator` | `gop.shared`, `gop.config` | `etl`, `rebalancer`, `optimizer` |
| `gop.optimizer` | `gop.shared`, `gop.config` | `etl`, `rebalancer`, `simulator` |
| `gop.entrypoints` | `gop.shared`, `gop.config`, all feature modules | — |

**Rule:** Feature modules MUST NOT import from each other. Communication happens only via `shared` or through entrypoints.

**Enforcement:** Use `import-linter` in CI to validate boundaries:

```toml
# pyproject.toml
[tool.importlinter]
root_package = "gop"

[[tool.importlinter.contracts]]
name = "Feature modules are independent"
type = "independence"
modules = ["gop.rebalancer", "gop.simulator", "gop.optimizer", "gop.etl"]
```

### **4.2. API Layer Structure**

```
api/
├── schemas/     # Pydantic models for HTTP request/response (DTOs)
│   └── {feature}.py
├── routers/     # Thin endpoint handlers (delegate to services)
│   └── {feature}.py
└── main.py      # App factory, middleware, lifespan
```

**Convention:** Routers should be thin. Business logic lives in `{feature}/service.py`.

### **4.3. UI Layer Structure**

```
ui/
├── app.py           # Entry point (Home/Dashboard)
├── pages/           # Streamlit multi-page apps (auto-navigation)
│   └── N_name.py    # No emoji in filenames for cross-platform compatibility
└── components/      # Reusable visualization widgets
    └── {component}.py
```

**Convention:** Pages call services directly or via API. Components are pure display functions.

## **5. Key Design Patterns**

### **5.1. Data Validation & Typing**

- **Strict Pydantic:** All data crossing module boundaries must be validated by Pydantic models.
- **Abstract Base Classes (ABC):** All external dependencies (Solver, Storage) must be hidden behind abstractions in `gop/shared/base.py`.
- **Type Aliases:** Common types defined in `gop/shared/types.py` (e.g., `DistanceMatrix`, `NodeId`).
- **py.typed Marker:** `gop/py.typed` enables strict mypy checking for the package.

### **5.2. Exception Handling**

All domain exceptions inherit from `GraphOptError` in `gop/shared/exceptions.py`:

```
GraphOptError (base)
├── DataError
│   ├── ValidationError
│   ├── DataNotFoundError
│   └── DataIntegrityError
├── SolverError
│   ├── SolverTimeoutError
│   ├── InfeasibleProblemError
│   └── SolverConfigError
├── InfrastructureError
│   ├── StorageError
│   ├── DatabaseError
│   └── ConnectionError
└── RebalancerError (feature-specific)
    ├── InvalidRouteError
    └── CapacityExceededError
```

### **5.3. Vectorization First**

- Usage of `for` loops for data array processing is **forbidden**.
- All distance matrix preparation and time-series processing must be performed via **NumPy/Pandas vector operations**.

**Allowed operations:**

- `np.where`, `np.select` — conditional logic
- `np.einsum`, `np.dot`, `@` operator — matrix operations
- `pd.DataFrame.apply` with `axis=1` — row-wise operations (when unavoidable)
- Broadcasting — element-wise operations across arrays

**Explicitly forbidden:**

- `np.vectorize` — it's a Python loop wrapper, provides no performance benefit
- `for row in df.iterrows()` — use vectorized alternatives
- List comprehensions over large arrays — use NumPy instead

### **5.4. Observability Strategy (OpenTelemetry)**

- **Tracing:** All HTTP requests and DB queries are automatically traced via auto-instrumentation.
- **Manual Spans:** Critical sections (e.g., `vrp_solver.solve`) must be wrapped in manual OTel spans:

    ```python
    with tracer.start_as_current_span("run_optimization"):
        solver.solve(...)
    ```

- **Structured Logging:** Use `structlog` with OTel processor for Log-Trace correlation:

    ```python
    logger.info("optimization_started", task_id="123", num_nodes=500)
    ```

- **Collector Ports:**
    - gRPC: `4317` (default for `OTEL_EXPORTER_OTLP_ENDPOINT`)
    - HTTP: `4318` (use `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`)

### **5.5. Documentation as Code**

- **Source:** Docstrings in code (Google Style).
- **Engine:** Sphinx with `autodoc`, `napoleon` (Google Style support), `myst-parser` (Markdown support) extensions.
- **Deployment:** GitHub Action `deploy-docs.yaml` builds HTML and pushes to `gh-pages` branch on merge to `main`.

## **6. Data Architecture (Medallion)**

1. **Bronze (Raw):** Raw files (JSON/CSV) in Object Storage (MinIO/ADLS).
2. **Silver (Graph):** Normalized graph model (Nodes, Edges, TimeSeries) in Parquet/Postgres.
3. **Gold (Feature):** Aggregates specific to the task (e.g., Distance Matrix for Rebalancer).

## **7. Engineering Culture & Style Guide**

### **7.1. General Rules**

- **Flat is better than nested:** Avoid deep nesting of folders and classes.
- **No Config Monsters:** Configuration is a flat dataclass or YAML, not a complex object with logic.
- **Explicit is better than implicit:** No magic with auto-injection. Pass dependencies explicitly in `__init__`.

### **7.2. Coding Style**

- **Naming:**

    - Variables/Functions: `snake_case` (e.g., `num_iterations`, `evaluate_model`).
    - Classes: `PascalCase` (e.g., `VRPSolver`, `GraphConfig`).
    - Constants: `UPPER_CASE`.
    - Private Methods: `_` prefix (e.g., `_extract_route`).

- **Type Hints:** Mandatory for public methods and complex data structures.
- **Docstrings:** Google Style. Mandatory for all modules, classes, and public functions.

### **7.3. Documentation Strategy**

- **Single Source of Truth:** Documentation lives in the code (docstrings).
- **Automation:** GitHub Action `deploy-docs.yaml` builds Sphinx documentation on every push to `main`.

### **7.4. Performance & Vectorization**

- **Pure Functions:** The optimizer core must be pure functions (or stateless classes).
- **No Loops:** Use `np.where`, broadcasting, or Pandas operations instead of `for` loops over data.
- **Profiling:** Use `cProfile` or timing decorators for critical sections.

## **8. Quality Assurance Strategy**

### **8.1. Testing (Pytest)**

- **Unit Tests:** For domain logic (`gop/rebalancer/domain`). No DB mocks, pure math.
- **Integration Tests:** For adapters (`gop/etl`). Verification of writing to test DB/MinIO.
- **E2E Tests:** Verification of API endpoints via `TestClient`.

### **8.2. Static Analysis**

- **Ruff:** Linter and formatter (replaces Black, Isort, Flake8).
- **Mypy:** Type checking (Strict mode). Enabled via `py.typed` marker.
- **import-linter:** Module boundary enforcement.
- **Pre-commit hooks:** Automatic execution of Ruff before commit.

### **8.3. CI/CD Pipeline (GitHub Actions)**

1. **PR Check:**
    - Setup Python + uv.
    - Run Ruff & Mypy.
    - Run import-linter.
    - Run Pytest.

2. **CD (Merge to Main):**
    - Build Docker Image.
    - Push to Azure Container Registry (ACR) / GHCR.
    - Build Sphinx Docs → Deploy to GitHub Pages.
    - (Optional) Terraform Apply for Staging.

## **9. Configuration Management**

Use `pydantic-settings` for configuration management. The application must automatically switch between Local/Azure modes.

Settings location: `gop/config/settings.py`

| Variable | Description | Default |
|----------|-------------|---------|
| `ENV` | Environment mode (LOCAL, DEV, PROD) | `LOCAL` |
| `DB_DSN` | Postgres Connection String | `postgresql+asyncpg://...` |
| `LOG_LEVEL` | Logging verbosity | `INFO` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OpenTelemetry Collector URL (gRPC) | `http://otel-collector:4317` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | Protocol: `grpc` or `http/protobuf` | `grpc` |
| `MINIO_ENDPOINT` | MinIO/S3 endpoint | `http://localhost:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key | `minioadmin` |
| `MINIO_BUCKET` | Default storage bucket | `data` |

## **10. AI Usage Policy (Anti-Vibe Coding)**

To ensure deep understanding and engineering growth, we strictly limit the role of AI in this project.

### **10.1. Allowed AI Usage**

- **Smart Autocomplete:** GitHub Copilot (or equivalent) for boilerplate reduction and line completion is permitted.
- **Code Discussion & Mentorship:** Using LLMs to discuss architecture, debug errors, or explain complex concepts is encouraged.
- **Stylistic Refactoring:** Asking AI to "make this code cleaner" or "add docstrings" is allowed, provided the logic remains unchanged.
- **Skeleton Generation:** AI may generate file skeletons with TODOs for core logic. The algorithmic core must be written by hand.

### **10.2. Prohibited AI Usage**

- **No Full Logic Generation:** AI agents must not generate complete algorithmic implementations.
- **No Logic Outsourcing:** Core algorithms (VRP logic, ETL pipelines) must be written by hand to ensure authorial understanding.
- **"Vibe Coding" is Banned:** Committing code that "looks right" but is not fully understood by the author is strictly prohibited.

## **11. Development Workflow**

### **11.1. Setup**

```bash
# Create virtual environment
uv venv --python 3.11

# Activate (Windows)
.venv\Scripts\activate

# Activate (Linux/macOS)
source .venv/bin/activate

# Install dependencies (creates uv.lock if not exists)
uv pip install -e ".[dev]"

# Start Infrastructure (includes Postgres, MinIO, Grafana, Tempo)
docker-compose up -d

# Apply Migrations
alembic upgrade head
```

### **11.2. Run Components**

```bash
# Run API (with OTel auto-instrumentation)
opentelemetry-instrument uvicorn gop.entrypoints.api.main:app --reload

# Run UI
streamlit run gop/entrypoints/ui/app.py
```

### **11.3. Generate Docs**

```bash
cd docs && make html
# Open _build/html/index.html
```

### **11.4. Run Tests**

```bash
# All tests
pytest

# Unit tests only
pytest tests/unit/

# With coverage
pytest --cov=gop --cov-report=html

# Check module boundaries
import-linter
```

## **12. pyproject.toml Reference**

```toml
[project]
name = "graph-optimization-platform"
version = "0.1.0"
description = "Unified modular engine for graph-based optimization problems"
readme = "README.md"
requires-python = ">=3.11"
dependencies = [
    # Core
    "pandas>=2.0.0",
    "numpy>=1.24.0",
    "pydantic>=2.0.0",
    "pydantic-settings>=2.0.0",
    
    # API & Web
    "fastapi>=0.100.0",
    "uvicorn[standard]>=0.23.0",
    "streamlit>=1.25.0",
    
    # Database
    "sqlalchemy[asyncio]>=2.0.0",
    "asyncpg>=0.28.0",
    "alembic>=1.11.0",
    
    # Math Engine
    "ortools>=9.6.0",
    
    # Observability
    "opentelemetry-api>=1.20.0",
    "opentelemetry-sdk>=1.20.0",
    "opentelemetry-exporter-otlp>=1.20.0",
    "opentelemetry-instrumentation-fastapi>=0.41b0",
    "opentelemetry-instrumentation-sqlalchemy>=0.41b0",
    "structlog>=23.1.0",
    
    # Storage
    "boto3>=1.28.0",
    "pyarrow>=12.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "pytest-asyncio>=0.21.0",
    "pytest-cov>=4.1.0",
    "httpx>=0.24.0",
    "ruff>=0.1.0",
    "mypy>=1.5.0",
    "pre-commit>=3.3.0",
    "import-linter>=2.0.0",
]
docs = [
    "sphinx>=7.0.0",
    "myst-parser>=2.0.0",
    "sphinx-autodoc-typehints>=1.24.0",
]
geo = [
    "geoalchemy2>=0.14.0",
    "shapely>=2.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["gop"]

[tool.ruff]
target-version = "py311"
line-length = 100
select = ["E", "W", "F", "I", "B", "C4", "UP", "D"]
ignore = ["D100", "D104"]

[tool.ruff.pydocstyle]
convention = "google"

[tool.mypy]
python_version = "3.11"
strict = true
warn_return_any = true
warn_unused_ignores = true

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
addopts = "-v --cov=gop --cov-report=term-missing"

[tool.importlinter]
root_package = "gop"

[[tool.importlinter.contracts]]
name = "Feature modules are independent"
type = "independence"
modules = ["gop.rebalancer", "gop.simulator", "gop.optimizer", "gop.etl"]
```