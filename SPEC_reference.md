# Graph Optimization Platform - Technical Specification v2

## **1. Overview**

A modular platform for graph-based optimization problems. The platform transforms arbitrary data into graph structures and applies optimization algorithms (rebalancing, simulation, optimization) to solve real-world logistics and network problems.

### **Key Objectives**

- Build a **reusable Python library** (`gop-core`) for graph-based optimization.
- Provide **production-ready services** (API, UI) as separate deployable units.
- Implement **full local infrastructure** for deep understanding of components.
- Deploy to **Azure** as a learning exercise for cloud-native architecture.
- Create a **portfolio project** demonstrating end-to-end engineering skills.

### **Project Philosophy**

This project prioritizes **understanding over convenience**:

- No black-box solutions — every component is inspectable locally.
- Manual implementation of core algorithms (Anti-Vibe Coding).
- Infrastructure-as-Code for reproducibility.
- Documentation as a first-class citizen.

## **2. Architectural Strategy**

### **2.1 Monorepo Multi-Package Architecture**

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

### **2.2 Package Responsibilities**

| Package | Type | Purpose | Can Import |
|---------|------|---------|------------|
| `gop-core` | Library | Optimization algorithms, graph models | External libs only |
| `gop-api` | Service | REST API, async processing | `gop-core` |
| `gop-ui` | Application | Visualization, user interaction | `gop-core` or `gop-api` |

### **2.3 Deployment Modes**

**Local Development:**
- All packages run locally.
- UI imports `gop-core` directly for fast iteration.
- Docker Compose provides infrastructure (DB, storage, observability).

**Production (Azure):**
- `gop-api` runs as Azure Container App.
- `gop-ui` runs as separate Container App.
- UI communicates with API over HTTP.
- Managed services for DB and storage.

## **3. Technology Stack**

### **Core Library (`gop-core`)**

| Category | Technology | Purpose |
|----------|------------|---------|
| Language | Python 3.11+ | Modern features, good typing |
| Data | Pandas 2.0+, NumPy | Vectorized operations |
| Validation | Pydantic V2 | Strict typing, serialization |
| Math Engine | Google OR-Tools | VRP, LP, MIP solvers |
| Abstractions | Python ABC | Dependency inversion |

### **API Service (`gop-api`)**

| Category | Technology | Purpose |
|----------|------------|---------|
| Framework | FastAPI | Async REST API |
| Server | Uvicorn | ASGI server |
| Database | SQLAlchemy 2.0 + asyncpg | Async ORM for PostgreSQL |
| Migrations | Alembic | Schema versioning |
| Background | (Future) Celery / ARQ | Long-running tasks |

### **UI Application (`gop-ui`)**

| Category | Technology | Purpose |
|----------|------------|---------|
| Framework | Streamlit | Rapid UI development |
| Visualization | Plotly, Folium | Charts and maps |
| HTTP Client | httpx | Async API calls |

### **Infrastructure**

| Category | Local | Azure |
|----------|-------|-------|
| Compute | Docker containers | Container Apps |
| Database | PostgreSQL 16 | Azure Database for PostgreSQL |
| Object Storage | MinIO | Azure Blob Storage / ADLS Gen2 |
| Secrets | .env files | Azure Key Vault |
| Observability | Grafana + Tempo + Loki | Azure Monitor + Managed Grafana |
| IaC | Docker Compose | Terraform |

### **DevOps & Quality**

| Category | Technology | Purpose |
|----------|------------|---------|
| Package Manager | uv | Fast dependency management |
| Build Backend | hatchling | Modern Python packaging |
| Linter/Formatter | Ruff | Replaces Black, isort, flake8 |
| Type Checker | mypy (strict) | Static type analysis |
| Testing | pytest + pytest-asyncio | Unit and integration tests |
| Module Boundaries | import-linter | Enforce architecture |
| CI/CD | GitHub Actions | Automated pipelines |
| Documentation | Sphinx + MyST | Auto-generated docs |

## **4. Detailed Project Structure**

```
graph-optimization-platform/
│
├── packages/
│   │
│   ├── gop-core/                      # ══════ PYTHON LIBRARY ══════
│   │   ├── gop/
│   │   │   ├── __init__.py            # Public API exports
│   │   │   ├── py.typed               # PEP 561 marker
│   │   │   │
│   │   │   ├── shared/                # ─── Shared Kernel ───
│   │   │   │   ├── __init__.py
│   │   │   │   ├── base.py            # ABC: Solver, Storage, DataLoader
│   │   │   │   ├── graph_model.py     # Node, Edge, Graph Pydantic models
│   │   │   │   ├── exceptions.py      # Exception hierarchy
│   │   │   │   └── types.py           # Type aliases
│   │   │   │
│   │   │   ├── rebalancer/            # ─── Feature: Rebalancer ───
│   │   │   │   ├── __init__.py
│   │   │   │   ├── models.py          # VehicleConfig, Route, Result
│   │   │   │   ├── solver.py          # VRPSolver (OR-Tools)
│   │   │   │   └── service.py         # RebalancerService orchestrator
│   │   │   │
│   │   │   ├── simulator/             # ─── Feature: Simulator (Future) ───
│   │   │   │   └── __init__.py
│   │   │   │
│   │   │   └── optimizer/             # ─── Feature: Optimizer (Future) ───
│   │   │       └── __init__.py
│   │   │
│   │   ├── tests/
│   │   │   ├── conftest.py
│   │   │   ├── test_graph_model.py
│   │   │   └── test_rebalancer.py
│   │   │
│   │   ├── pyproject.toml             # Package configuration
│   │   └── README.md
│   │
│   ├── gop-api/                       # ══════ FASTAPI SERVICE ══════
│   │   ├── api/
│   │   │   ├── __init__.py
│   │   │   ├── main.py                # App factory, lifespan
│   │   │   ├── config.py              # pydantic-settings
│   │   │   ├── dependencies.py        # FastAPI dependencies
│   │   │   │
│   │   │   ├── routers/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── health.py          # /health, /ready
│   │   │   │   └── rebalancer.py      # /api/v1/rebalancer/*
│   │   │   │
│   │   │   ├── schemas/               # Request/Response DTOs
│   │   │   │   ├── __init__.py
│   │   │   │   ├── common.py          # ErrorResponse, Pagination
│   │   │   │   └── rebalancer.py      # OptimizeRequest/Response
│   │   │   │
│   │   │   ├── db/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── engine.py          # Async SQLAlchemy engine
│   │   │   │   └── models.py          # ORM models
│   │   │   │
│   │   │   ├── storage/
│   │   │   │   ├── __init__.py
│   │   │   │   └── minio_client.py    # S3-compatible storage
│   │   │   │
│   │   │   └── observability/
│   │   │       ├── __init__.py
│   │   │       ├── tracing.py         # OpenTelemetry setup
│   │   │       └── logging.py         # structlog configuration
│   │   │
│   │   ├── migrations/                # Alembic
│   │   │   ├── env.py
│   │   │   ├── script.py.mako
│   │   │   └── versions/
│   │   │
│   │   ├── tests/
│   │   │   ├── conftest.py
│   │   │   └── test_api.py
│   │   │
│   │   ├── Dockerfile
│   │   ├── pyproject.toml
│   │   └── README.md
│   │
│   └── gop-ui/                        # ══════ STREAMLIT APP ══════
│       ├── ui/
│       │   ├── __init__.py
│       │   ├── app.py                 # Entry point
│       │   ├── config.py              # UI settings
│       │   │
│       │   ├── pages/
│       │   │   ├── 1_rebalancer.py
│       │   │   ├── 2_simulator.py
│       │   │   └── 3_optimizer.py
│       │   │
│       │   ├── components/
│       │   │   ├── __init__.py
│       │   │   ├── map_view.py        # Network visualization
│       │   │   └── metrics_panel.py   # KPI display
│       │   │
│       │   └── api_client/            # Client for gop-api
│       │       ├── __init__.py
│       │       └── client.py
│       │
│       ├── Dockerfile
│       ├── pyproject.toml
│       └── README.md
│
├── infrastructure/
│   │
│   ├── local/                         # ══════ LOCAL DEV STACK ══════
│   │   ├── docker-compose.yml         # All services
│   │   ├── docker-compose.override.yml # Dev overrides
│   │   │
│   │   ├── postgres/
│   │   │   └── init.sql               # Initial schema
│   │   │
│   │   ├── minio/
│   │   │   └── setup.sh               # Bucket creation
│   │   │
│   │   ├── observability/
│   │   │   ├── prometheus.yml
│   │   │   ├── grafana/
│   │   │   │   ├── provisioning/
│   │   │   │   └── dashboards/
│   │   │   ├── tempo.yml
│   │   │   ├── loki.yml
│   │   │   └── otel-collector.yml
│   │   │
│   │   └── README.md                  # Local setup instructions
│   │
│   └── azure/                         # ══════ AZURE TERRAFORM ══════
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       │
│       ├── modules/
│       │   ├── container_apps/
│       │   ├── postgresql/
│       │   ├── storage/
│       │   ├── key_vault/
│       │   └── monitoring/
│       │
│       ├── environments/
│       │   ├── dev.tfvars
│       │   └── prod.tfvars
│       │
│       └── README.md                  # Azure deployment guide
│
├── tests/                             # ══════ INTEGRATION TESTS ══════
│   ├── conftest.py                    # Shared fixtures
│   ├── test_e2e_flow.py               # Full workflow tests
│   └── test_api_integration.py        # API + DB tests
│
├── docs/                              # ══════ DOCUMENTATION ══════
│   ├── conf.py
│   ├── index.md
│   ├── getting-started.md
│   ├── architecture.md
│   ├── api-reference.md
│   └── deployment.md
│
├── scripts/                           # ══════ UTILITIES ══════
│   ├── setup_local.py                 # Initialize local environment
│   ├── seed_data.py                   # Load sample data
│   └── run_all_tests.sh               # Cross-package test runner
│
├── .github/
│   └── workflows/
│       ├── ci.yml                     # Lint, test, type-check
│       ├── cd-azure.yml               # Deploy to Azure
│       └── docs.yml                   # Build and deploy docs
│
├── .gitignore
├── README.md                          # Project overview
└── Makefile                           # Common commands
```

## **5. Package Specifications**

### **5.1 gop-core**

**Purpose:** Pure Python library for graph-based optimization. No infrastructure dependencies.

**Public API (`gop/__init__.py`):**
```python
from gop.shared.graph_model import Node, Edge, Graph
from gop.shared.exceptions import GraphOptError
from gop.rebalancer import RebalancerService, VRPSolver
from gop.simulator import SimulatorService  # Future
from gop.optimizer import OptimizerService  # Future

__all__ = [
    "Node", "Edge", "Graph",
    "GraphOptError", 
    "RebalancerService", "VRPSolver",
]
```

**Usage example:**
```python
from gop import Graph, RebalancerService

graph = Graph.from_csv("network.csv")
service = RebalancerService()
result = service.optimize(graph, vehicle_capacity=100)
print(result.routes)
```

**pyproject.toml:**
```toml
[project]
name = "gop-core"
version = "0.1.0"
description = "Graph-based optimization library"
requires-python = ">=3.11"
dependencies = [
    "pandas>=2.0.0",
    "numpy>=1.24.0",
    "pydantic>=2.0.0",
    "ortools>=9.6.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "ruff>=0.1.0",
    "mypy>=1.5.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["gop"]
```

### **5.2 gop-api**

**Purpose:** REST API service. Depends on `gop-core`.

**Key endpoints:**
```
GET  /health                    # Health check
GET  /ready                     # Readiness (DB connected)
POST /api/v1/rebalancer/solve   # Submit optimization job
GET  /api/v1/rebalancer/{id}    # Get job result
```

**pyproject.toml:**
```toml
[project]
name = "gop-api"
version = "0.1.0"
description = "Graph Optimization Platform API"
requires-python = ">=3.11"
dependencies = [
    "gop-core",  # Local dependency
    "fastapi>=0.100.0",
    "uvicorn[standard]>=0.23.0",
    "pydantic-settings>=2.0.0",
    "sqlalchemy[asyncio]>=2.0.0",
    "asyncpg>=0.28.0",
    "alembic>=1.11.0",
    "boto3>=1.28.0",
    "opentelemetry-api>=1.20.0",
    "opentelemetry-sdk>=1.20.0",
    "opentelemetry-exporter-otlp>=1.20.0",
    "opentelemetry-instrumentation-fastapi>=0.41b0",
    "opentelemetry-instrumentation-sqlalchemy>=0.41b0",
    "structlog>=23.1.0",
]

[tool.uv.sources]
gop-core = { path = "../gop-core", editable = true }
```

### **5.3 gop-ui**

**Purpose:** Interactive visualization. Can work with `gop-core` directly or via `gop-api`.

**pyproject.toml:**
```toml
[project]
name = "gop-ui"
version = "0.1.0"
description = "Graph Optimization Platform UI"
requires-python = ">=3.11"
dependencies = [
    "gop-core",  # Direct import for local dev
    "streamlit>=1.25.0",
    "plotly>=5.15.0",
    "folium>=0.14.0",
    "httpx>=0.24.0",  # For API client
]

[tool.uv.sources]
gop-core = { path = "../gop-core", editable = true }
```

## **6. Module Boundaries & Dependencies**

### **6.1 Core Library Internal Boundaries**

```
gop/
├── shared/      ← imports: stdlib, external libs ONLY
├── rebalancer/  ← imports: shared
├── simulator/   ← imports: shared
└── optimizer/   ← imports: shared
```

**Rule:** Feature modules NEVER import from each other.

**Enforcement (in `gop-core/pyproject.toml`):**
```toml
[tool.importlinter]
root_package = "gop"

[[tool.importlinter.contracts]]
name = "Shared has no internal imports"
type = "forbidden"
source_modules = ["gop.shared"]
forbidden_modules = ["gop.rebalancer", "gop.simulator", "gop.optimizer"]

[[tool.importlinter.contracts]]
name = "Feature modules are independent"
type = "independence"
modules = ["gop.rebalancer", "gop.simulator", "gop.optimizer"]
```

### **6.2 Cross-Package Dependencies**

```
gop-ui ──────┐
             ├──→ gop-core (library)
gop-api ─────┘
             │
             └──→ External: PostgreSQL, MinIO, OTel Collector
```

## **7. Design Patterns & Conventions**

### **7.1 Data Validation**

All data crossing boundaries must be Pydantic models:

```python
# gop/shared/graph_model.py
from pydantic import BaseModel, Field

class Node(BaseModel):
    id: str
    x: float
    y: float
    demand: float = 0.0

class Edge(BaseModel):
    source: str
    target: str
    distance: float
    
class Graph(BaseModel):
    nodes: list[Node]
    edges: list[Edge]
    
    @classmethod
    def from_csv(cls, path: str) -> "Graph":
        ...
```

### **7.2 Exception Hierarchy**

```python
# gop/shared/exceptions.py
class GraphOptError(Exception):
    """Base exception for all gop errors."""

class ValidationError(GraphOptError):
    """Invalid input data."""

class SolverError(GraphOptError):
    """Optimization solver failed."""

class InfeasibleError(SolverError):
    """No feasible solution exists."""

class TimeoutError(SolverError):
    """Solver exceeded time limit."""
```

### **7.3 Service Pattern**

Each feature module exposes a Service class:

```python
# gop/rebalancer/service.py
from gop.shared.graph_model import Graph
from gop.rebalancer.models import RebalancerConfig, RebalancerResult
from gop.rebalancer.solver import VRPSolver

class RebalancerService:
    def __init__(self, solver: VRPSolver | None = None):
        self._solver = solver or VRPSolver()
    
    def optimize(
        self, 
        graph: Graph, 
        config: RebalancerConfig
    ) -> RebalancerResult:
        # 1. Validate input
        # 2. Build distance matrix (vectorized!)
        # 3. Call solver
        # 4. Return result
        ...
```

### **7.4 Vectorization Rules**

**Allowed:**
- `np.where`, `np.select`, `np.einsum`
- `pd.DataFrame` vectorized operations
- Broadcasting

**Forbidden:**
- `for` loops over data arrays
- `np.vectorize` (it's a Python loop wrapper)
- `df.iterrows()`

### **7.5 Coding Style**

- **Naming:** `snake_case` for functions/variables, `PascalCase` for classes
- **Type hints:** Mandatory for all public functions
- **Docstrings:** Google style, mandatory for public API
- **Line length:** 100 characters
- **Imports:** Sorted by isort (via Ruff)

## **8. Infrastructure**

### **8.1 Local Stack (Docker Compose)**

```yaml
# infrastructure/local/docker-compose.yml
services:
  # ═══ Databases ═══
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: gop
      POSTGRES_PASSWORD: gop
      POSTGRES_DB: gop
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql

  # ═══ Object Storage ═══
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data

  # ═══ Observability ═══
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config", "/etc/otel/config.yaml"]
    volumes:
      - ./observability/otel-collector.yml:/etc/otel/config.yaml
    ports:
      - "4317:4317"   # gRPC
      - "4318:4318"   # HTTP

  tempo:
    image: grafana/tempo:latest
    command: ["-config.file=/etc/tempo.yaml"]
    volumes:
      - ./observability/tempo.yml:/etc/tempo.yaml
    ports:
      - "3200:3200"   # Tempo API

  loki:
    image: grafana/loki:latest
    command: ["-config.file=/etc/loki/config.yaml"]
    volumes:
      - ./observability/loki.yml:/etc/loki/config.yaml
    ports:
      - "3100:3100"

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./observability/prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    volumes:
      - ./observability/grafana/provisioning:/etc/grafana/provisioning
      - ./observability/grafana/dashboards:/var/lib/grafana/dashboards
    ports:
      - "3000:3000"
    depends_on:
      - prometheus
      - tempo
      - loki

volumes:
  postgres_data:
  minio_data:
```

### **8.2 Azure Infrastructure (Terraform)**

```hcl
# infrastructure/azure/main.tf

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

locals {
  project = "gop"
  env     = var.environment
}

# Resource Group
resource "azurerm_resource_group" "main" {
  name     = "rg-${local.project}-${local.env}"
  location = var.location
}

# Modules
module "postgresql" {
  source              = "./modules/postgresql"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  project             = local.project
  env                 = local.env
}

module "storage" {
  source              = "./modules/storage"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  project             = local.project
  env                 = local.env
}

module "container_apps" {
  source              = "./modules/container_apps"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  project             = local.project
  env                 = local.env
  
  db_connection_string = module.postgresql.connection_string
  storage_connection   = module.storage.connection_string
}

module "monitoring" {
  source              = "./modules/monitoring"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  project             = local.project
  env                 = local.env
}
```

## **9. Configuration Management**

### **9.1 Environment Variables**

| Variable | Package | Local Default | Azure Source |
|----------|---------|---------------|--------------|
| `ENV` | all | `LOCAL` | App Setting |
| `DATABASE_URL` | api | `postgresql+asyncpg://gop:gop@localhost:5432/gop` | Key Vault |
| `MINIO_ENDPOINT` | api | `http://localhost:9000` | App Setting |
| `MINIO_ACCESS_KEY` | api | `minioadmin` | Key Vault |
| `MINIO_SECRET_KEY` | api | `minioadmin` | Key Vault |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | api | `http://localhost:4317` | App Setting |
| `API_URL` | ui | `http://localhost:8000` | App Setting |

### **9.2 Config Classes**

```python
# packages/gop-api/api/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    env: str = "LOCAL"
    database_url: str
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    otel_endpoint: str = "http://localhost:4317"
    
    class Config:
        env_file = ".env"
```

## **10. Development Workflow**

### **10.1 Initial Setup**

```bash
# Clone repository
git clone https://github.com/you/graph-optimization-platform
cd graph-optimization-platform

# Install uv (if not installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Setup all packages
cd packages/gop-core && uv venv && uv pip install -e ".[dev]" && cd ../..
cd packages/gop-api && uv venv && uv pip install -e ".[dev]" && cd ../..
cd packages/gop-ui && uv venv && uv pip install -e ".[dev]" && cd ../..

# Start infrastructure
cd infrastructure/local
docker-compose up -d

# Run migrations
cd ../../packages/gop-api
alembic upgrade head
```

### **10.2 Daily Development**

```bash
# Terminal 1: API
cd packages/gop-api
source .venv/Scripts/activate  # Windows
uvicorn api.main:app --reload

# Terminal 2: UI  
cd packages/gop-ui
source .venv/Scripts/activate
streamlit run ui/app.py

# Terminal 3: Tests (watch mode)
cd packages/gop-core
pytest --watch
```

### **10.3 Running Tests**

```bash
# Core library tests
cd packages/gop-core && pytest

# API tests (requires running infrastructure)
cd packages/gop-api && pytest

# All tests
./scripts/run_all_tests.sh
```

### **10.4 Code Quality**

```bash
# Linting (from any package)
ruff check .
ruff format .

# Type checking
mypy .

# Import boundaries (core only)
cd packages/gop-core && import-linter
```

## **11. CI/CD Pipeline**

### **11.1 PR Checks (`.github/workflows/ci.yml`)**

```yaml
name: CI

on:
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v1
      - run: |
          cd packages/gop-core && uv pip install -e ".[dev]"
          ruff check .
          mypy .

  test-core:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v1
      - run: |
          cd packages/gop-core
          uv pip install -e ".[dev]"
          pytest --cov=gop --cov-report=xml

  test-api:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: test
          POSTGRES_PASSWORD: test
          POSTGRES_DB: test
        ports:
          - 5432:5432
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v1
      - run: |
          cd packages/gop-api
          uv pip install -e ".[dev]"
          pytest
```

### **11.2 Deploy to Azure (`.github/workflows/cd-azure.yml`)**

```yaml
name: Deploy to Azure

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Login to Azure
        uses: azure/login@v1
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}
      
      - name: Build and push API image
        run: |
          az acr build \
            --registry ${{ secrets.ACR_NAME }} \
            --image gop-api:${{ github.sha }} \
            packages/gop-api
      
      - name: Deploy API
        run: |
          az containerapp update \
            --name gop-api \
            --resource-group rg-gop-prod \
            --image ${{ secrets.ACR_NAME }}.azurecr.io/gop-api:${{ github.sha }}
```

## **12. Development Phases**

### **Phase 1: Core Library (Weeks 1-3)**

**Goal:** Working `gop-core` with rebalancer module.

**Deliverables:**
- [ ] Graph model (Node, Edge, Graph)
- [ ] VRP solver wrapper for OR-Tools
- [ ] RebalancerService with tests
- [ ] CLI for testing: `python -m gop.cli solve input.csv`
- [ ] Published to TestPyPI

**Success criteria:** Can solve a basic rebalancing problem from CSV.

### **Phase 2: Local Infrastructure (Weeks 4-6)**

**Goal:** Full local development stack.

**Deliverables:**
- [ ] Docker Compose with all services
- [ ] FastAPI with CRUD operations
- [ ] Database schema and migrations
- [ ] MinIO integration for file storage
- [ ] Basic Streamlit UI

**Success criteria:** Can submit job via UI, see result in Grafana.

### **Phase 3: Observability Deep Dive (Weeks 7-8)**

**Goal:** Understand observability stack deeply.

**Deliverables:**
- [ ] OpenTelemetry tracing end-to-end
- [ ] Structured logging with correlation IDs
- [ ] Custom Grafana dashboards
- [ ] Alerting rules in Prometheus

**Success criteria:** Can trace a request from UI through API to solver.

### **Phase 4: Azure Deployment (Weeks 9-11)**

**Goal:** Production-like Azure environment.

**Deliverables:**
- [ ] Terraform modules for all resources
- [ ] GitHub Actions CD pipeline
- [ ] Key Vault integration
- [ ] Azure Monitor dashboards

**Success criteria:** Same functionality as local, running in Azure.

### **Phase 5: Polish & Documentation (Week 12+)**

**Goal:** Portfolio-ready project.

**Deliverables:**
- [ ] Complete Sphinx documentation
- [ ] Architecture decision records (ADRs)
- [ ] Performance benchmarks
- [ ] README with demo video

**Success criteria:** Someone can clone and run in 10 minutes.

## **13. AI Usage Policy**

### **Allowed**
- GitHub Copilot for boilerplate
- LLM discussions about architecture
- Code review assistance
- Documentation generation

### **Prohibited**
- Full algorithm implementation by AI
- Copy-pasting without understanding
- "Vibe coding" — committing code you can't explain

### **Rule**
Every line of core algorithm code must be written by hand and understood deeply.

## **14. Resources**

### **Books**
- "Architecture Patterns with Python" — Harry Percival
- "Designing Data-Intensive Applications" — Martin Kleppmann

### **Documentation**
- [FastAPI](https://fastapi.tiangolo.com/)
- [OR-Tools](https://developers.google.com/optimization)
- [OpenTelemetry Python](https://opentelemetry.io/docs/instrumentation/python/)
- [Terraform Azure Provider](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs)

### **Courses**
- [Microsoft Learn: Azure Fundamentals](https://learn.microsoft.com/en-us/training/paths/azure-fundamentals/)
- [Terraform on Azure](https://learn.hashicorp.com/collections/terraform/azure-get-started)