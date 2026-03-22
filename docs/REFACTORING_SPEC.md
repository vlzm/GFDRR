# Refactoring Specification: gbp Architecture Clarity

## Context

We have a graph-based logistics platform (`gbp`) built on a data model document (`docs/graph_data_model.md`). The data model was designed collaboratively — it's theoretically complete and mathematically sound (multi-commodity flow, multi-resolution temporal axis, N→M transformations, hierarchies, attribute system with grain groups).

The code was generated from this spec by an AI assistant. The result works correctly (tests pass, pipeline runs), but the codebase is hard to navigate and understand. This document describes the problems and a 4-step refactoring plan to fix them **without breaking any existing behavior**.

**Golden rule: zero breaking changes.** Every existing test, import, and call site must continue working after each step. Each step is an independent PR.

---

## Problem Diagnosis

### Problem 1: RawModelData / ResolvedModelData are flat walls of fields

**File:** `gbp/core/model.py`

`RawModelData` is a single dataclass with ~46 DataFrame fields. When you open the file, you see a flat list with no visual grouping. You can't tell at a glance:
- Which tables are required vs optional?
- Which tables relate to edges vs costs vs hierarchy?
- Which tables are even populated in a given instance?

The same applies to `ResolvedModelData` (52 fields).

**Impact:** The model is the central contract — everything flows through it. If it's opaque, everything downstream is opaque.

### Problem 2: `DataLoaderGraph._build_raw_model()` is a 350-line monolith

**File:** `gbp/loaders/dataloader_graph.py`

This single method does ALL of the following in one continuous block:
1. Build temporal tables (planning_horizon, segments, periods)
2. Build facility DataFrames from stations + depots
3. Build roles and operations for each facility
4. Build edges (with distance computation)
5. Build edge_commodities
6. Build inventory_initial from first timestep
7. Build operation_capacities from station capacity
8. Build operation_costs and transport_costs from cost tables
9. Build resource_fleet and compatibility tables
10. Assemble everything into `RawModelData(...)`

Each of these is a logically independent transformation, but they're all tangled together in one method. Variables from step 2 are used in step 7. There's no way to understand or modify one step without reading the entire method.

### Problem 3: DataSourceProtocol is bike-specific but pretends to be generic

**File:** `gbp/loaders/protocols.py`

The protocol has fields like `df_stations`, `df_depots`, `df_trips`, `df_telemetry_ts` — these are all bike-sharing concepts. But the platform is supposed to be domain-agnostic. This creates a conceptual gap: where does "bike-sharing" end and "universal graph" begin?

### Problem 4: No "quick start" path

To create even the simplest model, you need to construct ~9 required DataFrames manually. There's no helper that says "give me facilities and edges, I'll figure out the rest." This makes the learning curve steep and discourages experimentation.

---

## Refactoring Plan

### Step 1: Logical grouping in model.py

**What:** Add logical structure to `RawModelData` and `ResolvedModelData` WITHOUT changing the flat dataclass layout.

**Why:** This is the fastest win — immediately makes the central contract navigable.

**How:**

1. **Reorder fields by domain group** (with section comments):

```python
@dataclass
class RawModelData:
    # ── entity: what exists in the network ────────────────────────────
    facilities: pd.DataFrame                          # required
    commodity_categories: pd.DataFrame                # required
    resource_categories: pd.DataFrame                 # required
    commodities: pd.DataFrame | None = None           # L3 optional
    resources: pd.DataFrame | None = None             # L3 optional

    # ── temporal: planning horizon and period grid ────────────────────
    planning_horizon: pd.DataFrame = None             # required
    planning_horizon_segments: pd.DataFrame = None    # required
    periods: pd.DataFrame = None                      # required

    # ── behavior: roles, operations, rules ────────────────────────────
    facility_roles: pd.DataFrame = None               # required
    facility_operations: pd.DataFrame = None          # required
    facility_availability: pd.DataFrame | None = None
    edge_rules: pd.DataFrame = None                   # required

    # ── edge: identity and attributes ─────────────────────────────────
    edges: pd.DataFrame | None = None
    edge_commodities: pd.DataFrame | None = None
    edge_capacities: pd.DataFrame | None = None
    edge_commodity_capacities: pd.DataFrame | None = None
    edge_vehicles: pd.DataFrame | None = None

    # ── flow data: demand, supply, inventory ──────────────────────────
    demand: pd.DataFrame | None = None
    supply: pd.DataFrame | None = None
    inventory_initial: pd.DataFrame | None = None
    inventory_in_transit: pd.DataFrame | None = None

    # ── transformation: N:M commodity conversion ──────────────────────
    transformations: pd.DataFrame | None = None
    transformation_inputs: pd.DataFrame | None = None
    transformation_outputs: pd.DataFrame | None = None

    # ── resource: fleet, compatibility, availability ──────────────────
    resource_commodity_compatibility: pd.DataFrame | None = None
    resource_modal_compatibility: pd.DataFrame | None = None
    resource_fleet: pd.DataFrame | None = None
    resource_availability: pd.DataFrame | None = None

    # ── parameters: costs, capacities, pricing ────────────────────────
    operation_capacities: pd.DataFrame | None = None
    operation_costs: pd.DataFrame | None = None
    transport_costs: pd.DataFrame | None = None
    resource_costs: pd.DataFrame | None = None
    commodity_sell_price_tiers: pd.DataFrame | None = None
    commodity_procurement_cost_tiers: pd.DataFrame | None = None

    # ── hierarchy: facility + commodity trees ─────────────────────────
    facility_hierarchy_types: pd.DataFrame | None = None
    facility_hierarchy_levels: pd.DataFrame | None = None
    facility_hierarchy_nodes: pd.DataFrame | None = None
    facility_hierarchy_memberships: pd.DataFrame | None = None
    commodity_hierarchy_types: pd.DataFrame | None = None
    commodity_hierarchy_levels: pd.DataFrame | None = None
    commodity_hierarchy_nodes: pd.DataFrame | None = None
    commodity_hierarchy_memberships: pd.DataFrame | None = None

    # ── scenario: run configuration ───────────────────────────────────
    scenarios: pd.DataFrame | None = None
    scenario_edge_rules: pd.DataFrame | None = None
    scenario_manual_edges: pd.DataFrame | None = None
    scenario_parameter_overrides: pd.DataFrame | None = None
```

2. **Add `_GROUPS` class variable** — a dict mapping group name → list of field names:

```python
_GROUPS: ClassVar[dict[str, list[str]]] = {
    "entity": ["facilities", "commodity_categories", "resource_categories",
               "commodities", "resources"],
    "temporal": ["planning_horizon", "planning_horizon_segments", "periods"],
    "behavior": ["facility_roles", "facility_operations",
                 "facility_availability", "edge_rules"],
    "edge": ["edges", "edge_commodities", "edge_capacities",
             "edge_commodity_capacities", "edge_vehicles"],
    "flow_data": ["demand", "supply", "inventory_initial", "inventory_in_transit"],
    "transformation": ["transformations", "transformation_inputs",
                       "transformation_outputs"],
    "resource": ["resource_commodity_compatibility", "resource_modal_compatibility",
                 "resource_fleet", "resource_availability"],
    "parameters": ["operation_capacities", "operation_costs", "transport_costs",
                   "resource_costs", "commodity_sell_price_tiers",
                   "commodity_procurement_cost_tiers"],
    "hierarchy": ["facility_hierarchy_types", "facility_hierarchy_levels",
                  "facility_hierarchy_nodes", "facility_hierarchy_memberships",
                  "commodity_hierarchy_types", "commodity_hierarchy_levels",
                  "commodity_hierarchy_nodes", "commodity_hierarchy_memberships"],
    "scenario": ["scenarios", "scenario_edge_rules",
                 "scenario_manual_edges", "scenario_parameter_overrides"],
}
```

3. **Add property for each group:**

```python
@property
def entity_tables(self) -> dict[str, pd.DataFrame]:
    """Core entities: facilities, commodity/resource categories, L3 items."""
    return _collect_group(self, self._GROUPS["entity"])
```

Where `_collect_group` is a module-level helper:

```python
def _collect_group(obj: object, field_names: list[str]) -> dict[str, pd.DataFrame]:
    """Return {name: df} for non-None DataFrames in field_names."""
    result: dict[str, pd.DataFrame] = {}
    for name in field_names:
        val = getattr(obj, name, None)
        if val is not None and isinstance(val, pd.DataFrame):
            result[name] = val
    return result
```

Properties to add on both `RawModelData` and `ResolvedModelData`:
- `entity_tables`
- `temporal_tables`
- `behavior_tables`
- `edge_tables`
- `flow_tables`
- `transformation_tables`
- `resource_tables`
- `parameter_tables`
- `hierarchy_tables`
- `scenario_tables`
- `populated_tables` — all non-None DataFrames

`ResolvedModelData` additionally gets:
- `generated_tables` — `edge_lead_time_resolved`, `transformation_resolved`, `fleet_capacity`
- `spine_tables` — `facility_spines`, `edge_spines`, `resource_spines`

4. **Add `table_summary()` method:**

```python
def table_summary(self) -> str:
    """Human-readable overview of populated tables, grouped logically."""
    return _table_summary(self, self._GROUPS, self._REQUIRED)
```

Output looks like:
```
RawModelData — table summary
============================

  entity
  ──────
    facilities: 10 rows (required)
    commodity_categories: 1 rows (required)
    resource_categories: 1 rows (required)
    commodities: —
    resources: —

  temporal
  ────────
    planning_horizon: 1 rows (required)
    ...
```

5. **Field ordering trade-off:** Because fields are grouped by domain, some required fields follow optional fields from the previous group. Python dataclass requires all defaulted fields after non-defaulted ones. Solution: required fields that come after optional ones get `= None` as syntactic workaround. `_REQUIRED` and `validate()` remain the real enforcement. Add explicit note in docstring about this.

6. **Apply identical treatment to `ResolvedModelData`**, which additionally has:
   - `edge_lead_time_resolved`, `transformation_resolved`, `fleet_capacity` in a "generated" group
   - `facility_spines`, `edge_spines`, `resource_spines` (dict[str, DataFrame]) 
   - `_GROUPS` inherits from Raw and adds "generated"

**What NOT to change:**
- `_SCHEMAS` dict — keep identical
- `_REQUIRED` set — keep identical  
- `validate()` logic — keep identical
- All field names — keep identical
- All field types — keep identical

**Verification:** All existing tests must pass. `_GROUPS` must cover every non-underscore field (write a test for this).

---

### Step 2: Decompose `_build_raw_model()` in DataLoaderGraph

**File:** `gbp/loaders/dataloader_graph.py`

**What:** Break the monolithic `_build_raw_model()` into 6-8 focused private methods.

**Why:** Each transformation step (facilities, temporal, edges, costs...) becomes independently readable and testable.

**How:**

Split `_build_raw_model()` into these methods, each returning a `dict[str, pd.DataFrame]`:

```python
def _build_raw_model(self) -> RawModelData:
    """Assemble RawModelData from source DataFrames."""
    temporal = self._build_temporal()
    entities = self._build_entities()
    behavior = self._build_behavior(entities)
    edges_and_commodities = self._build_edges(entities, temporal) if self._config.build_edges else {}
    flow = self._build_flow_data(entities, temporal)
    costs = self._build_costs(entities, temporal)
    resources = self._build_resources()
    
    return RawModelData(**{
        **temporal,
        **entities,
        **behavior,
        **edges_and_commodities,
        **flow,
        **costs,
        **resources,
    })
```

Each builder method:

1. **`_build_temporal(self) -> dict`** — ~30 lines
   - Compute `start_d`, `end_d` from `self._source.timestamps`
   - Generate `periods` DataFrame (one per unique day)
   - Create `planning_horizon` and `planning_horizon_segments`
   - Return `{"planning_horizon": ..., "planning_horizon_segments": ..., "periods": ...}`

2. **`_build_entities(self) -> dict`** — ~30 lines
   - Build `facilities` from `df_stations` + `df_depots`
   - Create `commodity_categories` and `resource_categories` (constants)
   - Return `{"facilities": ..., "commodity_categories": ..., "resource_categories": ...}`

3. **`_build_behavior(self, entities: dict) -> dict`** — ~40 lines
   - Extract `station_ids` and `depot_ids` from entities
   - Generate `facility_roles` and `facility_operations` per facility type
   - Generate `edge_rules`
   - Return `{"facility_roles": ..., "facility_operations": ..., "edge_rules": ...}`

4. **`_build_edges(self, entities: dict, temporal: dict) -> dict`** — ~60 lines
   - Distance computation (haversine/euclidean) for all facility pairs
   - Build `edges` DataFrame and `edge_commodities`
   - Return `{"edges": ..., "edge_commodities": ...}`

5. **`_build_flow_data(self, entities: dict, temporal: dict) -> dict`** — ~40 lines
   - Build `demand` from trips aggregation
   - Build `inventory_initial` from first timestep
   - Build `operation_capacities` from station capacities
   - Return `{"demand": ..., "inventory_initial": ..., "operation_capacities": ...}`

6. **`_build_costs(self, entities: dict, temporal: dict) -> dict`** — ~60 lines
   - Build `operation_costs` from `df_station_costs`
   - Build `transport_costs` from `df_truck_rates`
   - Return `{"operation_costs": ..., "transport_costs": ...}`

7. **`_build_resources(self) -> dict`** — ~20 lines
   - Build `resource_fleet`, `resource_commodity_compatibility`, `resource_modal_compatibility`
   - Return the dict

**Key implementation detail:** Each method reads from `self._source` and `self._config`. Shared intermediate results (like `station_ids`, `depot_ids`) are either passed as arguments or extracted from the `entities` dict. No instance state is created between steps — this keeps each method pure.

**Verification:** All tests in `tests/test_graph_loader.py` must pass unchanged.

---

### Step 3: Separate bike-specific source from generic protocol

**File:** `gbp/loaders/protocols.py` + new file `gbp/loaders/bike_source.py`

**What:** The current `DataSourceProtocol` has bike-specific attributes (`df_stations`, `df_trips`, etc.). Rename it to make the domain-specificity explicit and add a minimal generic base.

**Why:** Makes it clear where "bike-sharing" ends and "universal graph" begins.

**How:**

1. **Keep `DataSourceProtocol` as-is but rename it to `BikeShareSourceProtocol`:**

```python
class BikeShareSourceProtocol(Protocol):
    """Bike-sharing data source — stations, depots, trips, telemetry."""
    df_stations: pd.DataFrame
    df_depots: pd.DataFrame
    df_resources: pd.DataFrame
    timestamps: pd.DatetimeIndex
    df_inventory_ts: pd.DataFrame
    df_telemetry_ts: pd.DataFrame
    df_trips: pd.DataFrame
    df_station_costs: pd.DataFrame
    df_truck_rates: pd.DataFrame
    def load_data(self) -> None: ...
```

2. **Add backward-compatible alias:**

```python
# Backward compatibility
DataSourceProtocol = BikeShareSourceProtocol
```

3. **Add a minimal generic protocol** (aspirational, for future loaders):

```python
class GenericSourceProtocol(Protocol):
    """Minimal interface: any data source that can produce DataFrames."""
    def load_data(self) -> None: ...
    def get_dataframes(self) -> dict[str, pd.DataFrame]: ...
```

4. **Update `DataLoaderGraph` type hint** to use `BikeShareSourceProtocol` explicitly:

```python
class DataLoaderGraph:
    def __init__(self, source: BikeShareSourceProtocol, config: ...):
```

5. **Update `GraphLoaderProtocol.source`** to return `BikeShareSourceProtocol`.

**Important:** `DataLoaderMock` already satisfies `BikeShareSourceProtocol` — no changes needed there.

**Verification:** All imports and tests must pass. The `DataSourceProtocol` alias ensures existing code works.

---

### Step 4: Add quick-start factory function

**File:** new file `gbp/core/factory.py`

**What:** A convenience function that creates a valid `RawModelData` from minimal inputs.

**Why:** Lowers the entry barrier for understanding and experimentation. Instead of constructing 9+ DataFrames, you pass 2-3 and get a working model.

**How:**

```python
def make_raw_model(
    facilities: pd.DataFrame,
    commodity_categories: pd.DataFrame,
    resource_categories: pd.DataFrame,
    *,
    planning_start: date,
    planning_end: date,
    period_type: str = "day",
    edge_rules: pd.DataFrame | None = None,
    demand: pd.DataFrame | None = None,
    supply: pd.DataFrame | None = None,
    **extra_tables: pd.DataFrame,
) -> RawModelData:
    """Create a valid RawModelData from minimal inputs.

    Auto-generates:
    - planning_horizon + segments + periods from date range
    - facility_roles from facility_type using DEFAULT_ROLES
    - facility_operations from facility_type (all enabled)
    - edge_rules: all-to-all ROAD if not provided

    Example::

        raw = make_raw_model(
            facilities=pd.DataFrame({
                "facility_id": ["d1", "s1", "s2"],
                "facility_type": ["depot", "station", "station"],
                "name": ["Depot", "St 1", "St 2"],
            }),
            commodity_categories=pd.DataFrame({
                "commodity_category_id": ["bike"],
                "name": ["Bike"], "unit": ["unit"],
            }),
            resource_categories=pd.DataFrame({
                "resource_category_id": ["truck"],
                "name": ["Truck"],
                "base_capacity": [20.0], "capacity_unit": ["unit"],
            }),
            planning_start=date(2025, 1, 1),
            planning_end=date(2025, 1, 8),
        )
    """
```

Implementation details:
- Generate `periods` from `planning_start` to `planning_end` using `period_type`
- Derive `facility_roles` using `gbp.core.roles.derive_roles` (already exists)
- Derive `facility_operations` by enabling all operations for each facility_type
- Default `edge_rules`: single rule `source_type=None, target_type=None, modal_type="road", enabled=True` (matches all pairs)
- Pass through `**extra_tables` for optional tables like `demand`, `inventory_initial`, etc.
- Call `raw.validate()` before returning

**Export from `gbp.core.__init__`:**

```python
from gbp.core.factory import make_raw_model
```

**Verification:** Add a test that creates a minimal model with `make_raw_model` and runs `build_model()` on it successfully.

---

## Implementation Order

1. **Step 1** (model.py grouping) — do first, zero risk, immediate clarity
2. **Step 2** (_build_raw_model decomposition) — do second, moderate scope
3. **Step 3** (protocol separation) — do third, small scope but conceptual clarity
4. **Step 4** (factory function) — do last, new code only, no refactoring risk

Each step should be a separate commit/PR. Run `pytest` after each step.

---

## Files Affected Per Step

### Step 1
- `gbp/core/model.py` — major rewrite (same fields, new structure + properties)
- No other files change

### Step 2
- `gbp/loaders/dataloader_graph.py` — refactor `_build_raw_model()` into methods
- No other files change (public API unchanged)

### Step 3
- `gbp/loaders/protocols.py` — rename + add alias + add generic protocol
- `gbp/loaders/dataloader_graph.py` — update type hint
- `gbp/loaders/__init__.py` — export new names
- No other files need changes (alias preserves backward compat)

### Step 4
- `gbp/core/factory.py` — new file
- `gbp/core/__init__.py` — add export
- `tests/unit/core/test_factory.py` — new test file

---

## Test Expectations

After ALL steps are done, these should all hold:

```bash
# All existing tests pass
pytest tests/ -v

# New capabilities work
python -c "
from gbp.core import RawModelData
# ... construct a model ...
print(raw.table_summary())           # grouped overview
print(raw.entity_tables.keys())      # {'facilities', 'commodity_categories', ...}
print(len(raw.populated_tables))     # only non-None tables
"

# Quick-start works
python -c "
from datetime import date
from gbp.core import make_raw_model
import pandas as pd

raw = make_raw_model(
    facilities=pd.DataFrame({
        'facility_id': ['d1', 's1'],
        'facility_type': ['depot', 'station'],
        'name': ['Depot', 'Station'],
    }),
    commodity_categories=pd.DataFrame({
        'commodity_category_id': ['bike'],
        'name': ['Bike'], 'unit': ['unit'],
    }),
    resource_categories=pd.DataFrame({
        'resource_category_id': ['truck'],
        'name': ['Truck'], 'base_capacity': [20.0], 'capacity_unit': ['unit'],
    }),
    planning_start=date(2025, 1, 1),
    planning_end=date(2025, 1, 4),
)
print(raw.table_summary())
"
```
