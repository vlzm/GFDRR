# Architecture Clarity Refactoring — Completed

**Status: ALL STEPS DONE.** This document records what was done and why.

---

## Context

The original codebase worked correctly but was hard to navigate: a flat wall of ~46 DataFrame fields in the model, a 350-line monolithic loader method, bike-specific names in a supposedly generic protocol, and no quick-start path. Four steps fixed this without breaking any existing behavior.

---

## Step 1: Logical grouping in `model.py` ✓

**What:** Added logical structure to `RawModelData` and `ResolvedModelData` via section comments, `_GROUPS` class variable, group properties, and `table_summary()`.

**Why:** The model is the central contract. An opaque model makes everything downstream opaque.

**Result:** `RawModelData` fields are now grouped as:

```python
# ── entity: what exists in the network ────────────────────────────
facilities, commodity_categories, resource_categories, commodities, resources

# ── temporal: planning horizon and period grid ────────────────────
planning_horizon, planning_horizon_segments, periods

# ── behavior: roles, operations, rules ────────────────────────────
facility_roles, facility_operations, facility_availability, edge_rules

# ── edge: identity and attributes ─────────────────────────────────
edges, edge_commodities, edge_capacities, edge_commodity_capacities, edge_vehicles

# ── flow data: demand, supply, inventory ──────────────────────────
demand, supply, inventory_initial, inventory_in_transit

# ── transformation: N:M commodity conversion ──────────────────────
transformations, transformation_inputs, transformation_outputs

# ── resource: fleet, compatibility, availability ──────────────────
resource_commodity_compatibility, resource_modal_compatibility,
resource_fleet, resource_availability

# ── hierarchy: facility + commodity trees ─────────────────────────
facility_hierarchy_types/levels/nodes/memberships,
commodity_hierarchy_types/levels/nodes/memberships

# ── scenario: run configuration ───────────────────────────────────
scenarios, scenario_edge_rules, scenario_manual_edges, scenario_parameter_overrides

# ── parametric attribute system ───────────────────────────────────
attributes: AttributeRegistry  # replaces old fixed parametric fields
```

Note: the old fixed parametric fields (`operation_costs`, `transport_costs`, `resource_costs`, `operation_capacities`, `commodity_sell_price_tiers`, `commodity_procurement_cost_tiers`) were removed as part of the attribute system refactoring (see `attribute_system.md`).

`_GROUPS`, group properties (`entity_tables`, `temporal_tables`, etc.), `populated_tables`, and `table_summary()` are all on both `RawModelData` and `ResolvedModelData`.

---

## Step 2: Decompose `_build_raw_model()` in `DataLoaderGraph` ✓

**What:** Broke the monolithic 350-line method into focused private methods.

**Why:** Each transformation step became independently readable and testable.

**Result:** `_build_raw_model()` now orchestrates:

```python
def _build_raw_model(self) -> RawModelData:
    temporal = self._build_temporal()
    entities = self._build_entities()
    behavior = self._build_behavior(entities)
    distance_data = self._build_distance_matrix(entities) if self._config.build_edges else {}
    flow = self._build_node_parameters(entities, temporal)
    resources = self._build_resources(entities)

    registry = AttributeRegistry()
    self._register_costs(registry, temporal)
    self._register_resource_costs(registry, entities)

    return RawModelData(**{**temporal, **entities, **behavior, ...}, attributes=registry)
```

Note: `_build_costs()` became `_register_costs()` — attributes are registered into the `AttributeRegistry` directly instead of returning raw DataFrames. Same for `_register_resource_costs()`.

Methods:
- `_build_temporal()` — planning_horizon, segments, periods
- `_build_entities()` — facilities, commodity_categories, resource_categories
- `_build_behavior(entities)` — facility_roles, facility_operations, edge_rules
- `_build_distance_matrix(entities)` — distance_matrix (pairwise distances + duration)
- `_build_node_parameters(entities, temporal)` — demand, inventory_initial
- `_build_resources(entities)` — resource_fleet, resource_commodity_compatibility, resource_modal_compatibility
- `_register_costs(registry, temporal)` — registers operation_cost, transport_cost
- `_register_resource_costs(registry, entities)` — registers resource_fixed_cost, resource_maintenance_cost

---

## Step 3: Separate bike-specific source from generic protocol ✓

**What:** Renamed `DataSourceProtocol` → `BikeShareSourceProtocol`, added `GenericSourceProtocol`.

**Why:** Makes it clear where "bike-sharing" ends and "universal graph" begins.

**Result** (`gbp/loaders/protocols.py`):

```python
class GenericSourceProtocol(Protocol):
    """Minimal interface: any data source that can produce DataFrames."""
    def load_data(self) -> None: ...
    def get_dataframes(self) -> dict[str, pd.DataFrame]: ...

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

`DataLoaderGraph` accepts `BikeShareSourceProtocol`. `DataLoaderMock` satisfies it without changes. The backward-compatible `DataSourceProtocol` alias was removed during architecture cleanup.

---

## Step 4: Quick-start factory function ✓

**What:** `make_raw_model()` in `gbp/core/factory.py` — creates a valid `RawModelData` from minimal inputs.

**Why:** Lowers the barrier for experimentation. Without it, you need 9+ DataFrames to get started.

**Result:**

```python
from gbp.core import make_raw_model
from datetime import date
import pandas as pd

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
        "name": ["Truck"], "base_capacity": [20.0], "capacity_unit": ["unit"],
    }),
    planning_start=date(2025, 1, 1),
    planning_end=date(2025, 1, 8),
)
```

Auto-generates: `planning_horizon`, `segments`, `periods`, `facility_roles`, `facility_operations`, `edge_rules`. Optional tables (`demand`, `inventory_initial`, etc.) passed as keyword arguments.

---

## Invariants maintained throughout

- Zero breaking changes in public API
- All field names, types, and `_SCHEMAS` unchanged (where fields still exist)
- All existing tests pass after each step
- `_REQUIRED` enforcement and `validate()` logic unchanged
