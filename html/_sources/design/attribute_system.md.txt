# Design: Dynamic Attribute System

**Status: IMPLEMENTED.** This document describes the current architecture as built.

---

## 1. Problem this solves

Parametric data (costs, capacities, prices) has an inherently variable structure: in bike-sharing, `operation_cost` has grain `(facility_id, operation_type, commodity_category, date)`. In another domain it might be `(facility_id, date)`. A fixed set of DataFrame fields cannot represent this.

The attribute system solves this with a registry-based approach: specs describe the grain and semantics, data is registered per attribute, and the build pipeline and spine assembly are fully generic.

---

## 2. Structural vs parametric tables

Not all tables are equal. The model has two distinct categories:

### Structural tables (fixed fields in the model)

Define network topology and rules. Consumers (optimizer, simulator) address them by name — they are a contract:

```
entity:       facilities, commodity_categories, resource_categories, commodities, resources
temporal:     planning_horizon, planning_horizon_segments, periods
behavior:     facility_roles, facility_operations, facility_availability, edge_rules
edge:         edges, edge_commodities, edge_capacities, edge_commodity_capacities, edge_vehicles
flow_data:    demand, supply, inventory_initial, inventory_in_transit
transform:    transformations, transformation_inputs, transformation_outputs
resource:     resource_fleet, resource_commodity_compatibility, resource_modal_compatibility,
              resource_availability
hierarchy:    facility_hierarchy_*, commodity_hierarchy_*
scenario:     scenarios, scenario_edge_rules, scenario_manual_edges, scenario_parameter_overrides
```

### Parametric tables (dynamic — live in `AttributeRegistry`)

Numeric values keyed by an entity grain. The set of attributes, their grain, and names are domain-defined. In the model:

```python
attributes: AttributeRegistry  # on both RawModelData and ResolvedModelData
```

Standard bike-sharing attributes registered via `register_bike_sharing_defaults()`:

| Attribute name | entity_type | kind | grain |
|---|---|---|---|
| `operation_cost` | facility | COST | facility × operation_type × commodity_category × date |
| `operation_capacity` | facility | CAPACITY | facility × operation_type × commodity_category |
| `transport_cost` | edge | COST | source × target × modal_type × resource_category × date |
| `resource_fixed_cost` | resource | COST | resource_category × date |
| `resource_maintenance_cost` | resource | COST | resource_category × date |

Custom attributes with any grain can be registered alongside these.

---

## 3. Key classes

### `AttributeSpec` (`core/attributes/spec.py`)

Immutable descriptor for one numeric attribute: name, kind, entity_type, grain, resolved_grain (date→period_id), value_column, aggregation, unit, nullable, eav_filter.

### `AttributeRegistry` (`core/attributes/registry.py`)

Central API for the model's parametric data.

```python
registry.register(
    name="operation_cost",
    data=df,
    entity_type="facility",
    kind=AttributeKind.COST,
    grain=("facility_id", "operation_type", "commodity_category", "date"),
    value_column="cost_per_unit",
    aggregation="mean",
    unit="USD",
)

registry.get("operation_cost")                    # → RegisteredAttribute(spec, data)
registry.get_by_entity("facility")               # → list[RegisteredAttribute]
registry.get_by_kind(AttributeKind.COST)         # → list[RegisteredAttribute]
registry.specs                                   # → list[AttributeSpec]
registry.names                                   # → list[str]
registry.to_dict()                               # → {name: DataFrame}
registry.summary()                               # → human-readable string
```

`register()` validates: grain columns exist in data, value_column exists, numeric values satisfy kind constraints (COST ≥ 0, CAPACITY > 0).

### `RegisteredAttribute`

Frozen dataclass: `spec: AttributeSpec` + `data: pd.DataFrame`.

### `AttributeBuilder` (`core/attributes/builder.py`)

Assembles spine DataFrames for one entity type. Used internally by `assemble_spines()`.

```python
builder = AttributeBuilder(entity_type="facility")
builder.register(spec)
spines = builder.build_spines(base_df, attribute_data)
# → {"group_0": wide_df, "group_1": wide_df, ...}
```

Groups are determined by grain compatibility (`auto_group_attributes()`). Within each group, attributes are left-joined onto the base entity table.

---

## 4. How the build pipeline uses attributes

### Step: Time resolution (`build/time_resolution.py`)

`resolve_registry_attributes(raw.attributes, periods)` iterates over `raw.attributes.specs`. For each time-varying attribute (grain contains `"date"`), it resolves `date → period_id` using `ceil(hours / period_duration)` and re-registers with resolved grain into a new registry.

```python
# in build_model():
resolved_attrs = resolve_registry_attributes(raw.attributes, periods)
```

No hardcoded attribute names — the pipeline is driven by specs.

### Step: Spine assembly (`build/spine.py`)

`assemble_spines(resolved)` reads `resolved.attributes.specs`, groups by entity type, builds one `AttributeBuilder` per entity, and calls `build_spines()`.

```python
# in build_model():
spines = assemble_spines(resolved)
resolved.facility_spines = spines["facility"] or None
resolved.edge_spines = spines["edge"] or None
resolved.resource_spines = spines["resource"] or None
```

Structural attributes (those defined in `get_structural_attribute_specs()`) are also included in spines.

---

## 5. How dataloaders use attributes

`DataLoaderGraph` registers attributes directly into `raw.attributes`:

```python
def _build_raw_model(self) -> RawModelData:
    ...
    registry = AttributeRegistry()
    self._register_costs(registry, temporal)
    self._register_resource_costs(registry, entities)
    return RawModelData(..., attributes=registry)
```

`_register_costs()` and `_register_resource_costs()` build DataFrames and call `registry.register()` with appropriate grain.

---

## 6. `defaults.py`: convenience, not configuration

`defaults.py` provides a convenience wrapper for standard bike-sharing attributes:

```python
register_bike_sharing_defaults(
    registry,
    operation_costs=df1,
    transport_costs=df2,
    operation_capacities=df3,
    resource_costs=df4,
)
```

This calls `registry.register()` with typical bike-sharing grains. Users call `registry.register()` directly for custom attributes.

Functions `get_all_default_specs()`, `get_facility_attribute_specs()`, `get_edge_attribute_specs()` are **deprecated** — they exist for backward compatibility in `spine.py` fallback logic only.

---

## 7. Serialization

### Dict/JSON

```python
{
    "facilities": [...],
    "attributes": {
        "operation_cost": {
            "spec": {
                "name": "operation_cost",
                "kind": "cost",
                "entity_type": "facility",
                "grain": ["facility_id", "operation_type", "commodity_category", "date"],
                "value_column": "cost_per_unit",
                "aggregation": "mean"
            },
            "data": [...]
        }
    }
}
```

`raw_to_dict()` / `dict_to_raw()` in `gbp/io/` handle attribute serialization via `AttributeRegistry.to_dict()` and `register_raw()`.

### Parquet

```
model_dir/
├── _metadata.json
├── facilities.parquet
├── ...
└── attributes/
    ├── _specs.json
    ├── operation_cost.parquet
    └── transport_cost.parquet
```

---

## 8. Full example

```python
from gbp.core import RawModelData, AttributeRegistry, AttributeKind
from gbp.build import build_model

raw = RawModelData(
    facilities=facilities_df,
    commodity_categories=commodity_cats_df,
    # ... other structural tables ...
)

# Standard attributes via convenience wrapper
from gbp.core.attributes.defaults import register_bike_sharing_defaults
register_bike_sharing_defaults(
    raw.attributes,
    operation_costs=op_costs_df,
    transport_costs=transport_costs_df,
)

# Custom attribute with non-standard grain
raw.attributes.register(
    name="weather_penalty",
    data=weather_df,
    entity_type="edge",
    kind=AttributeKind.RATE,
    grain=("source_id", "target_id", "modal_type", "date"),
    value_column="penalty_factor",
    aggregation="mean",
)

print(raw.attributes.summary())
# operation_cost:   960 rows  facility  COST  [facility_id × operation_type × commodity_category × date]
# transport_cost:   180 rows  edge      COST  [source_id × target_id × modal_type × resource_category × date]
# weather_penalty:   90 rows  edge      RATE  [source_id × target_id × modal_type × date]

resolved = build_model(raw)
# resolved.attributes contains time-resolved versions of all attributes
# resolved.facility_spines, edge_spines, resource_spines are assembled
```

---

## 9. What does NOT change with new attributes

- `core/attributes/spec.py` — AttributeSpec is stable
- `core/attributes/grain_groups.py` — auto_group_attributes is stable
- `core/attributes/merge_plan.py` — plan_merges is stable
- `core/attributes/builder.py` — AttributeBuilder.build_spines() is stable
- All structural tables and their Pydantic schemas
- `build_model()` steps: validation, edge building, lead times, transformation, fleet capacity

---

## 10. Design alternatives considered

**Alt 1: Fixed fields + extra_attributes registry** — rejected: two paths for the same concept, unclear which to use.

**Alt 2: Make demand/supply/edges dynamic too** — rejected: structural tables are a typed contract with consumers.

**Alt 3: EAV (one table for all attributes)** — rejected: no per-attribute grain validation, expensive pivots on every access.
