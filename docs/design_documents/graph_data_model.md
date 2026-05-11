# Graph-Based Logistics Platform — Data Model

## Overview

This document describes a universal graph-based data model for network flow problems. The platform enables modeling the movement of commodities (bikes in sharing systems, goods, money, etc.) through a network of facilities using transport resources. The data model is built on a multi-commodity flow formulation and tabular structures to support vectorized operations (pandas, PySpark).

The document describes **only the data model** — structures, tables, grains, and relationships. The data model describes the "world" (what objects, connections, and parameters exist) and is **shared** across different consumers: optimizer (all periods simultaneously), simulator (step by step), analytics (reporting). Each consumer uses the same `ResolvedModelData` but processes it differently (§15).

---

## 1. Core Entities

The platform is built on three base entities, each of which is a graph node:

### 1.1. Commodity — what is being moved

The primary entity that moves through the network. Split into two levels:

**CommodityCategory (L2)** — the type of flow in the network. The main unit by which flow data is indexed: `flow[edge, commodity_category, period]`. Each commodity category has its own demand/supply and its own throughput capacities.

**Commodity (L3)** — a specific instance or lot. Needed for instance-level tracking (which specific bike moved where). Optional level.


| Domain                | CommodityCategory (L2)    | Commodity (L3)              |
| --------------------- | ------------------------- | --------------------------- |
| Bike-sharing          | WORKING_BIKE, BROKEN_BIKE | Bike #4521                  |
| Financial transactions| USD, EUR                  | Transfer #TX-789            |
| Retail logistics      | SKU_A, SKU_B (example)    | Lot LOT-2024-001            |


### 1.2. Resource — what performs the movement

The entity that performs the movement of commodities between nodes. Split into two levels (analogous to Commodity):

**ResourceCategory (L2)** — the type of resource. Defines base characteristics: capacity, compatible commodities, compatible modal types, cost profile. Indexed by `transport_costs` (a table in the model).

**Resource (L2/L3, optional)** — a specific instance. Assigned to a home facility, has availability windows. Needed for tasks where the specific resource matters (fleet management, VRP). For aggregated tasks, `resource_fleet` is sufficient.


| Domain                | ResourceCategory (L2)      | Resource (L3)        |
| --------------------- | -------------------------- | -------------------- |
| Bike-sharing          | REBALANCING_TRUCK          | Truck #REB-07        |
| Financial transactions| SWIFT, SEPA                | Channel SWIFT-EU-001 |
| Retail logistics      | Delivery van (example)     | Vehicle #VAN-112     |


### 1.3. Facility — where commodities reside

A unified entity that replaces separate warehouse and receiver. Differences between object types are expressed through **roles** (semantic behavior in the network) and **operations** (available operations with cost/capacity).


| Domain                | Examples                                 |
| --------------------- | ---------------------------------------- |
| Bike-sharing          | Station, Depot, Maintenance Hub          |
| Financial transactions| Bank, Person, Payment Gateway            |
| Retail logistics      | Warehouse, store, cross-docking hub      |


**Why warehouse and receiver are unified into Facility:** the boundary between storage and receiver is artificial. A bike station simultaneously stores (warehouse) and issues/accepts (receiver). A bank is both. A bike-sharing depot *receives* bikes from stations and *returns* them after rebalancing or repair. Instead of a rigid separation, roles are used.

---

## 2. Facility: type, operations, roles

A Facility is described by three orthogonal dimensions.

### 2.1. FacilityType — what it physically is

Determines which operations are available and which costs/capacities exist.

```python
class FacilityType(Enum):
    """L3 default in gbp/core — bike-sharing."""
    STATION = "station"
    DEPOT = "depot"
    MAINTENANCE_HUB = "maintenance_hub"
```

### 2.2. Operations — what the facility can do

Each facility type supports a specific set of operations. Each operation has its own cost and capacity.

```python
class OperationType(Enum):
    """L3 default in gbp/core — bike-sharing plus universal network boundary operations."""
    RECEIVING = "receiving"      # receiving commodity
    STORAGE = "storage"          # storage
    DISPATCH = "dispatch"        # dispatch (issue / shipment)
    HANDLING = "handling"        # loading/unloading
    REPAIR = "repair"            # transformation BROKEN_BIKE → WORKING_BIKE (Maintenance Hub)
    CONSUMPTION = "consumption"  # node destroys flow (exit beyond network boundary)
    PRODUCTION = "production"    # node creates flow (entry into network from outside)
```

The first five operations describe physical actions *within* the network. `CONSUMPTION` and `PRODUCTION` describe interaction with the network boundary and are used in L2 domains where the consumer or producer itself is a graph node (gas customer, well). In bike-sharing, these two operations are not used — demand is handled by the `DemandPhase` phase without an explicit operation on the node.

Mapping of types to operations (bike-sharing, as in `gbp/core`):


| FacilityType     | RECEIVING | STORAGE | REPAIR | HANDLING | DISPATCH |
| ---------------- | --------- | ------- | ------ | -------- | -------- |
| Station          | ✓         | ✓       | —      | ✓        | ✓        |
| Depot            | ✓         | ✓       | —      | ✓        | ✓        |
| Maintenance Hub  | ✓         | ✓       | ✓      | ✓        | ✓        |


Maintenance Hub — the type with the REPAIR operation and associated `operation_costs` / `operation_capacities` for bike restoration.

### 2.3. FacilityRole — semantic behavior in the network

Roles from network flow theory, adapted for logistics. A role determines **how a node participates in the flow network** — what data is associated with it and what semantics it carries.

```python
class FacilityRole(Enum):
    SOURCE = "source"
    SINK = "sink"
    STORAGE = "storage"
    TRANSSHIPMENT = "transshipment"
```

**SOURCE** — a node that introduces commodity into the network. It has no incoming flow (or it is negligibly small). In bike-sharing: for example, a depot from which bikes enter the network after servicing, or a station with modeled **supply** during rebalancing. Associated data: `supply` (how much is available per period).

**SINK** — a node that consumes or removes commodity from the network. In bike-sharing: a station or zone where user demand "takes" bikes from the modeled network (net outflow), or abstract trip demand. Associated data: `demand` (how much is required per period).

**STORAGE** — a node that holds commodity over time. Key point: has capacity and state (how much is currently stored). Associated data: `storage_capacity`, `inventory_initial`.

**TRANSSHIPMENT** — a node that redirects commodity further. Does not produce or consume anything. A pure relay point.

One node can combine multiple roles:


| FacilityType     | Roles                         |
| ---------------- | ----------------------------- |
| Station          | SOURCE, SINK, STORAGE         |
| Depot            | STORAGE, TRANSSHIPMENT        |
| Maintenance Hub  | TRANSSHIPMENT, STORAGE        |


### 2.4. Role Derivation

Roles are derived from the type (defaults) with the ability to adjust based on operations and manual override:

```python
# Matches gbp.core.roles.DEFAULT_ROLES
DEFAULT_ROLES: dict[FacilityType, set[FacilityRole]] = {
    FacilityType.STATION:           {SOURCE, SINK, STORAGE},
    FacilityType.DEPOT:             {STORAGE, TRANSSHIPMENT},
    FacilityType.MAINTENANCE_HUB:   {TRANSSHIPMENT, STORAGE},
}

class Facility:
    facility_type: FacilityType
    operations: dict[OperationType, Operation]
    _role_overrides: set[FacilityRole] | None = None

    @property
    def roles(self) -> set[FacilityRole]:
        if self._role_overrides is not None:
            return self._role_overrides

        roles = set(DEFAULT_ROLES.get(self.facility_type, set()))

        # Adjustment based on operations
        if OperationType.STORAGE not in self.operations:
            roles.discard(FacilityRole.STORAGE)

        if (OperationType.RECEIVING in self.operations
                and OperationType.DISPATCH in self.operations):
            roles.add(FacilityRole.TRANSSHIPMENT)

        if OperationType.CONSUMPTION in self.operations:
            roles.add(FacilityRole.SINK)

        if OperationType.PRODUCTION in self.operations:
            roles.add(FacilityRole.SOURCE)

        return roles
```

All four roles are derived symmetrically:

- `STORAGE` — removed if the node has no `storage` operation.
- `TRANSSHIPMENT` — added when `receiving` + `dispatch` are present (pass-through node).
- `SINK` — added by the `consumption` operation (node destroys flow at the network exit).
- `SOURCE` — added by the `production` operation (node creates flow at the network entry).

`DEFAULT_ROLES` carries L3 bike-sharing domain semantics: for `station` / `depot` / `maintenance_hub`, roles are assigned by type because in bike-sharing the customer is not modeled as a `Facility` — demand is handled by a simulator phase. In L2 domains where the end consumer is itself a graph node (for example, gas delivery: depot → customer → gas consumption), the `SINK` role is achieved via an explicit `consumption` operation on the customer node, without modifying `DEFAULT_ROLES`. Symmetrically, a producer (well, factory) is described via a `production` operation.

### 2.5. Why roles are needed when type and operations exist

The three layers serve different purposes:

- **facility_type** → *what it is* (for business logic and UI)
- **operations** → *what it can do and how much it costs* (parameters)
- **roles** → *how it behaves in the flow network* (semantics)

Roles define the **semantics** of a node (SOURCE provides supply, SINK consumes demand, STORAGE holds inventory), operations define the **parameters** (costs, capacities). Depot and Maintenance Hub can both have TRANSSHIPMENT + STORAGE, but Maintenance Hub has REPAIR and the BROKEN_BIKE → WORKING_BIKE transformation; Station has user demand/supply roles (SINK/SOURCE) plus storage.

When porting to a new domain, facility_types and operations will be different, but roles will remain the same — semantics are reused without changes.

---

## 3. Temporal Axis

### 3.1. The Problem

In logistics, time is not just another column in the grain, but a **fundamental axis of the model**. Without an explicit temporal axis, it is impossible to express inventory carry-over between periods:

```
inventory[t] = inventory[t-1] + inflow[t] - outflow[t]
```

All flow and inventory data is indexed by periods: `flow[edge, commodity_category, period]`, `inventory[facility, commodity_category, period]`. Here edge = `(source_id, target_id, modal_type)`. Without this, the model is a static snapshot.

### 3.2. PlanningHorizon, Segments, and Period

Planning horizon — an **independent entity** (with its own PK) that describes the temporal grid. A scenario references the horizon via FK — the same horizon can be reused across multiple scenarios (A/B testing with different parameters but the same temporal grid).

A horizon can contain **multiple segments with different granularity** — multi-resolution planning. The nearest days are planned daily, the following weeks — weekly, the distant horizon — monthly.

```python
class PeriodType(Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
```

Example: 6-month horizon:


| Segment | Period Type | Range           | Periods  |
| ------- | ----------- | --------------- | -------- |
| 0       | DAY         | 1 Jan – 14 Jan  | 14       |
| 1       | WEEK        | 15 Jan – 11 Mar | 8        |
| 2       | MONTH       | 12 Mar – 30 Jun | ~4       |


`period_index` — global and contiguous (0, 1, ..., 25) across all segments for carry-over. Each period has its own `period_type` (inherited from the segment) and its own duration — the time resolution pipeline accounts for this when mapping raw dates.

For simple cases (single granularity), a single segment is used — fully backward compatible.

### 3.3. Storing raw data vs. resolved data

Parametric tables `operation_costs`, `transport_costs` (and others from `resolve_all_time_varying` in `gbp/build/time_resolution.py`) **remain with `date`** in the raw data, where the schema provides for it. During model assembly, a **time resolution** step is added — mapping dates to period_id with aggregation.

**Why not replace `date` with `period_id` immediately:**

- The same raw data can be used with different granularity (daily plan vs. weekly)
- ETL is simpler — data arrives with dates, no need to know period_id in advance
- Aggregation is a separate, testable step in the pipeline

### 3.4. Time Resolution Pipeline

Resolution maps raw `date` to `period_id` and aggregates values within a period:

```python
def resolve_to_periods(
    param_df: DataFrame,       # with date column
    periods: DataFrame,        # period_id, start_date, end_date
    value_columns: list[str],  # columns for aggregation
    group_grain: list[str],    # grain without date
    agg_func: str = "mean",    # how to aggregate within a period
) -> DataFrame:
    """
    Maps date → period_id and aggregates.
    A date falls into a period if period.start_date <= date < period.end_date.
    """
    merged = param_df.merge(
        periods,
        how="inner",
        left_on=lambda row: True,  # cross-like join with filter
        right_on=lambda row: True,
    )
    merged = merged[
        (merged["date"] >= merged["start_date"])
        & (merged["date"] < merged["end_date"])
    ]

    result = merged.groupby(group_grain + ["period_id"])[value_columns].agg(agg_func)
    return result.reset_index()
```

### 3.5. Demand, Supply, Initial Inventory

With a temporal axis, demand and supply become **explicit time-varying data**.

**Demand** — how much commodity is required by a facility with the SINK role in each period. Indexed by `commodity_category`. Raw data is stored with `date`, resolved to `period_id`.

**Supply** — how much commodity is available from a facility with the SOURCE role in each period. Indexed by `commodity_category`. Similarly time-varying.

**Initial Inventory** — a boundary condition: how much commodity is stored at a facility with the STORAGE role at the start of the horizon. Indexed by `commodity_category`. Set once, not time-varying.

**In-Transit Inventory** — commodity that is "in transit" at the start of the horizon. Sent before `planning_horizon.start_date` but not yet arrived. Without this, a facility receives nothing in the first `lead_time` periods, even if shipments were planned before the horizon start.

---

## 4. Multi-Commodity Flow and Transformation

### 4.1. Multi-Commodity Flow

The platform uses a multi-commodity flow formulation (Ahuja, Magnanti, Orlin, ch. 17; Williamson, ch. 10). Each `commodity_category` is a separate flow type in the network with its own demand, supply, and throughput capacities.

**Flow variable:** `flow[edge, commodity_category, period]` — the amount of commodity_category passing through an edge in a given period.

**Per-commodity conservation:** for each commodity_category at each node, rules apply separately depending on the role (SOURCE: limited by supply, SINK: satisfies demand, TRANSSHIPMENT: inflow = outflow, STORAGE: inventory balance).

**Shared edge capacity:** all commodity_categories on an edge share a common throughput capacity. Data about this is stored in the `edge_capacity` table. Since different commodities may have different units of measurement (bike count, trips, tons in other domains), shared capacity is expressed in **edge capacity units** (`edge_capacity.capacity_unit`). To convert flow to capacity units, a `capacity_consumption` coefficient in the `edge_commodity` table is used — how many capacity units one unit of the given commodity "costs" on the given edge.

For single-commodity tasks, the commodity dimension trivially collapses to a single element, and `flow[edge, commodity_category, period]` ≡ `flow[edge, period]`. No overhead.

### 4.2. Allowed commodities on edges

Each edge (`source_id × target_id × modal_type`) has a set of **allowed commodity_categories** — a filter determining which flow types can traverse this edge. One edge can carry multiple commodity_categories (a rebalancing truck carries both WORKING_BIKE and BROKEN_BIKE; a digital channel carries USD and EUR).


| Source           | Target           | Modal | Allowed Commodities             |
| ---------------- | ---------------- | ----- | ------------------------------- |
| Depot            | Station          | ROAD  | WORKING_BIKE, BROKEN_BIKE       |
| Station          | Depot            | ROAD  | WORKING_BIKE, BROKEN_BIKE       |
| Maintenance Hub  | Depot            | ROAD  | WORKING_BIKE                    |
| Depot            | Maintenance Hub  | ROAD  | BROKEN_BIKE                     |


### 4.3. Transformation — commodity conversion

Some facilities convert one commodity_category into another. **Maintenance Hub** in bike-sharing accepts BROKEN_BIKE and outputs WORKING_BIKE (REPAIR operation). A bank converts USD to EUR. An oil refinery accepts CRUDE and outputs GASOLINE + DIESEL + KEROSENE (splitting/co-production). A chemical plant blends two reagents into one product (blending).

Transformation is a **property of the facility**, tied to a specific operation. To support arbitrary N→M conversions (blending, splitting, co-production), the transformation is split into three tables:

```python
class Transformation:
    transformation_id: str
    facility_id: str
    operation_type: OperationType    # REPAIR (bike-sharing), CONVERSION, BLENDING, ...
    loss_rate: float = 0.0          # overall process losses (0.02 = 2%)
    batch_size: float | None = None # output multiplicity (null = continuous)

class TransformationInput:
    transformation_id: str
    commodity_category: CommodityCategory  # BROKEN_BIKE
    ratio: float                           # units of input per 1 "cycle"

class TransformationOutput:
    transformation_id: str
    commodity_category: CommodityCategory  # WORKING_BIKE
    ratio: float                           # units of output per 1 "cycle"
```

For the simple 1→1 case (Maintenance Hub: 1 BROKEN_BIKE → 1 WORKING_BIKE accounting for `loss_rate`): one TransformationInput (BROKEN_BIKE, ratio=1.0) and one TransformationOutput (WORKING_BIKE, ratio=1.0). For blending (2→1): two TransformationInputs. For splitting (1→2): two TransformationOutputs.

### 4.4. Impact on Roles

Roles **do not change**. TRANSSHIPMENT still means "relay point", but for nodes with transformation, the flow balance is expressed through transformation ratios instead of direct equality. The role describes the **semantics** of the node, the transformation describes the **parameters** of the conversion.

For nodes without transformation (Depot, Station without REPAIR), nothing changes — inflow and outflow of the same commodity, the balance remains `inflow = outflow`.

### 4.5. Transformation Examples

**1→1 (simple conversion):**


| Domain        | Facility        | Operation  | Inputs           | Outputs           | Loss  |
| ------------- | --------------- | ---------- | ---------------- | ----------------- | ----- |
| Bike-sharing  | Maintenance Hub | REPAIR     | BROKEN_BIKE ×1.0 | WORKING_BIKE ×1.0 | 0.05  |
| Finance       | Bank            | CONVERSION | USD ×1.0         | EUR ×(at rate)    | 0.001 |


**1→N (splitting / co-production):**


| Domain | Facility | Operation    | Inputs     | Outputs                                      | Loss |
| ------ | -------- | ------------ | ---------- | -------------------------------------------- | ---- |
| Oil    | Refinery | DISTILLATION | CRUDE ×1.0 | GASOLINE ×0.45, DIESEL ×0.30, KEROSENE ×0.15 | 0.10 |


**N→1 (blending):**


| Domain    | Facility | Operation | Inputs                         | Outputs      | Loss |
| --------- | -------- | --------- | ------------------------------ | ------------ | ---- |
| Chemistry | Blender  | BLENDING  | REAGENT_A ×0.6, REAGENT_B ×0.4 | PRODUCT ×1.0 | 0.02 |


---

## 5. Edges and Edge Rules

### 5.1. Edge Identity — multi-modal edges

Between two facilities, multiple edges with different **modal_type** (transport type) can exist: road, rail, sea, pipeline. Each edge is a separate "channel" with its own cost, capacity, lead_time.

PK of an edge: `source_id × target_id × modal_type`.

```python
class ModalType(Enum):
    ROAD = "road"
    RAIL = "rail"
    SEA = "sea"
    PIPELINE = "pipeline"
    AIR = "air"
    DIGITAL = "digital"        # for financial transactions
```

### 5.2. Separation of Concerns

Roles on nodes **do not determine** which edges are allowed. Allowed edges are a separate scenario configuration. Similarly, allowed commodities on an edge are a separate configuration, not a consequence of roles.

### 5.3. Edge Attributes

Each edge has a set of attributes of different natures:


| Attribute       | Grain (raw)                      | Time-varying | Kind       | Description                           |
| --------------- | -------------------------------- | ------------ | ---------- | ------------------------------------- |
| distance        | edge                             | no           | ADDITIONAL | Distance between nodes                |
| modal_type      | edge                             | no           | ADDITIONAL | Part of PK — transport type           |
| lead_time_hours | edge                             | no           | ADDITIONAL | Delivery time in hours (raw)          |
| capacity        | edge × date                      | yes          | CAPACITY   | Shared capacity (all commodities share)|
| max_shipment    | edge × commodity_category × date | yes          | CAPACITY   | Max volume per period per commodity   |
| min_shipment    | edge × commodity_category        | no           | CAPACITY   | Min shipment volume (nullable)        |
| transport_costs | edge × resource_category × date  | yes          | COST       | Transport cost (`transport_costs` table)|
| reliability     | edge                             | no           | ADDITIONAL | On-time delivery probability (0–1)    |


Here "edge" = `source_id × target_id × modal_type`.

**Lead Time** — delivery time, stored in absolute units (hours). Resolved to a whole number of periods during model assembly. In uniform resolution (one segment): `ceil(lead_time_hours / period_duration_hours)`. In multi-resolution mode, lead_time_periods is **not a scalar per edge** but an attribute `edge × period`, because the same lead_time = 48 hours yields 2 periods for DAY but 0 for WEEK. The resolved lead time is stored in a separate table `edge_lead_time_resolved` (§11.6).

Lead time defines the time shift: flow sent in period `t` arrives at `t + lead_time_periods`. Flows sent in the last `lead_time` periods of the horizon exceed its boundary — a standard property of time-expanded networks.

**Max Shipment** — per-commodity constraint on the maximum volume per period. Time-varying (can change seasonally). Unlike shared capacity (total across all commodities), max_shipment limits each commodity_category separately.

**Reliability** — nullable, for risk-aware tasks.

### 5.4. Edge Rules

Rules determine which types of facilities can be connected, for which commodities, and with which modal_types:

```python
class EdgeRule:
    source_type: FacilityType
    target_type: FacilityType
    commodity_category: CommodityCategory | None = None  # None = all categories
    modal_type: ModalType | None = None                  # None = all modalities
    enabled: bool = True
```

Standard scenario (bike-sharing, rebalancing and repair):


| Source           | Target           | Commodity                 | Modal |
| ---------------- | ---------------- | ------------------------- | ----- |
| Depot            | Station          | WORKING_BIKE, BROKEN_BIKE | ROAD  |
| Station          | Depot            | WORKING_BIKE, BROKEN_BIKE | ROAD  |
| Maintenance Hub  | Depot            | WORKING_BIKE              | ROAD  |
| Depot            | Maintenance Hub  | BROKEN_BIKE               | ROAD  |


An extended scenario adds, for example: the same Depot → Station via **RAIL** (inter-city logistics) — an alternative channel with different cost/lead_time.

### 5.5. Manual Edge Overrides

For targeted customizations — manual source_id × target_id × modal_type triples specifying commodity_category, which are added to rule-based edges.

### 5.6. Edge Builder (vectorized)

```python
def build_edges(
    facilities: DataFrame,
    edge_rules: DataFrame,
    manual_pairs: DataFrame,
    distance_matrix: DataFrame,
    modal_attributes: DataFrame,    # modal_type → default lead_time_hours, etc.
) -> DataFrame:
    sources = facilities.rename(columns={
        "facility_id": "source_id",
        "facility_type": "source_type",
    })
    targets = facilities.rename(columns={
        "facility_id": "target_id",
        "facility_type": "target_type",
    })

    # Rule-based: cross join + inner join with rules
    candidates = sources.merge(edge_rules, on="source_type")
    edges = candidates.merge(targets, on="target_type")
    edges = edges[edges["source_id"] != edges["target_id"]]

    # Manual overrides: union
    edges = pd.concat([
        edges[["source_id", "target_id", "modal_type", "commodity_category"]],
        manual_pairs[["source_id", "target_id", "modal_type", "commodity_category"]],
    ]).drop_duplicates()

    # Enrich with distance and modal attributes
    edges = edges.merge(distance_matrix, on=["source_id", "target_id"])
    edges = edges.merge(modal_attributes, on=["source_id", "target_id", "modal_type"],
                        how="left")

    return edges
```

---

## 6. Discrete Parameters

### 6.1. The Problem

Real logistics is full of discrete constraints: minimum shipment volume, batch multiplicity, whole number of trips. The data model must store these parameters, allowing the consumer (optimizer, analytics, reporting) to use them in its own way.

### 6.2. General Pattern: nullable = LP-compatible

All discrete parameters in the data model are **nullable**. This is a key design decision:

- When null — the parameter is not set, the consumer interprets it as "no constraint"
- When set — the consumer uses the value (optimizer can create a MILP constraint, analytics — a filter)

This allows using the same data model for different tasks and accuracy levels without changing the input data.

### 6.3. Types of Discrete Parameters

**Min shipment on an edge** (`edge_commodity_capacity.min_shipment`) — minimum shipment volume. Flow on an edge is either 0 or ≥ min_shipment.

**Min order quantity at SINK** (`demand.min_order_quantity`) — the customer does not accept an order smaller than X units per period.

**Batch size on transformation** (`transformation.batch_size`) — output is a multiple of batch_size. Maintenance Hub can repair in batches of N bikes per cycle (nullable = continuous process).

**Vehicle trips** (`edge_vehicle`) — discrete number of trips with fixed vehicle capacity.

### 6.4. Where Parameters Reside


| Parameter           | Table                   | Field                   | Nullable                  |
| ------------------- | ----------------------- | ----------------------- | ------------------------- |
| Min shipment        | edge_commodity_capacity | min_shipment            | yes                       |
| Max shipment        | edge_commodity_capacity | max_shipment            | no                        |
| Min order quantity  | demand                  | min_order_quantity      | yes                       |
| Batch size          | transformation          | batch_size              | yes                       |
| Vehicle capacity    | edge_vehicle            | vehicle_capacity        | no (table is optional)    |
| Max vehicles/period | edge_vehicle            | max_vehicles_per_period | yes                       |


---

## 7. Hierarchy and Aggregation

### 7.1. The Scaling Problem

A network with 10,000 customers and 50 depots does not scale as a flat graph: too many edges, too large a model. Hierarchy solves this through **nested grouping** of entities, allowing data aggregation to the desired level, task decomposition, and reporting with drill-down.

### 7.2. Three Hierarchies

The platform supports three orthogonal hierarchies — one for each main axis of the model:

**Facility Hierarchy** — geographic/organizational grouping of facilities. Country → Region → City → District. Or Division → Business Unit → Depot. One facility can belong to multiple hierarchies of different types simultaneously.

**Commodity Hierarchy** — grouping of commodity_categories. In bike-sharing: Bike → by condition (WORKING_BIKE / BROKEN_BIKE) or by product line. Needed for demand/supply aggregation at different levels — for example, planning at the level of "all bikes" without distinguishing between working and broken ones.

**Temporal Hierarchy** — multi-resolution planning (§3.2). Implemented through PlanningHorizon segments with different granularity, not through a separate tree. Day → Week → Month is expressed through segment_index.

### 7.3. Hierarchy Structure

All hierarchies (facility, commodity) follow a single **pattern**: hierarchy type → levels → nodes (tree) → leaf entity binding. The temporal hierarchy is implemented through segments (§3.2), not through this pattern.

```
hierarchy_type     →  "geographic", "organizational", "product_group"
hierarchy_level    →  level_index + level_name ("country", "region", ...)
hierarchy_node     →  tree with parent_node_id
membership         →  binding of leaf entity to node
```

Within a single hierarchy, each entity belongs to exactly one node (leaf level). Across hierarchies of different types, an entity can belong to different nodes.

### 7.4. Aggregation — how it is used

**Pre-solve aggregation** — for strategic planning: all facilities in a region collapse into a "super-node". Demand is summed, capacity is summed, intra-regional edges are removed, inter-regional ones remain. Similarly for commodities — demand by parent category.

**Post-solve disaggregation** — after solving at the aggregated level, the result is distributed back to individual facilities/commodities. Inter-regional flows → specific routes.

**Decomposition** — master problem at the top level of the hierarchy, sub-problems within each region. A standard approach for large-scale problems (Benders, Dantzig-Wolfe).

**Reporting** — group-by at any level: total demand by region, depot utilization by business unit, consumption by product group.

### 7.5. Aggregation in the Build Pipeline

Aggregation is an **optional step**. For small networks (< 1000 nodes), the consumer works with the full model. For large ones, the aggregation level is specified in the scenario configuration.

### 7.6. Domain Examples of Hierarchies

**Facility:**


| Domain        | Type           | Levels                         |
| ------------- | -------------- | ------------------------------ |
| Bike-sharing  | geographic     | City → District → Station zone |
| Bike-sharing  | organizational | Operator → Region → Depot      |
| Finance       | organizational | Country → Bank → Branch        |

**Commodity:**


| Domain        | Type           | Levels                                         |
| ------------- | -------------- | ---------------------------------------------- |
| Bike-sharing  | condition      | Bike → WORKING_BIKE / BROKEN_BIKE              |
| Bike-sharing  | product_line   | Fleet → e-bike / classic (example)             |
| Finance       | currency_class | Currency → Major (USD, EUR) → Minor (CZK, PLN) |


**Temporal (multi-resolution segments):**


| Domain                      | Segments                                    |
| --------------------------- | ------------------------------------------- |
| Bike-sharing (operational)  | 7 days DAY + 3 weeks WEEK                   |
| Bike-sharing (tactical)     | 2 weeks DAY + 8 weeks WEEK + 3 months MONTH |
| Finance (strategic)         | 1 month WEEK + 11 months MONTH              |


---

## 8. Nested Architecture (L1 → L2 → L3)

### L1 — Abstract Graph

Pure graph primitives: Node, Edge. No domain logic.

### L2 — Domain-Agnostic Model

CommodityCategory, ResourceCategory, Facility (with roles and operations). Edge Rules with commodity filtering and multi-modal edges. Temporal axis (PlanningHorizon with multi-resolution segments). Transformation as an N→M conversion mechanism between commodity categories. Resource fleet, resource-commodity and resource-modal compatibility. Multi-commodity flow data with capacity_consumption for mixed units. Discrete parameters (nullable = LP-compatible). Facility and Commodity hierarchies for aggregation and decomposition. Solution/Simulation output tables and Historical layer with shared grain. Reused across domains and across consumers (optimizer, simulator, analytics — §15).

### L3 — Domain-Specific Model

Specific types: Station, Depot, Maintenance Hub, REBALANCING_TRUCK. Specific operations: REPAIR, DISPATCH. Specific costs/capacities with their own grains. Specific commodity instances (bike #4521). Specific resource instances (rebalancing truck #REB-07 with GPS). Implemented through inheritance or composition from L2.

---

## 9. Attribute System

### 9.1. The Granularity Problem

Different attributes live at different granularity levels. The Attribute System works identically for all three entity types — Facility, Edge, Resource — the only difference is in entity_grain.

**Facility attributes** (entity_grain = `["facility_id"]`):


| Attribute        | Grain (raw)                                              | Resolved Grain                                                | Kind       |
| ---------------- | -------------------------------------------------------- | ------------------------------------------------------------- | ---------- |
| facility_type    | facility_id                                              | facility_id                                                   | ADDITIONAL |
| storage_capacity | facility_id × commodity_category                         | facility_id × commodity_category                              | CAPACITY   |
| throughput_rate  | facility_id × commodity_category × date                  | facility_id × commodity_category × period_id                  | RATE       |
| handling_cost    | facility_id × operation_type × date                      | facility_id × operation_type × period_id                      | COST       |
| repair_cost      | facility_id × operation_type × commodity_category × date | facility_id × operation_type × commodity_category × period_id | COST       |


**Edge attributes** (entity_grain = `["source_id", "target_id", "modal_type"]`):


| Attribute       | Grain (raw)                     | Resolved Grain                       | Kind       |
| --------------- | ------------------------------- | ------------------------------------ | ---------- |
| distance        | edge                            | edge                                 | ADDITIONAL |
| lead_time_hours | edge                            | edge                                 | ADDITIONAL |
| transport_costs | edge × resource_category × date | edge × resource_category × period_id | COST       |
| reliability     | edge                            | edge                                 | ADDITIONAL |


**Resource attributes** (entity_grain = `["resource_category"]`):


| Attribute             | Grain (raw)              | Resolved Grain                | Kind     |
| --------------------- | ------------------------ | ----------------------------- | -------- |
| base_capacity         | resource_category        | resource_category             | CAPACITY |
| fixed_cost_per_period | resource_category × date | resource_category × period_id | COST     |
| depreciation_rate     | resource_category        | resource_category             | RATE     |
| maintenance_cost      | resource_category × date | resource_category × period_id | COST     |


Resource attributes are fully customizable. The user defines as many cost/rate/capacity attributes for resources as needed. In bike-sharing — fuel + maintenance + driver_time for REBALANCING_TRUCK. In finance — license_fee + compliance_cost. The model does not dictate which specific costs should exist. For location-dependent resource costs, the grain is extended: `resource_category × facility_id × date`.

### 9.2. AttributeKind — semantic classification

All numeric attributes are structurally the same (a value at a specific grain). The difference is semantic:

```python
class AttributeKind(Enum):
    COST = "cost"          # costs (≥ 0)
    REVENUE = "revenue"    # revenue (≥ 0)
    RATE = "rate"          # rates/speeds (≥ 0)
    CAPACITY = "capacity"  # capacities (> 0)
    ADDITIONAL = "additional"  # other (no constraints)
```

Kind determines validation rules and usage semantics: COST — expenses, REVENUE — income, CAPACITY — capacity constraints, RATE — throughput rates. The model consumer (optimizer, analytics) uses kind for automatic mapping: all attributes with `kind=COST` are collected into the cost part, with `kind=REVENUE` — into the revenue part. No need to enumerate specific costs — any custom attribute participates through its kind.

### 9.3. AttributeSpec — unified attribute description

```python
class AttributeSpec:
    name: str                  # "handling_cost"
    kind: AttributeKind        # COST
    grain: list[str]           # ["facility_id", "operation_type", "date"] — raw grain
    resolved_grain: list[str]  # ["facility_id", "operation_type", "period_id"] — after resolution
    value_column: str          # "cost_per_unit"
    unit: str | None           # "€/ton"
    time_varying: bool         # True if "date" is in grain
    aggregation: str           # "mean", "sum", "max" — how to aggregate during time resolution
    nullable: bool             # whether it can be absent
```

`grain` — raw grain with `date`. `resolved_grain` — grain after time resolution, where `date` is replaced with `period_id`. For non-time-varying attributes, `grain == resolved_grain`.

`aggregation` determines how values are aggregated during resolution: costs are typically aggregated by `mean` (average over the period), demand/supply — by `sum` (total for the period), capacities — by `min` (bottleneck for the period).

This mechanism is reused for all three entity types without changes — the only difference is entity_grain: `["facility_id"]` for nodes, `["source_id", "target_id", "modal_type"]` for edges, `["resource_category"]` for resources. The user can register an arbitrary number of custom attributes of any kind on any entity.

### 9.4. AttributeBuilder — spine assembly

The Builder takes a list of AttributeSpecs and automatically builds a spine table:

```python
class AttributeBuilder:
    def __init__(self, entity_grain: list[str]):
        self.entity_grain = entity_grain
        self.attributes: list[AttributeSpec] = []

    def register(self, attr: AttributeSpec):
        if not set(self.entity_grain).issubset(set(attr.grain)):
            raise ValueError(
                f"Attribute {attr.name} grain {attr.grain} "
                f"must include entity grain {self.entity_grain}"
            )
        self.attributes.append(attr)

    def build_spine(
        self,
        base_df: DataFrame,
        attribute_data: dict[str, DataFrame],
    ) -> DataFrame:
        spine = base_df
        sorted_attrs = sorted(self.attributes, key=lambda a: len(a.grain))

        for attr in sorted_attrs:
            if attr.name not in attribute_data:
                if not attr.nullable:
                    raise ValueError(f"Missing required: {attr.name}")
                continue
            df = attribute_data[attr.name]
            self._validate_grain(df, attr)
            self._validate_values(df, attr)
            merge_keys = list(set(spine.columns) & set(attr.grain))
            spine = spine.merge(
                df[attr.grain + [attr.value_column]],
                on=merge_keys,
                how="left",
            )
        return spine
```

### 9.5. Commodity Pricing and Procurement

Commodity pricing is a special case that is not fully covered by a scalar AttributeSpec. The model supports **two levels**:

**Flat pricing (via AttributeSpec)** — a simple per-unit price. Registered as a regular attribute with `kind=REVENUE` (for sale at SINK) or `kind=COST` (for procurement at SOURCE):

```python
# Flat commodity price — scalar, via Attribute System
AttributeSpec(
    name="commodity_sell_price",
    kind=AttributeKind.REVENUE,
    grain=["facility_id", "commodity_category", "date"],
    resolved_grain=["facility_id", "commodity_category", "period_id"],
    value_column="price_per_unit",
    time_varying=True,
    aggregation="mean",
)
```

Suitable for cases with a fixed price. Automatically participates through `kind=REVENUE`.

**Tiered pricing (separate mechanism)** — a piecewise-linear function of volume. Covers volume discounts, contract terms, minimum charges. Does not fit into a scalar AttributeSpec, so it is implemented through separate tables:

```python
class PriceTier:
    facility_id: str              # SINK or SOURCE facility
    commodity_category: str
    date: date                    # time-varying
    tier_index: int               # 0, 1, 2 (order)
    min_volume: float             # lower bound (inclusive)
    max_volume: float | None      # upper bound (None = unlimited)
    price_per_unit: float         # price in this range
```

Tiered pricing examples:


| Scenario                        | Tier 0                         | Tier 1       | Tier 2    |
| ------------------------------- | ------------------------------ | ------------ | --------- |
| Corporate pass (bike-sharing)   | 0–500 trips: €0.10/trip        | 500–2000: €0.08 | 2000+: €0.05 |
| Flat (bike-sharing, B2C)        | 0+: €0.50/hr                   | —               | —            |
| Minimum charge (finance)        | 0–1000€: €5 flat               | 1000+: 0.1%  | —         |


When tiered pricing is specified for a facility × commodity, it **overrides** the flat AttributeSpec for that pair. Flat pricing is the default, tiers are the override.

---

## 10. Grain Groups — solving the cross join problem

### 10.1. The Problem

When grains contain independent dimensions (operation_type and commodity_category), merging into a single spine leads to a cross join — the table explodes, values are duplicated.

### 10.2. Solution: multiple spines by grain groups

Attributes whose grains form a chain (one nests within the other) fall into one group. Groups are not merged with each other.

```python
class GrainGroup:
    name: str
    grain: list[str]
    attributes: list[AttributeSpec]
```

Grouping example:


| Group  | Grain                                        | Attributes                        |
| ------ | -------------------------------------------- | --------------------------------- |
| A      | facility_id × commodity_category × period_id | storage_capacity, dock_throughput |
| B      | facility_id × operation_type × period_id     | handling_cost_base, handling_cost |
| C      | facility_id                                  | facility_type                     |


### 10.3. Automatic Grouping

```python
def auto_group_attributes(
    entity_grain: list[str],
    attributes: list[AttributeSpec],
) -> list[GrainGroup]:
    groups: list[GrainGroup] = []

    for attr in sorted(attributes, key=lambda a: len(a.grain)):
        placed = False
        for group in groups:
            group_dims = set(group.grain)
            attr_dims = set(attr.grain)
            if attr_dims.issubset(group_dims) or group_dims.issubset(attr_dims):
                group.attributes.append(attr)
                group.grain = list(group_dims | attr_dims)
                placed = True
                break
        if not placed:
            groups.append(GrainGroup(
                name=f"group_{len(groups)}",
                grain=attr.grain,
                attributes=[attr],
            ))

    return groups
```

### 10.4. Automatic Merge Order

Within each group, the algorithm determines the optimal merge order:

1. Start with entity_grain
2. At each step, find attributes whose grain is already covered by the current spine grain — these are "free" merges
3. If there are no free ones — choose the attribute with the minimum expansion cost (fewest new dimensions)
4. Repeat until exhausted

```python
class MergePlan:
    attribute_name: str
    merge_keys: list[str]
    causes_expansion: bool
    expansion_dims: list[str]

def plan_merges(
    entity_grain: list[str],
    attributes: list[AttributeSpec],
) -> list[MergePlan]:
    plans = []
    current_grain = set(entity_grain)
    remaining = list(attributes)

    while remaining:
        free = [a for a in remaining if set(a.grain).issubset(current_grain)]
        if free:
            for attr in free:
                plans.append(MergePlan(
                    attribute_name=attr.name,
                    merge_keys=attr.grain,
                    causes_expansion=False,
                    expansion_dims=[],
                ))
                remaining.remove(attr)
            continue

        remaining.sort(key=lambda a: len(set(a.grain) - current_grain))
        best = remaining[0]
        new_dims = set(best.grain) - current_grain
        plans.append(MergePlan(
            attribute_name=best.name,
            merge_keys=list(current_grain & set(best.grain)),
            causes_expansion=True,
            expansion_dims=list(new_dims),
        ))
        current_grain |= set(best.grain)
        remaining.remove(best)

    return plans
```

---

## 11. Tabular Data Architecture

### 11.1. Entity tables — object descriptions

```
facility
├── facility_id (PK)
├── facility_type
├── name
├── lat, lon

resource_category
├── resource_category_id (PK)
├── name                        # "rebalancing_truck", "swift", ...
├── base_capacity: float        # base carrying capacity per unit
├── capacity_unit: str          # "ton", "unit", "transaction"
├── description

resource                        # L2/L3, optional (for individual tracking)
├── resource_id (PK)
├── resource_category (FK)
├── home_facility_id (FK → facility)  # which node the resource is assigned to
├── capacity_override: float | null   # null = use base_capacity
├── description

commodity_category
├── commodity_category_id (PK)
├── name                        # "WORKING_BIKE", "BROKEN_BIKE", ...
├── unit: str                   # "bike", "trip", "USD", ...
├── description

commodity                       # L3, optional
├── commodity_id (PK)
├── commodity_category (FK)
├── description
```

### 11.2. Temporal tables — temporal axis

```
planning_horizon
├── planning_horizon_id (PK)
├── name                       # "Q1-2025-tactical", "H1-2025-strategic"
├── start_date: date
├── end_date: date

planning_horizon_segment
├── planning_horizon_id (FK)
├── segment_index: int (PK)    # 0, 1, 2 — segment order
├── start_date: date
├── end_date: date
├── period_type: PeriodType    # DAY, WEEK, MONTH

period
├── period_id (PK)
├── planning_horizon_id (FK → planning_horizon)
├── segment_index: int         # which segment it belongs to
├── period_index: int          # global order (0, 1, 2, ... across all segments)
├── period_type: PeriodType    # inherited from segment
├── start_date: date
├── end_date: date             # exclusive upper bound
```

`planning_horizon` — an **independent entity** with its own PK. A scenario references it via FK (`scenario.planning_horizon_id`). One horizon can be reused across multiple scenarios.

`planning_horizon_segment` splits the horizon into segments with different granularity (§3.2). For simple cases — a single segment. `period` is generated from segments; `period_index` is global and contiguous for carry-over; `period_type` is inherited from the segment, used during lead_time resolution.

### 11.3. Behavior tables — what objects can do

```
facility_role
├── facility_id (FK)
├── role

facility_operation
├── facility_id (FK)
├── operation_type
├── enabled: bool

facility_availability
├── facility_id (FK)
├── date                        # raw, resolved to period_id
├── available: bool             # whether it operates on this day
├── capacity_factor: float | null  # 1.0 = full capacity, 0.5 = half (null = 1.0)

edge_rule
├── source_type
├── target_type
├── commodity_category (nullable = all categories)
├── modal_type (nullable = all modalities)
├── enabled: bool
```

`facility_availability` — operating time windows. Allows modeling weekends, holidays, seasonality. When resolved to a period, it is aggregated: average `capacity_factor` for the period (or proportion of available days). By default (if no record exists), a facility is 100% available.

### 11.4. Transformation tables

```
transformation
├── transformation_id (PK)
├── facility_id (FK)
├── operation_type               # REPAIR, CONVERSION, BLENDING, DISTILLATION, ...
├── loss_rate: float             # 0.0–1.0, overall process losses
├── batch_size: float | null     # null = continuous (LP-compatible)
├── batch_size_unit: str | null

transformation_input
├── transformation_id (FK)
├── commodity_category (FK → commodity_category)
├── ratio: float                 # units of input per 1 cycle

transformation_output
├── transformation_id (FK)
├── commodity_category (FK → commodity_category)
├── ratio: float                 # units of output per 1 cycle
```

Grain: `transformation_id`. One facility can have multiple transformations. Each transformation can have N inputs and M outputs (blending: N→1, splitting: 1→M, conversion: 1→1, co-production: N→M). For the simple 1→1 case: one record in `transformation_input`, one in `transformation_output` — no overhead.

### 11.5. Resource tables

```
resource_commodity_compatibility
├── resource_category (FK)
├── commodity_category (FK)
├── enabled: bool
```

Determines which commodity_categories a given resource_category can transport. During model assembly, it is verified that for each edge × commodity there exists a compatible resource.

```
resource_modal_compatibility
├── resource_category (FK)
├── modal_type: str
├── enabled: bool
```

Determines on which edge types (modal_type) a given resource_category operates. For example: REBALANCING_TRUCK → ROAD, SWIFT → DIGITAL. Without this table, it is impossible to determine which resources serve which edges, and impossible to correctly compute fleet capacity.

```
resource_fleet
├── facility_id (FK)            # home base
├── resource_category (FK)
├── count: int                  # number of units of this category
```

Aggregated view: how many resources of each category are assigned to a facility. **Effective capacity** is computed during model assembly (§13.6): if individual resources (L3) with `capacity_override` are used, capacity is summed accounting for overrides; otherwise — `count × resource_category.base_capacity`. The table stores only `count`, capacity is computed.

Determines **facility-level outgoing capacity** — the total throughput from a facility on edges served by a given resource_category.

```
resource_availability            # L2/L3, optional
├── resource_id (FK)
├── date                        # raw, resolved to period_id
├── available: bool             # whether available on this day
├── available_capacity: float | null  # if partially available
```

Per-resource availability windows. When resolved to a period, it is aggregated (proportion of available days or average available_capacity). Needed only when individual resources are used.

### 11.6. Edge tables

```
edge
├── source_id (FK → facility)    ┐
├── target_id (FK → facility)    ├── PK
├── modal_type: str              ┘   # "road", "rail", "sea", "pipeline", "digital"
├── distance: float
├── distance_unit: str
├── lead_time_hours: float       # raw, resolved to edge_lead_time_resolved during assembly
├── reliability: float | null    # 0.0–1.0, nullable

edge_commodity
├── source_id (FK)               ┐
├── target_id (FK)               ├── FK → edge
├── modal_type                   ┘
├── commodity_category (FK)      # allowed commodity on this edge
├── enabled: bool
├── capacity_consumption: float  # units of shared capacity per 1 unit of commodity (default 1.0)

edge_capacity
├── source_id (FK)               ┐
├── target_id (FK)               ├── FK → edge
├── modal_type                   ┘
├── date                         # raw, resolved to period_id
├── capacity: float              # shared capacity (all commodities share)
├── capacity_unit: str

edge_commodity_capacity
├── source_id (FK)               ┐
├── target_id (FK)               ├── FK → edge
├── modal_type                   ┘
├── commodity_category (FK)
├── date                         # raw, resolved to period_id (time-varying)
├── min_shipment: float | null   # null = no minimum (LP-compatible)
├── max_shipment: float          # max volume per period for this commodity
├── shipment_unit: str

edge_vehicle
├── source_id (FK)               ┐
├── target_id (FK)               ├── FK → edge
├── modal_type                   ┘
├── resource_category (FK → resource_category)
├── vehicle_capacity: float      # carrying capacity per trip
├── vehicle_capacity_unit: str
├── max_vehicles_per_period: int | null  # max trips per period (null = no limit)

edge_lead_time_resolved          # GENERATED during assembly (§13.4), not entered manually
├── source_id (FK)               ┐
├── target_id (FK)               ├── FK → edge
├── modal_type                   ┘
├── period_id (FK)               # departure period
├── lead_time_periods: int       # how many periods until arrival
├── arrival_period_id (FK, nullable)  # null = exceeds horizon
```

`edge` — PK = `source_id × target_id × modal_type`. Between two facilities, there can be multiple edges (road and rail). `lead_time_hours` is stored in absolute units.

`edge_commodity` — a filter of allowed commodity_categories on an edge. `capacity_consumption` solves the shared capacity problem with different units of measurement: if edge capacity is in **trips** or **tons** and the commodity is bikes in **units**, then the coefficient converts units to edge capacity units (for example, an e-bike is "heavier" for one truck slot). Default = 1.0 for homogeneous units.

`edge_capacity` — shared throughput capacity across all commodities on an edge.

`edge_commodity_capacity` — per-commodity constraints: `min_shipment` (nullable, LP-compatible) and `max_shipment`. **Time-varying** (with `date`): max_shipment can change seasonally.

`edge_vehicle` — discrete trips. Grain: `edge × resource_category`. Optional layer for tasks where transport is discrete.

`edge_lead_time_resolved` — a **generated** table, created during model assembly (§13.4). In uniform resolution (one period_type), `lead_time_periods` is the same for all periods. In multi-resolution, `lead_time_periods` depends on the departure period: 48 hours = 2 periods for DAY, 0 for WEEK. Grain: `edge × period_id`.

### 11.7. Demand, Supply, Initial Inventory, In-Transit

Explicit time-varying data. All indexed by `commodity_category`.

```
demand
├── facility_id (FK)            # facility with SINK role
├── commodity_category (FK)
├── date                        # raw, resolved to period_id via sum
├── quantity: float
├── min_order_quantity: float | null  # null = no minimum (LP-compatible)
├── quantity_unit: str

supply
├── facility_id (FK)            # facility with SOURCE role
├── commodity_category (FK)
├── date                        # raw, resolved to period_id via sum
├── quantity: float
├── quantity_unit: str

inventory_initial
├── facility_id (FK)            # facility with STORAGE role
├── commodity_category (FK)
├── quantity: float             # stock at the time of planning_horizon.start_date
├── quantity_unit: str

inventory_in_transit
├── source_id (FK → facility)    ┐
├── target_id (FK → facility)    ├── FK → edge
├── modal_type                   ┘
├── commodity_category (FK)
├── quantity: float             # how much is in transit
├── quantity_unit: str
├── departure_date: date        # when it was sent
├── expected_arrival_date: date # when arrival is expected
```

`inventory_in_transit` — a boundary condition: commodity that is "in transit" at the start of the horizon. Sent before `planning_horizon.start_date` but not yet arrived. During model assembly, `expected_arrival_date` is mapped to `period_id` — this flow "appears" at the target facility in the corresponding period.

### 11.8. Parameter tables — numeric parameters with grain

Raw data is stored with `date`. During model assembly, it passes through time resolution (§3.4) for mapping to `period_id`.

```
operation_capacity
├── facility_id (FK)
├── operation_type
├── commodity_category (nullable = "all categories")
├── capacity
├── capacity_unit

operation_costs
├── facility_id (FK)
├── operation_type
├── commodity_category
├── date
├── cost_per_unit
├── cost_unit

transport_costs
├── source_id (FK)               ┐
├── target_id (FK)               ├── FK → edge
├── modal_type                   ┘
├── resource_category (FK → resource_category)
├── date
├── cost_per_unit
├── cost_unit

resource_costs                   # custom costs/rates for resources (via Attribute System)
├── resource_category (FK)
├── facility_id (FK, nullable)   # null = same everywhere
├── attribute_name: str          # "fixed_cost", "maintenance_cost", "insurance" — user-defined
├── date (nullable)              # nullable = not time-varying
├── value: float
├── value_unit: str
```

`resource_costs` — a generic table for arbitrary resource attributes with `kind=COST` or `kind=RATE`. Each `attribute_name` is a separate attribute registered via AttributeSpec. The user defines as many custom costs as needed.

**Design note: EAV trade-off.** `resource_costs` uses the Entity-Attribute-Value pattern (`attribute_name` as a column), whereas `operation_costs` and `transport_costs` use explicit columns. This is a deliberate choice: the set of resource costs is fully domain-specific and unpredictable (fuel, maintenance, insurance, license_fee, compliance_cost...), whereas `operation_costs` and `transport_costs` have a stable structure. EAV trade-offs: (+) arbitrary number of custom attributes without schema migration, (+) unified registration mechanism via AttributeSpec; (−) harder validation of allowed `attribute_name` values, (−) no database-level type safety, (−) pivot operations in queries. Validation of allowed `attribute_name` values is ensured through registration in AttributeSpec — unregistered attributes are rejected at the Validation step (§13.2).

### 11.9. Pricing tables — commodity pricing

**Flat pricing** is implemented via AttributeSpec with `kind=REVENUE` / `kind=COST` (§9.5). **Tiered pricing** — via separate tables:

```
commodity_sell_price_tier
├── facility_id (FK)            # facility with SINK role
├── commodity_category (FK)
├── date                        # raw, resolved to period_id
├── tier_index: int             # 0, 1, 2 — fill order
├── min_volume: float           # lower bound (inclusive)
├── max_volume: float | null    # upper bound (null = unlimited)
├── price_per_unit: float
├── price_unit: str

commodity_procurement_cost_tier
├── facility_id (FK)            # facility with SOURCE role
├── commodity_category (FK)
├── date                        # raw, resolved to period_id
├── tier_index: int
├── min_volume: float
├── max_volume: float | null
├── cost_per_unit: float
├── cost_unit: str
```

For flat pricing (single price, no volume discounts) — one tier with `min_volume=0`, `max_volume=null`. Or simpler: use an AttributeSpec with `kind=REVENUE` without tier tables. Tier tables override flat AttributeSpec for those facility × commodity where they are specified.

### 11.10. Hierarchy tables

Facility and commodity hierarchies follow a single pattern (§7.3). Separate table sets for clear FK constraints.

**Facility hierarchy:**

```
facility_hierarchy_type
├── hierarchy_type_id (PK)
├── name                           # "geographic", "organizational"
├── description

facility_hierarchy_level
├── hierarchy_type_id (FK)
├── level_index: int               # 0 = root
├── level_name: str                # "country", "region", "city"

facility_hierarchy_node
├── node_id (PK)
├── hierarchy_type_id (FK)
├── level_index: int
├── parent_node_id (FK → facility_hierarchy_node, nullable)  # null = root
├── name: str                      # "France", "Île-de-France"

facility_hierarchy_membership
├── facility_id (FK)
├── hierarchy_type_id (FK)
├── node_id (FK → facility_hierarchy_node)
```

**Commodity hierarchy:**

```
commodity_hierarchy_type
├── hierarchy_type_id (PK)
├── name                           # "product_group", "currency_class"
├── description

commodity_hierarchy_level
├── hierarchy_type_id (FK)
├── level_index: int
├── level_name: str                # "category", "subcategory", "grade"

commodity_hierarchy_node
├── node_id (PK)
├── hierarchy_type_id (FK)
├── level_index: int
├── parent_node_id (FK → commodity_hierarchy_node, nullable)
├── name: str                      # "Bike", "WORKING_BIKE", ...

commodity_hierarchy_membership
├── commodity_category_id (FK)
├── hierarchy_type_id (FK)
├── node_id (FK → commodity_hierarchy_node)
```

Within a single hierarchy, each entity belongs to exactly one node. Across hierarchies of different types — to different nodes.

### 11.11. Scenario tables — run configuration

```
scenario
├── scenario_id (PK)
├── planning_horizon_id (FK → planning_horizon)  # reference to the temporal grid
├── name
├── description
├── facility_hierarchy_type: str | null     # null = no aggregation
├── facility_aggregation_level: int | null
├── commodity_hierarchy_type: str | null
├── commodity_aggregation_level: int | null

scenario_edge_rules
├── scenario_id (FK)
├── source_type
├── target_type
├── commodity_category (nullable)
├── modal_type (nullable)
├── enabled

scenario_manual_edges
├── scenario_id (FK)
├── source_id
├── target_id
├── modal_type
├── commodity_category

scenario_parameter_overrides
├── scenario_id (FK)
├── attribute_name
├── entity_id
├── override_value
```

Aggregation parameters in `scenario` describe the **configuration** for consumers (§7.4). In the current **`gbp/build` there is no separate graph aggregation step** — `build_model()` does not collapse nodes by hierarchy. This can be done by the optimizer, a separate module, or future code on top of `ResolvedModelData`.

### 11.12. Output tables — consumer output data

Define the grain of consumer output data (optimizer, simulator, etc.). These tables are **generated** by the consumer, not entered manually. Row schemas: `gbp/core/schemas/output.py`; they are **not included in `ResolvedModelData`** and `build_model()` does not populate them. The grain matches the planning model for direct comparison with the Historical layer (§12). More details on different consumers — §15.

#### 11.12.1. Solution tables (optimizer output)

```
solution_flow
├── scenario_id (FK)
├── source_id, target_id, modal_type (FK → edge)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # planned flow
├── quantity_unit: str

solution_inventory
├── scenario_id (FK)
├── facility_id (FK)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # planned inventory at end of period
├── quantity_unit: str

solution_unmet_demand
├── scenario_id (FK)
├── facility_id (FK)
├── commodity_category (FK)
├── period_id (FK)
├── shortfall: float             # demand − delivered (≥ 0)
├── quantity_unit: str

solution_metadata
├── scenario_id (PK)
├── solve_timestamp: datetime
├── objective_value: float | null
├── solve_time_seconds: float
├── solver_status: str           # "optimal", "feasible", "infeasible", "timeout"
├── gap: float | null            # optimality gap for MILP
```

#### 11.12.2. Simulation tables (simulator output)

The simulator steps through periods sequentially and maintains a **log** of decisions and states. Log tables have the same grain as solution tables, allowing direct comparison of optimizer and simulator output.

```
simulation_flow_log
├── scenario_id (FK)
├── source_id, target_id, modal_type (FK → edge)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # actual flow for the period in the simulation
├── quantity_unit: str

simulation_inventory_log
├── scenario_id (FK)
├── facility_id (FK)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # inventory at end of period in the simulation
├── quantity_unit: str

simulation_resource_log
├── scenario_id (FK)
├── resource_id (FK, nullable)   # null for aggregate-level (resource_category only)
├── resource_category (FK)
├── period_id (FK)
├── facility_id (FK)             # where the resource is at end of period
├── status: str                  # "idle", "in_transit", "loading", "unloading"
├── trips_completed: int         # how many trips completed during the period

simulation_metadata
├── scenario_id (PK)
├── simulation_timestamp: datetime
├── total_periods: int
├── total_cost: float | null
├── unmet_demand_total: float
├── solver_type: str             # which step-solver was used ("vrp", "rebalance", "greedy")
```

`simulation_resource_log` — a key difference from the optimizer: the simulator tracks the **position and status of each resource** across periods. The optimizer does not know about positions — for it, resources are a capacity constraint on a facility. More details — §15.

---

## 12. Historical Data Layer

> **Note:** This layer is **not** part of `RawModelData` / `ResolvedModelData` in the current `gbp/core` package. It describes a target analytics / ETL shape aligned with the same keys as the planning model.

### 12.1. Purpose

The main model describes what is **possible** and what is **planned**. The historical data layer records what **actually happened**. The two layers live on the same structure (same facility_id, edge PK, commodity_category), enabling direct comparison of plan vs. actual.

### 12.2. L3: Raw Shipments — individual trips

Raw operational data about each specific delivery. Event-level grain, not planning grain:

```
shipment
├── shipment_id (PK)
├── source_id (FK → facility)
├── target_id (FK → facility)
├── modal_type
├── resource_id (FK → resource, nullable)
├── resource_category (FK)
├── commodity_category (FK)
├── commodity_id (FK, nullable)       # L3, if tracked
├── departure_datetime: datetime
├── arrival_datetime: datetime
├── quantity: float
├── quantity_unit: str
├── actual_cost: float | null
├── status: str                       # "completed", "delayed", "cancelled"
```

From raw shipments, the following are computed: actual lead time (`arrival - departure`), actual resource utilization, failure and delay patterns.

### 12.3. L2: Aggregated Historical — planning grain

Raw shipments are aggregated to planning grain through the same time resolution pipeline (§3.4) as parametric data. The result is tables **directly comparable** to solution tables (§11.12).

**Important:** `period_id` is tied to a specific `planning_horizon`. The historical tables below are **materialized views, generated for a specific planning_horizon** during assembly, not persistent storage. The same raw shipments are re-aggregated for each horizon. Persistent storage is `shipment` (L3).

```
historical_flow
├── planning_horizon_id (FK)
├── source_id, target_id, modal_type (FK → edge)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # actual flow
├── quantity_unit: str

historical_inventory
├── planning_horizon_id (FK)
├── facility_id (FK)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # actual inventory at end of period
├── quantity_unit: str

historical_demand_fulfilled
├── planning_horizon_id (FK)
├── facility_id (FK)
├── commodity_category (FK)
├── period_id (FK)
├── demanded: float              # how much was required
├── delivered: float             # how much was actually delivered
├── fulfilment_rate: float       # delivered / demanded
```

### 12.4. Aggregation: Shipments → Historical

An ETL step, analogous to `date → period_id` resolution:

```python
def aggregate_shipments_to_flows(
    shipments: DataFrame,
    periods: DataFrame,          # periods for a specific planning_horizon
) -> DataFrame:
    """
    Aggregates raw shipments into historical_flow.
    departure_datetime is mapped to period via time resolution.
    quantity is summed by [edge, commodity_category, period_id].
    """
    shipments_with_period = resolve_datetime_to_period(
        shipments, periods, datetime_col="departure_datetime"
    )
    return shipments_with_period.groupby(
        ["source_id", "target_id", "modal_type",
         "commodity_category", "period_id"]
    )["quantity"].sum().reset_index()
```

### 12.5. Usage

**Parameter estimation** — model parameters are derived from historical data: average lead_time_hours (from `arrival - departure`), demand forecast (from historical_demand_fulfilled), transport cost estimation (from actual_cost), reliability (from the proportion of completed shipments).

**Plan vs Fact comparison** — comparing `solution_flow` (§11.12) vs `historical_flow` on the same grain (both indexed by `planning_horizon_id + edge + commodity_category + period_id`).

**Warm-starting** — historical flow pattern as an initial approximation.

**What-if analysis** — running on historical demand/supply with an alternative configuration, comparing with the actual result.

### 12.6. Relationship with the Main Model

The Historical layer **does not change** the main model. The connection is through shared keys:

```
Planning model (main)             Historical layer
─────────────────────────         ─────────────────────────
facility, edge, commodity    ←──  shipment (L3, raw, persistent)
demand, supply, capacity          historical_flow (L2, materialized view)
solution tables (§11.12)     ↔   historical_inventory (L2, materialized view)
                                  historical_demand_fulfilled (L2, materialized view)
```

---

## 13. Build Pipeline — model assembly

### 13.1. General Flow

```
Raw data (with date, lead_time_hours)
    ↓
Validation — validate_raw_model(raw) (§13.2)
    ↓
Time Resolution — resolve_all_time_varying(raw, periods)
    ↓
Edge Building — raw.edges or build_edges(facilities, edge_rules, scenario_manual_edges, ...)
    ↓
Lead Time Resolution — resolve_lead_times(edges_df, periods) → edge_lead_time_resolved
    ↓
Transformation Resolution — resolve_transformations(...)
    ↓
Fleet Capacity Computation — compute_fleet_capacity(...)
    ↓
Assemble ResolvedModelData + assemble_spines(resolved)
    ↓
Ready for consumption (optimizer, analytics, reporting)
```

The step order corresponds to `gbp/build/pipeline.py::build_model`. Hierarchical aggregation (§7.4, formerly the "after resolution" step in older schemas) **is not invoked in this pipeline** — see the *Implementation status* block at the beginning of the document.

### 13.2. Validation Step

Checks data integrity and consistency **before** assembly. Main checks:

**Unit consistency** — the code checks `demand`, `supply`, `inventory_initial`: `quantity_unit` vs `commodity_category.unit`. Units of `edge_capacity` / `shipment_unit` are not separately handled in `gbp/build/validation.py`.

**Referential integrity** — all `facility_id` in demand have the SINK role; all `facility_id` in supply have the SOURCE role; all `facility_id` in inventory_initial have the STORAGE role; all edge FKs reference existing facilities.

**Resource completeness** — for each `edge × commodity`, at least one compatible resource exists (via `resource_commodity_compatibility` × `resource_modal_compatibility`).

**Temporal coverage** — demand/supply cover the entire planning_horizon (warning if there are gaps).

**Graph connectivity** — every SINK is reachable from at least one SOURCE via edges with compatible commodities (warning, not an error).

**Transformation consistency** — for each transformation, input commodities are available on incoming edges of the facility, output commodities are on outgoing edges (checked against the **`raw.edge_commodities`** table, not against edges built later in `build_model`).

```python
# gbp/build/validation.py
@dataclass
class ValidationError:
    level: str  # "error" | "warning"
    category: str
    entity: str
    message: str

@dataclass
class ValidationResult:
    errors: list[ValidationError]  # both errors and warnings; only level == "error" blocks
    @property
    def is_valid(self) -> bool: ...
    def raise_if_invalid(self) -> None: ...
```

### 13.3. Time Resolution Step

Executed for all time-varying attributes and for demand/supply:

```python
def build_resolved_model(
    raw_params: dict[str, DataFrame],   # e.g. "operation_costs" → df with date
    periods: DataFrame,                  # period_id, start_date, end_date
    attr_specs: list[AttributeSpec],
) -> dict[str, DataFrame]:
    resolved = {}

    for spec in attr_specs:
        if not spec.time_varying:
            resolved[spec.name] = raw_params[spec.name]
            continue

        group_grain = [d for d in spec.grain if d != "date"]
        resolved[spec.name] = resolve_to_periods(
            param_df=raw_params[spec.name],
            periods=periods,
            value_columns=[spec.value_column],
            group_grain=group_grain,
            agg_func=spec.aggregation,
        )

    return resolved
```

### 13.4. Lead Time Resolution Step

Converts absolute `lead_time_hours` to the resolved table `edge_lead_time_resolved` (§11.6). Result grain: `edge × period_id`.

```python
def resolve_lead_times(
    edges: DataFrame,          # with lead_time_hours
    periods: DataFrame,        # with start_date, end_date, period_type, period_index
) -> DataFrame:
    """
    Returns edge_lead_time_resolved: edge × period_id → lead_time_periods.
    
    For uniform resolution (one segment): lead_time_periods is the same
    for all periods = ceil(hours / period_duration).
    
    For multi-resolution: for each edge and period t, computes
    how many periods the arrival is shifted by.
    """
    if periods["period_type"].nunique() == 1:
        # Uniform: scalar per edge, expand to all periods
        period_duration = get_duration_hours(periods["period_type"].iloc[0])
        lt = np.ceil(edges["lead_time_hours"] / period_duration).astype(int)
        resolved = edges[["source_id", "target_id", "modal_type"]].assign(
            lead_time_periods=lt
        )
        resolved = resolved.merge(periods[["period_id"]], how="cross")
    else:
        # Multi-resolution: per-period computation
        resolved = resolve_lead_times_multi_resolution(edges, periods)

    # Compute arrival_period_id
    resolved = resolved.merge(
        periods[["period_id", "period_index"]], on="period_id"
    )
    resolved["arrival_period_index"] = (
        resolved["period_index"] + resolved["lead_time_periods"]
    )
    resolved = resolved.merge(
        periods[["period_index", "period_id"]].rename(
            columns={"period_id": "arrival_period_id",
                     "period_index": "arrival_period_index"}
        ),
        on="arrival_period_index",
        how="left",  # null = exceeds horizon
    )
    return resolved
```

### 13.5. Transformation Resolution Step

Determines for each facility which commodity categories enter and which exit, accounting for N→M transformations:

```python
def resolve_transformations(
    facilities: DataFrame,
    transformations: DataFrame,
    transformation_inputs: DataFrame,
    transformation_outputs: DataFrame,
) -> DataFrame:
    """
    Returns a table facility_id → transformation details
    with expanded inputs/outputs.
    """
    t = transformations.merge(transformation_inputs, on="transformation_id")
    t = t.merge(transformation_outputs, on="transformation_id",
                suffixes=("_in", "_out"))
    return facilities.merge(t, on="facility_id", how="left")
```

### 13.6. Fleet Capacity Computation Step

Computes the effective capacity for each record in `resource_fleet`:

```python
def compute_fleet_capacity(
    resource_fleet: DataFrame,
    resource_categories: DataFrame,
    resources: DataFrame | None,     # L3, if available
) -> DataFrame:
    """
    If individual resources (L3) are available:
        capacity = sum(capacity_override ?? base_capacity) for resources at the facility.
    Otherwise:
        capacity = count × base_capacity.
    """
    if resources is not None:
        per_resource = resources.merge(
            resource_categories[["resource_category_id", "base_capacity"]],
            left_on="resource_category", right_on="resource_category_id",
        )
        per_resource["effective_capacity"] = per_resource["capacity_override"].fillna(
            per_resource["base_capacity"]
        )
        fleet_capacity = per_resource.groupby(
            ["home_facility_id", "resource_category"]
        )["effective_capacity"].sum().reset_index()
    else:
        fleet_capacity = resource_fleet.merge(
            resource_categories[["resource_category_id", "base_capacity"]],
            left_on="resource_category", right_on="resource_category_id",
        )
        fleet_capacity["effective_capacity"] = (
            fleet_capacity["count"] * fleet_capacity["base_capacity"]
        )

    return fleet_capacity
```

### 13.7. Spine Assembly Step

After resolution, data is assembled into a spine through AttributeBuilder and GrainGroups (§9–10), but now grains use `resolved_grain` (with `period_id` instead of `date`).

### 13.8. Hierarchical aggregation (scenario fields vs build)

The fields `facility_hierarchy_type`, `commodity_hierarchy_type` and aggregation levels in the `scenario` table describe the **configuration** for consumers (§7.4). In the current **`gbp/build` there is no separate graph aggregation step** — `build_model()` does not collapse nodes by hierarchy. This can be done by the optimizer, a separate module, or future code on top of `ResolvedModelData`.

---

## 14. Domain Configuration Examples

### 14.1. Bike-sharing (the project's primary domain, `gbp/core`)


| FacilityType    | Roles                  | Operations                           | Transformation                            |
| --------------- | ---------------------- | ------------------------------------ | ----------------------------------------- |
| Station         | SOURCE, SINK, STORAGE  | RECEIVING, STORAGE, DISPATCH         | —                                         |
| Depot           | STORAGE, TRANSSHIPMENT | RECEIVING, STORAGE, DISPATCH         | —                                         |
| Maintenance Hub | TRANSSHIPMENT, STORAGE | RECEIVING, STORAGE, REPAIR, DISPATCH | BROKEN_BIKE → WORKING_BIKE (1:1, loss 5%) |


CommodityCategories: WORKING_BIKE, BROKEN_BIKE.

ResourceCategories: REBALANCING_TRUCK (compatible: WORKING_BIKE + BROKEN_BIKE, modal: ROAD).

### 14.2. Financial Transactions


| FacilityType    | Roles                 | Operations                   | Transformation                    |
| --------------- | --------------------- | ---------------------------- | --------------------------------- |
| Bank            | SOURCE, SINK, STORAGE | RECEIVING, STORAGE, DISPATCH | USD → EUR (at rate, loss 0.1%)    |
| Person          | SOURCE, SINK          | RECEIVING, DISPATCH          | —                                 |
| Payment Gateway | TRANSSHIPMENT         | RECEIVING, DISPATCH          | —                                 |


CommodityCategories: USD, EUR (and other currencies).

ResourceCategories: SWIFT (compatible: USD+EUR, modal: DIGITAL), SEPA (compatible: EUR, modal: DIGITAL).

---

## 15. Consumption Model: Optimizer, Simulator, Analytics

### 15.1. The Problem

The same entities (facility, commodity, resource, edge) participate in fundamentally different computational models. A bike-sharing operator wants to: (a) find the optimal nightly rebalancing plan over the horizon (Network Flow), (b) simulate daytime user trips and nightly truck runs accounting for positions (Simulation + VRP / rebalancing), (c) compare plan vs. actual (Analytics). All three tasks operate on the same data — but process it differently.

### 15.2. General Architecture

```
L1 Graph + L2 Data Model (shared)
        ↓
    Build Pipeline (§13)
        ↓
    ResolvedModelData (shared)
        ↓
┌───────────────────────────┬──────────────────────────────┬───────────────────┐
│   Consumer A: Optimizer   │  Consumer B: Simulator       │  Consumer C:      │
│                           │                              │  Analytics        │
│ - all periods at once     │ - steps through periods      │                   │
│ - resources = capacity    │ - resources have STATE       │ - reads output    │
│ - LP/MILP solver          │ - step-solver (VRP, greedy)  │ - compares with   │
│ - output: solution_*      │ - output: simulation_*_log   │   historical      │
└───────────────────────────┴──────────────────────────────┴───────────────────┘
```

**The data model (L1 + L2) does not change between consumers.** The difference is in how the consumer interprets and processes `ResolvedModelData`.

### 15.3. Consumer A: Optimizer (Network Flow)

Sees **all periods simultaneously**. Builds a mathematical program (LP/MILP) and finds the optimal flow allocation across the entire horizon.

Resources are a **capacity constraint** on a facility: "from Depot A, no more than X bikes can be shipped per period because N rebalancing trucks with known capacity are assigned." The optimizer does not know which specific truck will go or where it will end up after delivery.

Data used from the model: demand, supply, edge capacities, costs, fleet capacity, transformation ratios. Output: `solution_flow`, `solution_inventory`, `solution_unmet_demand` (§11.12.1).

Suitable for: strategic planning, optimal flow allocation, what-if scenario analysis.

### 15.4. Consumer B: Simulator

Steps **through periods sequentially**. At each step: examines the current state → makes decisions → updates the state → moves to the next period.

The key difference is **state**. The Simulator maintains mutable state that carries over between steps:

```python
@dataclass
class SimulationState:
    """Runtime state — not part of the data model, but the consumer's state."""

    period_index: int

    # Where each resource is and what it is doing
    resource_state: pd.DataFrame
    # resource_id | resource_category   | facility_id | status     | available_at_period
    # REB-042     | rebalancing_truck   | depot_A     | idle       | null
    # REB-043     | rebalancing_truck   | null        | in_transit | 5
    # REB-044     | rebalancing_truck   | station_12  | unloading  | 4

    # How much commodity is at each facility right now
    inventory: pd.DataFrame
    # facility_id | commodity_category | quantity

    # What is currently in transit
    in_transit: pd.DataFrame
    # resource_id | source_id | target_id | commodity_category | quantity | arrival_period
```

At each step, the simulator calls a **step-solver** — a decision-making strategy:

```python
def simulation_step(
    model: ResolvedModelData,   # L2 model — does NOT change
    state: SimulationState,     # current state — changes every step
    solver: StepSolver,         # decision strategy (VRP, rebalancing, greedy)
) -> SimulationState:
    """
    1. Arrivals: what arrived in this period → update inventory + resource_state
    2. Demand: what was consumed → decrease inventory
    3. Decide: solver decides what to do (who to send where)
    4. Dispatch: send resources → update resource_state + in_transit
    5. Return new state
    """
```

**Step-solver** — a swappable component. For bike-sharing: a rebalancing algorithm (nightly bike redistribution) or VRP for truck routes. For baseline: greedy (nearest deficit → first surplus). Other domains plug in their own strategies (VRP, greedy heuristics, etc.).

```python
class StepSolver(Protocol):
    def decide(
        self,
        model: ResolvedModelData,
        state: SimulationState,
    ) -> list[Dispatch]:
        """Returns a list of decisions: which resource, from where, to where, how much commodity."""
        ...

@dataclass
class Dispatch:
    resource_id: str | None      # specific resource (L3) or None for aggregate
    resource_category: str
    source_id: str               # facility from
    target_id: str               # facility to
    modal_type: str
    commodity_category: str
    quantity: float
```

Output: `simulation_flow_log`, `simulation_inventory_log`, `simulation_resource_log` (§11.12.2).

**SimulationState is the consumer's runtime state, not part of the data model.** Just as the optimizer needs binary variables and an LP tableau at runtime — that is its internal affair, not the data model. The data model defines only input (`ResolvedModelData`) and output (log tables in §11.12.2).

### 15.5. Resource tracking: three levels

Different tasks require different levels of resource detail:

|Level|Resource model|Knows position?|Consumer|Example|
|---|---|---|---|---|
|Aggregate|`resource_fleet` (count × category)|No|Optimizer|"3 trucks at Depot A, capacity = 60 bikes/night"|
|Round-trip aware|`resource_fleet` + `edge_vehicle`|No (implicit)|Optimizer with round-trip|"1 truck = 3 trips/week (48h round-trip)"|
|Instance-level|`resource` (L3) + `SimulationState`|Yes|Simulator|"Truck #42 is currently at station_12, returns in 4h"|

**Aggregate** — the current model in optimizer mode. `resource_fleet.count × base_capacity` = facility capacity constraint. The resource instantly "returns."

**Round-trip aware** — an intermediate level. The resource is not tracked individually, but `max_vehicles_per_period` in `edge_vehicle` is computed accounting for round-trip time:

```
effective_trips = floor(period_hours / (2 × lead_time_hours + handling_hours))
max_vehicles_per_period = effective_trips × vehicle_count
```

This is computed during model assembly or by the consumer. The data model stores `lead_time_hours` on the edge; a separate `handling_hours` field may not exist in `gbp/core` schemas — it is set as a custom attribute or derived from `operation_costs` / the consumer. The consumer computes effective trips itself.

**Instance-level** — a simulator with `SimulationState.resource_state`. Each resource has a position, status, and time until availability. A rebalancing truck that left Depot A for station_12 is **unavailable** for Depot A until it returns. The simulator tracks this through `resource_state`.

All three levels use the same data model — the difference is in which tables the consumer uses and how it interprets them:

|Table|Aggregate|Round-trip|Instance|
|---|---|---|---|
|`resource_category`|✓|✓|✓|
|`resource_fleet`|✓|✓|✓|
|`resource` (L3)|—|—|✓|
|`resource_modal_compatibility`|✓|✓|✓|
|`resource_commodity_compatibility`|✓|✓|✓|
|`edge_vehicle`|—|✓|✓|
|`resource_availability` (L3)|—|—|✓|
|`SimulationState` (runtime)|—|—|✓|

### 15.6. Consumer C: Analytics

Reads output from the optimizer or simulator and compares it with historical data (§12). Does not require additional tables — works with `solution_*`, `simulation_*_log` and `historical_*` on the same grain (`edge × commodity × period_id`).

### 15.7. Domain Examples

**Bike-sharing — 1-year horizon:**

- Optimizer: optimal plan for nightly rebalancing between stations and depots by weeks/days; how many WORKING_BIKE/BROKEN_BIKE to move along each edge in each period.
- Simulator: daytime — demand (users take/return bikes, station inventory changes). Nighttime — step-solver (rebalancing / VRP) moves bikes with REBALANCING_TRUCK trucks; separately, the flow of BROKEN_BIKE → Maintenance Hub → WORKING_BIKE can be modeled. The next day — new demand on updated inventory.
- Analytics: compare optimizer plan vs simulation vs historical trips/inventory snapshots; service level (% of stations with available bikes), shortfall against plan, truck shortage.

---

## 16. Recommended Diagrams

For documenting the model, it is recommended to use a **Property Graph Schema Diagram** — a format from the world of graph databases. Nodes as boxes with type, roles, and key attributes. Edges as arrows with type, allowed commodities, and attributes.

Additionally: UML Class Diagram for detailed attributes and class hierarchy.

For different nesting levels — separate diagrams with different levels of detail:

- L1: abstract graph (Node, Edge)
- L2: logistics model (Facility with roles, CommodityCategory, ResourceCategory, PlanningHorizon with segments, Transformation (N→M), Resource-Modal/Commodity compatibility, Solution tables, Facility/Commodity Hierarchies)
- L3: specific domain (Station, Depot, Maintenance Hub, REBALANCING_TRUCK, bike_id)

---

## References

- Ford, Fulkerson — "Flows in Networks" (Потоки в сетях) — the original source of source/sink/transshipment terminology
- Christofides — "Graph Theory: An Algorithmic Approach" (Теория графов. Алгоритмический подход) — graphs + flows + location algorithms
- Williamson — "Network Flow Algorithms" (Cambridge, 2019) — a modern textbook, free PDF at networkflowalgs.com
- Ahuja, Magnanti, Orlin — "Network Flows: Theory, Algorithms, and Applications" (1993) — the main reference on network flows, including multi-commodity flow (ch. 17)
