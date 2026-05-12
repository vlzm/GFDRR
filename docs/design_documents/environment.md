# Design Doc: Environment (Simulation Engine)

> Status: IMPLEMENTED
> Vertical: Citi Bike historical replay
> Source of truth: `notebooks/canonical_scenario.ipynb`

---

## 1. What is Environment

Environment is the simulation engine. It steps through time periods sequentially,
executing phases that move commodities (bikes) through a network of facilities
(stations, depots). At each step, events occur (trips, arrivals), tasks are
launched (rebalancing), and the world state is updated.

### Input and Output

```
ResolvedModelData (immutable, from build pipeline)
        |
    Environment.run()
        |
SimulationLog (10 typed log tables per step)
```

Environment does not modify `ResolvedModelData`. It reads parameters from it
(capacities, coordinates, fleet) and uses them for decision-making. All mutable
state lives in `SimulationState`.

---

## 2. Architectural Decisions

### 2.1. Granularity: periods from the model

Environment steps through periods from `ResolvedModelData` -- whatever they may
be (hour, day, week). Granularity is determined by the data loader configuration,
not by the Environment.

### 2.2. Sub-steps: logical phases, not temporal ones

Within each period, **phases** are executed -- logical operations in a fixed order.
Phases are not "morning/afternoon/night" but the order of event processing.

Canonical scenario (period = 1 hour):

```
Period "2025-01-01 00:00"
  |-- HistoricalLatentDemandPhase   <-- compute O_i, D_j marginals
  |-- HistoricalODStructurePhase    <-- build P(j | i) distribution
  |-- DeparturePhysicsPhase         <-- subtract departures from inventory
  |-- HistoricalTripSamplingPhase   <-- sample trips with durations -> in_transit
  |-- ArrivalsPhase                 <-- deliver arrived shipments
  |-- OverflowRedirectPhase         <-- redirect overflow (skeleton)
  |-- DispatchPhase(RebalancerTask) <-- rebalance every 6 periods
  |-- InvariantCheckPhase           <-- verify per-commodity conservation
```

A phase can have a **schedule**: "run every period", "once every N periods",
"only when condition is met". For example, the rebalancer runs every 6 periods
via `Schedule.every_n(6)`.

### 2.3. Solver inside a phase -- black box

The DISPATCH phase calls a Task (e.g. `RebalancerTask`), which internally uses
an OR-Tools PDP solver. Environment only sees the input (state) and output
(dispatches DataFrame). The solver can operate at any granularity internally.

### 2.4. Immutable state

Each phase receives `SimulationState` and returns a **new** `SimulationState`
via `PhaseResult`. The old one is not modified. This provides:

- **Rollback**: return to any step.
- **Debugging**: state at any step can be saved and reproduced.
- **Purity**: phases are pure functions `(state, resolved, period) -> state'`.

---

## 3. SimulationState

A runtime object that exists only while Environment is running. Not part of the
data model. Created from `ResolvedModelData` at the start of simulation, updated
by each phase.

### 3.1. Structure

```python
@dataclass(frozen=True)
class SimulationState:
    period_index: int
    period_id: str
    inventory: pd.DataFrame       # facility_id x commodity_category -> quantity
    in_transit: pd.DataFrame      # shipments currently en route
    resources: pd.DataFrame       # resource_id -> position, status, available_at
    intermediates: dict[str, Any] # phase-to-phase data within a period
```

Update methods: `with_inventory()`, `with_in_transit()`, `with_resources()`,
`with_intermediates()`, `advance_period()`. Each returns a new frozen instance.

`intermediates` allows phases to pass data within a single period. For example,
`HistoricalLatentDemandPhase` stores marginals that `DeparturePhysicsPhase` reads.

### 3.2. Inventory

How much commodity is at each facility right now.

```
facility_id | commodity_category | quantity
s1          | working_bike       | 12.0
s1          | electric_bike      | 3.0
d1          | working_bike       | 50.0
```

Grain: `facility_id x commodity_category`.
Columns defined in `INVENTORY_COLUMNS`.

### 3.3. In-transit

Shipments currently en route between facilities.

```
shipment_id | source_id | target_id | commodity_category | quantity | resource_id | departure_period | arrival_period
shp_001     | d1        | s1        | working_bike       | 5.0      | truck_01    | 3                | 5
```

Grain: `shipment_id` (PK, generated upon dispatch).
Columns defined in `IN_TRANSIT_COLUMNS`.

### 3.4. Resources

Position and status of each specific resource (truck).

```
resource_id | resource_category   | home_facility_id | current_facility_id | status    | available_at_period
truck_01    | rebalancing_truck   | depot_1          | station_5           | in_transit| 7
truck_02    | rebalancing_truck   | depot_1          | depot_1             | available | null
```

Grain: `resource_id` (PK).
Columns defined in `RESOURCE_COLUMNS`.

### 3.5. Initialization

`init_state(resolved)` creates the initial state:

- **inventory**: from `resolved.inventory_initial`
- **in_transit**: from `resolved.supply` converted to shipments with `source_id="EXT"`,
  or empty if no supply
- **resources**: from `resolved.resources` (explicit) or generated from
  `resolved.resource_fleet` (N instances per category per facility)

### 3.6. What is NOT in SimulationState

- **Parameters** (capacities, coordinates) -- read from `ResolvedModelData`.
- **History** (inventory at each past period) -- written to `SimulationLog`.
- **Accumulated metrics** (total unmet demand) -- live in `SimulationLog`.

---

## 4. Engine Loop + Phases

### 4.1. Phase Protocol

```python
@runtime_checkable
class Phase(Protocol):
    name: str

    def should_run(self, period: PeriodRow) -> bool: ...

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> PhaseResult: ...
```

### 4.2. PhaseResult

```python
@dataclass
class PhaseResult:
    state: SimulationState
    events: dict[str, pd.DataFrame]   # keyed by log table short_name
```

Event keys used by phases: `"flow_events"`, `"unmet_demand"`,
`"rejected_dispatches"`, `"latent_demand"`, `"lost_demand"`, `"dock_blocking"`,
`"redirected_flow"`, `"invariant_violation"`.

`PhaseResult.empty(state)` creates a no-op result passing state through unchanged.

### 4.3. Schedule

```python
@dataclass(frozen=True)
class Schedule:
    predicate: Callable[[PeriodRow], bool]

    def should_run(self, period: PeriodRow) -> bool: ...

    @staticmethod
    def every() -> Schedule: ...

    @staticmethod
    def every_n(n: int, offset: int = 0) -> Schedule: ...

    @staticmethod
    def custom(predicate: Callable[[PeriodRow], bool]) -> Schedule: ...
```

### 4.4. Environment Class

```python
class Environment:
    def __init__(
        self,
        resolved: ResolvedModelData,
        config: EnvironmentConfig,
    ): ...

    @property
    def state(self) -> SimulationState: ...

    @property
    def log(self) -> SimulationLog: ...

    @property
    def is_done(self) -> bool: ...

    def run(self) -> SimulationLog: ...
    def step(self) -> SimulationState: ...
    def step_phase(self, phase_name: str) -> SimulationState: ...
```

Three levels of granularity:

```python
# 1. Full run
env = Environment(resolved, config)
log = env.run()

# 2. Step by period (debugging)
env = Environment(resolved, config)
for i in range(10):
    env.step()
    print(env.state.inventory)

# 3. By phase (testing)
env = Environment(resolved, config)
env.step_phase("HISTORICAL_LATENT_DEMAND")
```

### 4.5. EnvironmentConfig

```python
@dataclass(frozen=True)
class EnvironmentConfig:
    phases: list[Phase]
    seed: int | None = None
    scenario_id: str = "default"
```

Phase order in the list = execution order. Explicit, no magic.

---

## 5. Phases in the Canonical Scenario

### 5.1. Historical replay phases

These phases reconstruct observed Citi Bike trips step by step:

| Phase | What it does |
|---|---|
| `HistoricalLatentDemandPhase` | Computes per-station origin (O_i) and destination (D_j) marginals from `observed_flow` |
| `HistoricalODStructurePhase` | Builds conditional distribution P(j \| i) from `observed_flow` |
| `DeparturePhysicsPhase` | Subtracts departures from inventory; `mode="permissive"` allows overdraw |
| `HistoricalTripSamplingPhase` | Samples observed trips with durations, appends to `in_transit` |

### 5.2. Movement phases

| Phase | What it does |
|---|---|
| `ArrivalsPhase` | Delivers shipments where `arrival_period == current_period_index` to inventory; updates resource status |
| `OverflowRedirectPhase` | Detects facility overflow; redirect logic is a skeleton (TODO: nearest-with-capacity arg-min) |

### 5.3. Dispatch phase

| Phase | What it does |
|---|---|
| `DispatchPhase(RebalancerTask)` | Delegates to `RebalancerTask`, validates and applies dispatches via dispatch lifecycle |

### 5.4. Verification phase

| Phase | What it does |
|---|---|
| `InvariantCheckPhase` | Verifies per-commodity conservation (inventory + in_transit = baseline); `fail_on_violation=False` logs violations instead of raising |

### 5.5. Other available phases (not in canonical scenario)

| Phase | Purpose |
|---|---|
| `DemandPhase` | Generic demand from `resolved.demand` (not used in historical replay) |
| `OrganicFlowPhase` | Wraps `OrganicDeparturePhase` + `OrganicArrivalPhase` |
| `LatentDemandInflatorPhase` | Scales latent demand by multiplier (for what-if scenarios) |
| `DockCapacityPhase` | Enforces facility storage capacity on arrivals |
| `EndOfPeriodDeficitPhase` | Records negative inventory and attempts in-transit cancellation |

---

## 6. Task + Dispatch Lifecycle

### 6.1. Task Protocol

```python
@runtime_checkable
class Task(Protocol):
    name: str
    def run(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> pd.DataFrame: ...
```

Returns a dispatches DataFrame with columns defined in `DISPATCH_COLUMNS`:
`source_id`, `target_id`, `commodity_category`, `quantity`, `resource_id`,
`modal_type`, `arrival_period`.

### 6.2. Dispatch lifecycle

`DispatchPhase` delegates to `run_dispatch_lifecycle()` which runs three steps:

1. **Assign** (`_assign_resources`): fill missing `resource_id` from available
   resources at the source facility.
2. **Validate** (`_validate_dispatches`): split into valid and rejected; rejection
   reasons (in order): `invalid_arrival`, `invalid_edge`,
   `no_available_resource`, `over_capacity`, `insufficient_inventory`.
3. **Apply** (`_apply_dispatches`): decrement source inventory, append to
   in_transit, update resource status to `in_transit`.

Rejected dispatches are logged, not queued.

### 6.3. RebalancerTask

The canonical scenario uses `RebalancerTask` -- a multi-stop rebalancing task:

1. **Build node state**: per-station inventory, capacity, coordinates.
2. **Compute imbalance**: stations below `min_threshold` are destinations,
   above `max_threshold` are sources.
3. **Plan pairs**: `IntervalOverlapPlanner` matches sources to destinations
   via geography-blind cumulative interval overlap.
4. **Solve PDP**: OR-Tools Pickup-and-Delivery Problem solver builds
   multi-stop truck routes.
5. **Extract dispatches**: convert routes to `DISPATCH_COLUMNS` DataFrame.

Configuration in canonical scenario:

```python
RebalancerTask(
    min_threshold=0.3,
    max_threshold=0.7,
    time_limit_seconds=5,
    commodity_category="electric_bike",
)
```

---

## 7. SimulationLog

### 7.1. Overview

`SimulationLog` accumulates the complete simulation history across 10 typed
log tables, each with a defined schema (`LogTableSchema`).

```python
class SimulationLog:
    def record_period(self, state: SimulationState, period: PeriodRow) -> None: ...
    def record_events(self, result: PhaseResult, phase_name: str, period: PeriodRow) -> None: ...
    def to_dataframes(self) -> dict[str, pd.DataFrame]: ...
```

### 7.2. Log tables

| Output key | Short name | Grain |
|---|---|---|
| `simulation_inventory_log` | `inventory` | `period_index x facility_id x commodity_category` |
| `simulation_flow_log` | `flow_events` | `period_index x source_id x target_id x commodity_category x phase_name` |
| `simulation_resource_log` | `resource` | `period_index x resource_id` |
| `simulation_unmet_demand_log` | `unmet_demand` | `period_index x facility_id x commodity_category` |
| `simulation_rejected_dispatches_log` | `rejected_dispatches` | `period_index x source_id x target_id x commodity_category` |
| `simulation_latent_demand_log` | `latent_demand` | `period_index x facility_id x commodity_category` |
| `simulation_lost_demand_log` | `lost_demand` | `period_index x facility_id x commodity_category` |
| `simulation_dock_blocking_log` | `dock_blocking` | `period_index x facility_id x commodity_category` |
| `simulation_redirected_flow_log` | `redirected_flow` | `period_index x source_id x original_target_id x redirected_target_id` |
| `simulation_invariant_violation_log` | `invariant_violation` | `period_index x commodity_category` |

Phases emit events into `PhaseResult.events` using the short name as key.
`SimulationLog.record_events()` routes them to the correct bucket. End-of-period
`record_period()` snapshots inventory and resources.

Internal storage: lists of DataFrames (one per period), concatenated at the end
via `to_dataframes()`.

---

## 8. Utility Modules

| Module | Purpose |
|---|---|
| `inventory.py` | `to_inventory_delta()`, `apply_delta()`, `merge_with_inventory()` -- vectorized inventory operations |
| `_period_helpers.py` | `period_duration_hours()` -- compute period length from resolved data |
| `exceptions.py` | `SimulatorConfigError` -- raised when resolved model lacks required inputs |
| `tasks/rebalancer_planner.py` | `Planner` protocol + `IntervalOverlapPlanner` -- source-destination matching strategies |

---

## 9. File Structure

```
gbp/consumers/simulator/
|-- engine.py              # Environment class
|-- config.py              # EnvironmentConfig
|-- state.py               # SimulationState, PeriodRow, init_state
|-- phases.py              # Phase protocol, PhaseResult, Schedule
|-- log.py                 # SimulationLog, LogTableSchema, RejectReason
|-- built_in_phases.py     # All built-in phases (historical replay, arrivals, etc.)
|-- dispatch_phase.py      # DispatchPhase (delegates to Task)
|-- dispatch_lifecycle.py  # assign -> validate -> apply pipeline
|-- task.py                # Task protocol, DISPATCH_COLUMNS
|-- inventory.py           # Vectorized inventory delta operations
|-- _period_helpers.py     # Period duration utilities
|-- exceptions.py          # SimulatorConfigError
`-- tasks/
    |-- rebalancer.py          # RebalancerTask (OR-Tools PDP)
    `-- rebalancer_planner.py  # Planner protocol + IntervalOverlapPlanner
```
