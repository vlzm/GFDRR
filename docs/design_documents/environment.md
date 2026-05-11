# Design Doc: Environment (Simulation Engine)

> Status: READY FOR IMPLEMENTATION  
> Roadmap phase: 2 (after Foundation)  
> Depends on: `graph_data_model.md`, `ResolvedModelData`, `AttributeRegistry`  
> Implementation: 7 steps (Step 8 = roadmap phase 3)

---

## 1. What is Environment

Environment is the core of the platform. A space where commodities move through a network of facilities across time periods. At each step, events occur (trips, deliveries), tasks are launched (rebalancing, maintenance), and the world state is updated.

Environment is **Consumer B** from `graph_data_model.md §15`: it steps through periods sequentially, unlike the Optimizer (Consumer A), which sees all periods at once.

### Input and Output

```
ResolvedModelData (immutable, from build pipeline)
        ↓
    Environment.run()
        ↓
SimulationLog (flow_log, inventory_log, resource_log per step)
```

Environment **does not modify** ResolvedModelData. It reads parameters from it (costs, capacities, lead times) and uses them for decision-making. All mutable state lives in `SimulationState`.

---

## 2. Architectural Decisions

### 2.1. Granularity: periods, not a custom time axis

Environment steps through periods from `ResolvedModelData` — whatever they may be (hour, day, week). Granularity is determined by the `planning_horizon_segments` configuration, not by the Environment.

If hourly visibility is needed — use hourly periods. Environment is agnostic to granularity.

### 2.2. Sub-steps: logical phases, not temporal ones

Within each period, **phases** are executed — logical operations in a specific order. Phases are not "morning/afternoon/night" by clock time, but the order of event processing.

Example for bike-sharing (period = day):

```
Period "2025-01-15"
  ├── Phase: DEMAND        ← users pick up/return bikes
  ├── Phase: ARRIVALS      ← trucks dispatched earlier arrive
  └── Phase: DISPATCH      ← step-solver decides rebalancing
```

Example for gas logistics (period = day):

```
Period "2025-01-15"
  ├── Phase: SUPPLY        ← terminal receives gas
  ├── Phase: TRANSFORM     ← filling plant fills cylinders
  ├── Phase: DEMAND        ← customers consume
  └── Phase: DISPATCH      ← dispatch from depot
```

A phase can have a **schedule**: "run every period", "once every N periods", "only when hour == 23". This allows, for example, running the rebalancer only at night, even if Environment steps by the hour.

### 2.3. Solver inside a phase — black box

The DISPATCH phase calls a solver (VRP, greedy, rebalancing). The solver receives the current `SimulationState` and constraints, and returns a set of decisions (dispatches). The solver can internally operate at any granularity (minutes for VRP with time windows), but Environment only sees the input and output.

Environment does not know how the solver makes its decision. Analogy: the build pipeline does not know how haversine computes distance.

### 2.4. Immutable state

Each phase receives `SimulationState` and returns a **new** `SimulationState`. The old one is not modified. This provides:

- **Rollback**: you can return to any step.
- **Debugging**: state at any step can be saved and reproduced.
- **Purity**: no side effects, phases are pure functions `(state, resolved, period) → state'`.

Copying state (~50 KB for a network of 500 stations) is negligible even at 8760 steps/year.

---

## 3. SimulationState

A runtime object that exists only while Environment is running. **Not part of the data model** (not in RawModelData / ResolvedModelData). Created from ResolvedModelData at the start of simulation, updated by each phase.

### 3.1. Structure

```python
@dataclass(frozen=True)
class SimulationState:
    """Immutable snapshot of the world at a point in time."""

    # ── Temporal position ─────────────────────────────────
    period_index: int
    period_id: str

    # ── Commodity state ───────────────────────────────────
    inventory: pd.DataFrame       # facility_id × commodity_category → quantity
    in_transit: pd.DataFrame      # shipments currently en route

    # ── Resource state (instance-level) ───────────────────
    resources: pd.DataFrame       # resource_id → position, status, available_at

    # ── Update helpers ────────────────────────────────────
    def with_inventory(self, new_inventory: pd.DataFrame) -> SimulationState: ...
    def with_in_transit(self, new_in_transit: pd.DataFrame) -> SimulationState: ...
    def with_resources(self, new_resources: pd.DataFrame) -> SimulationState: ...
    def advance_period(self, period_index: int, period_id: str) -> SimulationState: ...
```

`frozen=True` signals intent — do not modify directly, use `with_*` methods. Pandas DataFrames inside are technically mutable, but the convention is: a phase always does `.copy()` before modification and returns a new state via `with_*`.

### 3.2. Inventory

How much commodity is at each facility right now.

```
facility_id | commodity_category | quantity
s1          | working_bike       | 12.0
s1          | broken_bike        | 3.0
s2          | working_bike       | 7.0
d1          | working_bike       | 50.0
```

**Grain:** `facility_id × commodity_category`.  
**Init:** from `resolved.inventory_initial`.  
**Updated by:** DEMAND (−), ARRIVALS (+), DISPATCH (−), TRANSFORM (±) phases.

### 3.3. In-transit

Shipments currently en route between facilities. Each shipment has a departure and arrival period.

```
shipment_id | source_id | target_id | commodity_category | quantity | resource_id | departure_period | arrival_period
shp_001     | d1        | s1        | working_bike       | 5.0      | truck_01    | 3                | 5
shp_002     | s2        | d1        | broken_bike        | 2.0      | truck_02    | 4                | 5
```

**Grain:** `shipment_id` (PK, generated by Environment upon dispatch).  
**Init:** from `resolved.inventory_in_transit` (if available), otherwise empty DataFrame.  
**Updated by:**
- ARRIVALS phase: filters `arrival_period == current_period_index`, transfers quantity to the target facility's inventory, removes from in_transit.
- DISPATCH phase: solver creates new shipments, adds them to in_transit.

`resource_id` — which specific resource is carrying the shipment (instance-level tracking).

### 3.4. Resources (instance-level)

Position and status of each specific resource.

```
resource_id | resource_category | home_facility_id | current_facility_id | status      | available_at_period
truck_01    | REBALANCING_TRUCK | depot_1          | station_5           | IN_TRANSIT  | 7
truck_02    | REBALANCING_TRUCK | depot_1          | depot_1             | AVAILABLE   | null
truck_03    | REBALANCING_TRUCK | depot_2          | depot_2             | MAINTENANCE | 10
```

**Grain:** `resource_id` (PK).

**Statuses:**

```python
class ResourceStatus(str, Enum):
    AVAILABLE = "available"        # at facility, ready for a task
    IN_TRANSIT = "in_transit"      # en route between facilities
    BUSY = "busy"                  # at facility, performing a task (loading/unloading)
    MAINTENANCE = "maintenance"    # under maintenance
```

**`available_at_period`:** period_index starting from which the resource is AVAILABLE again. For AVAILABLE — null. For IN_TRANSIT — arrival period. For MAINTENANCE — end-of-maintenance period.

**Init:** from `resolved.resources` (L3 table) + `resolved.resource_fleet`. If L3 resources are not specified — generated from resource_fleet (N resources of category X, all AVAILABLE at home_facility).

**Updated by:**
- DISPATCH phase: resource transitions AVAILABLE → IN_TRANSIT, `current_facility_id` and `available_at_period` are updated.
- ARRIVALS phase: resource transitions IN_TRANSIT → AVAILABLE (if `available_at_period == current_period_index`), `current_facility_id` is updated.

### 3.5. Initialization

```python
def init_state(resolved: ResolvedModelData) -> SimulationState:
    """Create initial SimulationState from resolved model data."""

    # Inventory: from resolved.inventory_initial
    inventory = resolved.inventory_initial[
        ["facility_id", "commodity_category", "quantity"]
    ].copy()

    # In-transit: from resolved.inventory_in_transit or empty
    if resolved.inventory_in_transit is not None:
        in_transit = resolved.inventory_in_transit.copy()
    else:
        in_transit = pd.DataFrame(columns=[
            "shipment_id", "source_id", "target_id",
            "commodity_category", "quantity", "resource_id",
            "departure_period", "arrival_period",
        ])

    # Resources: from resolved.resources (L3) or generated from resource_fleet
    resources = _init_resources_from_resolved(resolved)

    first_period = resolved.periods.iloc[0]
    return SimulationState(
        period_index=int(first_period["period_index"]),
        period_id=str(first_period["period_id"]),
        inventory=inventory,
        in_transit=in_transit,
        resources=resources,
    )
```

### 3.6. What is NOT in SimulationState

- **Parameters** (costs, capacities, lead times) — read from `ResolvedModelData`, not copied into state.
- **History** (inventory at each past period) — written to `SimulationLog`, not stored in state. State is only the current snapshot.
- **Accumulated metrics** (total unmet demand, total cost) — live in `SimulationLog`.

---

## 4. Engine Loop + Phases

### 4.1. Phase Protocol

A phase is a unit of work within a period. Each phase is a class implementing the Protocol:

```python
class Phase(Protocol):
    """One logical operation within a period."""

    name: str

    def should_run(self, period: PeriodRow) -> bool:
        """Whether this phase should execute in the given period."""
        ...

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> PhaseResult:
        """Execute the phase logic. Returns PhaseResult (new state + events)."""
        ...
```

`PhaseResult` bundles the new state with events for logging (see §7.7):

```python
@dataclass
class PhaseResult:
    state: SimulationState
    flow_events: pd.DataFrame          # → flow_log
    unmet_demand: pd.DataFrame         # → unmet_demand_log
    rejected_dispatches: pd.DataFrame  # → rejected_dispatches_log
```

Contract:
- `execute()` — a pure function: receives state, returns a **new** state. Does not modify the input state.
- `should_run()` — determines whether to run in the given period. Used by the Engine for scheduling.
- `name` — for logging and debugging.

### 4.2. Schedule

Schedule determines in which periods a phase runs. Implemented via a callable predicate — maximally flexible, with no constraints on future use cases.

```python
@dataclass
class Schedule:
    """When a phase should run."""

    predicate: Callable[[PeriodRow], bool]

    def should_run(self, period: PeriodRow) -> bool:
        return self.predicate(period)

    # ── Convenience constructors ──────────────────────────

    @staticmethod
    def every() -> Schedule:
        """Run every period."""
        return Schedule(predicate=lambda p: True)

    @staticmethod
    def every_n(n: int, offset: int = 0) -> Schedule:
        """Run every N-th period, starting from offset."""
        return Schedule(predicate=lambda p: p.period_index % n == offset)

    @staticmethod
    def custom(predicate: Callable[[PeriodRow], bool]) -> Schedule:
        """Run when predicate returns True."""
        return Schedule(predicate=predicate)
```

Examples:

```python
Schedule.every()                          # every period
Schedule.every_n(24, offset=23)           # every 24th period, starting from the 23rd (at night)
Schedule.custom(lambda p: p.period_type == "day")  # only daytime periods
```

Phases use Schedule in `should_run()`:

```python
class DemandPhase:
    name = "DEMAND"

    def __init__(self, schedule: Schedule = Schedule.every()):
        self._schedule = schedule

    def should_run(self, period: PeriodRow) -> bool:
        return self._schedule.should_run(period)

    def execute(self, state, resolved, period) -> SimulationState: ...
```

### 4.3. Environment Class

Environment is a class, not a function. This provides three levels of invocation granularity: `run()` (all at once), `step()` (one period), `step_phase()` (one phase). Useful for debugging, testing, and UI.

```python
class Environment:
    """Step-by-step simulation engine."""

    def __init__(
        self,
        resolved: ResolvedModelData,
        config: EnvironmentConfig,
    ):
        self._resolved = resolved
        self._config = config
        self._state = init_state(resolved)
        self._log = SimulationLog()
        self._periods = list(resolved.periods.itertuples())
        self._period_cursor = 0      # index into _periods
        self._phase_cursor = 0       # index into config.phases (within current period)

    # ── Properties ────────────────────────────────────────

    @property
    def state(self) -> SimulationState:
        """Current simulation state (read-only access)."""
        return self._state

    @property
    def log(self) -> SimulationLog:
        """Accumulated simulation log."""
        return self._log

    @property
    def is_done(self) -> bool:
        """Whether all periods have been processed."""
        return self._period_cursor >= len(self._periods)

    # ── Execution ─────────────────────────────────────────

    def run(self) -> SimulationLog:
        """Run full simulation through all periods. Returns log."""
        while not self.is_done:
            self.step()
        return self._log

    def step(self) -> SimulationState:
        """Execute all phases for the current period, advance to next.
        Returns state after the step."""
        period = self._periods[self._period_cursor]

        for phase in self._config.phases:
            if phase.should_run(period):
                result = phase.execute(
                    self._state, self._resolved, period,
                )
                self._state = result.state
                self._log.record_events(result, phase.name, period)

        self._log.record_period(self._state, period)
        self._state = self._state.advance_period(
            next_period_index=...,
            next_period_id=...,
        )
        self._period_cursor += 1
        self._phase_cursor = 0

        return self._state

    def step_phase(self, phase_name: str) -> SimulationState:
        """Execute a single named phase in the current period.
        For debugging and testing."""
        period = self._periods[self._period_cursor]
        phase = next(p for p in self._config.phases if p.name == phase_name)

        if phase.should_run(period):
            result = phase.execute(
                self._state, self._resolved, period,
            )
            self._state = result.state
            self._log.record_events(result, phase.name, period)

        return self._state
```

Three levels of usage:

```python
# 1. Full run
env = Environment(resolved, config)
log = env.run()

# 2. Step by period (debugging, UI)
env = Environment(resolved, config)
for i in range(10):
    env.step()
    print(env.state.inventory)

# 3. By phase (testing)
env = Environment(resolved, config)
env.step_phase("DEMAND")
assert env.state.inventory.loc["s1", "working_bike"] == 10
env.step_phase("ARRIVALS")
```

### 4.4. EnvironmentConfig

Run configuration — which phases, in what order, with what parameters.

```python
@dataclass
class EnvironmentConfig:
    """Configuration for a simulation run."""

    phases: list[Phase]          # order in list = execution order
    seed: int | None = None      # for reproducibility of stochastic solvers
```

Phase order = execution order. Explicit, simple, no magic.

Configuration examples:

```python
# Bike-sharing (period = day)
bike_config = EnvironmentConfig(
    phases=[
        DemandPhase(schedule=Schedule.every()),
        ArrivalsPhase(schedule=Schedule.every()),
        DispatchPhase(
            solver=GreedyRebalancer(),
            schedule=Schedule.every_n(24, offset=23),
        ),
    ],
)

# Gas logistics (period = day)
gas_config = EnvironmentConfig(
    phases=[
        SupplyPhase(schedule=Schedule.every()),
        TransformPhase(schedule=Schedule.every()),
        DemandPhase(schedule=Schedule.every()),
        PlanPhase(
            solver=DailyPlanner(),
            schedule=Schedule.every_n(24, offset=8),
        ),
        DispatchPhase(
            solver=VRPSolver(time_windows=True),
            schedule=Schedule.every_n(24, offset=8),
        ),
    ],
)
```

### 4.5. Built-in vs Custom Phases

All phases implement the same `Phase` Protocol. "Built-in" simply means "shipped with the library."

**Built-in** (universal logic, domain-independent):

- **DemandPhase** — reads demand from `resolved` for the current period, decreases inventory, logs unmet demand.
- **ArrivalsPhase** — filters `in_transit` by `arrival_period == current`, transfers quantity to the target facility's inventory, updates resource status (IN_TRANSIT → AVAILABLE).

**Custom** (domain-dependent, tied to the solver):

- **DispatchPhase** — calls the solver, creates shipments from dispatches, updates inventory and in_transit.
- **SupplyPhase** — generates supply (for SOURCE facilities).
- **TransformPhase** — applies commodity transformations (N→M, conversion_ratio, loss_rate).

---

## 5. Task + Solver Architecture

### 5.1. Three layers of decision-making

```
Phase (when + validate + apply)
  └── Task (prepare + solve + postprocess)  — for complex domain phases
        └── Solver (pure math)
        └── DataLoader (data preparation)
```

**Phase** — a thin wrapper. Knows *when* to run (schedule), *validates* dispatches, *applies* them to state. Does not know about the domain.

**Task** — domain manager. Combines prepare → solve → postprocess. Knows how to prepare data for the solver and how to translate its solution back into dispatches. Each task has its own dataloader, its own solver, its own postprocessing.

**Solver** — pure math. Works with its own data structures (matrices, graphs, OR-Tools models). Does not know about SimulationState or ResolvedModelData.

Simple phases (DemandPhase, ArrivalsPhase) do not need a Task — all their logic is directly inside the Phase. Task is only needed for complex domain phases.

### 5.2. Task Protocol

```python
class Task(Protocol):
    """Domain-specific task: prepare → solve → postprocess.
    
    Returns dispatches as DataFrame. Task may compute arrival_period
    from lead times in resolved data. resource_id may be null —
    Phase will auto-assign available resources.
    """

    name: str

    def run(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> pd.DataFrame:
        """Execute task. Returns dispatches DataFrame with columns:
        source_id, target_id, commodity_category, quantity,
        resource_id (nullable), modal_type (nullable),
        arrival_period (computed from lead times).
        """
        ...
```

Task receives the full state and resolved — it decides internally what to use from them. Returns a dispatches DataFrame, ready for validation and application by the Phase.

### 5.3. Internal structure of a Task

Task is a manager with three components inside:

```python
class RebalancerTask:
    """Bike-sharing rebalancing: move bikes from overstocked to understocked."""

    name = "rebalancer"

    def __init__(self, solver: RebalancerSolver, config: RebalancerConfig):
        self._solver = solver
        self._loader = RebalancerDataLoader()
        self._config = config

    def run(self, state, resolved, period) -> pd.DataFrame:
        # 1. PREPARE: state + resolved → solver-specific structures
        solver_input = self._loader.prepare(
            state, resolved, period, self._config,
        )

        # 2. SOLVE: solver works with its own data structures
        solver_output = self._solver.solve(solver_input)

        # 3. POSTPROCESS: solver output → dispatches DataFrame
        dispatches = self._loader.to_dispatches(
            solver_output, state, resolved, period,
        )
        return dispatches
```

**DataLoader** (`_loader`) — prepares data from state+resolved into the format the solver understands. For example, for VRP: distance matrix, demand vector, vehicle capacities. Also translates solver output back into a dispatches DataFrame, including computing `arrival_period` from lead times.

**Solver** (`_solver`) — pure math. Does not know about SimulationState or ResolvedModelData. Works with its own structures:

```python
class RebalancerSolver:
    """Pure solver — works with solver-specific data structures."""

    def solve(self, input: RebalancerInput) -> RebalancerOutput:
        # OR-Tools VRP, greedy heuristic, etc.
        ...
```

**Config** (`_config`) — task parameters (target inventory levels, max vehicles, etc.).

This separation allows testing each component in isolation:
- Solver — unit test with manual matrices, no pandas
- DataLoader — verify transformation state → solver input → dispatches
- Task — integration test via run()

### 5.4. Dispatches DataFrame

A unified result format for all Tasks. Phase accepts this DataFrame, validates it, and applies it to state.

```
Dispatches DataFrame columns:

source_id            str       — where to ship from (facility_id)
target_id            str       — where to ship to (facility_id)
commodity_category   str       — what to ship
quantity             float     — how much
resource_id          str|null  — which resource to use (null = Phase auto-assign)
modal_type           str|null  — which transport mode (null = default from edge definition)
arrival_period       int       — arrival period_index (computed by Task from lead times)
```

`arrival_period` is computed by the Task (via DataLoader), because Task knows about lead times from resolved and can factor them into the solver's decision (for example, do not ship to s1 if lead_time = 3 and the deficit needs to be covered tomorrow).

`resource_id` — optional. If the Task/Solver knows which resource to use — it specifies it. If not — Phase will automatically assign the first available resource of the appropriate category.

### 5.5. Phase validation and application

Phase receives dispatches from the Task and performs two steps:

**Validation** (Phase is responsible, Task may be optimistic):

```python
def _validate_dispatches(
    self, dispatches: pd.DataFrame, state: SimulationState, resolved: ResolvedModelData,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (valid_dispatches, rejected_dispatches)."""
    
    # 1. Is there enough inventory at source?
    # 2. Does the edge source → target exist in resolved.edges?
    # 3. If resource_id is specified — is it available (status == AVAILABLE)?
    # 4. Does quantity exceed resource capacity?
    # 5. arrival_period >= current_period?
    
    # Invalid dispatches are not applied, logged as warnings
    ...
```

**Application** (universal logic, the same for all Tasks):

```python
def _apply_dispatches(
    self, state: SimulationState, dispatches: pd.DataFrame, period: PeriodRow,
) -> SimulationState:
    """Apply valid dispatches to state. Returns new state."""
    
    # 1. Auto-assign resource_id where null
    # 2. Generate shipment_id for each dispatch
    # 3. inventory[source] -= quantity
    # 4. Append to in_transit (with departure_period, arrival_period)
    # 5. resources[resource_id].status = IN_TRANSIT
    # 6. resources[resource_id].available_at_period = arrival_period
    # 7. Return new state via with_* methods
    ...
```

### 5.6. DispatchPhase — the glue layer

```python
class DispatchPhase:
    """Phase that delegates to a Task and applies dispatches to state."""

    def __init__(self, task: Task, schedule: Schedule = Schedule.every()):
        self.name = f"DISPATCH_{task.name}"
        self._task = task
        self._schedule = schedule

    def should_run(self, period: PeriodRow) -> bool:
        return self._schedule.should_run(period)

    def execute(self, state, resolved, period) -> SimulationState:
        # 1. Task produces dispatches
        dispatches = self._task.run(state, resolved, period)

        if dispatches.empty:
            return state

        # 2. Phase validates
        valid, rejected = self._validate_dispatches(dispatches, state, resolved)

        # 3. Phase applies valid dispatches to state
        return self._apply_dispatches(state, valid, period)
```

### 5.7. Task examples

**NoopTask** — does nothing. For engine testing:

```python
class NoopTask:
    name = "noop"
    def run(self, state, resolved, period) -> pd.DataFrame:
        return pd.DataFrame(columns=DISPATCH_COLUMNS)
```

**RebalancerTask** — bike rebalancing:

```python
class RebalancerTask:
    name = "rebalancer"
    # inside: RebalancerDataLoader + RebalancerSolver (VRP / greedy)
    # prepare: state.inventory → demand vector, distance matrix
    # solve: VRP/greedy → routes
    # postprocess: routes → dispatches with arrival_period
```

**GasDispatchTask** — gas dispatch from depots to customers:

```python
class GasDispatchTask:
    name = "gas_dispatch"
    # inside: GasDataLoader + DispatchSolver
    # prepare: state.inventory + resolved.demand → allocation problem
    # solve: LP / heuristic
    # postprocess: allocation → dispatches
```

---

## 6. Resource Tracking Details

### 6.1. Resource lifecycle

Example: one truck during a single rebalancing cycle.

```
Period 5: truck_01 at depot_1, status=AVAILABLE
    │
    ├── DispatchPhase: Task decides to send 5 working_bike to station_3
    │   dispatches: {source=depot_1, target=station_3, resource_id=truck_01, ...}
    │   Phase applies:
    │     inventory[depot_1, working_bike] -= 5
    │     truck_01.status = IN_TRANSIT
    │     truck_01.current_facility_id = station_3  (destination)
    │     truck_01.available_at_period = 7  (current + lead_time=2)
    │     in_transit += shipment
    │
Period 6: truck_01 status=IN_TRANSIT, nothing happens
    │
Period 7: ArrivalsPhase:
    │   in_transit filter: arrival_period == 7 → shipment with truck_01
    │   Phase applies:
    │     inventory[station_3, working_bike] += 5
    │     remove shipment from in_transit
    │     truck_01.status = AVAILABLE
    │     truck_01.current_facility_id = station_3
    │     truck_01.available_at_period = null
```

### 6.2. Resource return after delivery

**The resource stays where it arrived.** After delivery, truck_01 becomes AVAILABLE at station_3 (not at home depot_1). If a return trip is needed — the solver/Task must explicitly create a dispatch station_3 → depot_1.

This is maximally flexible: the solver can build chains (depot → s1, then s1 → s2, then s2 → depot), or leave the resource in place for use in the next period. Resource positioning is the responsibility of the solver/Task.

### 6.3. Auto-assign resources

When dispatches arrive with `resource_id = null`, Phase assigns a resource automatically:

```
1. Find all resources with status == AVAILABLE
2. Filter: current_facility_id == dispatch.source_id
   (the resource must be at the dispatch origin)
3. Filter by resource_commodity_compatibility
   (the resource must be capable of carrying this commodity)
4. Filter by resource_modal_compatibility
   (the resource must support the edge's modal_type)
5. From the remaining — take the first one
6. If none available — reject the dispatch, log as warning
```

**Reject + log, not a queue.** If no resource is available — the dispatch is rejected and logged. The solver/Task can see available resources in the state and should account for this during planning. A dispatch queue is a potential future feature, but is not needed in the MVP: it creates a staleness problem (dispatch created in period 5, resource appears in period 8, situation has changed).

### 6.4. Dispatch grain and capacity

**1 dispatch = 1 commodity + 1 resource.** One row in the dispatches DataFrame = one tuple (source, target, commodity, resource).

If a truck carries two commodities (working_bike there, broken_bike back) — that is **two dispatches** with the same resource_id:

```
dispatch_1: depot → station_3, working_bike, qty=5, resource=truck_01, arrival=7
dispatch_2: station_3 → depot, broken_bike,  qty=3, resource=truck_01, arrival=7
```

**Capacity validation** — Phase checks the total per resource:

```python
capacity_used = dispatches.groupby("resource_id")["quantity"].sum()
capacity_limit = resources.set_index("resource_id")["base_capacity"]
over_capacity = capacity_used > capacity_limit
# over_capacity dispatches → rejected, logged as warning
```

If more needs to be sent than a single resource can carry — **the Task splits** into multiple dispatches with different resource_ids. Task knows resource capacities and does this in postprocessing.

### 6.5. Multi-stop routes

**MVP: Task plans only the current period.** Simple routes: depot → station (and back). Multi-stop (depot → s1 → s2 → s3 → depot) requires future dispatches and coordination between periods — this is the next level of complexity.

For multi-stop VRP in the future: Task creates dispatches for all legs of the route, Phase places future dispatches in in_transit with the correct departure/arrival periods. But this will require extending Phase (not checking "resource at source" for future dispatches) and extending in_transit (departure_period > current_period).

### 6.6. Resource initialization

During `init_state(resolved)`:

```python
def _init_resources(resolved: ResolvedModelData) -> pd.DataFrame:
    """Create initial resources DataFrame."""
    
    if resolved.resources is not None:
        # L3 resources are specified explicitly — use them
        resources = resolved.resources[[
            "resource_id", "resource_category", "home_facility_id",
        ]].copy()
        resources["current_facility_id"] = resources["home_facility_id"]
        resources["status"] = ResourceStatus.AVAILABLE
        resources["available_at_period"] = None
    else:
        # Generate from resource_fleet
        # resource_fleet: facility_id × resource_category × count
        # → create count instances for each row
        rows = []
        for _, row in resolved.resource_fleet.iterrows():
            for i in range(int(row["count"])):
                rows.append({
                    "resource_id": f"{row['resource_category']}_{row['facility_id']}_{i}",
                    "resource_category": row["resource_category"],
                    "home_facility_id": row["facility_id"],
                    "current_facility_id": row["facility_id"],
                    "status": ResourceStatus.AVAILABLE,
                    "available_at_period": None,
                })
        resources = pd.DataFrame(rows)
    
    return resources
```

---

## 7. SimulationLog

### 7.1. Overview

SimulationLog is the complete simulation history. It consists of 5 typed DataFrames, each with a clear grain. Used for analytics (plan vs actual), visualization (UI), and debugging.

```python
class SimulationLog:
    """Accumulated simulation output."""

    # ── Core logs ─────────────────────────────────────────
    inventory_log: list[pd.DataFrame]
    flow_log: list[pd.DataFrame]
    resource_log: list[pd.DataFrame]

    # ── Event logs ────────────────────────────────────────
    unmet_demand_log: list[pd.DataFrame]
    rejected_dispatches_log: list[pd.DataFrame]

    def record_period(self, state: SimulationState, period: PeriodRow) -> None:
        """Record end-of-period snapshot (inventory, resources)."""
        ...

    def record_events(self, events: pd.DataFrame, phase_name: str, period: PeriodRow) -> None:
        """Record phase events (flows, unmet demand, rejected dispatches)."""
        ...

    def to_dataframes(self) -> dict[str, pd.DataFrame]:
        """Finalize: concat all per-period logs into full DataFrames."""
        return {
            "simulation_inventory_log": pd.concat(self.inventory_log, ignore_index=True),
            "simulation_flow_log": pd.concat(self.flow_log, ignore_index=True),
            "simulation_resource_log": pd.concat(self.resource_log, ignore_index=True),
            "simulation_unmet_demand_log": pd.concat(self.unmet_demand_log, ignore_index=True),
            "simulation_rejected_dispatches_log": pd.concat(self.rejected_dispatches_log, ignore_index=True),
        }
```

Internal storage: lists of DataFrames (one per period), concatenated at the end via `to_dataframes()`. This is more efficient than calling `pd.concat` on every step.

### 7.2. Inventory Log

Inventory snapshot at the end of each period (after all phases).

```
period_index | period_id | facility_id | commodity_category | quantity
0            | p0        | s1          | working_bike       | 12.0
0            | p0        | s1          | broken_bike        | 3.0
0            | p0        | s2          | working_bike       | 7.0
1            | p1        | s1          | working_bike       | 10.0
...
```

**Grain:** `period_index × facility_id × commodity_category`.  
**Recorded by:** Engine after all phases of a period (`record_period`).  
**Usage:** "inventory at station X over time" chart, comparison with plan.

### 7.3. Flow Log

All commodity movements during the simulation. A single DataFrame with `phase_name` to distinguish flow types.

```
period_index | source_id | target_id | commodity_category | modal_type | quantity | phase_name | resource_id
1            | d1        | s1        | working_bike       | road       | 5.0      | DISPATCH   | truck_01
1            | s2        | d1        | broken_bike        | road       | 3.0      | DISPATCH   | truck_02
1            | EXT       | s1        | working_bike       | null       | 8.0      | DEMAND     | null
1            | s1        | EXT       | working_bike       | null       | 10.0     | DEMAND     | null
```

**Grain:** `period_index × source_id × target_id × commodity_category × phase_name`.  
**Recorded by:** each Phase via PhaseResult.events.  
**`source_id = "EXT"` / `target_id = "EXT"`:** for demand (commodity enters/exits the system).  
**Usage:** total flow per edge, phase contribution analysis, comparison with optimizer solution.

### 7.4. Resource Log

Resource snapshot at the end of each period.

```
period_index | resource_id | resource_category   | current_facility_id | status     | available_at_period
0            | truck_01    | REBALANCING_TRUCK   | depot_1             | AVAILABLE  | null
1            | truck_01    | REBALANCING_TRUCK   | station_3           | IN_TRANSIT | 3
3            | truck_01    | REBALANCING_TRUCK   | station_3           | AVAILABLE  | null
```

**Grain:** `period_index × resource_id`.  
**Recorded by:** Engine after all phases of a period (`record_period`).  
**Usage:** "where was truck_01 at each point in time", utilization rate (% of time IN_TRANSIT vs AVAILABLE).

### 7.5. Unmet Demand Log

Demand that could not be fulfilled due to insufficient inventory.

```
period_index | facility_id | commodity_category | requested | fulfilled | deficit
1            | s3          | working_bike       | 5.0       | 3.0       | 2.0
2            | s1          | working_bike       | 8.0       | 8.0       | 0.0
```

**Grain:** `period_index × facility_id × commodity_category`.  
**Recorded by:** DemandPhase via PhaseResult.events.  
**Usage:** service level (% of fulfilled demand), problematic stations.

Rows with `deficit == 0` are optional (may or may not be recorded, depending on analytics needs).

### 7.6. Rejected Dispatches Log

Dispatches rejected by Phase during validation.

```
period_index | source_id | target_id | commodity_category | quantity | resource_id | reason
1            | d1        | s5        | working_bike       | 10.0     | null        | no_available_resource
1            | d1        | s3        | working_bike       | 25.0     | truck_01    | over_capacity
2            | s2        | d1        | broken_bike        | 3.0      | truck_02    | insufficient_inventory
```

**Grain:** `period_index × source_id × target_id × commodity_category` (+ resource_id if applicable).  
**Recorded by:** DispatchPhase via PhaseResult.events.  
**`reason` enum:**

```python
class RejectReason(str, Enum):
    NO_AVAILABLE_RESOURCE = "no_available_resource"
    INSUFFICIENT_INVENTORY = "insufficient_inventory"
    OVER_CAPACITY = "over_capacity"
    INVALID_EDGE = "invalid_edge"
    INVALID_ARRIVAL = "invalid_arrival"
```

**Usage:** "why the solver failed to execute its plan", solver debugging.

### 7.7. PhaseResult — the link between Phase and Log

Phases return `PhaseResult` instead of a bare `SimulationState`:

```python
@dataclass
class PhaseResult:
    """Output of a Phase execution."""

    state: SimulationState
    flow_events: pd.DataFrame          # movements (→ flow_log)
    unmet_demand: pd.DataFrame         # unfulfilled demand (→ unmet_demand_log)
    rejected_dispatches: pd.DataFrame  # rejected dispatches (→ rejected_dispatches_log)
```

Each DataFrame can be empty (if the phase does not generate events of that type). DemandPhase populates `flow_events` + `unmet_demand`. DispatchPhase populates `flow_events` + `rejected_dispatches`. ArrivalsPhase populates only `flow_events`.

Engine collects PhaseResult from each phase and writes to SimulationLog:

```python
# In Engine.step():
for phase in phases:
    if phase.should_run(period):
        result = phase.execute(state, resolved, period)
        state = result.state
        log.record_events(result, phase.name, period)

log.record_period(state, period)  # snapshot after all phases
```

---

## 8. File Structure

Planned structure (from `docs/repo_struct.md`):

```
gbp/consumers/simulator/
├── state.py             # SimulationState, ResourceStatus, init_state
├── engine.py            # Environment class, simulation loop
├── config.py            # EnvironmentConfig
├── phases.py            # Phase protocol, PhaseResult, Schedule
├── task.py              # Task protocol, DISPATCH_COLUMNS
├── built_in_phases.py   # DemandPhase, ArrivalsPhase (universal)
├── dispatch_phase.py    # DispatchPhase (delegates to Task, validates, applies)
├── log.py               # SimulationLog, RejectReason
└── tasks/               # domain-specific tasks
    ├── noop.py           # NoopTask for testing
    └── ...               # rebalancer, gas_dispatch, etc.
```

---

## 9. Implementation Plan

Each step is a separate PR. `pytest` after each one. The order is determined by dependencies: types and contracts first, then logic, then integration.

### Step 1: SimulationState + init_state

**Files:** `gbp/consumers/simulator/state.py`

**What:**
- `ResourceStatus` enum (AVAILABLE, IN_TRANSIT, BUSY, MAINTENANCE)
- `SimulationState` frozen dataclass (period_index, period_id, inventory, in_transit, resources)
- `with_*` methods (with_inventory, with_in_transit, with_resources, advance_period)
- `init_state(resolved: ResolvedModelData) -> SimulationState`
- `_init_resources(resolved)` — L3 resources or generation from resource_fleet

**Verification:**
- Unit test: create state from toy ResolvedModelData (from `make_raw_model` + `build_model`)
- Check shapes of inventory / resources / in_transit
- Check immutability: `with_inventory()` returns a new object, the old one is unchanged
- Check `_init_resources`: generation from resource_fleet produces the correct number of resources

**Dependencies:** ResolvedModelData, make_raw_model (already exist)

---

### Step 2: Phase Protocol + PhaseResult + Schedule

**Files:** `gbp/consumers/simulator/phases.py`

**What:**
- `PhaseResult` dataclass (state, flow_events, unmet_demand, rejected_dispatches)
- `Phase` Protocol (name, should_run, execute → PhaseResult)
- `Schedule` dataclass with callable predicate
- `Schedule.every()`, `Schedule.every_n()`, `Schedule.custom()` constructors

**Verification:**
- Unit test: `Schedule.every()` returns True for any period
- Unit test: `Schedule.every_n(24, offset=23)` returns True only for period_index % 24 == 23
- Unit test: `Schedule.custom(lambda p: ...)` works with an arbitrary predicate
- Type check: a class implementing Phase Protocol passes mypy

**Dependencies:** SimulationState (Step 1)

---

### Step 3: SimulationLog

**Files:** `gbp/consumers/simulator/log.py`

**What:**
- `RejectReason` enum
- `SimulationLog` class with 5 internal DataFrame lists
- `record_period(state, period)` — snapshot inventory + resources
- `record_events(result: PhaseResult, phase_name, period)` — flow events, unmet demand, rejected dispatches
- `to_dataframes() -> dict[str, pd.DataFrame]` — finalization via concat

**Verification:**
- Unit test: create log, record 3 periods, `to_dataframes()` returns 5 DataFrames with correct shapes
- Unit test: empty log → `to_dataframes()` returns empty DataFrames (not an error)
- Unit test: `record_events` with empty events DataFrame → nothing crashes

**Dependencies:** PhaseResult (Step 2), SimulationState (Step 1)

---

### Step 4: Built-in phases (DemandPhase, ArrivalsPhase)

**Files:** `gbp/consumers/simulator/built_in_phases.py`

**What:**
- `DemandPhase` — reads demand from resolved for the current period, decreases inventory, logs unmet demand in PhaseResult
- `ArrivalsPhase` — filters in_transit by arrival_period == current, transfers to inventory, updates resource status IN_TRANSIT → AVAILABLE

**Verification:**
- Unit test DemandPhase: state with inventory=10, demand=7 → new inventory=3, flow_events recorded, unmet_demand empty
- Unit test DemandPhase: state with inventory=3, demand=7 → new inventory=0, unmet_demand.deficit=4
- Unit test ArrivalsPhase: state with in_transit (arrival=5), current period=5 → shipment transferred to inventory, resource AVAILABLE
- Unit test ArrivalsPhase: state with in_transit (arrival=7), current period=5 → nothing happened

**Dependencies:** Phase Protocol (Step 2), SimulationState (Step 1)

---

### Step 5: Task Protocol + DispatchPhase

**Files:** `gbp/consumers/simulator/task.py`, `gbp/consumers/simulator/dispatch_phase.py`

**What:**
- `DISPATCH_COLUMNS` — list of dispatches DataFrame columns
- `Task` Protocol (name, run → DataFrame)
- `DispatchPhase` — calls task.run(), validates dispatches, auto-assigns resources, applies to state
- `_validate_dispatches()` — inventory check, edge check, resource check, capacity check
- `_apply_dispatches()` — inventory −=, in_transit +=, resources status update
- `_auto_assign_resources()` — assign the first available resource by compatibility

**Verification:**
- Unit test: DispatchPhase with mock Task, valid dispatch → inventory updated, in_transit added, resource IN_TRANSIT
- Unit test: dispatch with insufficient inventory → rejected, logged in PhaseResult.rejected_dispatches
- Unit test: dispatch with resource_id=null → auto-assign, resource becomes IN_TRANSIT
- Unit test: dispatch with resource_id=null, no available resource → rejected
- Unit test: two dispatches on one resource, total quantity > capacity → over_capacity rejection
- Unit test: capacity validation via groupby resource_id

**Dependencies:** Phase Protocol (Step 2), SimulationState (Step 1)

---

### Step 6: Environment class + EnvironmentConfig

**Files:** `gbp/consumers/simulator/engine.py`, `gbp/consumers/simulator/config.py`

**What:**
- `EnvironmentConfig` dataclass (phases, seed)
- `Environment` class (resolved, config → state, log)
- `run() → SimulationLog`
- `step() → SimulationState`
- `step_phase(phase_name) → SimulationState`
- Properties: state, log, is_done

**Verification:**
- Unit test: Environment with DemandPhase + ArrivalsPhase (no dispatch), 3 periods → log contains 3 inventory records
- Unit test: `step()` advances period_cursor, `is_done` is correct
- Unit test: `step_phase("DEMAND")` executes only one phase
- Unit test: Schedule.every_n — phase is skipped in the appropriate periods

**Dependencies:** all previous Steps

---

### Step 7: NoopTask + Integration test

**Files:** `gbp/consumers/simulator/tasks/noop.py`, `tests/integration/test_environment.py`

**What:**
- `NoopTask` — returns empty dispatches DataFrame
- Integration test: full cycle `make_raw_model → build_model → Environment(resolved, config).run() → log.to_dataframes()`

**Verification:**
- NoopTask integration: Environment with DemandPhase + ArrivalsPhase + DispatchPhase(NoopTask), bike-sharing toy data, 7 periods → inventory_log shape is correct, flow_log contains demand events, no rejected dispatches
- Verify that all 5 DataFrames in log are non-empty (except rejected_dispatches)
- Verify that inventory at the last period = inventory_initial − total demand + total arrivals

**Dependencies:** all previous Steps, make_raw_model + build_model (already exist)

---

### Step 8: (Phase 3 roadmap) GreedyRebalancerTask

> This step belongs to roadmap phase 3 (Rebalancer). Included for completeness, but implemented after Environment stabilization.

**Files:** `gbp/consumers/simulator/tasks/rebalancer/` (task, dataloader, solver)

**What:**
- `RebalancerDataLoader` — prepare (state → demand vector + distance matrix) + to_dispatches (solution → dispatches DataFrame)
- `GreedyRebalancerSolver` — simple heuristic (overstocked → understocked matching)
- `RebalancerTask` — manager (loader + solver + config)

**Verification:**
- Unit test solver: manual matrices → correct routes
- Unit test dataloader: state → solver input → dispatches round-trip
- Integration test: Environment with GreedyRebalancerTask, toy bike-sharing → inventory levels out

---

### Summary

| Step | What | Files | Depends on |
|------|------|-------|------------|
| 1 | SimulationState | state.py | ResolvedModelData |
| 2 | Phase Protocol + Schedule | phases.py | Step 1 |
| 3 | SimulationLog | log.py | Steps 1-2 |
| 4 | DemandPhase, ArrivalsPhase | built_in_phases.py | Steps 1-2 |
| 5 | Task Protocol + DispatchPhase | task.py, dispatch_phase.py | Steps 1-2 |
| 6 | Environment + Config | engine.py, config.py | Steps 1-5 |
| 7 | NoopTask + Integration | tasks/noop.py, tests/ | Steps 1-6 |
| 8 | GreedyRebalancerTask | tasks/rebalancer/ | Steps 1-7 (phase 3) |

Steps 3, 4, 5 can be done in parallel (all depend only on 1-2). Step 6 brings everything together. Step 7 is a smoke test of the full pipeline.

---

## Open Questions

- Format of `PeriodRow` — namedtuple from itertuples() or a custom dataclass
- Whether to record rows with `deficit == 0` in unmet_demand_log (completeness vs size)
- Whether `TransformPhase` is needed in the MVP or only DEMAND / ARRIVALS / DISPATCH
- How a Task "remembers" context between periods (for future multi-stop routes)
