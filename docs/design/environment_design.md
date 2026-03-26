# Design Doc: Environment (Simulation Engine)

> Status: READY FOR IMPLEMENTATION  
> Фаза roadmap: 2 (после Foundation)  
> Зависит от: `graph_data_model.md`, `ResolvedModelData`, `AttributeRegistry`  
> Implementation: 7 steps (Step 8 = фаза 3 roadmap)

---

## 1. Что такое Environment

Environment — ядро платформы. Пространство, в котором commodity перемещаются через сеть объектов по периодам времени. На каждом шаге происходят события (поездки, поставки), запускаются задачи (ребалансировка, ремонт), обновляется состояние мира.

Environment — это **Consumer B** из `graph_data_model.md §15`: шагает по периодам последовательно, в отличие от Optimizer (Consumer A), который видит все периоды сразу.

### Вход и выход

```
ResolvedModelData (immutable, из build pipeline)
        ↓
    Environment.run()
        ↓
SimulationLog (flow_log, inventory_log, resource_log per step)
```

Environment **не модифицирует** ResolvedModelData. Он читает из неё параметры (costs, capacities, lead times) и использует их для принятия решений. Всё изменяемое состояние живёт в `SimulationState`.

---

## 2. Архитектурные решения

### 2.1. Гранулярность: periods, а не собственная временная ось

Environment шагает по periods из `ResolvedModelData` — какими бы они ни были (час, день, неделя). Гранулярность определяется конфигурацией `planning_horizon_segments`, а не Environment'ом.

Если нужна часовая видимость — используй часовые periods. Environment агностичен к гранулярности.

### 2.2. Sub-steps: логические фазы, не временные

Внутри каждого period выполняются **фазы** (phases) — логические операции в определённом порядке. Фазы — это не "утро/день/ночь" по часам, а порядок обработки событий.

Пример для велошеринга (period = день):

```
Period "2025-01-15"
  ├── Phase: DEMAND        ← пользователи берут/возвращают велосипеды
  ├── Phase: ARRIVALS      ← грузовики, отправленные ранее, прибывают
  └── Phase: DISPATCH      ← step-solver решает ребалансировку
```

Пример для газовой логистики (period = день):

```
Period "2025-01-15"
  ├── Phase: SUPPLY        ← терминал получает газ
  ├── Phase: TRANSFORM     ← filling plant наполняет баллоны
  ├── Phase: DEMAND        ← клиенты потребляют
  └── Phase: DISPATCH      ← отправка с депо
```

Фаза может иметь **schedule**: "выполнять каждый period", "раз в N periods", "только когда hour == 23". Это позволяет, например, запускать ребалансировщик только ночью, даже если Environment шагает по часам.

### 2.3. Solver внутри фазы — чёрный ящик

Фаза DISPATCH вызывает solver (VRP, greedy, rebalancing). Solver получает текущий `SimulationState` и constraint'ы, возвращает набор решений (dispatches). Solver может внутри себя оперировать любой гранулярностью (минуты для VRP с time windows), но Environment видит только вход и выход.

Environment не знает, как solver принимает решение. Аналогия: build pipeline не знает, как haversine считает расстояние.

### 2.4. Immutable state

Каждая фаза получает `SimulationState`, возвращает **новый** `SimulationState`. Старый не модифицируется. Это даёт:

- **Откат**: можно вернуться к любому шагу.
- **Дебаг**: state на любом шаге можно сохранить и воспроизвести.
- **Чистота**: нет побочных эффектов, фазы — чистые функции `(state, resolved, period) → state'`.

Копирование state (~50 KB для сети из 500 станций) пренебрежимо даже при 8760 steps/год.

---

## 3. SimulationState

Runtime-объект, который существует только во время работы Environment. **Не часть data model** (не в RawModelData / ResolvedModelData). Создаётся из ResolvedModelData в начале симуляции, обновляется каждой фазой.

### 3.1. Структура

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

`frozen=True` сигнализирует intent — не модифицируй напрямую, используй `with_*` методы. Pandas DataFrames внутри технически mutable, но convention: фаза всегда делает `.copy()` перед модификацией и возвращает новый state через `with_*`.

### 3.2. Inventory

Сколько commodity находится на каждом facility прямо сейчас.

```
facility_id | commodity_category | quantity
s1          | working_bike       | 12.0
s1          | broken_bike        | 3.0
s2          | working_bike       | 7.0
d1          | working_bike       | 50.0
```

**Grain:** `facility_id × commodity_category`.  
**Init:** из `resolved.inventory_initial`.  
**Обновляется:** фазами DEMAND (−), ARRIVALS (+), DISPATCH (−), TRANSFORM (±).

### 3.3. In-transit

Shipments, которые сейчас в пути между facilities. Каждый shipment имеет departure и arrival period.

```
shipment_id | source_id | target_id | commodity_category | quantity | resource_id | departure_period | arrival_period
shp_001     | d1        | s1        | working_bike       | 5.0      | truck_01    | 3                | 5
shp_002     | s2        | d1        | broken_bike        | 2.0      | truck_02    | 4                | 5
```

**Grain:** `shipment_id` (PK, генерируется Environment при dispatch).  
**Init:** из `resolved.inventory_in_transit` (если есть), иначе пустой DataFrame.  
**Обновляется:**
- Фаза ARRIVALS: фильтрует `arrival_period == current_period_index`, переносит quantity в inventory целевого facility, удаляет из in_transit.
- Фаза DISPATCH: solver создаёт новые shipments, добавляет в in_transit.

`resource_id` — какой конкретный ресурс везёт shipment (instance-level tracking).

### 3.4. Resources (instance-level)

Позиция и статус каждого конкретного ресурса.

```
resource_id | resource_category | home_facility_id | current_facility_id | status      | available_at_period
truck_01    | REBALANCING_TRUCK | depot_1          | station_5           | IN_TRANSIT  | 7
truck_02    | REBALANCING_TRUCK | depot_1          | depot_1             | AVAILABLE   | null
truck_03    | REBALANCING_TRUCK | depot_2          | depot_2             | MAINTENANCE | 10
```

**Grain:** `resource_id` (PK).

**Статусы:**

```python
class ResourceStatus(str, Enum):
    AVAILABLE = "available"        # на facility, готов к задаче
    IN_TRANSIT = "in_transit"      # в пути между facilities
    BUSY = "busy"                  # на facility, выполняет задачу (loading/unloading)
    MAINTENANCE = "maintenance"    # на ремонте
```

**`available_at_period`:** period_index, начиная с которого ресурс снова AVAILABLE. Для AVAILABLE — null. Для IN_TRANSIT — period прибытия. Для MAINTENANCE — period окончания ремонта.

**Init:** из `resolved.resources` (L3 таблица) + `resolved.resource_fleet`. Если L3 ресурсы не заданы — генерируются из resource_fleet (N ресурсов категории X, все AVAILABLE на home_facility).

**Обновляется:**
- Фаза DISPATCH: ресурс переходит AVAILABLE → IN_TRANSIT, обновляется `current_facility_id` и `available_at_period`.
- Фаза ARRIVALS: ресурс переходит IN_TRANSIT → AVAILABLE (если `available_at_period == current_period_index`), обновляется `current_facility_id`.

### 3.5. Инициализация

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

### 3.6. Что НЕ входит в SimulationState

- **Параметры** (costs, capacities, lead times) — читаются из `ResolvedModelData`, не копируются в state.
- **История** (inventory на каждый прошедший period) — пишется в `SimulationLog`, не хранится в state. State — только текущий snapshot.
- **Accumulated metrics** (total unmet demand, total cost) — живут в `SimulationLog`.

---

## 4. Engine Loop + Phases

### 4.1. Phase Protocol

Фаза — единица работы внутри period'а. Каждая фаза — класс, реализующий Protocol:

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

Контракт:
- `execute()` — чистая функция: принимает state, возвращает **новый** state. Не модифицирует входной state.
- `should_run()` — определяет, запускаться ли в данном period'е. Используется Engine'ом для schedule.
- `name` — для логирования и дебага.

### 4.2. Schedule

Schedule определяет, в каких periods фаза запускается. Реализуется через callable predicate — максимально гибко, без ограничений на будущие use cases.

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

Примеры:

```python
Schedule.every()                          # каждый period
Schedule.every_n(24, offset=23)           # каждый 24-й period, начиная с 23-го (ночью)
Schedule.custom(lambda p: p.period_type == "day")  # только дневные periods
```

Фазы используют Schedule в `should_run()`:

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

Environment — класс, а не функция. Это даёт три уровня гранулярности вызова: `run()` (всё сразу), `step()` (один period), `step_phase()` (одна фаза). Полезно для дебага, тестов и UI.

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

Три уровня использования:

```python
# 1. Полный запуск
env = Environment(resolved, config)
log = env.run()

# 2. Пошагово по периодам (дебаг, UI)
env = Environment(resolved, config)
for i in range(10):
    env.step()
    print(env.state.inventory)

# 3. По фазам (тесты)
env = Environment(resolved, config)
env.step_phase("DEMAND")
assert env.state.inventory.loc["s1", "working_bike"] == 10
env.step_phase("ARRIVALS")
```

### 4.4. EnvironmentConfig

Конфигурация запуска — какие фазы, в каком порядке, с какими параметрами.

```python
@dataclass
class EnvironmentConfig:
    """Configuration for a simulation run."""

    phases: list[Phase]          # порядок в списке = порядок выполнения
    seed: int | None = None      # для воспроизводимости стохастических solver'ов
```

Порядок фаз = порядок выполнения. Явный, простой, без магии.

Примеры конфигурации:

```python
# Велошеринг (period = день)
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

# Газовая логистика (period = день)
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

Все фазы реализуют один и тот же `Phase` Protocol. "Встроенная" означает только "поставляется с библиотекой".

**Встроенные** (универсальная логика, не зависит от домена):

- **DemandPhase** — читает demand из `resolved` для текущего period, уменьшает inventory, логирует unmet demand.
- **ArrivalsPhase** — фильтрует `in_transit` по `arrival_period == current`, переносит quantity в inventory целевого facility, обновляет resource status (IN_TRANSIT → AVAILABLE).

**Кастомные** (зависят от домена и solver'а):

- **DispatchPhase** — вызывает solver, создаёт shipments из dispatches, обновляет inventory и in_transit.
- **SupplyPhase** — генерирует supply (для SOURCE facilities).
- **TransformPhase** — применяет commodity transformations (N→M, conversion_ratio, loss_rate).

---

## 5. Task + Solver Architecture

### 5.1. Три слоя принятия решений

```
Phase (when + validate + apply)
  └── Task (prepare + solve + postprocess)  — для сложных доменных фаз
        └── Solver (чистая математика)
        └── DataLoader (подготовка данных)
```

**Phase** — тонкая обёртка. Знает *когда* запускать (schedule), *валидирует* dispatches, *применяет* их к state. Не знает про домен.

**Task** — доменный manager. Объединяет prepare → solve → postprocess. Знает, как подготовить данные для solver'а и как перевести его решение обратно в dispatches. У каждого task'а свой dataloader, свой solver, своя постобработка.

**Solver** — чистая математика. Работает со своими структурами данных (матрицы, графы, OR-Tools модели). Не знает про SimulationState и ResolvedModelData.

Простые фазы (DemandPhase, ArrivalsPhase) не нуждаются в Task — у них вся логика внутри Phase напрямую. Task нужен только для сложных доменных фаз.

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

Task получает полный state и resolved — внутри себя решает, что из этого использовать. Возвращает dispatches DataFrame, готовый для валидации и применения Phase'ой.

### 5.3. Внутренняя структура Task'а

Task — это manager, у которого внутри три компонента:

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

**DataLoader** (`_loader`) — подготавливает данные из state+resolved в формат, который понимает solver. Например, для VRP: distance matrix, demand vector, vehicle capacities. Также переводит solver output обратно в dispatches DataFrame, включая вычисление `arrival_period` из lead times.

**Solver** (`_solver`) — чистая математика. Не знает про SimulationState и ResolvedModelData. Работает со своими структурами:

```python
class RebalancerSolver:
    """Pure solver — works with solver-specific data structures."""

    def solve(self, input: RebalancerInput) -> RebalancerOutput:
        # OR-Tools VRP, greedy heuristic, etc.
        ...
```

**Config** (`_config`) — параметры task'а (target inventory levels, max vehicles, etc.).

Это разделение позволяет тестировать каждый компонент изолированно:
- Solver — unit test с ручными матрицами, без pandas
- DataLoader — проверить трансформацию state → solver input → dispatches
- Task — integration test через run()

### 5.4. Dispatches DataFrame

Единый формат результата всех Task'ов. Phase принимает этот DataFrame, валидирует, и применяет к state.

```
Колонки dispatches DataFrame:

source_id            str       — откуда отправляем (facility_id)
target_id            str       — куда отправляем (facility_id)
commodity_category   str       — что отправляем
quantity             float     — сколько
resource_id          str|null  — каким ресурсом (null = Phase auto-assign)
modal_type           str|null  — каким способом (null = default из edge definition)
arrival_period       int       — period_index прибытия (вычисляется Task'ом из lead times)
```

`arrival_period` вычисляется Task'ом (через DataLoader), потому что Task знает про lead times из resolved и может учитывать их в решении solver'а (например, не отправлять в s1, если lead_time = 3 и дефицит нужно покрыть завтра).

`resource_id` — опциональный. Если Task/Solver знает, какой ресурс использовать — указывает. Если нет — Phase автоматически назначит первый доступный ресурс подходящей категории.

### 5.5. Phase валидация и применение

Phase получает dispatches от Task'а и выполняет два шага:

**Валидация** (Phase отвечает, Task может быть оптимистичным):

```python
def _validate_dispatches(
    self, dispatches: pd.DataFrame, state: SimulationState, resolved: ResolvedModelData,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (valid_dispatches, rejected_dispatches)."""
    
    # 1. Хватает ли inventory на source?
    # 2. Существует ли edge source → target в resolved.edges?
    # 3. Если resource_id указан — доступен ли (status == AVAILABLE)?
    # 4. Не превышает ли quantity capacity ресурса?
    # 5. arrival_period >= current_period?
    
    # Невалидные dispatches не применяются, логируются как warnings
    ...
```

**Применение** (универсальная логика, одна для всех Task'ов):

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

### 5.6. DispatchPhase — связующий слой

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

### 5.7. Примеры Task'ов

**NoopTask** — ничего не делает. Для тестирования engine:

```python
class NoopTask:
    name = "noop"
    def run(self, state, resolved, period) -> pd.DataFrame:
        return pd.DataFrame(columns=DISPATCH_COLUMNS)
```

**RebalancerTask** — ребалансировка велосипедов:

```python
class RebalancerTask:
    name = "rebalancer"
    # внутри: RebalancerDataLoader + RebalancerSolver (VRP / greedy)
    # prepare: state.inventory → demand vector, distance matrix
    # solve: VRP/greedy → routes
    # postprocess: routes → dispatches with arrival_period
```

**GasDispatchTask** — отправка газа с депо клиентам:

```python
class GasDispatchTask:
    name = "gas_dispatch"
    # внутри: GasDataLoader + DispatchSolver
    # prepare: state.inventory + resolved.demand → allocation problem
    # solve: LP / heuristic
    # postprocess: allocation → dispatches
```

---

## 6. Resource Tracking Details

### 6.1. Жизненный цикл ресурса

Пример: один грузовик за один цикл ребалансировки.

```
Period 5: truck_01 на depot_1, status=AVAILABLE
    │
    ├── DispatchPhase: Task решает отправить 5 working_bike на station_3
    │   dispatches: {source=depot_1, target=station_3, resource_id=truck_01, ...}
    │   Phase applies:
    │     inventory[depot_1, working_bike] -= 5
    │     truck_01.status = IN_TRANSIT
    │     truck_01.current_facility_id = station_3  (destination)
    │     truck_01.available_at_period = 7  (current + lead_time=2)
    │     in_transit += shipment
    │
Period 6: truck_01 status=IN_TRANSIT, ничего не происходит
    │
Period 7: ArrivalsPhase:
    │   in_transit фильтр: arrival_period == 7 → shipment с truck_01
    │   Phase applies:
    │     inventory[station_3, working_bike] += 5
    │     remove shipment from in_transit
    │     truck_01.status = AVAILABLE
    │     truck_01.current_facility_id = station_3
    │     truck_01.available_at_period = null
```

### 6.2. Возврат ресурса после доставки

**Ресурс остаётся где доехал.** После доставки truck_01 становится AVAILABLE на station_3 (не на home depot_1). Если нужно вернуть — solver/Task должен явно создать dispatch station_3 → depot_1.

Это максимально гибко: solver может строить цепочки (depot → s1, потом s1 → s2, потом s2 → depot), или оставить ресурс на месте для использования в следующем period'е. Позиционирование ресурсов — ответственность solver'а/Task'а.

### 6.3. Auto-assign ресурсов

Когда dispatches приходят с `resource_id = null`, Phase назначает ресурс автоматически:

```
1. Найти все ресурсы с status == AVAILABLE
2. Отфильтровать: current_facility_id == dispatch.source_id
   (ресурс должен быть на точке отправления)
3. Отфильтровать по resource_commodity_compatibility
   (ресурс должен уметь везти этот commodity)
4. Отфильтровать по resource_modal_compatibility
   (ресурс должен поддерживать modal_type ребра)
5. Из оставшихся — взять первый
6. Если нет доступных — reject dispatch, логировать как warning
```

**Reject + log, не очередь.** Если ресурса нет — dispatch отклоняется и логируется. Solver/Task видит доступные ресурсы в state и должен учитывать это при планировании. Очередь dispatches — потенциальная feature на будущее, но в MVP не нужна: создаёт проблему устаревания (dispatch создан в period 5, ресурс появился в period 8, ситуация изменилась).

### 6.4. Dispatch grain и capacity

**1 dispatch = 1 commodity + 1 resource.** Одна строка dispatches DataFrame = одна пара (source, target, commodity, resource).

Если грузовик везёт два commodity (working_bike туда, broken_bike обратно) — это **два dispatch'а** с одним resource_id:

```
dispatch_1: depot → station_3, working_bike, qty=5, resource=truck_01, arrival=7
dispatch_2: station_3 → depot, broken_bike,  qty=3, resource=truck_01, arrival=7
```

**Capacity валидация** — Phase проверяет суммарно per resource:

```python
capacity_used = dispatches.groupby("resource_id")["quantity"].sum()
capacity_limit = resources.set_index("resource_id")["base_capacity"]
over_capacity = capacity_used > capacity_limit
# over_capacity dispatches → rejected, logged as warning
```

Если нужно отправить больше, чем один ресурс может увезти — **Task split'ит** на несколько dispatch'ов с разными resource_id. Task знает capacity ресурсов и делает это в postprocess.

### 6.5. Multi-stop маршруты

**MVP: Task планирует только текущий period.** Простые маршруты: depot → station (и обратно). Multi-stop (depot → s1 → s2 → s3 → depot) требует future dispatches и координации между period'ами — это следующий уровень сложности.

Для VRP с multi-stop в будущем: Task создаёт dispatches для всех ног маршрута, Phase ставит future dispatches в in_transit с правильными departure/arrival period'ами. Но это потребует расширения Phase (не проверять "ресурс на source" для future dispatches) и расширения in_transit (departure_period > current_period).

### 6.6. Инициализация ресурсов

При `init_state(resolved)`:

```python
def _init_resources(resolved: ResolvedModelData) -> pd.DataFrame:
    """Create initial resources DataFrame."""
    
    if resolved.resources is not None:
        # L3 ресурсы заданы явно — использовать их
        resources = resolved.resources[[
            "resource_id", "resource_category", "home_facility_id",
        ]].copy()
        resources["current_facility_id"] = resources["home_facility_id"]
        resources["status"] = ResourceStatus.AVAILABLE
        resources["available_at_period"] = None
    else:
        # Генерировать из resource_fleet
        # resource_fleet: facility_id × resource_category × count
        # → создать count экземпляров для каждой строки
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

### 7.1. Обзор

SimulationLog — полная история симуляции. Состоит из 5 typed DataFrames, каждый с чётким grain. Используется для аналитики (plan vs fact), визуализации (UI), дебага.

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

Internal storage: lists of DataFrames (one per period), concatenated at the end via `to_dataframes()`. Это эффективнее, чем `pd.concat` на каждом step'е.

### 7.2. Inventory Log

Snapshot inventory на конец каждого period'а (после всех фаз).

```
period_index | period_id | facility_id | commodity_category | quantity
0            | p0        | s1          | working_bike       | 12.0
0            | p0        | s1          | broken_bike        | 3.0
0            | p0        | s2          | working_bike       | 7.0
1            | p1        | s1          | working_bike       | 10.0
...
```

**Grain:** `period_index × facility_id × commodity_category`.  
**Записывает:** Engine после всех фаз period'а (`record_period`).  
**Использование:** график "inventory на station X по дням", сравнение с plan.

### 7.3. Flow Log

Все перемещения commodity за симуляцию. Один DataFrame с `phase_name` для различения типов потоков.

```
period_index | source_id | target_id | commodity_category | modal_type | quantity | phase_name | resource_id
1            | d1        | s1        | working_bike       | road       | 5.0      | DISPATCH   | truck_01
1            | s2        | d1        | broken_bike        | road       | 3.0      | DISPATCH   | truck_02
1            | EXT       | s1        | working_bike       | null       | 8.0      | DEMAND     | null
1            | s1        | EXT       | working_bike       | null       | 10.0     | DEMAND     | null
```

**Grain:** `period_index × source_id × target_id × commodity_category × phase_name`.  
**Записывает:** каждая Phase через PhaseResult.events.  
**`source_id = "EXT"` / `target_id = "EXT"`:** для demand (commodity входит/выходит из системы).  
**Использование:** total flow per edge, phase contribution analysis, comparison with optimizer solution.

### 7.4. Resource Log

Snapshot ресурсов на конец каждого period'а.

```
period_index | resource_id | resource_category   | current_facility_id | status     | available_at_period
0            | truck_01    | REBALANCING_TRUCK   | depot_1             | AVAILABLE  | null
1            | truck_01    | REBALANCING_TRUCK   | station_3           | IN_TRANSIT | 3
3            | truck_01    | REBALANCING_TRUCK   | station_3           | AVAILABLE  | null
```

**Grain:** `period_index × resource_id`.  
**Записывает:** Engine после всех фаз period'а (`record_period`).  
**Использование:** "где был truck_01 в каждый момент", utilization rate (% времени IN_TRANSIT vs AVAILABLE).

### 7.5. Unmet Demand Log

Demand, который не удалось удовлетворить из-за нехватки inventory.

```
period_index | facility_id | commodity_category | requested | fulfilled | deficit
1            | s3          | working_bike       | 5.0       | 3.0       | 2.0
2            | s1          | working_bike       | 8.0       | 8.0       | 0.0
```

**Grain:** `period_index × facility_id × commodity_category`.  
**Записывает:** DemandPhase через PhaseResult.events.  
**Использование:** service level (% удовлетворённого demand'а), проблемные станции.

Строки с `deficit == 0` опциональны (можно не записывать, или записывать для полноты — зависит от потребности аналитики).

### 7.6. Rejected Dispatches Log

Dispatches, отклонённые Phase при валидации.

```
period_index | source_id | target_id | commodity_category | quantity | resource_id | reason
1            | d1        | s5        | working_bike       | 10.0     | null        | no_available_resource
1            | d1        | s3        | working_bike       | 25.0     | truck_01    | over_capacity
2            | s2        | d1        | broken_bike        | 3.0      | truck_02    | insufficient_inventory
```

**Grain:** `period_index × source_id × target_id × commodity_category` (+ resource_id if applicable).  
**Записывает:** DispatchPhase через PhaseResult.events.  
**`reason` enum:**

```python
class RejectReason(str, Enum):
    NO_AVAILABLE_RESOURCE = "no_available_resource"
    INSUFFICIENT_INVENTORY = "insufficient_inventory"
    OVER_CAPACITY = "over_capacity"
    INVALID_EDGE = "invalid_edge"
    INVALID_ARRIVAL = "invalid_arrival"
```

**Использование:** "почему solver'у не удалось выполнить план", дебаг solver'а.

### 7.7. PhaseResult — связь Phase → Log

Фазы возвращают `PhaseResult` вместо голого `SimulationState`:

```python
@dataclass
class PhaseResult:
    """Output of a Phase execution."""

    state: SimulationState
    flow_events: pd.DataFrame          # перемещения (→ flow_log)
    unmet_demand: pd.DataFrame         # неудовлетворённый demand (→ unmet_demand_log)
    rejected_dispatches: pd.DataFrame  # отклонённые dispatches (→ rejected_dispatches_log)
```

Каждый DataFrame может быть пустым (если фаза не генерирует события этого типа). DemandPhase заполняет `flow_events` + `unmet_demand`. DispatchPhase заполняет `flow_events` + `rejected_dispatches`. ArrivalsPhase заполняет только `flow_events`.

Engine собирает PhaseResult от каждой фазы и пишет в SimulationLog:

```python
# В Engine.step():
for phase in phases:
    if phase.should_run(period):
        result = phase.execute(state, resolved, period)
        state = result.state
        log.record_events(result, phase.name, period)

log.record_period(state, period)  # snapshot после всех фаз
```

---

## 8. File Structure

Планируемая структура (из `docs/repo_struct.md`):

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

Каждый step — отдельный PR. `pytest` после каждого. Порядок определяется зависимостями: сначала типы и контракты, потом логика, потом интеграция.

### Step 1: SimulationState + init_state

**Файлы:** `gbp/consumers/simulator/state.py`

**Что:**
- `ResourceStatus` enum (AVAILABLE, IN_TRANSIT, BUSY, MAINTENANCE)
- `SimulationState` frozen dataclass (period_index, period_id, inventory, in_transit, resources)
- `with_*` методы (with_inventory, with_in_transit, with_resources, advance_period)
- `init_state(resolved: ResolvedModelData) -> SimulationState`
- `_init_resources(resolved)` — L3 resources или генерация из resource_fleet

**Verification:**
- Unit test: создать state из toy ResolvedModelData (из `make_raw_model` + `build_model`)
- Проверить shapes inventory / resources / in_transit
- Проверить immutability: `with_inventory()` возвращает новый объект, старый не изменён
- Проверить `_init_resources`: генерация из resource_fleet даёт правильное количество ресурсов

**Зависимости:** ResolvedModelData, make_raw_model (уже есть)

---

### Step 2: Phase Protocol + PhaseResult + Schedule

**Файлы:** `gbp/consumers/simulator/phases.py`

**Что:**
- `PhaseResult` dataclass (state, flow_events, unmet_demand, rejected_dispatches)
- `Phase` Protocol (name, should_run, execute → PhaseResult)
- `Schedule` dataclass с callable predicate
- `Schedule.every()`, `Schedule.every_n()`, `Schedule.custom()` constructors

**Verification:**
- Unit test: `Schedule.every()` returns True для любого period
- Unit test: `Schedule.every_n(24, offset=23)` returns True только для period_index % 24 == 23
- Unit test: `Schedule.custom(lambda p: ...)` работает с произвольным predicate
- Type check: класс, реализующий Phase Protocol, проходит mypy

**Зависимости:** SimulationState (Step 1)

---

### Step 3: SimulationLog

**Файлы:** `gbp/consumers/simulator/log.py`

**Что:**
- `RejectReason` enum
- `SimulationLog` class с 5 внутренними списками DataFrames
- `record_period(state, period)` — snapshot inventory + resources
- `record_events(result: PhaseResult, phase_name, period)` — flow events, unmet demand, rejected dispatches
- `to_dataframes() -> dict[str, pd.DataFrame]` — финализация через concat

**Verification:**
- Unit test: создать лог, записать 3 period'а, `to_dataframes()` возвращает 5 DataFrames с правильными shapes
- Unit test: пустой лог → `to_dataframes()` возвращает пустые DataFrames (не ошибку)
- Unit test: `record_events` с пустыми events DataFrame → ничего не падает

**Зависимости:** PhaseResult (Step 2), SimulationState (Step 1)

---

### Step 4: Built-in phases (DemandPhase, ArrivalsPhase)

**Файлы:** `gbp/consumers/simulator/built_in_phases.py`

**Что:**
- `DemandPhase` — читает demand из resolved для текущего period, уменьшает inventory, логирует unmet demand в PhaseResult
- `ArrivalsPhase` — фильтрует in_transit по arrival_period == current, переносит в inventory, обновляет resource status IN_TRANSIT → AVAILABLE

**Verification:**
- Unit test DemandPhase: state с inventory=10, demand=7 → new inventory=3, flow_events записан, unmet_demand пустой
- Unit test DemandPhase: state с inventory=3, demand=7 → new inventory=0, unmet_demand.deficit=4
- Unit test ArrivalsPhase: state с in_transit (arrival=5), current period=5 → shipment перенесён в inventory, ресурс AVAILABLE
- Unit test ArrivalsPhase: state с in_transit (arrival=7), current period=5 → ничего не произошло

**Зависимости:** Phase Protocol (Step 2), SimulationState (Step 1)

---

### Step 5: Task Protocol + DispatchPhase

**Файлы:** `gbp/consumers/simulator/task.py`, `gbp/consumers/simulator/dispatch_phase.py`

**Что:**
- `DISPATCH_COLUMNS` — список колонок dispatches DataFrame
- `Task` Protocol (name, run → DataFrame)
- `DispatchPhase` — вызывает task.run(), валидирует dispatches, auto-assigns resources, applies to state
- `_validate_dispatches()` — inventory check, edge check, resource check, capacity check
- `_apply_dispatches()` — inventory −=, in_transit +=, resources status update
- `_auto_assign_resources()` — назначить первый доступный ресурс по compatibility

**Verification:**
- Unit test: DispatchPhase с mock Task, valid dispatch → inventory обновлён, in_transit добавлен, resource IN_TRANSIT
- Unit test: dispatch с insufficient inventory → rejected, logged в PhaseResult.rejected_dispatches
- Unit test: dispatch с resource_id=null → auto-assign, resource становится IN_TRANSIT
- Unit test: dispatch с resource_id=null, no available resource → rejected
- Unit test: два dispatch'а на один resource, суммарный quantity > capacity → over_capacity rejection
- Unit test: capacity validation через groupby resource_id

**Зависимости:** Phase Protocol (Step 2), SimulationState (Step 1)

---

### Step 6: Environment class + EnvironmentConfig

**Файлы:** `gbp/consumers/simulator/engine.py`, `gbp/consumers/simulator/config.py`

**Что:**
- `EnvironmentConfig` dataclass (phases, seed)
- `Environment` class (resolved, config → state, log)
- `run() → SimulationLog`
- `step() → SimulationState`
- `step_phase(phase_name) → SimulationState`
- Properties: state, log, is_done

**Verification:**
- Unit test: Environment с DemandPhase + ArrivalsPhase (no dispatch), 3 periods → log содержит 3 записи inventory
- Unit test: `step()` advances period_cursor, `is_done` корректно
- Unit test: `step_phase("DEMAND")` выполняет только одну фазу
- Unit test: Schedule.every_n — фаза пропускается в нужных period'ах

**Зависимости:** все предыдущие Steps

---

### Step 7: NoopTask + Integration test

**Файлы:** `gbp/consumers/simulator/tasks/noop.py`, `tests/integration/test_environment.py`

**Что:**
- `NoopTask` — возвращает пустой dispatches DataFrame
- Integration test: полный цикл `make_raw_model → build_model → Environment(resolved, config).run() → log.to_dataframes()`

**Verification:**
- NoopTask integration: Environment с DemandPhase + ArrivalsPhase + DispatchPhase(NoopTask), bike-sharing toy data, 7 periods → inventory_log shape корректный, flow_log содержит demand events, no rejected dispatches
- Проверить что все 5 DataFrames в log непустые (кроме rejected_dispatches)
- Проверить что inventory на последнем period'е = inventory_initial − total demand + total arrivals

**Зависимости:** все предыдущие Steps, make_raw_model + build_model (уже есть)

---

### Step 8: (Phase 3 roadmap) GreedyRebalancerTask

> Этот step — уже фаза 3 roadmap'а (Rebalancer). Включён для полноты картины, но реализуется после стабилизации Environment.

**Файлы:** `gbp/consumers/simulator/tasks/rebalancer/` (task, dataloader, solver)

**Что:**
- `RebalancerDataLoader` — prepare (state → demand vector + distance matrix) + to_dispatches (solution → dispatches DataFrame)
- `GreedyRebalancerSolver` — простая эвристика (overstocked → understocked matching)
- `RebalancerTask` — manager (loader + solver + config)

**Verification:**
- Unit test solver: ручные матрицы → правильные маршруты
- Unit test dataloader: state → solver input → dispatches round-trip
- Integration test: Environment с GreedyRebalancerTask, toy bike-sharing → inventory выравнивается

---

### Сводка

| Step | Что | Файлы | Зависит от |
|------|-----|-------|------------|
| 1 | SimulationState | state.py | ResolvedModelData |
| 2 | Phase Protocol + Schedule | phases.py | Step 1 |
| 3 | SimulationLog | log.py | Steps 1-2 |
| 4 | DemandPhase, ArrivalsPhase | built_in_phases.py | Steps 1-2 |
| 5 | Task Protocol + DispatchPhase | task.py, dispatch_phase.py | Steps 1-2 |
| 6 | Environment + Config | engine.py, config.py | Steps 1-5 |
| 7 | NoopTask + Integration | tasks/noop.py, tests/ | Steps 1-6 |
| 8 | GreedyRebalancerTask | tasks/rebalancer/ | Steps 1-7 (фаза 3) |

Steps 3, 4, 5 можно делать параллельно (все зависят только от 1-2). Step 6 объединяет всё. Step 7 — smoke test полного пайплайна.

---

## Open Questions

- Формат `PeriodRow` — namedtuple из itertuples() или свой dataclass
- Записывать ли строки с `deficit == 0` в unmet_demand_log (полнота vs размер)
- Нужен ли `TransformPhase` в MVP или только DEMAND / ARRIVALS / DISPATCH
- Как Task "помнит" контекст между period'ами (для future multi-stop маршрутов)
