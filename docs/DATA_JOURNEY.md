# Путь данных: от велосипеда до оптимизатора

Этот документ проводит через **полный путь данных** на конкретном примере велошеринга. Не абстрактная архитектура, а конкретные строки, конкретные числа, конкретные трансформации.

Для справочных диаграмм по отдельным концепциям см. `docs/architecture_diagrams.md`. Здесь — сквозной маршрут.

---

## 0. Реальный мир

Представим сеть велошеринга в городе:

```
8 станций (s1–s8)        — стойки с велосипедами на улицах
2 депо (d1, d2)           — склады + грузовики для ребалансировки
1 тип товара              — working_bike
1 тип ресурса             — rebalancing_truck (вместимость: 20 велосипедов)
48 часовых снапшотов      — 2 дня наблюдений
```

Источник данных — API, похожий на Citi Bike GBFS. Каждый час мы знаем: сколько велосипедов на каждой станции, какие поездки были, какая вместимость стоек.

---

## 1. DataLoaderMock: генерация сырых данных

`DataLoaderMock` создаёт синтетические данные в формате Citi Bike. Это **доменно-специфичные** таблицы — колонки называются `station_id`, `num_bikes_available`, `start_station_name`.

```
DataLoaderMock({"n": 8, "n_depots": 2, "n_timestamps": 48})
```

Что появляется после `mock.load_data()`:

| DataFrame         | Строк | Что внутри                                         |
|--------------------|-------|-----------------------------------------------------|
| `df_stations`      | 8     | station_id, name, lat, lon, capacity, region_id     |
| `df_depots`        | 2     | node_id, name, lat, lon, inventory_capacity         |
| `df_resources`     | 2     | resource_id, depot_id, type                         |
| `df_inventory_ts`  | 48×8  | матрица: timestamp × station_id → кол-во байков     |
| `df_telemetry_ts`  | 48×8  | num_bikes_available, num_docks_available, ...        |
| `df_trips`         | ~500  | ride_id, start_station_id, end_station_id, times    |
| `df_station_costs` | 8     | fixed_cost_per_visit, cost_per_bike_moved           |
| `df_truck_rates`   | 2     | cost_per_km, cost_per_hour, fixed_dispatch_cost     |

Конкретный пример — станция s1:

```
df_stations, строка 0:
  station_id: "s1"
  name: "5th Ave & 23rd St"
  lat: 40.7425
  lon: -73.9891
  capacity: 25
  region_id: "NYC"

df_inventory_ts, первая строка:
  timestamp: 2025-01-01 00:00
  s1: 12    ← 12 велосипедов в полночь
  s2: 7
  s3: 18
  ...
```

Ключевой момент: эти данные **не являются** графовой моделью. Слово "station" и формат данных — чисто велосипедные. Протокол, через который mock отдаёт эти данные, называется `BikeShareSourceProtocol` (раньше — `DataSourceProtocol`), и он явно доменно-специфичный.

---

## 2. DataLoaderGraph: перевод в универсальные таблицы

`DataLoaderGraph` берёт доменные данные и собирает из них `RawModelData` — универсальный контракт, где нет слов "station" или "bike" в структуре (только в значениях строк).

```python
loader = DataLoaderGraph(mock, GraphLoaderConfig(distance_backend="haversine"))
loader.load_data()
```

Внутри `_build_raw_model()` вызывается 7 builder-методов. Пройдём по каждому.

### 2.1. `_build_temporal()` → временная ось

Из 48 часовых снапшотов (2 дня) создаётся:

```
planning_horizon:
  planning_horizon_id: "h1"
  name: "mock_horizon"
  start_date: 2025-01-01
  end_date: 2025-01-03           ← exclusive upper bound

planning_horizon_segments:
  planning_horizon_id: "h1"
  segment_index: 0
  period_type: "day"             ← одна гранулярность на всё

periods (2 строки):
  period_id │ period_index │ start_date │ end_date
  ──────────┼──────────────┼────────────┼──────────
  p0        │ 0            │ 2025-01-01 │ 2025-01-02
  p1        │ 1            │ 2025-01-02 │ 2025-01-03
```

Почему 2 периода из 48 снапшотов? Потому что `period_type = DAY`, и 48 часов = 2 дня. Все 24 снапшота одного дня схлопнутся в один period при time resolution.

### 2.2. `_build_entities()` → объекты сети

Станции и депо сливаются в одну таблицу `facilities`:

```
facilities (10 строк):
  facility_id │ facility_type │ name              │ lat     │ lon
  ────────────┼───────────────┼───────────────────┼─────────┼────────
  d1          │ depot         │ "d1"              │ 40.751  │ -73.994
  d2          │ depot         │ "d2"              │ 40.738  │ -73.981
  s1          │ station       │ "5th Ave & 23rd"  │ 40.742  │ -73.989
  s2          │ station       │ "Broadway & 14th" │ 40.735  │ -73.991
  ...         │ ...           │ ...               │ ...     │ ...
```

Плюс две одностроковые таблицы — `commodity_categories` и `resource_categories`:

```
commodity_categories:
  commodity_category_id: "working_bike"
  name: "Working bike"
  unit: "bike"

resource_categories:
  resource_category_id: "rebalancing_truck"
  name: "Truck"
  base_capacity: 20.0
  capacity_unit: "bike"
```

### 2.3. `_build_behavior()` → роли и операции

Каждый facility получает **роли** (как он ведёт себя в потоке) и **операции** (что он умеет делать):

```
facility_roles (26 строк):
  facility_id │ role
  ────────────┼────────────────
  d1          │ storage              ← депо хранит велосипеды
  d1          │ transshipment        ← и перегружает (не конечная точка)
  s1          │ sink                 ← станция потребляет (спрос)
  s1          │ source               ← станция отдаёт (поездки начинаются)
  s1          │ storage              ← и хранит

facility_operations (30 строк):
  facility_id │ operation_type │ enabled
  ────────────┼────────────────┼────────
  d1          │ receiving      │ true
  d1          │ storage        │ true
  d1          │ dispatch       │ true
  s1          │ receiving      │ true
  s1          │ storage        │ true
  s1          │ dispatch       │ true
```

Плюс `edge_rules` — правила генерации рёбер:

```
edge_rules (2 строки):
  source_type │ target_type │ modal_type │ enabled
  ────────────┼─────────────┼────────────┼────────
  null        │ null        │ road       │ true       ← все со всеми
  null        │ null        │ road       │ true
```

`null` в source_type/target_type означает "любой тип" — между всеми парами facility создаётся ребро.

### 2.4. `_build_edges()` → рёбра графа

Для каждой пары facility (кроме пары с самим собой) вычисляется расстояние и создаётся ребро:

```
edges (90 строк = 10 × 9):
  source_id │ target_id │ modal_type │ distance │ lead_time_hours
  ──────────┼───────────┼────────────┼──────────┼────────────────
  d1        │ d2        │ road       │ 1.82 km  │ 0.036 h
  d1        │ s1        │ road       │ 1.12 km  │ 0.022 h
  d1        │ s2        │ road       │ 1.85 km  │ 0.037 h
  s1        │ s2        │ road       │ 0.78 km  │ 0.016 h
  ...       │ ...       │ ...        │ ...      │ ...
```

`lead_time_hours = distance / default_speed_kmh` (50 км/ч по умолчанию).

Параллельно создаётся `edge_commodities` — какие товары могут перемещаться по каждому ребру:

```
edge_commodities (90 строк):
  source_id │ target_id │ modal_type │ commodity_category │ capacity_consumption
  ──────────┼───────────┼────────────┼────────────────────┼─────────────────────
  d1        │ s1        │ road       │ working_bike       │ 1.0
  d1        │ s2        │ road       │ working_bike       │ 1.0
  ...
```

`capacity_consumption = 1.0` — один велосипед занимает одну единицу ёмкости.

### 2.5. `_build_flow_and_operations()` → спрос и запасы

Начальный инвентарь берётся из **первого** снапшота `df_inventory_ts`:

```
inventory_initial (8 строк — только станции):
  facility_id │ commodity_category │ quantity │ quantity_unit
  ────────────┼────────────────────┼──────────┼──────────────
  s1          │ working_bike       │ 12.0     │ bike
  s2          │ working_bike       │ 7.0      │ bike
  s3          │ working_bike       │ 18.0     │ bike
  ...
```

Вместимость станций → `operation_capacities`:

```
operation_capacities (8 строк):
  facility_id │ operation_type │ commodity_category │ capacity │ capacity_unit
  ────────────┼────────────────┼────────────────────┼──────────┼──────────────
  s1          │ storage        │ working_bike       │ 25.0     │ bike
  s2          │ storage        │ working_bike       │ 30.0     │ bike
  ...
```

### 2.6. `_build_costs()` → стоимости

Из `df_station_costs` и `df_truck_rates` генерируются time-varying таблицы:

```
operation_costs (960 строк = 8 станций × 2 типа × ~60 дат):
  facility_id │ operation_type │ commodity_category │ date       │ cost_per_unit │ cost_unit
  ────────────┼────────────────┼────────────────────┼────────────┼───────────────┼──────────
  s1          │ visit          │ working_bike       │ 2025-01-01 │ 15.0          │ USD
  s1          │ handling       │ working_bike       │ 2025-01-01 │ 2.5           │ USD

transport_costs:
  source_id │ target_id │ modal_type │ resource_category     │ date       │ cost_per_unit
  ──────────┼───────────┼────────────┼───────────────────────┼────────────┼──────────────
  d1        │ s1        │ road       │ rebalancing_truck     │ 2025-01-01 │ 1.2
```

### 2.7. `_build_resources()` → автопарк

```
resource_fleet:
  facility_id │ resource_category     │ count
  ────────────┼───────────────────────┼──────
  d1          │ rebalancing_truck     │ 1
  d2          │ rebalancing_truck     │ 1

resource_commodity_compatibility:
  resource_category     │ commodity_category │ enabled
  ──────────────────────┼────────────────────┼────────
  rebalancing_truck     │ working_bike       │ true

resource_modal_compatibility:
  resource_category     │ modal_type │ enabled
  ──────────────────────┼────────────┼────────
  rebalancing_truck     │ road       │ true
```

### Итог шага 2

Все 7 методов вернули dict'ы, которые собрались в один `RawModelData`. Можно проверить:

```python
print(loader.raw.table_summary())
```

```
RawModelData — table summary
============================

  entity
  ──────
    facilities: 10 rows (required)
    commodity_categories: 1 rows (required)
    resource_categories: 1 rows (required)

  temporal
  ────────
    planning_horizon: 1 rows (required)
    planning_horizon_segments: 1 rows (required)
    periods: 2 rows (required)

  behavior
  ────────
    facility_roles: 26 rows (required)
    facility_operations: 30 rows (required)
    edge_rules: 2 rows (required)

  edge
  ────
    edges: 90 rows
    edge_commodities: 90 rows

  flow_data
  ─────────
    demand: —
    inventory_initial: 8 rows

  parameters
  ──────────
    operation_capacities: 8 rows
    operation_costs: 960 rows
    transport_costs: ...

  resource
  ────────
    resource_fleet: 2 rows
    resource_commodity_compatibility: 1 rows
    resource_modal_compatibility: 1 rows
```

---

## 3. build_model(): 8 шагов от Raw до Resolved

```python
from gbp.build import build_model
resolved = build_model(raw)
```

### Шаг 1: Валидация

Проверяет, что данные согласованы:
- Каждый facility_id в demand имеет роль SINK
- Каждый facility_id в inventory_initial имеет роль STORAGE
- Для каждого edge × commodity существует compatible resource
- Граф связен: каждый SINK достижим из хотя бы одного SOURCE

Если есть ошибки — exception. Если предупреждения — лог.

### Шаг 2: Time Resolution

Все таблицы с колонкой `date` преобразуются: `date` → `period_id`, значения агрегируются внутри периода.

Пример с `operation_costs`. Было:

```
facility_id │ date       │ cost_per_unit
────────────┼────────────┼──────────────
s1          │ 2025-01-01 │ 15.0            ← попадает в period p0
s1          │ 2025-01-01 │ 15.0            ← тоже p0 (другой operation_type)
s1          │ 2025-01-02 │ 15.0            ← попадает в period p1
```

Стало:

```
facility_id │ period_id │ cost_per_unit
────────────┼───────────┼──────────────
s1          │ p0        │ 15.0              ← mean за все даты внутри p0
s1          │ p1        │ 15.0              ← mean за все даты внутри p1
```

Правило агрегации зависит от типа: `mean` для стоимостей, `sum` для demand/supply.

### Шаг 3: Построение рёбер

Если `raw.edges` уже заполнен (наш случай — DataLoaderGraph их построил), используются как есть. Если нет — рёбра генерируются из `edge_rules` и `facilities`.

### Шаг 4: Lead Time Resolution

Конвертирует `lead_time_hours` в количество периодов:

```
lead_time_hours = 0.022 (1.1 км / 50 км/ч)
period_type = DAY (24 часа)
lead_time_periods = ceil(0.022 / 24) = 1    ← минимум 1 период
```

Результат — `edge_lead_time_resolved`:

```
source_id │ target_id │ modal_type │ period_id │ lead_time_periods │ arrival_period_id
──────────┼───────────┼────────────┼───────────┼───────────────────┼──────────────────
d1        │ s1        │ road       │ p0        │ 1                 │ p1
d1        │ s1        │ road       │ p1        │ 1                 │ null  ← за горизонтом
```

В multi-resolution (DAY + WEEK сегменты) одно и то же ребро с `lead_time_hours = 48` даёт 2 DAY-периода, но 0 WEEK-периодов. Поэтому lead_time зависит от периода отправления.

### Шаг 5: Transformation Resolution

В нашем примере трансформаций нет (нет Maintenance Hub). Но если бы были:

```
BROKEN_BIKE ×1.0 → WORKING_BIKE ×1.0, loss_rate = 5%
```

Этот шаг создаёт развёрнутую таблицу: для каждого facility с трансформацией — какие commodity входят, какие выходят, с какими коэффициентами.

### Шаг 6: Fleet Capacity Computation

```
resource_fleet:  d1, rebalancing_truck, count=1
resource_categories: rebalancing_truck, base_capacity=20

fleet_capacity:
  facility_id │ resource_category     │ effective_capacity
  ────────────┼───────────────────────┼───────────────────
  d1          │ rebalancing_truck     │ 20.0    (= 1 × 20)
  d2          │ rebalancing_truck     │ 20.0
```

Если бы были L3-ресурсы (конкретные грузовики с `capacity_override`), суммировались бы индивидуальные вместимости.

### Шаг 7: Сборка ResolvedModelData

Все resolved-таблицы + generated-таблицы собираются в один `ResolvedModelData`.

### Шаг 8: Spine Assembly

Attribute tables (costs, capacities) с разными grain'ами собираются в "спайны" — широкие DataFrame'ы, где все атрибуты одной сущности объединены через серию left-join'ов.

```
facility_spines["group_0"]:
  facility_id │ period_id │ operation_cost │ storage_capacity │ ...
  ────────────┼───────────┼────────────────┼──────────────────┼────
  s1          │ p0        │ 15.0           │ 25.0             │ ...
  s1          │ p1        │ 15.0           │ 25.0             │ ...
  s2          │ p0        │ 12.0           │ 30.0             │ ...
```

Спайны — это то, что потребители (оптимизатор, симулятор) используют напрямую: одна таблица со всеми параметрами, готовая для векторизованных операций.

---

## 4. ResolvedModelData: готово для потребителей

```python
print(loader.resolved.table_summary())
```

Тот же набор групп, что и в Raw, плюс:

```
  generated
  ─────────
    edge_lead_time_resolved: 180 rows
    fleet_capacity: 2 rows
```

И спайны (dict[str, DataFrame]):

```python
loader.resolved.facility_spines   # {"group_0": DataFrame, ...}
loader.resolved.edge_spines       # {"group_0": DataFrame, ...}
```

### Кто потребляет

Один и тот же `ResolvedModelData` используется тремя способами:

```
Оптимизатор  — видит все периоды сразу
               формулирует LP/MILP: min cost s.t. flow conservation
               переменная решения: flow[edge, commodity, period]
               результат: solution_flow, solution_inventory

Симулятор    — идёт по периодам шаг за шагом
               на каждом шаге решает VRP или greedy dispatch
               результат: simulation_flow_log, simulation_inventory_log

Аналитика    — сравнивает plan vs fact
               группирует по иерархии (город → район → зона)
               результат: отчёты
```

Модель данных при этом **не меняется** — меняется только способ обработки.

---

## 5. Полная картина: одна строка

Проследим путь одной конкретной строки данных от реального мира до решения:

```
РЕАЛЬНЫЙ МИР
  Станция "5th Ave & 23rd St", 12 велосипедов в 00:00
                    │
                    ▼
DataLoaderMock.df_inventory_ts
  timestamp: 2025-01-01 00:00, s1: 12
                    │
                    ▼
DataLoaderGraph._build_flow_and_operations()
  inventory_initial: facility_id="s1", commodity_category="working_bike", quantity=12.0
                    │
                    ▼
RawModelData.inventory_initial (строка 0)
  facility_id="s1", commodity_category="working_bike", quantity=12.0, quantity_unit="bike"
                    │
                    ▼
build_model() — шаг 2 (time resolution)
  inventory_initial не time-varying → проходит без изменений
                    │
                    ▼
ResolvedModelData.inventory_initial (строка 0)
  facility_id="s1", commodity_category="working_bike", quantity=12.0
                    │
                    ▼
Оптимизатор: inventory["s1", "working_bike", p0] = 12.0
  → это начальное условие для flow conservation constraint на узле s1 в периоде p0
  → inventory[s1, p1] = inventory[s1, p0] + inflow[s1, p0] - outflow[s1, p0]
```

---

## 6. Карта пакетов

```
gbp.loaders                    gbp.core                       gbp.build
────────────                   ────────                       ─────────
DataLoaderMock                 RawModelData                   build_model()
  │ генерирует df_*              │ 46 таблиц в 10 группах       │ 8 шагов pipeline
  ▼                              │                               │
BikeShareSourceProtocol        entity_tables                   validate
  │ df_stations, df_trips...     temporal_tables                resolve_time_varying
  ▼                              behavior_tables                build_edges
DataLoaderGraph                  edge_tables                    resolve_lead_times
  │ 7 builder-методов            flow_tables                    resolve_transformations
  │ _build_temporal()            transformation_tables          compute_fleet_capacity
  │ _build_entities()            resource_tables                assemble
  │ _build_behavior()            parameter_tables               assemble_spines
  │ _build_edges()               hierarchy_tables                    │
  │ _build_flow_and_ops()        scenario_tables                     ▼
  │ _build_costs()               │                             ResolvedModelData
  │ _build_resources()           │                               │ + generated tables
  ▼                              ▼                               │ + spines
  RawModelData ──────────────────────────────────────────────► build_model()
                                                                     │
                                                                     ▼
                                                               ResolvedModelData
                                                                     │
                                                      ┌──────────────┼──────────────┐
                                                      ▼              ▼              ▼
                                                  Optimizer      Simulator      Analytics
```

---

## 7. Быстрый старт: make_raw_model()

Если не нужен полный loader, можно создать модель напрямую:

```python
from datetime import date
from gbp.core import make_raw_model
import pandas as pd

raw = make_raw_model(
    facilities=pd.DataFrame({
        "facility_id": ["d1", "s1", "s2"],
        "facility_type": ["depot", "station", "station"],
        "name": ["Depot", "Station 1", "Station 2"],
    }),
    commodity_categories=pd.DataFrame({
        "commodity_category_id": ["working_bike"],
        "name": ["Bike"], "unit": ["bike"],
    }),
    resource_categories=pd.DataFrame({
        "resource_category_id": ["truck"],
        "name": ["Truck"], "base_capacity": [20.0], "capacity_unit": ["bike"],
    }),
    planning_start=date(2025, 1, 1),
    planning_end=date(2025, 1, 4),
)

print(raw.table_summary())
# facility_roles, operations, edge_rules, periods — сгенерированы автоматически
```

Фабрика автоматически создаёт: временные таблицы из диапазона дат, роли из `facility_type` через `derive_roles`, операции (все enabled), и default edge_rules (все со всеми, road).

---

## Навигация по связанным документам

| Что | Где | Когда читать |
|-----|-----|--------------|
| Полная модель данных | `docs/graph_data_model.md` | Когда нужны детали конкретной таблицы |
| Справочные диаграммы | `docs/architecture_diagrams.md` | Когда нужна визуальная схема одной концепции |
| Mermaid-файлы | `docs/diagrams/*.mermaid` | Когда нужна диаграмма для встраивания |
| Спецификация рефакторинга | `docs/REFACTORING_SPEC.md` | Когда нужно понять, почему код устроен так |
| Тесты как примеры | `tests/unit/core/test_factory.py` | Минимальные примеры создания модели |
| Playground notebook | `notebooks/04_graph_model_playground.ipynb` | Интерактивная проверка pipeline |
