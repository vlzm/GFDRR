# Путь данных: от велосипеда до оптимизатора

Этот документ проводит через **полный путь данных** на конкретном примере велошеринга. Не абстрактная архитектура, а конкретные строки, конкретные числа, конкретные трансформации.

Для справочных диаграмм по отдельным концепциям см. `docs/architecture_diagrams.md`. Здесь — сквозной маршрут.

---

## 0. Два мира: структура и параметры

Прежде чем погружаться в данные, важно понять ключевое архитектурное разделение.

**Структурные таблицы** описывают топологию графа — что существует и как связано: facilities, edges, demand, supply, roles, operations. У них фиксированные поля в `RawModelData`. Optimizer и simulator обращаются к ним по имени: `raw.demand`, `raw.edges`.

**Параметрические атрибуты** — числовые значения на определённом grain: стоимости, ёмкости, цены. Они живут в `AttributeRegistry` и регистрируются через `raw.attributes.register()`. Их набор, grain и даже имена зависят от домена. В велошеринге — `operation_cost` с grain `(facility_id, operation_type, commodity_category, date)`. В другом домене тот же cost может иметь grain `(facility_id, date)` — без operation_type.

Это разделение проходит через всю систему: dataloader создаёт структурные таблицы и регистрирует параметры, build_model резолвит параметры через specs из registry, спайны собираются из registry.

---

## 1. Реальный мир

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

## 2. DataLoaderMock: генерация сырых данных

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
| `df_inventory_ts`  | 48x8  | матрица: timestamp x station_id -> кол-во байков    |
| `df_telemetry_ts`  | 48x8  | num_bikes_available, num_docks_available, ...        |
| `df_trips`         | ~500  | ride_id, start_station_id, end_station_id, times    |
| `df_station_costs` | 8     | fixed_cost_per_visit, cost_per_bike_moved           |
| `df_truck_rates`   | 2     | cost_per_km, cost_per_hour, fixed_dispatch_cost     |

Ключевой момент: эти данные **не являются** графовой моделью. Слово "station" и формат данных — чисто велосипедные. Протокол называется `BikeShareSourceProtocol` — он явно доменно-специфичный.

---

## 3. DataLoaderGraph: перевод в универсальные таблицы

`DataLoaderGraph` берёт доменные данные и собирает `RawModelData`. Внутри `_build_raw_model()` вызываются builder-методы для **структурных** таблиц и register-методы для **параметрических** атрибутов.

```python
from gbp.build.pipeline import build_model

loader = DataLoaderGraph(mock, GraphLoaderConfig(distance_backend="haversine"))
raw = loader.load()
resolved = build_model(raw)
```

### 3.1. `_build_temporal()` — временная ось

Из 48 часовых снапшотов (2 дня) создаётся:

```
planning_horizon: planning_horizon_id="h1", start=2025-01-01, end=2025-01-03

periods (2 строки):
  period_id | period_index | start_date | end_date
  ----------|--------------|------------|----------
  p0        | 0            | 2025-01-01 | 2025-01-02
  p1        | 1            | 2025-01-02 | 2025-01-03
```

### 3.2. `_build_entities()` — объекты сети

Станции и депо сливаются в одну таблицу `facilities` (10 строк). Плюс `commodity_categories` ("working_bike") и `resource_categories` ("rebalancing_truck", base_capacity=20).

### 3.3. `_build_behavior()` — роли и операции

Каждый facility получает роли (sink, source, storage, transshipment) и операции (receiving, storage, dispatch). Edge rules: все со всеми, road.

### 3.4. `_build_distance_matrix()` — матрица расстояний

90 пар (10 x 9), каждая с distance (haversine) и duration (distance / 50 км/ч). Матрица кладётся в `raw.distance_matrix` — декларативная таблица фактов о расстояниях. Материализация рёбер (`edges`, `edge_commodities`) происходит позже в `build_model()`.

### 3.5. `_build_node_parameters()` — начальный инвентарь (структурное)

```
inventory_initial (8 строк):
  facility_id | commodity_category | quantity
  ------------|--------------------|---------
  s1          | working_bike       | 12.0
  s2          | working_bike       | 7.0
  ...
```

Это **структурная** таблица — граничное условие для оптимизатора. Живёт как фиксированное поле `raw.inventory_initial`.

### 3.6. `_register_costs()` и `_register_capacities()` — параметры через registry

Здесь ключевое отличие. Стоимости и ёмкости — **параметрические**, и регистрируются через `AttributeRegistry` с явным grain:

```python
# Вместимость станций
registry.register(
    name="operation_capacity",
    data=op_cap_df,
    entity_type="facility",
    kind=AttributeKind.CAPACITY,
    grain=("facility_id", "operation_type", "commodity_category"),
    value_column="capacity",
    aggregation="min",
)

# Стоимость визита (time-varying!)
registry.register(
    name="operation_cost",
    data=cost_df,
    entity_type="facility",
    kind=AttributeKind.COST,
    grain=("facility_id", "operation_type", "commodity_category", "date"),
    value_column="cost_per_unit",
    aggregation="mean",
    unit="USD",
)
```

Каждый `register()` указывает **grain** — гранулярность этого атрибута. В другом домене тот же operation_cost может иметь grain `("facility_id", "date")` без operation_type. Это не hardcoded.

### 3.7. Оркестратор: `_build_raw_model()`

```python
def _build_raw_model(self) -> RawModelData:
    temporal = self._build_temporal()
    entities = self._build_entities()
    behavior = self._build_behavior(entities)
    distance_data = self._build_distance_matrix(entities)
    flow = self._build_node_parameters(entities)

    # Структурные таблицы -> RawModelData
    raw = RawModelData(**{**temporal, **entities.tables, **behavior, **distance_data, **flow})

    # Параметрические атрибуты -> registry
    self._register_costs(raw.attributes, temporal)
    self._register_capacities(raw.attributes, entities)
    self._register_resource_costs(raw.attributes, entities)

    return raw
```

### 3.8. Итог: table_summary()

```
RawModelData — table summary
============================

  entity
  ------
    facilities: 10 rows (required)
    commodity_categories: 1 rows (required)
    resource_categories: 1 rows (required)

  temporal
  --------
    planning_horizon: 1 rows (required)
    planning_horizon_segments: 1 rows (required)
    periods: 2 rows (required)

  behavior
  --------
    facility_roles: 26 rows (required)
    facility_operations: 30 rows (required)
    edge_rules: 2 rows (required)

  edge
  ----
    edges: 90 rows
    edge_commodities: 90 rows

  flow_data
  ---------
    inventory_initial: 8 rows

  resource
  --------
    resource_fleet: 1 rows
    resource_commodity_compatibility: 1 rows
    resource_modal_compatibility: 1 rows

  parameters (AttributeRegistry)
  ------------------------------
    operation_cost: 960 rows  facility  COST      [facility_id x operation_type x commodity_category x date]
    operation_capacity: 8 rows  facility  CAPACITY  [facility_id x operation_type x commodity_category]
    resource_cost_per_km: 2 rows  resource  COST  [resource_category x facility_id x resource_id]
```

Структурные таблицы — фиксированные поля. Параметрические — динамические, из registry, с grain'ами.

---

## 4. build_model(): 8 шагов от Raw до Resolved

### Шаг 1: Валидация

Проверяет структурные таблицы: FK integrity, роли, connectivity. Параметрические атрибуты уже провалидированы при `register()`.

### Шаг 2: Time Resolution

Параметрические атрибуты резолвятся через registry — pipeline итерирует по `raw.attributes.specs`:

```
for attr in raw.attributes.specs:
    if attr.time_varying:     # "date" в grain
        resolve_to_periods(data, periods, agg=attr.aggregation)
        # date -> period_id, aggregation по spec
```

Pipeline **не знает** имена атрибутов. Specs сами говорят: что time-varying, как агрегировать, какой grain.

Пример: operation_cost (960 строк с date) -> 2 строки с period_id, aggregation=mean.

### Шаги 3-6: Edge building, Lead times, Transformations, Fleet capacity

Работают со структурными таблицами, без изменений.

### Шаг 7: Сборка ResolvedModelData

Resolved структурные таблицы + resolved_registry -> `ResolvedModelData`.

### Шаг 8: Spine Assembly

Specs из **двух источников**: `resolved.attributes.specs` (параметрические) + `get_structural_attribute_specs()` (edge_distance, lead_time из структурных таблиц).

Engine автоматически:
1. Группирует specs по совместимости grain'ов (`auto_group_attributes`)
2. Определяет порядок join'ов (`plan_merges`)
3. Собирает спайны (`build_spines`)

Результат: facility_spines, edge_spines, resource_spines — широкие DataFrame'ы со всеми атрибутами.

---

## 5. Путь одной строки: от велосипеда до constraint'а

```
РЕАЛЬНЫЙ МИР
  Станция "5th Ave", вместимость 25 стоек

DataLoaderMock.df_stations
  station_id: "s1", capacity: 25

DataLoaderGraph._register_capacities()
  registry.register(name="operation_capacity", data=...,
      grain=("facility_id", "operation_type", "commodity_category"),
      kind=CAPACITY, value_column="capacity")

RawModelData.attributes.get("operation_capacity")
  spec.grain = ("facility_id", "operation_type", "commodity_category")
  spec.kind = CAPACITY

build_model(), шаг 2: не time-varying -> без изменений

build_model(), шаг 8: spine assembly
  -> facility_spines["group_1"]: facility_id="s1", operation_capacity=25.0

Оптимизатор:
  inventory["s1", "working_bike", t] <= 25.0
```

---

## 6. Кастомный атрибут: гибкость в действии

```python
raw.attributes.register(
    name="weather_penalty",
    data=weather_df,
    entity_type="edge",
    kind=AttributeKind.RATE,
    grain=("source_id", "target_id", "modal_type", "date"),
    value_column="penalty_factor",
    aggregation="mean",
)
```

При build_model: date -> period_id, engine автоматически группирует с другими edge-атрибутами по grain-совместимости. Для этого **не нужно менять ни строки** в core, build, или model.

---

## 7. Карта пакетов

```
gbp.loaders                    gbp.core                       gbp.build
------------                   --------                       ---------
DataLoaderMock                 RawModelData                   build_model()
  | генерирует df_*              | структурные таблицы           | 8 шагов
  v                              | + AttributeRegistry           |
BikeShareSourceProtocol          |                             validate
  | df_stations, df_trips...   entity / temporal / behavior    resolve (registry.specs)
  v                            edge / flow / resource          build_edges
DataLoaderGraph                hierarchy / scenario            resolve_lead_times
  | структурные:                                               compute_fleet_capacity
  |  _build_temporal()         parameter_tables                assemble_spines
  |  _build_entities()           = registry.to_dict()            (registry.specs
  |  _build_behavior()                                            + structural specs)
  |  _build_distance_matrix()   registry.register()                      |
  |  _build_node_parameters()        registry.get(name)                       v
  | параметрические:           registry.get_by_kind(COST)      ResolvedModelData
  |  _register_costs()                                           + resolved registry
  |  _register_capacities()                                      + spines
  |  _register_resource_costs()
  v
  RawModelData -------> build_model() -------> ResolvedModelData
                                                      |
                                         +------------+------------+
                                         v            v            v
                                     Optimizer    Simulator    Analytics
```

---

## 8. Быстрый старт: make_raw_model()

```python
from datetime import date
from gbp.core import make_raw_model, AttributeKind
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

# Параметры — через registry
raw.attributes.register(
    name="station_capacity",
    data=pd.DataFrame({"facility_id": ["s1", "s2"], "capacity": [25.0, 30.0]}),
    entity_type="facility",
    kind=AttributeKind.CAPACITY,
    grain=("facility_id",),
    value_column="capacity",
)

print(raw.table_summary())
```

---

## Навигация по связанным документам

| Что | Где | Когда читать |
|-----|-----|--------------|
| Полная модель данных | `docs/graph_data_model.md` | Детали конкретной таблицы |
| Система атрибутов | `docs/ATTRIBUTE_SYSTEM_DESIGN.md` | Почему registry устроен так |
| Справочные диаграммы | `docs/architecture_diagrams.md` | Визуальная схема одной концепции |
| Cleanup параметров | `docs/CLEANUP_PARAMETRIC_FIELDS.md` | Почему фиксированные поля убраны |
| Тесты как примеры | `tests/unit/core/test_registry.py` | Примеры register/get/build |
| Playground notebook | `notebooks/04_graph_model_playground.ipynb` | Интерактивная проверка pipeline |
