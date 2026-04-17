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
8 станций (station_1…station_8)   — стойки с велосипедами на улицах
2 депо (depot_1, depot_2)          — склады + грузовики для ребалансировки
2 типа товара                      — electric_bike, classic_bike
1 тип ресурса                      — rebalancing_truck (вместимость: 100)
48 часовых снапшотов               — 2 дня наблюдений
```

Источник данных — API, похожий на Citi Bike GBFS. Каждый час мы знаем: сколько велосипедов (электрических и классических) на каждой станции, какие поездки были, какая вместимость стоек по каждой категории.

---

## 2. DataLoaderMock: генерация сырых данных

`DataLoaderMock` создаёт синтетические данные в формате Citi Bike. Это **доменно-специфичные** таблицы — колонки называются `station_id`, `num_bikes_available`, `start_station_id`, `rideable_type`.

```python
DataLoaderMock({"n_stations": 8, "n_depots": 2, "n_timestamps": 48})
```

Что появляется после `mock.load_data()` (минимальный набор атрибутов на сущность — доменные бизнес-колонки. Полная спецификация — в докстринге `DataLoaderMock`):

| DataFrame                  | Строк   | Что внутри                                                    |
|----------------------------|---------|---------------------------------------------------------------|
| `df_stations`              | 8       | station_id, lat, lon                                          |
| `df_depots`                | 2       | node_id, lat, lon                                             |
| `df_resources`             | 3       | resource_id                                                   |
| `df_station_capacities`    | 16      | station_id, commodity_category, capacity (по 2 на станцию)    |
| `df_depot_capacities`      | 4       | node_id, commodity_category, capacity (по 2 на депо)          |
| `df_resource_capacities`   | 3       | resource_id, capacity                                         |
| `timestamps`               | 48      | DatetimeIndex (часовые)                                       |
| `df_inventory_ts`          | 48x20   | MultiIndex columns `(facility_id, commodity_category)`        |
| `df_telemetry_ts`          | 48x8    | num_bikes_available, num_ebikes_available, num_docks_available, ...|
| `df_trips`                 | ~500    | ride_id, rideable_type, start_station_id, end_station_id, … |
| `df_station_costs`         | 8       | station_id, fixed_cost_station                                |
| `df_depot_costs`           | 2       | node_id, fixed_cost_depot                                     |
| `df_truck_rates`           | 3       | resource_id, cost_per_km, cost_per_hour, fixed_dispatch_cost  |

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

Из 48 часовых снапшотов (2 дня) создаётся `planning_horizon` и один daily-сегмент, покрывающий диапазон источника. Сами `periods` на этом этапе не материализуются — их построит `build_model()` из сегментов (шаг Derivation):

```
planning_horizon: planning_horizon_id="h1", start=2025-01-01, end=2025-01-03
planning_horizon_segments: 1 сегмент, period_type="day"
```

### 3.2. `_build_entities()` — объекты сети

Станции и депо сливаются в одну таблицу `facilities` (10 строк). `commodity_categories` **обнаруживаются из `df_trips.rideable_type`** (в велошеринг-моке это `electric_bike`, `classic_bike`), `resource_categories` — один `rebalancing_truck` с `base_capacity`, равной максимальной `capacity` из `df_resource_capacities` (в стандартной конфигурации = 100).

### 3.3. `_build_behavior()` — операции и правила рёбер

Каждый facility получает набор операций (`receiving`, `storage`, `dispatch`) — это таблица `facility_operations`. Роли `facility_roles` лоадер **не эмитит** намеренно: их выведет `build_model` на шаге Derivation из `(facility_type, operations)` через `derive_roles`. Edge rules: все пары типов (`station↔station`, `depot↔station`, `depot↔depot`) по каждой commodity_category, modal = `road`.

### 3.4. `_build_distance_matrix()` — матрица расстояний

90 пар (10 x 9), каждая с `distance` (haversine) и `duration` (`distance / speed`). Матрица кладётся в `raw.distance_matrix` — декларативная таблица фактов о расстояниях. Материализация рёбер (`edges`, `edge_commodities`) происходит позже в `build_model()`.

### 3.5. `_build_node_parameters()` — начальный инвентарь (структурное) + ёмкости (параметрические)

Этот метод делает две вещи одновременно. Во-первых, строит **структурную** таблицу `inventory_initial` — по одной строке на `(facility, commodity_category)` (для 10 facilities × 2 categories = 20 строк):

```
inventory_initial (20 строк):
  facility_id  | commodity_category | quantity
  -------------|--------------------|---------
  station_1    | electric_bike      | 12.0
  station_1    | classic_bike       | 25.0
  ...
  depot_1      | electric_bike      | 60.0
  ...
```

Во-вторых, сразу же регистрирует в `registry` атрибут `operation_capacity` (по всем операциям `storage` для station'ов и депо с их per-commodity capacity).

### 3.6. `_register_facility_costs()` и `_register_resource_costs()` — параметры через registry

Отдельно регистрируются стоимости — **параметрические**, с явным grain:

```python
# Стоимость содержания facility (time-varying через daily разворот по горизонту)
registry.register(
    name="facility_fixed_cost",
    data=cost_df,                  # facility_id, date, cost_per_unit, cost_unit
    entity_type="facility",
    kind=AttributeKind.COST,
    grain=("facility_id", "date"),
    value_column="cost_per_unit",
    aggregation="mean",
    unit="USD",
)

# Стоимости резервных грузовиков (3 отдельных атрибута)
for cost_attr, col in [
    ("resource_cost_per_km",   "cost_per_km"),
    ("resource_cost_per_hour", "cost_per_hour"),
    ("resource_fixed_dispatch","fixed_dispatch_cost"),
]:
    registry.register(
        name=cost_attr,
        data=rate_rows,            # resource_category, facility_id, resource_id, value
        entity_type="resource",
        kind=AttributeKind.COST,
        grain=("resource_category", "facility_id", "resource_id"),
        value_column="value",
        aggregation="mean",
        unit="USD",
    )
```

Каждый `register()` указывает **grain** — гранулярность этого атрибута. В другом домене тот же `facility_fixed_cost` мог бы иметь grain `("facility_id", "operation_type", "commodity_category", "date")`. Это не hardcoded в модель.

### 3.7. Оркестратор: `_build_raw_model()`

```python
def _build_raw_model(self) -> RawModelData:
    temporal = self._build_temporal()
    entities = self._build_entities()
    behavior = self._build_behavior(entities)
    distance_data = self._build_distance_matrix(entities) if self._config.build_edges else {}
    resources = self._build_resources(entities)

    observations: dict[str, pd.DataFrame | None] = {}
    if self._config.build_observations:
        observations = self._build_observations(entities)

    # Параметрические атрибуты -> registry
    registry = AttributeRegistry()
    node_params = self._build_node_parameters(entities, registry)
    self._register_facility_costs(registry, temporal)
    self._register_resource_costs(registry, entities)

    all_tables = {
        **temporal,
        **entities.tables,
        **behavior,
        **distance_data,
        **node_params,
        **resources,
        **{k: v for k, v in observations.items() if v is not None},
    }
    return RawModelData(
        **{k: v for k, v in all_tables.items() if v is not None},
        attributes=registry,
    )
```

### 3.8. Итог: table_summary()

```
RawModelData — table summary
============================

  entity
  ------
    facilities: 10 rows (required)
    commodity_categories: 2 rows (required)
    resource_categories: 1 rows (required)
    commodities: 2 rows

  temporal
  --------
    planning_horizon: 1 rows (required)
    planning_horizon_segments: 1 rows (required)
    periods: —                   # будет построено build_model'ом

  behavior
  --------
    facility_roles: —            # будет выведено build_model'ом
    facility_operations: 30 rows (required)
    edge_rules: 16 rows (required)

  edge
  ----
    distance_matrix: 90 rows

  flow_data
  ---------
    inventory_initial: 20 rows

  observations
  ------------
    observed_flow: ~N rows
    observed_inventory: ~M rows

  resource
  --------
    resource_fleet: 1 rows
    resources: 3 rows
    resource_commodity_compatibility: 2 rows
    resource_modal_compatibility: 1 rows

  parameters (AttributeRegistry)
  ------------------------------
    operation_capacity:      20 rows   facility  CAPACITY  [facility_id × operation_type × commodity_category]
    facility_fixed_cost:     20 rows   facility  COST      [facility_id × date]
    resource_cost_per_km:     3 rows   resource  COST      [resource_category × facility_id × resource_id]
    resource_cost_per_hour:   3 rows   resource  COST      [resource_category × facility_id × resource_id]
    resource_fixed_dispatch:  3 rows   resource  COST      [resource_category × facility_id × resource_id]
```

Структурные таблицы — фиксированные поля. Параметрические — динамические, из registry, с grain'ами. Часть структурных таблиц может быть `None` после loader'а — их добьёт `build_model()` на шаге Derivation.

---

## 4. build_model(): шаги от Raw до Resolved

`build_model(raw)` в `gbp/build/pipeline.py` выполняет последовательность шагов. Каждый "падающий" шаг оборачивается в `BuildError(step, cause)`.

### Шаг 0: Derivation (`_apply_derivations`)

Автозаполнение того, что пользователь не задал явно, **до** валидации. Изменения записываются в `BuildReport`:

- `periods` — из `planning_horizon` + `planning_horizon_segments` (`build_periods_from_segments`).
- `commodity_categories` / `resource_categories` — дефолт на одну категорию, если отсутствуют.
- `facility_roles` — `derive_facility_roles(facilities, facility_operations)`.
- `demand` / `supply` — из `observed_flow` группировкой по source/target × date × cc.
- `inventory_initial` — из `observed_inventory` (первый снапшот) или seeded из `observed_flow` (outflow первого дня).

Важно: пустой DataFrame (явное "нет строк") не перезаписывается. Перезаписывается только `None`.

### Шаг 1: Validation (`validate_raw_model`)

Проверяет структурные таблицы: FK integrity, роли, connectivity. Параметрические атрибуты уже провалидированы при `register()`.

### Шаг 2: Time Resolution структурных таблиц (`resolve_all_time_varying`)

Структурные таблицы, у которых есть `date` (demand, supply, `edge_capacities`, `observed_*` и т. п.), резолвятся через periods.

### Шаг 3: Time Resolution атрибутов (`resolve_registry_attributes`)

Параметрические атрибуты резолвятся через registry — pipeline итерирует по `raw.attributes.specs`:

```
for spec in raw.attributes.specs:
    if "date" in spec.grain:
        resolve_to_periods(data, periods, agg=spec.aggregation)
        # date -> period_id, aggregation по spec
```

Pipeline **не знает** имена атрибутов. Specs сами говорят: что time-varying, как агрегировать, какой grain.

Пример: `facility_fixed_cost` (20 строк: 10 facilities × 2 дня) -> 20 строк с `period_id`, `aggregation=mean`.

### Шаг 4: Edge Building (`_ensure_edges_and_commodities` / `build_edges`)

Если `raw.edges` не задан, собирает из `edge_rules` + `distance_matrix` + `scenario_manual_edges`.

### Шаги 5-7: Lead times, Transformations, Fleet capacity

`resolve_lead_times` (часы -> период на каждом `edge × period`), `resolve_transformations`, `compute_fleet_capacity` (`count × base_capacity`).

### Шаг 8: Сборка ResolvedModelData (`ResolvedModelData.from_raw`)

Resolved структурные таблицы + resolved_registry + build-артефакты -> `ResolvedModelData`.

### Шаг 9: Spine Assembly (`assemble_spines`)

Specs из **двух источников**: `resolved.attributes.specs` (параметрические) + `get_structural_attribute_specs()` (edge_distance, edge_lead_time_hours, edge_reliability, resource_base_capacity — берутся из структурных таблиц `edges` / `resource_categories`).

Engine автоматически:
1. Группирует specs по совместимости grain'ов (`auto_group_attributes`)
2. Определяет порядок join'ов (`plan_merges`)
3. Собирает спайны (`AttributeBuilder.build_spines`)

Результат: `facility_spines`, `edge_spines`, `resource_spines` — словари `{group_name: DataFrame}` со всеми атрибутами. К `resolved` также прикрепляется `resolved.build_report` — отчёт о том, что было автодеривировано.

---

## 5. Путь одной строки: от велосипеда до constraint'а

```
РЕАЛЬНЫЙ МИР
  Станция "station_1", вместимость для classic_bike: 17 стоек

DataLoaderMock
  df_stations:            station_id="station_1"
  df_station_capacities:  (station_1, classic_bike, 17)

DataLoaderGraph._build_node_parameters()
  registry.register(name="operation_capacity", data=...,
      grain=("facility_id", "operation_type", "commodity_category"),
      kind=CAPACITY, value_column="capacity", aggregation="min")

RawModelData.attributes.get("operation_capacity")
  spec.grain = ("facility_id", "operation_type", "commodity_category")
  spec.kind = CAPACITY

build_model(), шаг 3 (resolve_registry_attributes):
  не time-varying -> passthrough

build_model(), шаг 9 (assemble_spines):
  -> facility_spines["<group>"]: facility_id="station_1",
                                  operation_type="storage",
                                  commodity_category="classic_bike",
                                  operation_capacity=17.0

Оптимизатор:
  inventory["station_1", "classic_bike", t] <= 17.0
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
  | генерирует df_*              | структурные таблицы           | _apply_derivations
  v                              | + AttributeRegistry           | validate_raw_model
BikeShareSourceProtocol          |                             resolve_all_time_varying
  | df_stations, df_trips...   entity / temporal / behavior    resolve_registry_attributes
  v                            edge / flow / resource          _ensure_edges_and_commodities
DataLoaderGraph                hierarchy / scenario            resolve_lead_times
  | структурные:                                               resolve_transformations
  |  _build_temporal()         parameter_tables                compute_fleet_capacity
  |  _build_entities()           = registry.to_dict()          ResolvedModelData.from_raw
  |  _build_behavior()                                         assemble_spines
  |  _build_distance_matrix()   registry.register()                (registry.specs
  |  _build_node_parameters()    registry.get(name)                 + structural specs)
  |  _build_resources()          registry.get_by_kind(COST)                |
  |  _build_observations()                                                v
  | параметрические:                                            ResolvedModelData
  |  _register_facility_costs()                                  + resolved registry
  |  _register_resource_costs()                                  + spines
  v                                                              + build_report
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
| Полная модель данных | `docs/design/graph_data_model.md` | Детали конкретной таблицы |
| Система атрибутов | `docs/design/attribute_system.md` | Почему registry устроен так |
| Environment (симулятор) | `docs/design/environment_design.md` | Как потребитель использует resolved модель |
| Справочные диаграммы | `docs/architecture_diagrams.md` | Визуальная схема одной концепции |
| Storytelling-гайды | `docs/story_telling/` | Narrative-версии design-документов |
| Тесты как примеры | `tests/unit/core/test_registry.py` | Примеры register/get/build |
| Playground notebook | `notebooks/04_graph_model_playground.ipynb` | Интерактивная проверка pipeline |
| Pipeline walkthrough | `notebooks/05_pipeline_walkthrough.ipynb` | Пошаговое исполнение `build_model()` |
