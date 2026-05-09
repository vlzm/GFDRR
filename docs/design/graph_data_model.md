# Graph-Based Logistics Platform — Data Model

## Overview

Данный документ описывает универсальную графовую модель данных для задач сетевых потоков. Платформа позволяет моделировать перемещение commodity (велосипеды в шеринге, товары, деньги и др.) через сеть объектов с использованием транспортных ресурсов. Модель данных построена на multi-commodity flow формулировке и табличных структурах для поддержки векторизированных операций (pandas, PySpark).

Документ описывает **только модель данных** — структуры, таблицы, грейны и отношения. Модель данных описывает "мир" (какие объекты, связи и параметры существуют) и является **общей** для разных потребителей: оптимизатор (все периоды одновременно), симулятор (шаг за шагом), аналитика (reporting). Каждый потребитель использует одну и ту же `ResolvedModelData`, но обрабатывает её по-своему (§15).

---

## 1. Основные сущности

Платформа строится на трёх базовых сущностях, каждая из которых является узлом графа:

### 1.1. Commodity — что перемещается

Основная сущность, которая движется по сети. Разделяется на два уровня:

**CommodityCategory (L2)** — тип потока в сети. Основная единица, по которой индексируются данные о потоках: `flow[edge, commodity_category, period]`. Каждый commodity category имеет свои demand/supply и свои пропускные способности.

**Commodity (L3)** — конкретный экземпляр или лот. Нужен для instance-level tracking (какой именно велосипед куда переместился). Опциональный уровень.


| Домен                 | CommodityCategory (L2)    | Commodity (L3)              |
| --------------------- | ------------------------- | --------------------------- |
| Велошеринг            | WORKING_BIKE, BROKEN_BIKE | Велосипед #4521             |
| Финансовые транзакции | USD, EUR                  | Перевод #TX-789             |
| Розничная логистика   | SKU_A, SKU_B (пример)     | Партия LOT-2024-001         |


### 1.2. Resource — чем перемещается

Сущность, которая производит перемещение commodity между узлами. Разделяется на два уровня (аналогично Commodity):

**ResourceCategory (L2)** — тип ресурса. Определяет базовые характеристики: capacity, compatible commodities, compatible modal types, cost profile. По нему индексируется `transport_costs` (таблица в модели).

**Resource (L2/L3, опциональный)** — конкретный экземпляр. Привязан к home facility, имеет availability windows. Нужен для задач, где важен конкретный ресурс (fleet management, VRP). Для агрегированных задач достаточно `resource_fleet`.


| Домен                 | ResourceCategory (L2)      | Resource (L3)      |
| --------------------- | -------------------------- | ------------------ |
| Велошеринг            | REBALANCING_TRUCK          | Грузовик #REB-07   |
| Финансовые транзакции | SWIFT, SEPA                | Канал SWIFT-EU-001 |
| Розничная логистика   | Фургон доставки (пример)   | ТС #VAN-112        |


### 1.3. Facility — где commodity находится

Объединённая сущность, которая заменяет раздельные warehouse и receiver. Различия между типами объектов выражаются через **roles** (семантическое поведение в сети) и **operations** (доступные операции с cost/capacity).


| Домен                 | Примеры                                  |
| --------------------- | ---------------------------------------- |
| Велошеринг            | Station, Depot, Maintenance Hub        |
| Финансовые транзакции | Bank, Person, Payment Gateway            |
| Розничная логистика   | Склад, магазин, хаб кросс-докинга       |


**Почему warehouse и receiver объединены в Facility:** граница между хранилищем и получателем искусственная. Велосипедная станция одновременно хранит (warehouse) и выдаёт/принимает (receiver). Банк — и то и другое. Депо велошеринга *принимает* велосипеды со станций и *отдаёт* их обратно после ребалансировки или ремонта. Вместо жёсткого разделения используются роли.

---

## 2. Facility: type, operations, roles

Facility описывается тремя ортогональными измерениями.

### 2.1. FacilityType — что это физически

Определяет, какие операции доступны и какие costs/capacities существуют.

```python
class FacilityType(Enum):
    """L3 default в gbp/core — велошеринг."""
    STATION = "station"
    DEPOT = "depot"
    MAINTENANCE_HUB = "maintenance_hub"
```

### 2.2. Operations — что объект умеет делать

Каждый тип facility поддерживает определённый набор операций. У каждой операции свои cost и capacity.

```python
class OperationType(Enum):
    """L3 default в gbp/core — велошеринг плюс универсальные операции границы сети."""
    RECEIVING = "receiving"      # приём commodity
    STORAGE = "storage"          # хранение
    DISPATCH = "dispatch"        # отправка (выдача / отгрузка)
    HANDLING = "handling"        # погрузка/разгрузка
    REPAIR = "repair"            # трансформация BROKEN_BIKE → WORKING_BIKE (Maintenance Hub)
    CONSUMPTION = "consumption"  # узел уничтожает поток (выход за пределы сети)
    PRODUCTION = "production"    # узел рождает поток (вход в сеть из внешнего мира)
```

Первые пять операций описывают физические действия *внутри* сети. `CONSUMPTION` и `PRODUCTION` описывают взаимодействие с границей сети и используются в L2-доменах, где потребитель или производитель сам является узлом графа (газовый клиент, скважина). В велошеринге эти две операции не используются — спрос обрабатывается фазой `DemandPhase` без явной операции на узле.

Маппинг типов на операции (велошеринг, как в `gbp/core`):


| FacilityType     | RECEIVING | STORAGE | REPAIR | HANDLING | DISPATCH |
| ---------------- | --------- | ------- | ------ | -------- | -------- |
| Station          | ✓         | ✓       | —      | ✓        | ✓        |
| Depot            | ✓         | ✓       | —      | ✓        | ✓        |
| Maintenance Hub  | ✓         | ✓       | ✓      | ✓        | ✓        |


Maintenance Hub — тип с операцией REPAIR и связанными `operation_costs` / `operation_capacities` для восстановления велосипедов.

### 2.3. FacilityRole — семантическое поведение в сети

Роли из теории сетевых потоков (network flow theory), адаптированные под логистику. Роль определяет, **как узел участвует в потоковой сети** — какие данные с ним ассоциируются и какую семантику он несёт.

```python
class FacilityRole(Enum):
    SOURCE = "source"
    SINK = "sink"
    STORAGE = "storage"
    TRANSSHIPMENT = "transshipment"
```

**SOURCE** — узел, который вводит commodity в сеть. У него нет входящего потока (или он пренебрежимо мал). В велошеринге: например депо, откуда в сеть поступают велосипеды после обслуживания, или станция с моделируемым **supply** при ребалансировке. Ассоциированные данные: `supply` (сколько доступно по периодам).

**SINK** — узел, который потребляет или выводит commodity из сети. В велошеринге: станция или зона, где пользовательский спрос «забирает» велосипеды из моделируемой сети (net outflow), или абстрактный спрос на поездки. Ассоциированные данные: `demand` (сколько требуется по периодам).

**STORAGE** — узел, который удерживает commodity во времени. Ключевое: есть ёмкость и состояние (сколько хранится сейчас). Ассоциированные данные: `storage_capacity`, `inventory_initial`.

**TRANSSHIPMENT** — узел, который перенаправляет commodity дальше. Ничего не производит и не потребляет. Чистый перевалочный пункт.

Один узел может совмещать несколько ролей:


| FacilityType     | Roles                         |
| ---------------- | ----------------------------- |
| Station          | SOURCE, SINK, STORAGE         |
| Depot            | STORAGE, TRANSSHIPMENT        |
| Maintenance Hub  | TRANSSHIPMENT, STORAGE        |


### 2.4. Вывод ролей

Роли выводятся из типа (дефолтные) с возможностью корректировки по операциям и ручного override:

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

        # Корректировка по операциям
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

Все четыре роли выводятся симметрично:

- `STORAGE` — снимается, если у узла нет операции `storage`.
- `TRANSSHIPMENT` — добавляется, когда есть `receiving` + `dispatch` (сквозной узел).
- `SINK` — добавляется операцией `consumption` (узел уничтожает поток на выходе из сети).
- `SOURCE` — добавляется операцией `production` (узел рождает поток на входе в сеть).

`DEFAULT_ROLES` несут L3-домен велошеринга: для `station` / `depot` / `maintenance_hub` роли назначаются по типу, потому что в велошеринге клиент не моделируется как `Facility` — спрос обрабатывается фазой симулятора. В L2-доменах, где конечный потребитель сам является узлом графа (например, газовая доставка: депо → клиент → потребление газа), роль `SINK` достигается явной операцией `consumption` на узле-клиенте, без вмешательства в `DEFAULT_ROLES`. Симметрично производитель (скважина, завод) описывается операцией `production`.

### 2.5. Зачем нужны роли при наличии type и operations

Три слоя отвечают за разное:

- **facility_type** → *что это* (для бизнес-логики и UI)
- **operations** → *что оно умеет и сколько стоит* (параметры)
- **roles** → *как оно себя ведёт в потоковой сети* (семантика)

Roles определяют **семантику** узла (SOURCE подаёт supply, SINK потребляет demand, STORAGE хранит inventory), operations определяют **параметры** (costs, capacities). Depot и Maintenance Hub оба могут иметь TRANSSHIPMENT + STORAGE, но у Maintenance Hub есть REPAIR и трансформация BROKEN_BIKE → WORKING_BIKE; у Station — роли пользовательского спроса/предложения (SINK/SOURCE) плюс хранение.

При переносе на новый домен facility_types и operations будут другими, но roles останутся те же — семантика переиспользуется без изменений.

---

## 3. Временная ось

### 3.1. Проблема

В логистике время — не просто ещё одна колонка в grain, а **фундаментальная ось модели**. Без явной временной оси нельзя выразить inventory carry-over между периодами:

```
inventory[t] = inventory[t-1] + inflow[t] - outflow[t]
```

Все данные о потоках и запасах индексируются по периодам: `flow[edge, commodity_category, period]`, `inventory[facility, commodity_category, period]`. Здесь edge = `(source_id, target_id, modal_type)`. Без этого модель — статический snapshot.

### 3.2. PlanningHorizon, Segments и Period

Planning horizon — **независимая сущность** (со своим PK), описывающая временну́ю сетку. Сценарий ссылается на горизонт через FK — один и тот же горизонт можно переиспользовать в нескольких сценариях (A/B тестирование с разными параметрами, но одной временно́й сеткой).

Горизонт может содержать **несколько сегментов с разной гранулярностью** — multi-resolution planning. Ближайшие дни планируем подневно, следующие недели — понедельно, дальний горизонт — помесячно.

```python
class PeriodType(Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
```

Пример: горизонт 6 месяцев:


| Segment | Period Type | Диапазон        | Периодов |
| ------- | ----------- | --------------- | -------- |
| 0       | DAY         | 1 Jan – 14 Jan  | 14       |
| 1       | WEEK        | 15 Jan – 11 Mar | 8        |
| 2       | MONTH       | 12 Mar – 30 Jun | ~4       |


`period_index` — глобальный и непрерывный (0, 1, ..., 25) по всем сегментам для carry-over. Каждый period имеет свой `period_type` (унаследованный от сегмента) и свою длительность — time resolution pipeline учитывает это при маппинге raw dates.

Для простых случаев (одна гранулярность) используется один сегмент — полностью обратно совместимо.

### 3.3. Хранение сырых данных vs. resolved данных

Параметрические таблицы `operation_costs`, `transport_costs` (и др. из `resolve_all_time_varying` в `gbp/build/time_resolution.py`) **остаются с `date`** в сырых данных, где это предусмотрено схемой. При сборке модели добавляется шаг **time resolution** — маппинг дат на period_id с агрегацией.

**Почему не заменять `date` на `period_id` сразу:**

- Одни и те же сырые данные можно использовать с разной гранулярностью (дневной план vs недельный)
- ETL проще — данные приходят с датами, не нужно заранее знать period_id
- Агрегация — отдельный, тестируемый шаг в pipeline

### 3.4. Time Resolution Pipeline

Resolution маппит raw `date` в `period_id` и агрегирует значения внутри периода:

```python
def resolve_to_periods(
    param_df: DataFrame,       # с колонкой date
    periods: DataFrame,        # period_id, start_date, end_date
    value_columns: list[str],  # колонки для агрегации
    group_grain: list[str],    # grain без date
    agg_func: str = "mean",    # как агрегировать внутри периода
) -> DataFrame:
    """
    Маппит date → period_id и агрегирует.
    date попадает в период, если period.start_date <= date < period.end_date.
    """
    merged = param_df.merge(
        periods,
        how="inner",
        left_on=lambda row: True,  # cross-like join с фильтром
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

С временной осью demand и supply становятся **явными time-varying данными**.

**Demand** — сколько commodity требуется facility с ролью SINK в каждом периоде. Индексируется по `commodity_category`. Сырые данные хранятся с `date`, резолвятся в `period_id`.

**Supply** — сколько commodity доступно от facility с ролью SOURCE в каждом периоде. Индексируется по `commodity_category`. Аналогично time-varying.

**Initial Inventory** — граничное условие: сколько commodity хранится на facility с ролью STORAGE на момент начала горизонта. Индексируется по `commodity_category`. Задаётся один раз, не time-varying.

**In-Transit Inventory** — commodity, находящиеся "в пути" на момент начала горизонта. Отправлены до `planning_horizon.start_date`, но ещё не прибыли. Без этого facility не получает ничего в первых `lead_time` периодах, даже если поставки были запланированы до начала горизонта.

---

## 4. Multi-Commodity Flow и Transformation

### 4.1. Multi-Commodity Flow

Платформа использует multi-commodity flow формулировку (Ahuja, Magnanti, Orlin, ch. 17; Williamson, ch. 10). Каждый `commodity_category` — отдельный тип потока в сети со своими demand, supply и пропускными способностями.

**Flow variable:** `flow[edge, commodity_category, period]` — количество commodity_category, проходящее по ребру в данном периоде.

**Per-commodity conservation:** для каждого commodity_category на каждом узле отдельно действуют правила в зависимости от роли (SOURCE: ограничен supply, SINK: удовлетворяет demand, TRANSSHIPMENT: вход = выход, STORAGE: баланс inventory).

**Shared edge capacity:** все commodity_categories на ребре делят общую пропускную способность. Данные об этом хранятся в таблице `edge_capacity`. Поскольку разные commodity могут иметь разные единицы измерения (штуки велосипедов, поездки, тонны в других доменах), shared capacity выражается в **capacity-единицах ребра** (`edge_capacity.capacity_unit`). Для приведения flow к единицам capacity используется коэффициент `capacity_consumption` в таблице `edge_commodity` — сколько единиц capacity "стоит" одна единица данного commodity на данном ребре.

Для single-commodity задач commodity dimension тривиально схлопывается в один элемент, и `flow[edge, commodity_category, period]` ≡ `flow[edge, period]`. Никаких накладных расходов.

### 4.2. Допустимые commodity на рёбрах

Каждое ребро (`source_id × target_id × modal_type`) имеет набор **допустимых commodity_categories** — фильтр, определяющий, какие типы потоков могут идти по этому ребру. Одно ребро может нести несколько commodity_categories (ребалансировочный грузовик везёт и WORKING_BIKE и BROKEN_BIKE; цифровой канал — USD и EUR).


| Source           | Target           | Modal | Допустимые Commodity        |
| ---------------- | ---------------- | ----- | --------------------------- |
| Depot            | Station          | ROAD  | WORKING_BIKE, BROKEN_BIKE   |
| Station          | Depot            | ROAD  | WORKING_BIKE, BROKEN_BIKE   |
| Maintenance Hub  | Depot            | ROAD  | WORKING_BIKE                |
| Depot            | Maintenance Hub  | ROAD  | BROKEN_BIKE                 |


### 4.3. Transformation — трансформация commodity

Некоторые facility преобразуют одни commodity_categories в другие. **Maintenance Hub** в велошеринге принимает BROKEN_BIKE и выдаёт WORKING_BIKE (операция REPAIR). Банк конвертирует USD в EUR. Нефтеперегонный завод принимает CRUDE и выдаёт GASOLINE + DIESEL + KEROSENE (splitting/co-production). Химическое производство смешивает два реагента в один продукт (blending).

Трансформация — это **свойство facility**, привязанное к конкретной операции. Для поддержки произвольных N→M преобразований (blending, splitting, co-production) трансформация разбита на три таблицы:

```python
class Transformation:
    transformation_id: str
    facility_id: str
    operation_type: OperationType    # REPAIR (велошеринг), CONVERSION, BLENDING, …
    loss_rate: float = 0.0          # общие потери процесса (0.02 = 2%)
    batch_size: float | None = None # кратность выхода (null = непрерывный)

class TransformationInput:
    transformation_id: str
    commodity_category: CommodityCategory  # BROKEN_BIKE
    ratio: float                           # сколько единиц входа на 1 "цикл"

class TransformationOutput:
    transformation_id: str
    commodity_category: CommodityCategory  # WORKING_BIKE
    ratio: float                           # сколько единиц выхода на 1 "цикл"
```

Для простого 1→1 случая (Maintenance Hub: 1 BROKEN_BIKE → 1 WORKING_BIKE с учётом `loss_rate`): один TransformationInput (BROKEN_BIKE, ratio=1.0) и один TransformationOutput (WORKING_BIKE, ratio=1.0). Для blending (2→1): два TransformationInput. Для splitting (1→2): два TransformationOutput.

### 4.4. Влияние на роли

Роли **не меняются**. TRANSSHIPMENT по-прежнему означает "перевалочный пункт", но для узлов с трансформацией баланс потоков выражается через transformation ratios вместо прямого равенства. Роль описывает **семантику** узла, трансформация — **параметры** преобразования.

Для узлов без трансформации (Depot, Station без REPAIR) ничего не меняется — inflow и outflow одного commodity, баланс остаётся `inflow = outflow`.

### 4.5. Примеры трансформаций

**1→1 (простая конверсия):**


| Домен      | Facility        | Operation  | Inputs           | Outputs           | Loss  |
| ---------- | --------------- | ---------- | ---------------- | ----------------- | ----- |
| Велошеринг | Maintenance Hub | REPAIR     | BROKEN_BIKE ×1.0 | WORKING_BIKE ×1.0 | 0.05  |
| Финансы    | Bank            | CONVERSION | USD ×1.0         | EUR ×(по курсу)   | 0.001 |


**1→N (splitting / co-production):**


| Домен | Facility | Operation    | Inputs     | Outputs                                      | Loss |
| ----- | -------- | ------------ | ---------- | -------------------------------------------- | ---- |
| Нефть | Refinery | DISTILLATION | CRUDE ×1.0 | GASOLINE ×0.45, DIESEL ×0.30, KEROSENE ×0.15 | 0.10 |


**N→1 (blending):**


| Домен | Facility | Operation | Inputs                         | Outputs      | Loss |
| ----- | -------- | --------- | ------------------------------ | ------------ | ---- |
| Химия | Blender  | BLENDING  | REAGENT_A ×0.6, REAGENT_B ×0.4 | PRODUCT ×1.0 | 0.02 |


---

## 5. Edges и Edge Rules

### 5.1. Edge Identity — multi-modal edges

Между двумя facility может существовать несколько рёбер с разными **modal_type** (тип транспорта): автотранспорт, ж/д, море, трубопровод. Каждое ребро — отдельный "канал" со своими cost, capacity, lead_time.

PK ребра: `source_id × target_id × modal_type`.

```python
class ModalType(Enum):
    ROAD = "road"
    RAIL = "rail"
    SEA = "sea"
    PIPELINE = "pipeline"
    AIR = "air"
    DIGITAL = "digital"        # для финансовых транзакций
```

### 5.2. Разделение ответственности

Роли на узлах **не определяют**, какие рёбра допустимы. Допустимые рёбра — это отдельная конфигурация сценария. Аналогично, допустимые commodity на ребре — отдельная конфигурация, а не следствие ролей.

### 5.3. Edge Attributes

Каждое ребро имеет набор атрибутов разной природы:


| Атрибут         | Grain (raw)                      | Time-varying | Kind       | Описание                              |
| --------------- | -------------------------------- | ------------ | ---------- | ------------------------------------- |
| distance        | edge                             | нет          | ADDITIONAL | Расстояние между узлами               |
| modal_type      | edge                             | нет          | ADDITIONAL | Часть PK — тип транспорта             |
| lead_time_hours | edge                             | нет          | ADDITIONAL | Время доставки в часах (raw)          |
| capacity        | edge × date                      | да           | CAPACITY   | Shared capacity (все commodity делят) |
| max_shipment    | edge × commodity_category × date | да           | CAPACITY   | Макс. объём за период per-commodity   |
| min_shipment    | edge × commodity_category        | нет          | CAPACITY   | Мин. объём отправки (nullable)        |
| transport_costs | edge × resource_category × date  | да           | COST       | Стоимость перевозки (таблица `transport_costs`) |
| reliability     | edge                             | нет          | ADDITIONAL | Вероятность доставки вовремя (0–1)    |


Здесь "edge" = `source_id × target_id × modal_type`.

**Lead Time** — время доставки, хранится в абсолютных единицах (часы). При сборке модели резолвится в целое число периодов. В uniform resolution (один segment): `ceil(lead_time_hours / period_duration_hours)`. В multi-resolution режиме lead_time_periods — **не скаляр на ребре**, а атрибут `edge × period`, потому что один и тот же lead_time = 48 часов даёт 2 периода при DAY, но 0 при WEEK. Resolved lead time хранится в отдельной таблице `edge_lead_time_resolved` (§11.6).

Lead time определяет сдвиг по времени: flow, отправленный в периоде `t`, прибывает в `t + lead_time_periods`. Потоки, отправленные в последних `lead_time` периодах горизонта, выходят за его границу — стандартное свойство time-expanded networks.

**Max Shipment** — per-commodity ограничение на максимальный объём за период. Time-varying (может меняться сезонно). В отличие от shared capacity (суммарное по всем commodity), max_shipment ограничивает каждый commodity_category отдельно.

**Reliability** — nullable, для risk-aware задач.

### 5.4. Edge Rules

Правила определяют, между какими типами facility допустимы связи, для каких commodity и с какими modal_type:

```python
class EdgeRule:
    source_type: FacilityType
    target_type: FacilityType
    commodity_category: CommodityCategory | None = None  # None = все категории
    modal_type: ModalType | None = None                  # None = все модальности
    enabled: bool = True
```

Стандартный сценарий (велошеринг, ребалансировка и ремонт):


| Source           | Target           | Commodity                 | Modal |
| ---------------- | ---------------- | ------------------------- | ----- |
| Depot            | Station          | WORKING_BIKE, BROKEN_BIKE | ROAD  |
| Station          | Depot            | WORKING_BIKE, BROKEN_BIKE | ROAD  |
| Maintenance Hub  | Depot            | WORKING_BIKE              | ROAD  |
| Depot            | Maintenance Hub  | BROKEN_BIKE               | ROAD  |


Расширенный сценарий добавляет, например: то же Depot → Station по **RAIL** (логистика между городами) — альтернативный канал с другим cost/lead_time.

### 5.5. Manual Edge Overrides

Для точечных кастомизаций — ручные тройки source_id × target_id × modal_type с указанием commodity_category, которые добавляются к rule-based edges.

### 5.6. Edge Builder (векторизированный)

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

    # Rule-based: cross join + inner join с rules
    candidates = sources.merge(edge_rules, on="source_type")
    edges = candidates.merge(targets, on="target_type")
    edges = edges[edges["source_id"] != edges["target_id"]]

    # Manual overrides: union
    edges = pd.concat([
        edges[["source_id", "target_id", "modal_type", "commodity_category"]],
        manual_pairs[["source_id", "target_id", "modal_type", "commodity_category"]],
    ]).drop_duplicates()

    # Обогащаем distance и modal attributes
    edges = edges.merge(distance_matrix, on=["source_id", "target_id"])
    edges = edges.merge(modal_attributes, on=["source_id", "target_id", "modal_type"],
                        how="left")

    return edges
```

---

## 6. Discrete Parameters

### 6.1. Проблема

Реальная логистика полна дискретных ограничений: минимальный объём отправки, кратность партий, целое число рейсов. Модель данных должна хранить эти параметры, позволяя потребителю (оптимизатор, аналитика, reporting) использовать их по-своему.

### 6.2. Общий паттерн: nullable = LP-compatible

Все discrete параметры в модели данных — **nullable**. Это ключевой design decision:

- Когда null — параметр не задан, потребитель интерпретирует как "ограничения нет"
- Когда задан — потребитель использует значение (оптимизатор может создать MILP constraint, аналитика — фильтр)

Это позволяет использовать одну и ту же модель данных для разных задач и уровней точности без изменения входных данных.

### 6.3. Типы discrete параметров

**Min shipment на ребре** (`edge_commodity_capacity.min_shipment`) — минимальный объём отправки. Поток по ребру либо 0, либо ≥ min_shipment.

**Min order quantity на SINK** (`demand.min_order_quantity`) — клиент не принимает заказ меньше X единиц за период.

**Batch size на трансформации** (`transformation.batch_size`) — выход кратен batch_size. Maintenance Hub может ремонтировать партиями по N велосипедов за цикл (nullable = непрерывный процесс).

**Vehicle trips** (`edge_vehicle`) — дискретное количество рейсов с фиксированной vehicle capacity.

### 6.4. Где живут параметры


| Параметр            | Таблица                 | Поле                    | Nullable                  |
| ------------------- | ----------------------- | ----------------------- | ------------------------- |
| Min shipment        | edge_commodity_capacity | min_shipment            | да                        |
| Max shipment        | edge_commodity_capacity | max_shipment            | нет                       |
| Min order quantity  | demand                  | min_order_quantity      | да                        |
| Batch size          | transformation          | batch_size              | да                        |
| Vehicle capacity    | edge_vehicle            | vehicle_capacity        | нет (таблица опциональна) |
| Max vehicles/period | edge_vehicle            | max_vehicles_per_period | да                        |


---

## 7. Иерархия и агрегация

### 7.1. Проблема масштабирования

Сеть с 10 000 клиентов и 50 depot не масштабируется как плоский граф: слишком много рёбер, слишком большая модель. Иерархия решает это через **вложенную группировку** сущностей, позволяя агрегировать данные до нужного уровня, декомпозировать задачу и строить reporting с drill-down.

### 7.2. Три иерархии

Платформа поддерживает три ортогональные иерархии — по одной на каждую основную ось модели:

**Facility Hierarchy** — географическая/организационная группировка facility. Country → Region → City → District. Или Division → Business Unit → Depot. Один facility может принадлежать нескольким иерархиям разных типов одновременно.

**Commodity Hierarchy** — группировка commodity_category. В велошеринге: Bike → по состоянию (WORKING_BIKE / BROKEN_BIKE) или по продуктовой линейке. Нужна для агрегации demand/supply на разных уровнях — например планирование на уровне «все велосипеды» без разделения на исправные и неисправные.

**Temporal Hierarchy** — multi-resolution planning (§3.2). Реализована через сегменты PlanningHorizon с разной гранулярностью, а не через отдельное дерево. Day → Week → Month выражается через segment_index.

### 7.3. Структура иерархии

Все иерархии (facility, commodity) следуют одному **паттерну**: тип иерархии → уровни → узлы (дерево) → привязка leaf-сущностей. Temporal hierarchy реализована через segments (§3.2), а не через этот паттерн.

```
hierarchy_type     →  "geographic", "organizational", "product_group"
hierarchy_level    →  level_index + level_name ("country", "region", ...)
hierarchy_node     →  дерево с parent_node_id
membership         →  привязка leaf entity к node
```

Внутри одной иерархии каждая сущность принадлежит ровно одному узлу (листовому уровню). Между иерархиями разных типов сущность может принадлежать разным узлам.

### 7.4. Агрегация — как используется

**Pre-solve агрегация** — для стратегического планирования: все facility в регионе схлопываются в "super-node". Demand суммируется, capacity суммируется, внутрирегиональные рёбра убираются, остаются межрегиональные. Аналогично для commodity — demand по parent category.

**Post-solve disaggregation** — после решения на агрегированном уровне результат распределяется обратно на отдельные facility/commodity. Межрегиональные потоки → конкретные маршруты.

**Decomposition** — master problem на верхнем уровне иерархии, sub-problems внутри каждого региона. Стандартный подход в large-scale задачах (Benders, Dantzig-Wolfe).

**Reporting** — group-by на любом уровне: суммарный demand по регионам, загрузка depot по бизнес-юнитам, потребление по product group.

### 7.5. Агрегация в Build Pipeline

Агрегация — **опциональный шаг**. Для малых сетей (< 1000 узлов) потребитель работает с полной моделью. Для больших — уровень агрегации задаётся в конфигурации сценария.

### 7.6. Доменные примеры иерархий

**Facility:**


| Домен      | Type           | Levels                        |
| ---------- | -------------- | ----------------------------- |
| Велошеринг | geographic     | City → District → Station zone |
| Велошеринг | organizational | Operator → Region → Depot     |
| Финансы    | organizational | Country → Bank → Branch       |


**Commodity:**


| Домен      | Type           | Levels                                         |
| ---------- | -------------- | ---------------------------------------------- |
| Велошеринг | condition      | Bike → WORKING_BIKE / BROKEN_BIKE              |
| Велошеринг | product_line   | Fleet → e-bike / classic (пример)            |
| Финансы    | currency_class | Currency → Major (USD, EUR) → Minor (CZK, PLN) |


**Temporal (multi-resolution segments):**


| Домен                    | Segments                                    |
| ------------------------ | ------------------------------------------- |
| Велошеринг (operational) | 7 days DAY + 3 weeks WEEK                   |
| Велошеринг (tactical)    | 2 weeks DAY + 8 weeks WEEK + 3 months MONTH |
| Финансы (strategic)      | 1 month WEEK + 11 months MONTH              |


---

## 8. Вложенная архитектура (L1 → L2 → L3)

### L1 — абстрактный граф

Чисто графовые примитивы: Node, Edge. Никакой доменной логики.

### L2 — домен-агностичная модель

CommodityCategory, ResourceCategory, Facility (с ролями и операциями). Edge Rules с commodity filtering и multi-modal edges. Временная ось (PlanningHorizon с multi-resolution segments). Transformation как N→M механизм конверсии между commodity categories. Resource fleet, resource-commodity и resource-modal compatibility. Multi-commodity flow данные с capacity_consumption для mixed units. Discrete parameters (nullable = LP-compatible). Facility и Commodity hierarchies для агрегации и decomposition. Solution/Simulation output tables и Historical layer с shared grain. Переиспользуется между доменами и между потребителями (optimizer, simulator, analytics — §15).

### L3 — домен-специфичная модель

Конкретные типы: Station, Depot, Maintenance Hub, REBALANCING_TRUCK. Конкретные операции: REPAIR, DISPATCH. Конкретные costs/capacities со своими grain'ами. Конкретные commodity instances (велосипед #4521). Конкретные resource instances (грузовик ребалансировки #REB-07 с GPS). Реализуется через наследование или композицию от L2.

---

## 9. Attribute System

### 9.1. Проблема гранулярности

Разные атрибуты живут на разных уровнях гранулярности. Attribute System работает одинаково для всех трёх типов сущностей — Facility, Edge, Resource — разница только в entity_grain.

**Facility attributes** (entity_grain = `["facility_id"]`):


| Атрибут          | Grain (raw)                                              | Resolved Grain                                                | Kind       |
| ---------------- | -------------------------------------------------------- | ------------------------------------------------------------- | ---------- |
| facility_type    | facility_id                                              | facility_id                                                   | ADDITIONAL |
| storage_capacity | facility_id × commodity_category                         | facility_id × commodity_category                              | CAPACITY   |
| throughput_rate  | facility_id × commodity_category × date                  | facility_id × commodity_category × period_id                  | RATE       |
| handling_cost    | facility_id × operation_type × date                      | facility_id × operation_type × period_id                      | COST       |
| repair_cost      | facility_id × operation_type × commodity_category × date | facility_id × operation_type × commodity_category × period_id | COST       |


**Edge attributes** (entity_grain = `["source_id", "target_id", "modal_type"]`):


| Атрибут         | Grain (raw)                     | Resolved Grain                       | Kind       |
| --------------- | ------------------------------- | ------------------------------------ | ---------- |
| distance        | edge                            | edge                                 | ADDITIONAL |
| lead_time_hours | edge                            | edge                                 | ADDITIONAL |
| transport_costs | edge × resource_category × date | edge × resource_category × period_id | COST       |
| reliability     | edge                            | edge                                 | ADDITIONAL |


**Resource attributes** (entity_grain = `["resource_category"]`):


| Атрибут               | Grain (raw)              | Resolved Grain                | Kind     |
| --------------------- | ------------------------ | ----------------------------- | -------- |
| base_capacity         | resource_category        | resource_category             | CAPACITY |
| fixed_cost_per_period | resource_category × date | resource_category × period_id | COST     |
| depreciation_rate     | resource_category        | resource_category             | RATE     |
| maintenance_cost      | resource_category × date | resource_category × period_id | COST     |


Resource attributes — полностью кастомные. Пользователь определяет сколько угодно cost/rate/capacity атрибутов для ресурсов. В велошеринге — fuel + maintenance + driver_time для REBALANCING_TRUCK. В финансовом — license_fee + compliance_cost. Модель не навязывает, какие конкретно costs должны быть. Для location-dependent resource costs — grain расширяется: `resource_category × facility_id × date`.

### 9.2. AttributeKind — семантическая классификация

Все числовые атрибуты — структурно одно и то же (значение на определённом grain). Разница семантическая:

```python
class AttributeKind(Enum):
    COST = "cost"          # стоимости (≥ 0)
    REVENUE = "revenue"    # выручка (≥ 0)
    RATE = "rate"          # нормы/скорости (≥ 0)
    CAPACITY = "capacity"  # ёмкости (> 0)
    ADDITIONAL = "additional"  # прочее (без ограничений)
```

Kind определяет валидационные правила и семантику использования: COST — расходы, REVENUE — доходы, CAPACITY — ограничения ёмкости, RATE — нормы throughput. Потребитель модели (оптимизатор, аналитика) использует kind для автоматического маппинга: все атрибуты с `kind=COST` собираются в cost part, с `kind=REVENUE` — в revenue part. Не нужно перечислять конкретные costs — любой кастомный атрибут участвует через свой kind.

### 9.3. AttributeSpec — единое описание атрибута

```python
class AttributeSpec:
    name: str                  # "handling_cost"
    kind: AttributeKind        # COST
    grain: list[str]           # ["facility_id", "operation_type", "date"] — raw grain
    resolved_grain: list[str]  # ["facility_id", "operation_type", "period_id"] — после resolution
    value_column: str          # "cost_per_unit"
    unit: str | None           # "€/ton"
    time_varying: bool         # True если "date" в grain
    aggregation: str           # "mean", "sum", "max" — как агрегировать при time resolution
    nullable: bool             # может ли отсутствовать
```

`grain` — сырой grain с `date`. `resolved_grain` — grain после time resolution, где `date` заменён на `period_id`. Для non-time-varying атрибутов `grain == resolved_grain`.

`aggregation` определяет, как агрегировать значения при resolution: costs обычно агрегируются по `mean` (средняя за период), demand/supply — по `sum` (суммарный за период), capacities — по `min` (bottleneck за период).

Этот механизм переиспользуется для всех трёх типов сущностей без изменений — разница только в entity_grain: `["facility_id"]` для узлов, `["source_id", "target_id", "modal_type"]` для рёбер, `["resource_category"]` для ресурсов. Пользователь может регистрировать произвольное количество кастомных атрибутов любого kind на любую сущность.

### 9.4. AttributeBuilder — сборка spine

Builder берёт список AttributeSpec и автоматически строит spine таблицу:

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

### 9.5. Commodity Pricing и Procurement

Ценообразование commodity — особый случай, который не полностью покрывается скалярным AttributeSpec. Модель поддерживает **два уровня**:

**Flat pricing (через AttributeSpec)** — простая цена per unit. Регистрируется как обычный атрибут с `kind=REVENUE` (для продажи на SINK) или `kind=COST` (для закупки на SOURCE):

```python
# Flat commodity price — скаляр, через Attribute System
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

Подходит для случаев с фиксированной ценой. Автоматически участвует через `kind=REVENUE`.

**Tiered pricing (отдельный механизм)** — piecewise-linear функция от объёма. Покрывает volume discounts, контрактные условия, минимальные charges. Не ложится в скалярный AttributeSpec, поэтому реализуется через отдельные таблицы:

```python
class PriceTier:
    facility_id: str              # SINK или SOURCE facility
    commodity_category: str
    date: date                    # time-varying
    tier_index: int               # 0, 1, 2 (порядок)
    min_volume: float             # нижняя граница (inclusive)
    max_volume: float | None      # верхняя граница (None = безлимит)
    price_per_unit: float         # цена в этом диапазоне
```

Примеры tiered pricing:


| Сценарий                 | Tier 0           | Tier 1       | Tier 2    |
| ------------------------ | ---------------- | ------------ | --------- |
| Corporate pass (велошеринг) | 0–500 поездок: €0.10/поездка | 500–2000: €0.08 | 2000+: €0.05 |
| Flat (велошеринг, B2C)      | 0+: €0.50/hr                 | —               | —            |
| Minimum charge (финансы) | 0–1000€: €5 flat | 1000+: 0.1%  | —         |


Когда tiered pricing задан для facility × commodity, он **override'ит** flat AttributeSpec для этой пары. Flat pricing — default, tiers — override.

---

## 10. Grain Groups — решение проблемы cross join

### 10.1. Проблема

Когда грейны содержат независимые измерения (operation_type и commodity_category), мёрдж в один spine приводит к cross join — таблица взрывается, значения дублируются.

### 10.2. Решение: несколько spine по grain groups

Атрибуты, чьи грейны образуют цепочку (один вкладывается в другой), попадают в одну группу. Группы между собой не мёрджатся.

```python
class GrainGroup:
    name: str
    grain: list[str]
    attributes: list[AttributeSpec]
```

Пример группировки:


| Группа | Grain                                        | Атрибуты                          |
| ------ | -------------------------------------------- | --------------------------------- |
| A      | facility_id × commodity_category × period_id | storage_capacity, dock_throughput |
| B      | facility_id × operation_type × period_id     | handling_cost_base, handling_cost |
| C      | facility_id                                  | facility_type                     |


### 10.3. Автоматическая группировка

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

### 10.4. Автоматический merge order

Внутри каждой группы алгоритм определяет оптимальный порядок мёрджей:

1. Начинаем с entity_grain
2. На каждом шаге ищем атрибуты, чей grain уже покрыт текущим spine grain — это «бесплатные» мёрджи
3. Если бесплатных нет — выбираем атрибут с минимальным expansion cost (наименьшее количество новых измерений)
4. Повторяем до исчерпания

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

## 11. Табличная архитектура данных

### 11.1. Entity tables — описание объектов

```
facility
├── facility_id (PK)
├── facility_type
├── name
├── lat, lon

resource_category
├── resource_category_id (PK)
├── name                        # "rebalancing_truck", "swift", …
├── base_capacity: float        # базовая грузоподъёмность единицы
├── capacity_unit: str          # "ton", "unit", "transaction"
├── description

resource                        # L2/L3, опциональный (для individual tracking)
├── resource_id (PK)
├── resource_category (FK)
├── home_facility_id (FK → facility)  # к какому узлу приписан
├── capacity_override: float | null   # null = используем base_capacity
├── description

commodity_category
├── commodity_category_id (PK)
├── name                        # "WORKING_BIKE", "BROKEN_BIKE", …
├── unit: str                   # "bike", "trip", "USD", …
├── description

commodity                       # L3, опциональный
├── commodity_id (PK)
├── commodity_category (FK)
├── description
```

### 11.2. Temporal tables — временная ось

```
planning_horizon
├── planning_horizon_id (PK)
├── name                       # "Q1-2025-tactical", "H1-2025-strategic"
├── start_date: date
├── end_date: date

planning_horizon_segment
├── planning_horizon_id (FK)
├── segment_index: int (PK)    # 0, 1, 2 — порядок сегментов
├── start_date: date
├── end_date: date
├── period_type: PeriodType    # DAY, WEEK, MONTH

period
├── period_id (PK)
├── planning_horizon_id (FK → planning_horizon)
├── segment_index: int         # к какому сегменту принадлежит
├── period_index: int          # глобальный порядок (0, 1, 2, ... по всем сегментам)
├── period_type: PeriodType    # унаследован от сегмента
├── start_date: date
├── end_date: date             # exclusive upper bound
```

`planning_horizon` — **независимая сущность** со своим PK. Сценарий ссылается на неё через FK (`scenario.planning_horizon_id`). Один горизонт можно переиспользовать в нескольких сценариях.

`planning_horizon_segment` разбивает горизонт на сегменты с разной гранулярностью (§3.2). Для простых случаев — один сегмент. `period` генерируется из сегментов; `period_index` — глобальный и непрерывный для carry-over; `period_type` — унаследован от сегмента, используется при lead_time resolution.

### 11.3. Behavior tables — что объекты умеют

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
├── date                        # raw, резолвится в period_id
├── available: bool             # работает ли в этот день
├── capacity_factor: float | null  # 1.0 = полная мощность, 0.5 = половина (null = 1.0)

edge_rule
├── source_type
├── target_type
├── commodity_category (nullable = все категории)
├── modal_type (nullable = все модальности)
├── enabled: bool
```

`facility_availability` — operating time windows. Позволяет моделировать выходные, праздники, сезонность. При resolution в период агрегируется: средний `capacity_factor` за период (или доля доступных дней). По умолчанию (если записи нет) facility доступна на 100%.

### 11.4. Transformation tables

```
transformation
├── transformation_id (PK)
├── facility_id (FK)
├── operation_type               # REPAIR, CONVERSION, BLENDING, DISTILLATION, …
├── loss_rate: float             # 0.0–1.0, общие потери процесса
├── batch_size: float | null     # null = непрерывный (LP-compatible)
├── batch_size_unit: str | null

transformation_input
├── transformation_id (FK)
├── commodity_category (FK → commodity_category)
├── ratio: float                 # единиц входа на 1 цикл

transformation_output
├── transformation_id (FK)
├── commodity_category (FK → commodity_category)
├── ratio: float                 # единиц выхода на 1 цикл
```

Grain: `transformation_id`. Один facility может иметь несколько трансформаций. Каждая трансформация может иметь N входов и M выходов (blending: N→1, splitting: 1→M, конверсия: 1→1, co-production: N→M). Для простого 1→1 случая: одна запись в `transformation_input`, одна в `transformation_output` — никакого overhead.

### 11.5. Resource tables

```
resource_commodity_compatibility
├── resource_category (FK)
├── commodity_category (FK)
├── enabled: bool
```

Определяет, какие commodity_categories может перевозить данный resource_category. При сборке модели проверяется, что для каждого ребра × commodity существует совместимый resource.

```
resource_modal_compatibility
├── resource_category (FK)
├── modal_type: str
├── enabled: bool
```

Определяет, на каких типах рёбер (modal_type) работает данный resource_category. Например: REBALANCING_TRUCK → ROAD, SWIFT → DIGITAL. Без этой таблицы невозможно определить, какие ресурсы обслуживают какие рёбра, и невозможно корректно вычислить fleet capacity.

```
resource_fleet
├── facility_id (FK)            # home base
├── resource_category (FK)
├── count: int                  # количество единиц данной category
```

Агрегированный view: сколько ресурсов каждой category приписано к facility. **Effective capacity** вычисляется при сборке модели (§13.6): если используются individual resources (L3) с `capacity_override`, capacity суммируется с учётом override'ов; иначе — `count × resource_category.base_capacity`. В таблице хранится только `count`, capacity — computed.

Определяет **facility-level outgoing capacity** — суммарная пропускная способность из facility по рёбрам, обслуживаемым данной resource_category.

```
resource_availability            # L2/L3, опциональный
├── resource_id (FK)
├── date                        # raw, резолвится в period_id
├── available: bool             # доступен ли в этот день
├── available_capacity: float | null  # если частично доступен
```

Per-resource availability windows. При resolution в период агрегируется (доля доступных дней или средняя available_capacity). Нужен только когда используются individual resources.

### 11.6. Edge tables

```
edge
├── source_id (FK → facility)    ┐
├── target_id (FK → facility)    ├── PK
├── modal_type: str              ┘   # "road", "rail", "sea", "pipeline", "digital"
├── distance: float
├── distance_unit: str
├── lead_time_hours: float       # raw, резолвится в edge_lead_time_resolved при сборке
├── reliability: float | null    # 0.0–1.0, nullable

edge_commodity
├── source_id (FK)               ┐
├── target_id (FK)               ├── FK → edge
├── modal_type                   ┘
├── commodity_category (FK)      # допустимый commodity на этом ребре
├── enabled: bool
├── capacity_consumption: float  # единиц shared capacity на 1 единицу commodity (default 1.0)

edge_capacity
├── source_id (FK)               ┐
├── target_id (FK)               ├── FK → edge
├── modal_type                   ┘
├── date                         # raw, резолвится в period_id
├── capacity: float              # shared capacity (все commodity делят)
├── capacity_unit: str

edge_commodity_capacity
├── source_id (FK)               ┐
├── target_id (FK)               ├── FK → edge
├── modal_type                   ┘
├── commodity_category (FK)
├── date                         # raw, резолвится в period_id (time-varying)
├── min_shipment: float | null   # null = нет минимума (LP-compatible)
├── max_shipment: float          # макс. объём за период для этого commodity
├── shipment_unit: str

edge_vehicle
├── source_id (FK)               ┐
├── target_id (FK)               ├── FK → edge
├── modal_type                   ┘
├── resource_category (FK → resource_category)
├── vehicle_capacity: float      # грузоподъёмность одного рейса
├── vehicle_capacity_unit: str
├── max_vehicles_per_period: int | null  # макс. рейсов за период (null = без лимита)

edge_lead_time_resolved          # GENERATED при сборке (§13.4), не вводится вручную
├── source_id (FK)               ┐
├── target_id (FK)               ├── FK → edge
├── modal_type                   ┘
├── period_id (FK)               # период отправления
├── lead_time_periods: int       # сколько периодов до прибытия
├── arrival_period_id (FK, nullable)  # null = выходит за горизонт
```

`edge` — PK = `source_id × target_id × modal_type`. Между двумя facility может быть несколько рёбер (road и rail). `lead_time_hours` хранится в абсолютных единицах.

`edge_commodity` — фильтр допустимых commodity_categories на ребре. `capacity_consumption` решает проблему shared capacity при разных единицах измерения: если capacity ребра в **рейсах** или **тоннах**, а commodity — велосипеды в **штуках**, то коэффициент переводит штуки в единицы ёмкости ребра (например e-bike «тяжелее» для одного слота кузова). Default = 1.0 для однородных единиц.

`edge_capacity` — shared пропускная способность по всем commodity на ребре.

`edge_commodity_capacity` — per-commodity ограничения: `min_shipment` (nullable, LP-compatible) и `max_shipment`. **Time-varying** (с `date`): max_shipment может меняться сезонно.

`edge_vehicle` — дискретные рейсы. Grain: `edge × resource_category`. Опциональный слой для задач, где транспорт дискретен.

`edge_lead_time_resolved` — **generated** таблица, создаётся при сборке модели (§13.4). В uniform resolution (один period_type) `lead_time_periods` одинаков для всех периодов. В multi-resolution `lead_time_periods` зависит от периода отправления: 48 часов = 2 периода при DAY, 0 при WEEK. Grain: `edge × period_id`.

### 11.7. Demand, Supply, Initial Inventory, In-Transit

Явные time-varying данные. Все индексируются по `commodity_category`.

```
demand
├── facility_id (FK)            # facility с ролью SINK
├── commodity_category (FK)
├── date                        # raw, резолвится в period_id через sum
├── quantity: float
├── min_order_quantity: float | null  # null = нет минимума (LP-compatible)
├── quantity_unit: str

supply
├── facility_id (FK)            # facility с ролью SOURCE
├── commodity_category (FK)
├── date                        # raw, резолвится в period_id через sum
├── quantity: float
├── quantity_unit: str

inventory_initial
├── facility_id (FK)            # facility с ролью STORAGE
├── commodity_category (FK)
├── quantity: float             # запас на момент planning_horizon.start_date
├── quantity_unit: str

inventory_in_transit
├── source_id (FK → facility)    ┐
├── target_id (FK → facility)    ├── FK → edge
├── modal_type                   ┘
├── commodity_category (FK)
├── quantity: float             # сколько в пути
├── quantity_unit: str
├── departure_date: date        # когда отправлено
├── expected_arrival_date: date # когда ожидается прибытие
```

`inventory_in_transit` — граничное условие: commodity, находящиеся "в пути" на момент начала горизонта. Отправлены до `planning_horizon.start_date`, но ещё не прибыли. При сборке модели `expected_arrival_date` маппится на `period_id` — этот flow "появляется" на target facility в соответствующем периоде.

### 11.8. Parameter tables — числовые параметры с grain

Сырые данные хранятся с `date`. При сборке модели проходят через time resolution (§3.4) для маппинга на `period_id`.

```
operation_capacity
├── facility_id (FK)
├── operation_type
├── commodity_category (nullable = "все категории")
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

resource_costs                   # кастомные costs/rates для ресурсов (через Attribute System)
├── resource_category (FK)
├── facility_id (FK, nullable)   # null = одинаково везде
├── attribute_name: str          # "fixed_cost", "maintenance_cost", "insurance" — пользовательские
├── date (nullable)              # nullable = не time-varying
├── value: float
├── value_unit: str
```

`resource_costs` — generic таблица для произвольных resource attributes с `kind=COST` или `kind=RATE`. Каждый `attribute_name` — отдельный атрибут, зарегистрированный через AttributeSpec. Пользователь определяет сколько угодно кастомных costs.

**Design note: EAV trade-off.** `resource_costs` использует Entity-Attribute-Value паттерн (`attribute_name` как колонка), тогда как `operation_costs` и `transport_costs` — explicit columns. Это сознательный выбор: набор resource costs полностью domain-specific и непредсказуем (fuel, maintenance, insurance, license_fee, compliance_cost...), тогда как `operation_costs` и `transport_costs` имеют стабильную структуру. EAV trade-offs: (+) произвольное количество кастомных атрибутов без schema migration, (+) единый механизм регистрации через AttributeSpec; (−) сложнее валидация допустимых `attribute_name`, (−) нет database-level type safety, (−) pivot-операции при запросах. Валидация допустимых `attribute_name` обеспечивается через регистрацию в AttributeSpec — незарегистрированные атрибуты отклоняются на этапе Validation (§13.2).

### 11.9. Pricing tables — ценообразование commodity

**Flat pricing** реализуется через AttributeSpec с `kind=REVENUE` / `kind=COST` (§9.5). **Tiered pricing** — через отдельные таблицы:

```
commodity_sell_price_tier
├── facility_id (FK)            # facility с ролью SINK
├── commodity_category (FK)
├── date                        # raw, резолвится в period_id
├── tier_index: int             # 0, 1, 2 — порядок заполнения
├── min_volume: float           # нижняя граница (inclusive)
├── max_volume: float | null    # верхняя граница (null = безлимит)
├── price_per_unit: float
├── price_unit: str

commodity_procurement_cost_tier
├── facility_id (FK)            # facility с ролью SOURCE
├── commodity_category (FK)
├── date                        # raw, резолвится в period_id
├── tier_index: int
├── min_volume: float
├── max_volume: float | null
├── cost_per_unit: float
├── cost_unit: str
```

Для flat pricing (одна цена, без volume discounts) — один tier с `min_volume=0`, `max_volume=null`. Или проще: использовать AttributeSpec с `kind=REVENUE` без tier таблиц. Tiered таблицы override'ят flat AttributeSpec для тех facility × commodity, где они заданы.

### 11.10. Hierarchy tables

Facility и commodity hierarchies следуют одному паттерну (§7.3). Отдельные наборы таблиц для чётких FK constraints.

**Facility hierarchy:**

```
facility_hierarchy_type
├── hierarchy_type_id (PK)
├── name                           # "geographic", "organizational"
├── description

facility_hierarchy_level
├── hierarchy_type_id (FK)
├── level_index: int               # 0 = корень
├── level_name: str                # "country", "region", "city"

facility_hierarchy_node
├── node_id (PK)
├── hierarchy_type_id (FK)
├── level_index: int
├── parent_node_id (FK → facility_hierarchy_node, nullable)  # null = корень
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
├── name: str                      # "Bike", "WORKING_BIKE", …

commodity_hierarchy_membership
├── commodity_category_id (FK)
├── hierarchy_type_id (FK)
├── node_id (FK → commodity_hierarchy_node)
```

Внутри одной иерархии каждая сущность принадлежит ровно одному узлу. Между иерархиями разных типов — разным узлам.

### 11.11. Scenario tables — конфигурация запуска

```
scenario
├── scenario_id (PK)
├── planning_horizon_id (FK → planning_horizon)  # ссылка на временну́ю сетку
├── name
├── description
├── facility_hierarchy_type: str | null     # null = без агрегации
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

Aggregation parameters в `scenario` задают намерение агрегации для потребителей (§7.4, §13.8). `planning_horizon_id` — FK на planning_horizon, позволяет переиспользовать один горизонт между сценариями.

### 11.12. Output tables — выходные данные потребителей

Определяют grain выходных данных потребителей (optimizer, simulator, etc.). Эти таблицы **генерируются** потребителем, не вводятся вручную. Row-схемы: `gbp/core/schemas/output.py`; в **`ResolvedModelData` они не входят** и `build_model()` их не заполняет. Grain совпадает с planning model для прямого сравнения с Historical layer (§12). Подробнее о разных потребителях — §15.

#### 11.12.1. Solution tables (optimizer output)

```
solution_flow
├── scenario_id (FK)
├── source_id, target_id, modal_type (FK → edge)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # запланированный flow
├── quantity_unit: str

solution_inventory
├── scenario_id (FK)
├── facility_id (FK)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # запланированный inventory на конец периода
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
├── gap: float | null            # optimality gap для MILP
```

#### 11.12.2. Simulation tables (simulator output)

Симулятор шагает по периодам последовательно и ведёт **лог** решений и состояний. Лог-таблицы имеют тот же grain, что и solution tables, что позволяет сравнивать output оптимизатора и симулятора напрямую.

```
simulation_flow_log
├── scenario_id (FK)
├── source_id, target_id, modal_type (FK → edge)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # фактический flow за период в симуляции
├── quantity_unit: str

simulation_inventory_log
├── scenario_id (FK)
├── facility_id (FK)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # inventory на конец периода в симуляции
├── quantity_unit: str

simulation_resource_log
├── scenario_id (FK)
├── resource_id (FK, nullable)   # null для aggregate-level (только resource_category)
├── resource_category (FK)
├── period_id (FK)
├── facility_id (FK)             # где ресурс находится на конец периода
├── status: str                  # "idle", "in_transit", "loading", "unloading"
├── trips_completed: int         # сколько рейсов совершил за период

simulation_metadata
├── scenario_id (PK)
├── simulation_timestamp: datetime
├── total_periods: int
├── total_cost: float | null
├── unmet_demand_total: float
├── solver_type: str             # какой step-solver использовался ("vrp", "rebalance", "greedy")
```

`simulation_resource_log` — ключевое отличие от optimizer: симулятор отслеживает **позицию и статус каждого ресурса** по периодам. Optimizer не знает про позиции — для него ресурсы это capacity constraint на facility. Подробнее — §15.

---

## 12. Historical Data Layer

> **Note:** This layer is **not** part of `RawModelData` / `ResolvedModelData` in the current `gbp/core` package. It describes a target analytics / ETL shape aligned with the same keys as the planning model.

### 12.1. Назначение

Основная модель описывает, что **возможно** и что **планируется**. Historical data layer фиксирует, что **реально произошло**. Два слоя живут на одной и той же структуре (те же facility_id, edge PK, commodity_category), что позволяет напрямую сравнивать план с фактом.

### 12.2. L3: Raw Shipments — отдельные поездки

Сырые операционные данные о каждой конкретной доставке. Event-level grain, не planning grain:

```
shipment
├── shipment_id (PK)
├── source_id (FK → facility)
├── target_id (FK → facility)
├── modal_type
├── resource_id (FK → resource, nullable)
├── resource_category (FK)
├── commodity_category (FK)
├── commodity_id (FK, nullable)       # L3, если tracked
├── departure_datetime: datetime
├── arrival_datetime: datetime
├── quantity: float
├── quantity_unit: str
├── actual_cost: float | null
├── status: str                       # "completed", "delayed", "cancelled"
```

Из raw shipments вычисляются: фактический lead time (`arrival - departure`), фактическая загрузка ресурсов, паттерны отказов и задержек.

### 12.3. L2: Aggregated Historical — planning grain

Raw shipments агрегируются до planning grain через тот же time resolution pipeline (§3.4), что и параметрические данные. Результат — таблицы, **напрямую сравнимые** с solution tables (§11.12).

**Важно:** `period_id` привязан к конкретному `planning_horizon`. Исторические таблицы ниже — это **materialized views, генерируемые под конкретный planning_horizon** при сборке, а не persistent storage. Одни и те же raw shipments переагрегируются заново под каждый горизонт. Persistent storage — это `shipment` (L3).

```
historical_flow
├── planning_horizon_id (FK)
├── source_id, target_id, modal_type (FK → edge)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # фактический flow
├── quantity_unit: str

historical_inventory
├── planning_horizon_id (FK)
├── facility_id (FK)
├── commodity_category (FK)
├── period_id (FK)
├── quantity: float              # фактический inventory на конец периода
├── quantity_unit: str

historical_demand_fulfilled
├── planning_horizon_id (FK)
├── facility_id (FK)
├── commodity_category (FK)
├── period_id (FK)
├── demanded: float              # сколько требовалось
├── delivered: float             # сколько реально доставлено
├── fulfilment_rate: float       # delivered / demanded
```

### 12.4. Агрегация: Shipments → Historical

ETL-шаг, аналогичный `date → period_id` resolution:

```python
def aggregate_shipments_to_flows(
    shipments: DataFrame,
    periods: DataFrame,          # periods для конкретного planning_horizon
) -> DataFrame:
    """
    Агрегирует raw shipments в historical_flow.
    departure_datetime маппится на period через time resolution.
    quantity суммируется по [edge, commodity_category, period_id].
    """
    shipments_with_period = resolve_datetime_to_period(
        shipments, periods, datetime_col="departure_datetime"
    )
    return shipments_with_period.groupby(
        ["source_id", "target_id", "modal_type",
         "commodity_category", "period_id"]
    )["quantity"].sum().reset_index()
```

### 12.5. Использование

**Parameter estimation** — из исторических данных выводятся параметры модели: средний lead_time_hours (из `arrival - departure`), demand forecast (из historical_demand_fulfilled), transport cost estimation (из actual_cost), reliability (из доли completed shipments).

**Plan vs Fact comparison** — сравнение `solution_flow` (§11.12) vs `historical_flow` на том же grain (оба индексируются по `planning_horizon_id + edge + commodity_category + period_id`).

**Warm-starting** — исторический flow pattern как начальное приближение.

**What-if analysis** — прогон на историческом demand/supply с альтернативной конфигурацией, сравнение с фактом.

### 12.6. Связь с основной моделью

Historical layer **не меняет** основную модель. Связь — через shared keys:

```
Planning model (основная)         Historical layer
─────────────────────────         ─────────────────────────
facility, edge, commodity    ←──  shipment (L3, raw, persistent)
demand, supply, capacity          historical_flow (L2, materialized view)
solution tables (§11.12)     ↔   historical_inventory (L2, materialized view)
                                  historical_demand_fulfilled (L2, materialized view)
```

---

## 13. Build Pipeline — сборка модели

### 13.1. Общий flow

```
Raw data (с date, lead_time_hours)
    ↓
Validation — validate_raw_model(raw) (§13.2)
    ↓
Time Resolution — resolve_all_time_varying(raw, periods)
    ↓
Edge Building — raw.edges или build_edges(facilities, edge_rules, scenario_manual_edges, …)
    ↓
Lead Time Resolution — resolve_lead_times(edges_df, periods) → edge_lead_time_resolved
    ↓
Transformation Resolution — resolve_transformations(…)
    ↓
Fleet Capacity Computation — compute_fleet_capacity(…)
    ↓
Assemble ResolvedModelData + assemble_spines(resolved)
    ↓
Ready for consumption (optimizer, analytics, reporting)
```

Порядок шагов соответствует `gbp/build/pipeline.py::build_model`. Иерархическая агрегация (§7.4, бывш. шаг «после resolution» в старых схемах) **в этом pipeline не вызывается** — см. блок *Implementation status* в начале документа.

### 13.2. Шаг Validation

Проверяет целостность и согласованность входных данных **до** сборки. Основные проверки:

**Unit consistency** — в коде проверяются `demand`, `supply`, `inventory_initial`: `quantity_unit` vs `commodity_category.unit`. Единицами `edge_capacity` / `shipment_unit` в `gbp/build/validation.py` отдельно не занимаются.

**Referential integrity** — все `facility_id` в demand имеют роль SINK; все `facility_id` в supply имеют роль SOURCE; все `facility_id` в inventory_initial имеют роль STORAGE; все edge FK ссылаются на существующие facility.

**Resource completeness** — для каждого `edge × commodity` существует хотя бы один compatible resource (через `resource_commodity_compatibility` × `resource_modal_compatibility`).

**Temporal coverage** — demand/supply покрывают весь planning_horizon (предупреждение, если есть пробелы).

**Graph connectivity** — каждый SINK достижим из хотя бы одного SOURCE по рёбрам с совместимыми commodity (предупреждение, не ошибка).

**Transformation consistency** — для каждой трансформации input commodities доступны на incoming edges facility, output commodities — на outgoing edges (по таблице **`raw.edge_commodities`**, не по рёбрам, собранным позже в `build_model`).

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
    errors: list[ValidationError]  # и ошибки, и предупреждения; блокирует только level == "error"

    @property
    def is_valid(self) -> bool: ...
    def raise_if_invalid(self) -> None: ...
```

### 13.3. Шаг Time Resolution

Выполняется для всех time-varying атрибутов и для demand/supply:

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

### 13.4. Шаг Lead Time Resolution

Конвертирует абсолютные `lead_time_hours` в resolved таблицу `edge_lead_time_resolved` (§11.6). Grain результата: `edge × period_id`.

```python
def resolve_lead_times(
    edges: DataFrame,          # с lead_time_hours
    periods: DataFrame,        # с start_date, end_date, period_type, period_index
) -> DataFrame:
    """
    Возвращает edge_lead_time_resolved: edge × period_id → lead_time_periods.
    
    Для uniform resolution (один segment): lead_time_periods одинаков
    для всех периодов = ceil(hours / period_duration).
    
    Для multi-resolution: для каждого ребра и периода t вычисляем,
    на сколько периодов сдвигается прибытие.
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

    # Вычисляем arrival_period_id
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
        how="left",  # null = выходит за горизонт
    )
    return resolved
```

### 13.5. Шаг Transformation Resolution

Определяет для каждого facility, какие commodity categories входят и какие выходят, с учётом N→M трансформаций:

```python
def resolve_transformations(
    facilities: DataFrame,
    transformations: DataFrame,
    transformation_inputs: DataFrame,
    transformation_outputs: DataFrame,
) -> DataFrame:
    """
    Возвращает таблицу facility_id → transformation details
    с развёрнутыми inputs/outputs.
    """
    t = transformations.merge(transformation_inputs, on="transformation_id")
    t = t.merge(transformation_outputs, on="transformation_id",
                suffixes=("_in", "_out"))
    return facilities.merge(t, on="facility_id", how="left")
```

### 13.6. Шаг Fleet Capacity Computation

Вычисляет effective capacity для каждой записи в `resource_fleet`:

```python
def compute_fleet_capacity(
    resource_fleet: DataFrame,
    resource_categories: DataFrame,
    resources: DataFrame | None,     # L3, если есть
) -> DataFrame:
    """
    Если individual resources (L3) доступны:
        capacity = sum(capacity_override ?? base_capacity) для ресурсов на facility.
    Иначе:
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

### 13.7. Шаг Spine Assembly

После resolution данные собираются в spine через AttributeBuilder и GrainGroups (§9–10), но теперь grain'ы используют `resolved_grain` (с `period_id` вместо `date`).

### 13.8. Hierarchical aggregation (scenario fields vs build)

Поля `facility_hierarchy_type`, `commodity_hierarchy_type` и уровни агрегации в таблице `scenario` описывают **конфигурацию** для потребителей (§7.4). В текущем **`gbp/build` отдельного шага агрегации графа нет** — `build_model()` не схлопывает узлы по иерархии. Это может делать оптимизатор, отдельный модуль или будущий код поверх `ResolvedModelData`.

---

## 14. Примеры доменных конфигураций

### 14.1. Велошеринг (основной домен проекта, `gbp/core`)


| FacilityType    | Roles                  | Operations                           | Transformation                            |
| --------------- | ---------------------- | ------------------------------------ | ----------------------------------------- |
| Station         | SOURCE, SINK, STORAGE  | RECEIVING, STORAGE, DISPATCH         | —                                         |
| Depot           | STORAGE, TRANSSHIPMENT | RECEIVING, STORAGE, DISPATCH         | —                                         |
| Maintenance Hub | TRANSSHIPMENT, STORAGE | RECEIVING, STORAGE, REPAIR, DISPATCH | BROKEN_BIKE → WORKING_BIKE (1:1, loss 5%) |


CommodityCategories: WORKING_BIKE, BROKEN_BIKE.

ResourceCategories: REBALANCING_TRUCK (compatible: WORKING_BIKE + BROKEN_BIKE, modal: ROAD).

### 14.2. Финансовые транзакции


| FacilityType    | Roles                 | Operations                   | Transformation                  |
| --------------- | --------------------- | ---------------------------- | ------------------------------- |
| Bank            | SOURCE, SINK, STORAGE | RECEIVING, STORAGE, DISPATCH | USD → EUR (по курсу, loss 0.1%) |
| Person          | SOURCE, SINK          | RECEIVING, DISPATCH          | —                               |
| Payment Gateway | TRANSSHIPMENT         | RECEIVING, DISPATCH          | —                               |


CommodityCategories: USD, EUR (и другие валюты).

ResourceCategories: SWIFT (compatible: USD+EUR, modal: DIGITAL), SEPA (compatible: EUR, modal: DIGITAL).

---

## 15. Модель потребления: Optimizer, Simulator, Analytics

### 15.1. Проблема

Одни и те же сущности (facility, commodity, resource, edge) участвуют в принципиально разных вычислительных моделях. Оператор велошеринга хочет: (a) найти оптимальный ночной план ребалансировки на горизонте (Network Flow), (b) просимулировать дневные поездки пользователей и ночные рейсы грузовиков с учётом позиций (Simulation + VRP / rebalancing), (c) сравнить план с фактом (Analytics). Все три задачи оперируют одними и теми же данными — но обрабатывают их по-разному.

### 15.2. Общая архитектура

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
│ - все периоды сразу       │ - шагает по периодам         │                   │
│ - ресурсы = capacity      │ - ресурсы имеют STATE        │ - читает output   │
│ - LP/MILP solver          │ - step-solver (VRP, greedy)  │ - сравнивает с    │
│ - output: solution_*      │ - output: simulation_*_log   │   historical      │
└───────────────────────────┴──────────────────────────────┴───────────────────┘
```

**Модель данных (L1 + L2) не меняется между потребителями.** Разница — в том, как потребитель интерпретирует и обрабатывает `ResolvedModelData`.

### 15.3. Consumer A: Optimizer (Network Flow)

Видит **все периоды одновременно**. Строит математическую программу (LP/MILP) и находит оптимальное распределение потоков по всему горизонту.

Ресурсы — это **capacity constraint** на facility: «из Depot A можно отгрузить не больше X велосипедов за период, потому что приписано N ребалансировочных грузовиков с известной вместимостью». Optimizer не знает, какой конкретно грузовик поедет и где он окажется после доставки.

Используемые данные из модели: demand, supply, edge capacities, costs, fleet capacity, transformation ratios. Output: `solution_flow`, `solution_inventory`, `solution_unmet_demand` (§11.12.1).

Подходит для: стратегическое планирование, оптимальное распределение потоков, what-if анализ сценариев.

### 15.4. Consumer B: Simulator

Шагает **по периодам последовательно**. На каждом шаге: смотрит текущее состояние → принимает решения → обновляет состояние → переходит к следующему периоду.

Ключевое отличие — **state**. Simulator поддерживает мутабельное состояние, которое переносится между шагами:

```python
@dataclass
class SimulationState:
    """Runtime state — не часть data model, а состояние потребителя."""

    period_index: int

    # Где каждый ресурс и что он делает
    resource_state: pd.DataFrame
    # resource_id | resource_category   | facility_id | status     | available_at_period
    # REB-042     | rebalancing_truck   | depot_A     | idle       | null
    # REB-043     | rebalancing_truck   | null        | in_transit | 5
    # REB-044     | rebalancing_truck   | station_12  | unloading  | 4

    # Сколько commodity на каждом facility прямо сейчас
    inventory: pd.DataFrame
    # facility_id | commodity_category | quantity

    # Что сейчас в пути
    in_transit: pd.DataFrame
    # resource_id | source_id | target_id | commodity_category | quantity | arrival_period
```

На каждом шаге simulator вызывает **step-solver** — стратегию принятия решений:

```python
def simulation_step(
    model: ResolvedModelData,   # L2 модель — НЕ меняется
    state: SimulationState,     # текущее состояние — меняется каждый шаг
    solver: StepSolver,         # стратегия решений (VRP, rebalancing, greedy)
) -> SimulationState:
    """
    1. Arrivals: что приехало в этом периоде → обновить inventory + resource_state
    2. Demand: что потребилось → уменьшить inventory
    3. Decide: solver решает, что делать (кого куда отправить)
    4. Dispatch: отправить ресурсы → обновить resource_state + in_transit
    5. Вернуть новое состояние
    """
```

**Step-solver** — сменный компонент. Для велошеринга: rebalancing algorithm (ночная перестановка велосипедов) или VRP по рейсам грузовиков. Для baseline: greedy (ближайший дефицит → первый избыток). Другие домены подключают свои стратегии (VRP, жадные эвристики и т.д.).

```python
class StepSolver(Protocol):
    def decide(
        self,
        model: ResolvedModelData,
        state: SimulationState,
    ) -> list[Dispatch]:
        """Возвращает список решений: какой ресурс, откуда, куда, сколько commodity."""
        ...

@dataclass
class Dispatch:
    resource_id: str | None      # конкретный ресурс (L3) или None для aggregate
    resource_category: str
    source_id: str               # facility откуда
    target_id: str               # facility куда
    modal_type: str
    commodity_category: str
    quantity: float
```

Output: `simulation_flow_log`, `simulation_inventory_log`, `simulation_resource_log` (§11.12.2).

**SimulationState — это runtime state потребителя, а не часть data model.** Так же как optimizer'у нужны binary variables и LP tableau в рантайме — это его внутреннее дело, не модель данных. Модель данных определяет только input (`ResolvedModelData`) и output (log tables в §11.12.2).

### 15.5. Resource tracking: три уровня

Разные задачи требуют разного уровня детализации ресурсов:

|Уровень|Resource model|Знает позицию?|Потребитель|Пример|
|---|---|---|---|---|
|Aggregate|`resource_fleet` (count × category)|Нет|Optimizer|"3 грузовика на Depot A, capacity = 60 велосипедов/ночь"|
|Round-trip aware|`resource_fleet` + `edge_vehicle`|Нет (implicit)|Optimizer с round-trip|"1 грузовик = 3 рейса/неделю (48ч round-trip)"|
|Instance-level|`resource` (L3) + `SimulationState`|Да|Simulator|"Грузовик #42 сейчас на station_12, вернётся через 4ч"|

**Aggregate** — текущая модель в optimizer-режиме. `resource_fleet.count × base_capacity` = facility capacity constraint. Ресурс мгновенно "возвращается".

**Round-trip aware** — промежуточный уровень. Ресурс не отслеживается поштучно, но `max_vehicles_per_period` в `edge_vehicle` вычисляется с учётом round-trip time:

```
effective_trips = floor(period_hours / (2 × lead_time_hours + handling_hours))
max_vehicles_per_period = effective_trips × vehicle_count
```

Это вычисляется при сборке модели или потребителем. Модель данных хранит `lead_time_hours` на ребре; отдельного поля `handling_hours` в схемах `gbp/core` может не быть — его задают как кастомный атрибут или выводят из `operation_costs` / потребителя. Потребитель сам считает effective trips.

**Instance-level** — simulator с `SimulationState.resource_state`. Каждый ресурс имеет позицию, статус, время до доступности. Грузовик ребалансировки, уехавший из Depot A на station_12, **недоступен** для Depot A до возвращения. Simulator отслеживает это через `resource_state`.

Все три уровня используют одну и ту же модель данных — разница в том, какие таблицы потребитель использует и как интерпретирует:

|Таблица|Aggregate|Round-trip|Instance|
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

Читает output от optimizer или simulator и сравнивает с historical data (§12). Не требует дополнительных таблиц — работает с `solution_*`, `simulation_*_log` и `historical_*` на одном и том же grain (`edge × commodity × period_id`).

### 15.7. Доменные примеры

**Велошеринг — горизонт 1 год:**

- Optimizer: оптимальный план ночной ребалансировки между станциями и депо по неделям/дням; сколько WORKING_BIKE/BROKEN_BIKE переместить по каждому ребру в каждом периоде.
- Simulator: днём — demand (пользователи берут/возвращают велосипеды, inventory на станциях меняется). Ночью — step-solver (rebalancing / VRP) перемещает велосипеды грузовиками REBALANCING_TRUCK; отдельно можно моделировать поток BROKEN_BIKE → Maintenance Hub → WORKING_BIKE. На следующий день — новый demand на обновлённых остатках.
- Analytics: сравнить optimizer plan vs simulation vs historical trips/inventory snapshots; service level (% станций с доступными велосипедами), недостача против плана, нехватка грузовиков.

---

## 16. Рекомендуемые диаграммы

Для документирования модели рекомендуется использовать **Property Graph Schema Diagram** — формат из мира графовых БД. Узлы как боксы с типом, ролями и ключевыми атрибутами. Рёбра как стрелки с типом, допустимыми commodity и атрибутами.

Дополнительно: UML Class Diagram для детализации атрибутов и иерархии классов.

Для разных уровней вложенности — отдельные диаграммы с разной детализацией:

- L1: абстрактный граф (Node, Edge)
- L2: логистическая модель (Facility с ролями, CommodityCategory, ResourceCategory, PlanningHorizon с segments, Transformation (N→M), Resource-Modal/Commodity compatibility, Solution tables, Facility/Commodity Hierarchies)
- L3: конкретный домен (Station, Depot, Maintenance Hub, REBALANCING_TRUCK, bike_id)

---

## Ссылки и литература

- Ford, Fulkerson — «Потоки в сетях» (перевод, изд. «Мир», 1966) — первоисточник терминологии source/sink/transshipment
- Кристофидес — «Теория графов. Алгоритмический подход» (перевод, изд. «Мир», 1978) — графы + потоки + алгоритмы размещения
- Williamson — «Network Flow Algorithms» (Cambridge, 2019) — современный учебник, бесплатный PDF на networkflowalgs.com
- Ahuja, Magnanti, Orlin — «Network Flows: Theory, Algorithms, and Applications» (1993) — главный справочник по network flows, включая multi-commodity flow (ch. 17)

