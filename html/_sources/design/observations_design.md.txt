# Design Doc: Observations — Historical Data in the Graph Model

> Status: READY FOR IMPLEMENTATION
> Расширение: Foundation (Phase 1)
> Зависит от: `graph_data_model.md`, `environment_design.md`
> Заменяет: `docs/temp_plan.md`

---

## 1. Проблема и почему она возникла

### 1.1. Что случилось

Trips (`df_trips`) и telemetry (`df_telemetry_ts`) живут только на объекте `source` (BikeShareSourceProtocol) и не являются частью модели данных (`RawModelData` / `ResolvedModelData`). Это зафиксировано явно в коде:

**`dataloader_graph.py` docstring:**
> "Extra wide tables from the source (telemetry, trips, hourly inventory matrix, etc.) are **not** part of that model; read them from `loader.source`"

**`notebooks/02_test_graph_loader.ipynb`, секция 4:**
> "Raw source only (not in `gbp.core` tables). Wide inventory, telemetry, and trips stay on `loader.source`."

Таким образом исключение trips/telemetry из модели — **не случайность, а осознанное решение**, принятое на этапе Foundation. Это решение было ошибочным.

### 1.2. Почему решение ошибочно

Фундаментальный принцип платформы: **все данные переводятся в единый domain-agnostic формат** (graph model). Любой код после loader'а работает только с `RawModelData` / `ResolvedModelData` и не знает ничего о природе исходных данных (велосипеды, газ, финансы). Trips и telemetry нарушают этот принцип — они остались в domain-specific формате (station_id, started_at, num_bikes_available) и доступны только через `loader.source`.

### 1.3. Почему это критично сейчас

Environment (Phase 2) готов и производит выходные данные:
- `simulation_flow_log` — все перемещения commodity за симуляцию
- `simulation_inventory_log` — снимки инвентаря по периодам

Исторические данные описывают **то же самое**: реальные перемещения commodity и реальные снимки инвентаря. Без единого формата сравнение simulation vs history невозможно.

### 1.4. Инструкция для реализации (Claude Code)

**Причина, по которой эта задача вызывала путаницу:** существующие документы и комментарии в коде явно говорят «trips и telemetry — не часть модели». Это создаёт впечатление, что так и задумано. Но это не так — это гэп, который нужно закрыть. При реализации:

1. **Не спрашивай** «а нужно ли это добавлять в модель?» — да, нужно.
2. **Обнови** docstring в `dataloader_graph.py` и комментарий в `02_test_graph_loader.ipynb` — убери утверждения что trips/telemetry «not part of the model».
3. **Все решения** по схемам, naming, time resolution описаны ниже — следуй им.

---

## 2. Архитектурное решение

### 2.1. Observations — входные исторические данные

Новая группа `observations` в `RawModelData` / `ResolvedModelData` содержит две таблицы:

| Таблица | Что описывает | Аналог в simulation output |
|---------|---------------|---------------------------|
| `observed_flow` | Наблюдённые перемещения commodity | `simulation_flow_log` |
| `observed_inventory` | Наблюдённые снимки инвентаря | `simulation_inventory_log` |

Это **входные данные** (наравне с demand, supply, inventory_initial). Simulation logs — **выходные данные**. Observations и simulation logs описывают одно и то же явление (потоки и инвентарь), но одни — исторические факты, другие — результат симуляции.

### 2.2. Naming: `observed_flow`, не `observed_trips`

`trips` — domain-specific термин (bike-sharing). В gas logistics это «поставки», в финансах — «транзакции». Domain-agnostic термин: **flow** (перемещение commodity по ребру). Это совпадает с naming в simulation output (`simulation_flow_log`) и в optimizer output (`solution_flow`).

Аналогично, `telemetry` → `observed_inventory` (domain-agnostic: наблюдённый уровень запасов).

### 2.3. Совместимость схем с simulation output

Ключевое требование: **observed_flow и simulation_flow_log должны быть структурно совместимы** для сравнения. Это значит одинаковые column names для общих полей.

**simulation_flow_log columns:**
```
period_index, period_id, phase_name, source_id, target_id,
commodity_category, modal_type, quantity, resource_id
```

**observed_flow columns (Raw, до time resolution):**
```
source_id, target_id, commodity_category, modal_type,
date, quantity, quantity_unit, resource_id
```

**observed_flow columns (Resolved, после time resolution):**
```
source_id, target_id, commodity_category, modal_type,
period_id, quantity, quantity_unit, resource_id
```

Разница с simulation_flow_log:
- Нет `phase_name` — у исторических данных нет фаз, весь flow однороден
- Нет `period_index` — добавляется тривиально через join с periods при сравнении
- Есть `quantity_unit` — для ссылочной целостности (как в demand/supply)
- `modal_type` nullable — для rider trips в bike-sharing modal_type не определён, для dispatch trips = "road"

Для сравнения: concat/join по `(source_id, target_id, commodity_category, period_id)`, добавив колонку `source` = "observed" / "simulated".

**simulation_inventory_log columns:**
```
period_index, period_id, facility_id, commodity_category, quantity
```

**observed_inventory columns (Raw):**
```
facility_id, commodity_category, date, quantity, quantity_unit
```

**observed_inventory columns (Resolved):**
```
facility_id, commodity_category, period_id, quantity, quantity_unit
```

Та же логика: один grain, одинаковые имена, тривиальный join для сравнения.

---

## 3. Schemas

### 3.1. Row schemas (`gbp/core/schemas/observations.py`, новый файл)

```python
class ObservedFlow(BaseModel):
    """Historical commodity movement between facilities (raw date)."""
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    target_id: str
    commodity_category: str
    date: dt.date
    quantity: float = Field(ge=0)
    quantity_unit: str
    modal_type: str | None = None
    resource_id: str | None = None

class ObservedInventory(BaseModel):
    """Historical inventory snapshot at a facility (raw date)."""
    model_config = ConfigDict(extra="forbid", frozen=True)

    facility_id: str
    commodity_category: str
    date: dt.date
    quantity: float = Field(ge=0)
    quantity_unit: str
```

### 3.2. Почему `source_id` / `target_id`, а не `origin_facility_id` / `destination_facility_id`

В модели данных edges используют `source_id` / `target_id`. Simulation flow_log использует `source_id` / `target_id`. Consistency требует тот же naming. `origin` / `destination` — альтернативные имена для того же самого, вносящие путаницу.

### 3.3. Почему `quantity`, а не `quantity_available`

`observed_inventory` описывает количество commodity на facility — это `quantity`, как в `inventory_initial`, `demand`, `supply`. Суффикс `_available` подразумевает domain-specific семантику (available vs disabled vs reserved), которая в domain-agnostic модели не нужна. Детали вроде `docks_available`, `num_bikes_disabled` — это domain-specific атрибуты, которые регистрируются через `AttributeRegistry`.

---

## 4. Место в модели данных

### 4.1. RawModelData

Два новых optional поля + новая группа:

```python
# fields
observed_flow: pd.DataFrame | None = None
observed_inventory: pd.DataFrame | None = None

# _GROUPS
"observations": ["observed_flow", "observed_inventory"]

# _SCHEMAS
"observed_flow": ObservedFlow,
"observed_inventory": ObservedInventory,

# NOT in _REQUIRED — observations are optional
```

Property для доступа:
```python
@property
def observation_tables(self) -> dict[str, pd.DataFrame]:
    """Observed historical data: flows and inventory snapshots."""
    return _collect_group(self, self._GROUPS["observations"])
```

### 4.2. ResolvedModelData

Те же два поля. После time resolution: `date` → `period_id`, аналогично demand/supply.

### 4.3. Time resolution

Observations — time-varying данные, как demand и supply. Они резолвятся в `build_model()` по стандартному механизму:

| Таблица | Grain (без date) | Value columns | Aggregation |
|---------|-------------------|---------------|-------------|
| `observed_flow` | `source_id, target_id, commodity_category` | `quantity` | `sum` |
| `observed_inventory` | `facility_id, commodity_category` | `quantity` | `mean` |

`sum` для flow — суммарный поток за период. `mean` для inventory — средний уровень за период (множество snapshot'ов за день → одно значение за period).

### 4.4. Validation

Ссылочная целостность (в `_check_observations`):
- `observed_flow.source_id` ∈ `facilities.facility_id`
- `observed_flow.target_id` ∈ `facilities.facility_id`
- `observed_flow.commodity_category` ∈ `commodity_categories.commodity_category`
- `observed_flow.resource_id` (если не null) ∈ `resources.resource_id` (если L3 resources есть)
- `observed_inventory.facility_id` ∈ `facilities.facility_id`
- `observed_inventory.commodity_category` ∈ `commodity_categories.commodity_category`

Вызывается из `validate_raw_model()`. Проверка выполняется только если таблица не None.

---

## 5. Demand derivation — отдельная задача loader'а

### 5.1. Принцип

`demand` — самостоятельная таблица в модели. Она может быть заполнена из разных источников:
- **Из observed_flow** — loader считает departures per facility per date
- **Рандомно** — если исторических данных нет
- **Из ML-модели** — в будущем
- **Вручную** — пользователь задаёт demand напрямую

Derivation demand из observed_flow — это **логика loader'а**, а не build pipeline. `build_model()` получает готовый `demand` и не знает, откуда он взялся.

### 5.2. Реализация в loader'е

В `DataLoaderGraph._build_demand_from_observations()`:

```
observed_flow
  → filter: resource_id is null (rider trips, not rebalancing dispatches)
  → groupby (source_id, date, commodity_category) → sum quantity
  → rename source_id → facility_id
  → add quantity_unit
  → return as demand DataFrame
```

Если `observed_flow` is None или пуст — loader не создаёт demand (или использует fallback). Это решение loader'а, не модели.

---

## 6. Loader: mapping domain data → observations

### 6.1. Bike-sharing: trips → observed_flow

```
df_trips (source)              →  observed_flow (model)
─────────────────                  ──────────────────
start_station_id               →  source_id
end_station_id                 →  target_id
"working_bike"                 →  commodity_category
started_at.date                →  date
1.0 (per trip row)             →  quantity
"bike"                         →  quantity_unit
None                           →  modal_type (rider trip)
None                           →  resource_id (rider trip)
```

Фильтрация: только `source_id` и `target_id` ∈ `facilities.facility_id`. Unknown station_ids → warning в лог.

Агрегация в loader'е: groupby `(source_id, target_id, commodity_category, date)` → sum quantity. Одна строка в observed_flow = суммарный flow за дату по паре facility, не отдельная поездка.

### 6.2. Bike-sharing: telemetry → observed_inventory

```
df_telemetry_ts (source)       →  observed_inventory (model)
────────────────────             ────────────────────────
station_id                     →  facility_id
"working_bike"                 →  commodity_category
timestamp.date                 →  date
num_bikes_available            →  quantity
"bike"                         →  quantity_unit
```

Фильтрация: только `facility_id` ∈ `facilities.facility_id`.

Агрегация в loader'е: groupby `(facility_id, commodity_category, date)` → mean quantity. Множество telemetry snapshot'ов за день → одно среднее значение.

Domain-specific атрибуты (docks_available, num_ebikes_available, is_renting) могут регистрироваться в `AttributeRegistry` с grain `("facility_id", "commodity_category", "date")`, но это опционально и не входит в MVP.

### 6.3. Gas logistics (будущее, для иллюстрации)

```
delivery_records               →  observed_flow
─────────────────                  ──────────────────
origin_depot                   →  source_id
destination_client             →  target_id
"lpg_cylinder"                 →  commodity_category
delivery_date                  →  date
cylinders_delivered            →  quantity
"cylinder"                     →  quantity_unit
"road"                         →  modal_type
truck_id                       →  resource_id
```

Тот же формат. Loader другой, модель та же.

---

## 7. Изменения в существующих файлах

### 7.1. Убрать утверждения об исключении trips/telemetry

**`gbp/loaders/dataloader_graph.py` docstring** — убрать:
> "Extra wide tables from the source (telemetry, trips, hourly inventory matrix, etc.) are **not** part of that model"

Заменить на:
> "Trips and telemetry from the source are mapped to `observed_flow` and `observed_inventory` tables in the model. Other wide tables (hourly inventory matrix) remain on `loader.source`."

**`notebooks/02_test_graph_loader.ipynb` секция 4** — обновить заголовок и описание.

### 7.2. Source validation

Новая Pandera schema в `contracts.py`:

```python
class TripsSourceSchema(pa.DataFrameModel):
    started_at: Series[pd.Timestamp]
    start_station_id: Series[str] = pa.Field(str_length={"min_value": 1})
    end_station_id: Series[str] = pa.Field(str_length={"min_value": 1})
    class Config:
        strict = False
        coerce = True
```

---

## 8. Implementation Plan

Порядок определяется зависимостями. Каждый step — отдельный коммит. `pytest` после каждого.

### Step 1: Schemas

**Файлы:** `gbp/core/schemas/observations.py` (новый), `gbp/core/schemas/__init__.py`

**Что:** `ObservedFlow`, `ObservedInventory` Pydantic models. Export в `__init__.py` + `__all__`.

**Зависимости:** нет.

### Step 2: Model fields

**Файлы:** `gbp/core/model.py`

**Что:**
- `RawModelData`: два поля, `_GROUPS["observations"]`, `_SCHEMAS`, property `observation_tables`
- `ResolvedModelData`: два поля, `_GROUPS`, `from_raw()` коалесцинг

**Verification:** существующие тесты `test_raw_groups_cover_all_fields` и `test_resolved_groups_cover_all_dataframe_fields` должны пройти (они проверяют что `_GROUPS` покрывают все поля).

**Зависимости:** Step 1.

### Step 3: Time resolution

**Файлы:** `gbp/build/time_resolution.py`

**Что:** добавить две спецификации в `resolve_all_time_varying()` для observed_flow и observed_inventory. Обработка None (skip если None).

**Зависимости:** Step 2.

### Step 4: Validation

**Файлы:** `gbp/build/validation.py`

**Что:** `_check_observations(raw, result)` — FK checks для обеих таблиц. Вызов из `validate_raw_model()`. Skip если None.

**Зависимости:** Step 2.

### Step 5: Loader

**Файлы:** `gbp/loaders/dataloader_graph.py`, `gbp/loaders/contracts.py`

**Что:**
- `TripsSourceSchema` в contracts.py
- `_build_observations(entities)` — mapping trips → observed_flow, telemetry → observed_inventory
- `_build_demand_from_observations(observed_flow)` — derivation demand из flow
- Обновить `_build_raw_model()` — включить observations и derived demand
- Обновить docstring

**Зависимости:** Steps 1–4.

### Step 6: Tests

**Файлы:** `tests/test_graph_loader.py`, возможно `tests/unit/core/test_model.py`

**Что:**
- `TestObservations`: observed_flow populated, columns match schema, FK valid, commodity valid
- `TestObservedInventory`: observed_inventory populated, FK valid
- `TestDemandFromObservations`: demand derived, quantities consistent
- `TestObservationsResolved`: resolved таблицы имеют period_id

**Зависимости:** Step 5.

### Step 7: Cleanup + Notebook

**Файлы:** `notebooks/02_test_graph_loader.ipynb`, опционально `notebooks/verify/04_observations.ipynb`

**Что:**
- Обновить секцию 4 в notebook — показать observed_flow и observed_inventory как часть модели
- Verification notebook: load → check observations → check demand derivation → check FK integrity

**Зависимости:** Step 5.

---

## 9. Что НЕ входит в эту задачу

- **AttributeRegistry для domain-specific полей** (trip duration, docks_available) — может быть добавлено позже, не блокирует основную функциональность.
- **Trip Generator** (Phase 4 roadmap) — генерация синтетических trips для симуляции. Отдельная фаза, использует observed_flow формат.
- **Analytics marts** — сравнение observed vs simulated. Отдельный consumer, использует совместимые схемы.
- **I/O (Parquet/JSON)** — сериализация observed таблиц. Должна работать автоматически через существующий механизм `to_dict()` / `to_parquet()`, так как observed_flow и observed_inventory — обычные optional поля в RawModelData.

---

## 10. Сводка файлов

| Файл | Действие |
|------|----------|
| `gbp/core/schemas/observations.py` | **Новый** — ObservedFlow, ObservedInventory |
| `gbp/core/schemas/__init__.py` | Изменить — добавить exports |
| `gbp/core/model.py` | Изменить — поля, _GROUPS, _SCHEMAS, from_raw(), property |
| `gbp/build/time_resolution.py` | Изменить — 2 спецификации |
| `gbp/build/validation.py` | Изменить — _check_observations() |
| `gbp/loaders/contracts.py` | Изменить — TripsSourceSchema |
| `gbp/loaders/dataloader_graph.py` | Изменить — _build_observations(), demand derivation, docstring |
| `tests/test_graph_loader.py` | Изменить — TestObservations, TestObservationsResolved |
| `notebooks/02_test_graph_loader.ipynb` | Изменить — секция 4 |

## 11. Verification

```bash
pytest tests/unit/core/test_model.py -v     # groups coverage
pytest tests/test_graph_loader.py -v         # observations + demand
pytest tests/unit/build/ -v                  # time resolution + validation
pytest                                       # полный прогон
mypy gbp/
ruff check gbp/ tests/
```
