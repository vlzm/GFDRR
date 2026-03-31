# Plan: Новая группа `observations` — trips и telemetry как сущности графа

## Context

Сырые данные поездок (`df_trips`) и телеметрии (`df_telemetry_ts`) живут только на source и никак не связаны с графовой моделью. Нужно сделать их **полноценными таблицами** в `RawModelData`/`ResolvedModelData`:
- Замаппить на сущности графа (facility_id, commodity_category, period_id)
- Валидировать ссылочную целостность
- Резолвить по времени (date → period_id)
- Поддержать расширение через `AttributeRegistry` (кастомные атрибуты по grain)

Бонус: `demand` заполняется из validated trips (departures → demand).

## Новая группа: `observations`

Две таблицы:

### `observed_trips` — перемещения commodity между facilities
**Обязательные колонки (grain + value):**

| Колонка | Тип | Описание |
|---------|-----|----------|
| `origin_facility_id` | str | Откуда (facility_id) |
| `destination_facility_id` | str | Куда (facility_id) |
| `commodity_category` | str | Тип commodity |
| `date` | dt.date | Дата поездки |
| `quantity` | float (≥0) | Количество |
| `quantity_unit` | str | Единица |
| `resource_id` | str \| None | Каким ресурсом (nullable — для rider trips нет ресурса) |

Time resolution: `date` → `period_id`, агрегация `sum` по quantity, grain = `(origin_facility_id, destination_facility_id, commodity_category)`.

Расширяемость: дополнительные атрибуты (duration, member_type, rideable_type) регистрируются в `AttributeRegistry` с grain `("origin_facility_id", "destination_facility_id", "commodity_category", "date")`.

### `observed_inventory` — snapshot инвентаря на facility
**Обязательные колонки:**

| Колонка | Тип | Описание |
|---------|-----|----------|
| `facility_id` | str | Станция (facility_id) |
| `commodity_category` | str | Тип commodity |
| `date` | dt.date | Дата наблюдения |
| `quantity_available` | float (≥0) | Доступный инвентарь |
| `quantity_unit` | str | Единица |

Time resolution: `date` → `period_id`, агрегация `mean` (среднее за период), grain = `(facility_id, commodity_category)`.

Расширяемость: docks_available, capacity, is_renting и т.д. — через `AttributeRegistry` с grain `("facility_id", "commodity_category", "date")`.

> **Почему `observed_inventory`, а не `observed_telemetry`?** Telemetry — domain-specific термин (bike-sharing). `observed_inventory` — это domain-agnostic: наблюдаемый уровень запасов на узле. Подходит для любой логистической сети.

## Шаги реализации

### 1. Pydantic row schemas — `gbp/core/schemas/observations.py` (новый файл)

```python
class ObservedTrip(BaseModel):
    origin_facility_id: str
    destination_facility_id: str
    commodity_category: str
    date: dt.date
    quantity: float = Field(ge=0)
    quantity_unit: str
    resource_id: str | None = None

class ObservedInventory(BaseModel):
    facility_id: str
    commodity_category: str
    date: dt.date
    quantity_available: float = Field(ge=0)
    quantity_unit: str
```

### 2. Export schemas — `gbp/core/schemas/__init__.py`

Добавить import из `observations` + в `__all__`.

### 3. Model fields — `gbp/core/model.py`

**RawModelData:**
- Два новых поля: `observed_trips: pd.DataFrame | None = None`, `observed_inventory: pd.DataFrame | None = None`
- Новая группа в `_GROUPS`: `"observations": ["observed_trips", "observed_inventory"]`
- Добавить в `_SCHEMAS`: mapping на `ObservedTrip`, `ObservedInventory`
- Не добавлять в `_REQUIRED` (optional)

**ResolvedModelData:**
- Те же два поля
- `_GROUPS` наследуется через `**RawModelData._GROUPS` (автоматически)
- Добавить в `from_raw()`: `observed_trips=_coalesce("observed_trips", raw.observed_trips)`, аналогично для inventory

**Domain property** на обоих классах:
```python
@property
def observation_tables(self) -> dict[str, pd.DataFrame]:
    """Observed historical data: trips and inventory snapshots."""
    return _collect_group(self, self._GROUPS["observations"])
```

### 4. Time resolution — `gbp/build/time_resolution.py`

Добавить 2 спеки в `resolve_all_time_varying()`:

```python
("observed_trips", raw.observed_trips,
 ["origin_facility_id", "destination_facility_id", "commodity_category"],
 ["quantity"], "sum"),

("observed_inventory", raw.observed_inventory,
 ["facility_id", "commodity_category"],
 ["quantity_available"], "mean"),
```

### 5. Validation — `gbp/build/validation.py`

Новая функция `_check_observations(raw, result)`:
- `observed_trips.origin_facility_id` ∈ facilities
- `observed_trips.destination_facility_id` ∈ facilities
- `observed_trips.commodity_category` ∈ commodity_categories
- `observed_trips.resource_id` (если не null) ∈ resources
- `observed_inventory.facility_id` ∈ facilities
- `observed_inventory.commodity_category` ∈ commodity_categories

Вызвать из `validate_raw_model()`.

### 6. Source schema — `gbp/loaders/contracts.py`

```python
class TripsSourceSchema(pa.DataFrameModel):
    started_at: Series[pd.Timestamp]
    start_station_id: Series[str] = pa.Field(str_length={"min_value": 1})
    end_station_id: Series[str] = pa.Field(str_length={"min_value": 1})
    class Config:
        strict = False; coerce = True
```

### 7. Loader — `gbp/loaders/dataloader_graph.py`

**a) `_validate_source()`**: валидация trips через `TripsSourceSchema` (если not empty).

**b) `_build_observations(entities)` — новый метод:**
- `df_trips` → `observed_trips`:
  - `start_station_id` → `origin_facility_id`
  - `end_station_id` → `destination_facility_id`
  - `pd.to_datetime(started_at).dt.date` → `date`
  - Все rideable_type → `COMMODITY_CATEGORY` ("working_bike")
  - quantity = 1.0 per trip, quantity_unit = "bike"
  - resource_id = None (rider trips)
  - Filter: только facility_ids ∈ entities.station_ids
  - Groupby `(origin, destination, commodity_category, date)` → sum quantity
  - Log warning для unknown station_ids

- `df_telemetry_ts` → `observed_inventory`:
  - `station_id` → `facility_id`
  - `pd.to_datetime(timestamp).dt.date` → `date`
  - `num_bikes_available` → `quantity_available`
  - commodity_category = `COMMODITY_CATEGORY`, quantity_unit = "bike"
  - Filter: только facility_ids ∈ entities.station_ids
  - Groupby `(facility_id, commodity_category, date)` → mean quantity_available

- Возвращает `dict[str, pd.DataFrame | None]`

**c) Опционально: регистрация доп. атрибутов в `AttributeRegistry`:**
- Из telemetry: `docks_available` с grain `("facility_id", "date")`
- Из trips: `avg_trip_duration` с grain `("origin_facility_id", "destination_facility_id", "date")`

**d) `_build_demand_from_observations(observed_trips)` — синтез demand:**
- Groupby `(origin_facility_id, date, commodity_category)` → sum quantity
- Rename `origin_facility_id` → `facility_id`
- Добавить `quantity_unit = "bike"`
- Возвращает `{"demand": df}`

**e) `_build_raw_model()` — обновить:**
```python
obs = self._build_observations(entities)
demand_data = self._build_demand_from_observations(obs.get("observed_trips"))

all_tables = {
    **temporal, **entities.tables, **behavior, **edge_data,
    **node_params, **resources, **obs, **demand_data,
}
```

### 8. Tests — `tests/test_graph_loader.py`

Новый класс `TestObservations`:
- `test_observed_trips_populated` — not None, not empty
- `test_observed_trips_columns` — соответствует ObservedTrip schema
- `test_observed_trips_facility_ids_valid` — все origin/destination ∈ facilities
- `test_observed_trips_commodity_valid` — commodity_category ∈ commodity_categories
- `test_observed_inventory_populated` — not None, not empty
- `test_observed_inventory_facility_ids_valid`
- `test_demand_derived_from_trips` — demand not None, quantities consistent

Новый класс `TestObservationsResolved`:
- `test_resolved_trips_has_period_id`
- `test_resolved_inventory_has_period_id`

### 9. Verification notebook — `notebooks/verify/04_observations_integration.ipynb`

Одна ячейка: загрузка mock → loader → показать observed_trips, observed_inventory, demand. Проверить что facility_ids совпадают с facilities.

## Файлы

| Файл | Действие |
|------|----------|
| `gbp/core/schemas/observations.py` | **Новый** — ObservedTrip, ObservedInventory |
| `gbp/core/schemas/__init__.py` | Изменить — добавить exports |
| `gbp/core/model.py` | Изменить — поля, _GROUPS, _SCHEMAS, from_raw(), property |
| `gbp/build/time_resolution.py` | Изменить — 2 новых спеки |
| `gbp/build/validation.py` | Изменить — _check_observations() |
| `gbp/loaders/contracts.py` | Изменить — TripsSourceSchema |
| `gbp/loaders/dataloader_graph.py` | Изменить — _build_observations(), _build_demand_from_observations(), wire |
| `tests/test_graph_loader.py` | Изменить — TestObservations, TestObservationsResolved |
| `notebooks/verify/04_observations_integration.ipynb` | **Новый** |

## Порядок реализации

1. Schema (`observations.py` + `__init__.py`) — нет зависимостей
2. Model (`model.py`) — зависит от (1)
3. Time resolution + Validation — зависит от (2)
4. Source schema (`contracts.py`) — нет зависимостей, параллельно с (1-3)
5. Loader (`dataloader_graph.py`) — зависит от (1-4)
6. Tests — зависит от (5)
7. Notebook — зависит от (5)

## Верификация

```bash
pytest tests/test_graph_loader.py -v
pytest tests/unit/build/ -v
pytest                              # полный прогон
mypy gbp/
ruff check gbp/ tests/
```
