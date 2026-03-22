# Cleanup: единый путь через AttributeRegistry

## Контекст

`AttributeRegistry` реализован и интегрирован: `registry.py` работает, `RawModelData.attributes` существует, `build_model()` резолвит атрибуты из registry, `DataLoaderGraph._register_costs()` использует registry, `register_bike_sharing_defaults()` есть в `defaults.py`, serialization (parquet + dict) поддерживает registry.

Но старые фиксированные поля (`operation_costs`, `transport_costs`, etc.) **остались** в `RawModelData` и `ResolvedModelData` как обычные поля. Это создаёт два пути для одного и того же — нарушает принцип из `ATTRIBUTE_SYSTEM_DESIGN.md` §11 (Alt 1 — отвергнут).

## Цель

Удалить фиксированные параметрические поля. Единственный путь для параметрических данных: `raw.attributes.register()` / `raw.attributes.get()`.

## Золотое правило

`pytest tests/ -v` должен проходить после каждого шага. Если тест ломается — чинить тест, а не откатывать изменение. Старые тесты, которые напрямую создавали `operation_costs=pd.DataFrame(...)` в конструкторе, переписываются на `raw.attributes.register(...)`.

---

## Шаг 1: Удалить фиксированные поля из RawModelData

### Файл: `gbp/core/model.py`

**Удалить эти dataclass-поля из `RawModelData`:**

```python
# УДАЛИТЬ:
operation_capacities: pd.DataFrame | None = None
operation_costs: pd.DataFrame | None = None
transport_costs: pd.DataFrame | None = None
resource_costs: pd.DataFrame | None = None
commodity_sell_price_tiers: pd.DataFrame | None = None
commodity_procurement_cost_tiers: pd.DataFrame | None = None
```

**Примечание по `edge_capacities`:** Поле `edge_capacities` нужно проанализировать. Если оно используется только как параметр (capacity value с grain) — удалить. Если оно используется в validation или build_model как структурная проверка (наличие capacity constraint на ребре) — оставить как структурное. Проверить все `raw.edge_capacities` и `resolved.edge_capacities` в codebase. Скорее всего удалить — capacity это параметр.

**Удалить группу `"parameters"` из `_GROUPS`:**

```python
# БЫЛО:
"parameters": [
    "operation_capacities", "operation_costs", "transport_costs",
    "resource_costs", "commodity_sell_price_tiers",
    "commodity_procurement_cost_tiers",
],

# УДАЛИТЬ целиком из dict
```

**Удалить из `_SCHEMAS`:**

```python
# УДАЛИТЬ эти записи:
"operation_capacities": OperationCapacity,
"operation_costs": OperationCost,
"transport_costs": TransportCost,
"resource_costs": ResourceCost,
"commodity_sell_price_tiers": CommoditySellPriceTier,
"commodity_procurement_cost_tiers": CommodityProcurementCostTier,
```

(Pydantic schemas в `gbp/core/schemas/parameters.py` НЕ удалять — они могут пригодиться для валидации данных при `register()`. Просто убрать из `_SCHEMAS` dict в model.py.)

**Обновить `parameter_tables` property:**

```python
@property
def parameter_tables(self) -> dict[str, pd.DataFrame]:
    """All registered parametric attribute tables."""
    return self.attributes.to_dict()
```

**Обновить `populated_tables` property** — добавить registry attributes:

```python
@property
def populated_tables(self) -> dict[str, pd.DataFrame]:
    """All non-None DataFrames: structural fields + registry attributes."""
    result: dict[str, pd.DataFrame] = {}
    for f in fields(self):
        if f.name.startswith("_") or f.name in self._NON_TABLE_FIELDS:
            continue
        val = getattr(self, f.name)
        if isinstance(val, pd.DataFrame):
            result[f.name] = val
    # Add registry attributes
    result.update(self.attributes.to_dict())
    return result
```

**Обновить `table_summary()`** — секция parameters берётся из registry:

```python
def table_summary(self) -> str:
    summary = _table_summary(self, self._GROUPS, self._REQUIRED)
    if self.attributes:
        summary += "\n\n  parameters (AttributeRegistry)"
        summary += f"\n  {'─' * 31}"
        summary += "\n" + self.attributes.summary()
    else:
        summary += "\n\n  parameters (AttributeRegistry)"
        summary += f"\n  {'─' * 31}"
        summary += "\n    (no attributes registered)"
    return summary
```

**Обновить тест `test_raw_groups_cover_all_fields`** — убрать удалённые поля из expected set.

### Применить те же изменения к ResolvedModelData

Те же 6 (или 7) полей удалить. Те же изменения в `_GROUPS`, `_SCHEMAS`, `parameter_tables`, `populated_tables`, `table_summary()`.

---

## Шаг 2: Обновить build_model() pipeline

### Файл: `gbp/build/pipeline.py`

**Time resolution (шаг 2):** Убрать hardcoded list таблиц. Итерировать по `raw.attributes.specs`:

```python
# Вместо:
for table_name in ["operation_costs", "transport_costs", ...]:
    df = getattr(raw, table_name)
    if df is not None:
        resolved_df = resolve_to_periods(df, periods, ...)

# Сделать:
resolved_registry = AttributeRegistry()
for attr in raw.attributes.specs:
    if attr.time_varying:
        data = raw.attributes.get(attr.name).data
        resolved_data = resolve_to_periods(
            param_df=data,
            periods=periods,
            value_columns=[attr.value_column],
            group_grain=[g for g in attr.grain if g != "date"],
            agg_func=attr.aggregation,
        )
        resolved_registry.register_raw(
            spec=AttributeSpec(
                name=attr.name,
                kind=attr.kind,
                entity_type=attr.entity_type,
                grain=attr.resolved_grain,
                resolved_grain=attr.resolved_grain,
                value_column=attr.value_column,
                source_table=attr.source_table,
                unit=attr.unit,
                aggregation=attr.aggregation,
                nullable=attr.nullable,
                eav_filter=attr.eav_filter,
            ),
            data=resolved_data,
        )
    else:
        # Non-time-varying: copy as-is
        resolved_registry.register_raw(
            spec=attr,
            data=raw.attributes.get(attr.name).data,
        )
```

**Spine assembly (шаг 8):** Использовать `resolved.attributes.specs`:

```python
# Вместо:
specs = get_all_default_specs()

# Сделать:
specs = list(resolved.attributes.specs) + get_structural_attribute_specs()
```

`get_structural_attribute_specs()` — specs для edge_distance, edge_lead_time_hours, edge_reliability, resource_base_capacity. Эти данные берутся из структурных таблиц (edges, resource_categories), а не из registry. Они нужны для spine assembly.

**Assemble ResolvedModelData (шаг 7):** Передать resolved_registry:

```python
resolved = ResolvedModelData(
    facilities=...,
    # ... структурные таблицы ...
    attributes=resolved_registry,
    # ... generated tables ...
)
```

### Файл: `gbp/build/validation.py`

Проверить, есть ли в validate_raw_model() прямые обращения к `raw.operation_costs`, `raw.transport_costs`. Если есть — заменить на `raw.attributes.get("operation_cost")` с обработкой KeyError. Или убрать параметрическую валидацию из validate_raw_model (она уже происходит при register()).

---

## Шаг 3: Обновить DataLoaderGraph

### Файл: `gbp/loaders/dataloader_graph.py`

**`_build_raw_model()`:** Убрать передачу параметрических данных через dict → RawModelData kwargs. Все параметрические данные идут только через registry.

```python
def _build_raw_model(self) -> RawModelData:
    temporal = self._build_temporal()
    entities = self._build_entities()
    behavior = self._build_behavior(entities)
    edge_data = self._build_edges(entities) if self._config.build_edges else {}
    flow = self._build_flow_data(entities)  # inventory_initial only, no operation_capacities

    raw = RawModelData(**{
        **temporal,
        **entities.tables,
        **behavior,
        **edge_data,
        **flow,
    })

    # Parametric data через registry
    self._register_costs(raw.attributes, temporal)
    self._register_capacities(raw.attributes, entities)
    self._register_resources(raw.attributes, entities)

    return raw
```

**`_build_flow_and_operations()`:** Разделить:
- `_build_flow_data()` → возвращает только `{"inventory_initial": df}` (это структурное — граничное условие)
- `_register_capacities()` → регистрирует operation_capacity через registry

**`_register_costs()`** — уже реализован, проверить что не дублирует в dict.

**`_register_resources()`** — если есть resource_costs, регистрировать через registry.

**Убрать двойную запись:** сейчас `_build_flow_and_operations` одновременно возвращает `operation_capacities` как DataFrame в dict И регистрирует через registry. Оставить только registry.

---

## Шаг 4: Обновить I/O

### Файл: `gbp/io/parquet.py`

Serialization registry уже работает (`_save_attribute_registry` / `_load_attribute_registry`). Нужно только убедиться, что удалённые поля не сохраняются/не загружаются как отдельные parquet-файлы. Проверить `_save_tables()` — она итерирует по `fields(obj)`, и удалённых полей там уже не будет.

**Backward compat при загрузке:** Если загружается старый parquet directory, где есть `operation_costs.parquet` но нет `attributes/`, нужно корректно обрабатывать. Варианты:
- Опция A (рекомендуется): при загрузке старых файлов — выбросить понятную ошибку: "This parquet was saved before AttributeRegistry migration. Re-save using current version."
- Опция B: auto-migration — при загрузке старого формата автоматически регистрировать через `register_bike_sharing_defaults()`. Сложнее, но дружелюбнее.

Выбрать опцию A — проще и безопаснее. Старые файлы итак пересоздаются за секунды из mock.

### Файл: `gbp/io/dict_io.py`

Аналогичные изменения. Registry serialization уже реализована. Убрать обработку удалённых полей, если она есть.

---

## Шаг 5: Обновить make_raw_model()

### Файл: `gbp/core/factory.py`

```python
def make_raw_model(
    facilities, commodity_categories, resource_categories,
    *,
    planning_start, planning_end, period_type="day",
    edge_rules=None,
    demand=None, supply=None,
    attributes: AttributeRegistry | None = None,
    **extra_tables,
) -> RawModelData:
    ...
    raw = RawModelData(
        facilities=facilities,
        ...,
    )
    if attributes is not None:
        raw.attributes = attributes
    ...
```

---

## Шаг 6: Обновить тесты

### Общий принцип

Все тесты, которые создавали `RawModelData(operation_costs=df, ...)`, переписываются:

```python
# БЫЛО:
raw = RawModelData(
    ...,
    operation_costs=costs_df,
)

# СТАЛО:
raw = RawModelData(...)
raw.attributes.register(
    name="operation_cost",
    data=costs_df,
    entity_type="facility",
    kind=AttributeKind.COST,
    grain=("facility_id", "operation_type", "commodity_category", "date"),
    value_column="cost_per_unit",
    aggregation="mean",
)
```

Или через convenience:

```python
from gbp.core.attributes.defaults import register_bike_sharing_defaults

raw = RawModelData(...)
register_bike_sharing_defaults(
    raw.attributes,
    operation_costs=costs_df,
    transport_costs=transport_df,
)
```

### Файлы для обновления

- `tests/unit/core/test_model.py` — убрать параметрические поля из _GROUPS coverage tests
- `tests/unit/build/fixtures.py` — `minimal_raw_model()` использует registry вместо kwargs
- `tests/test_graph_loader.py` — если проверяет `raw.operation_costs` напрямую → `raw.attributes.get("operation_cost")`
- `tests/unit/test_io/test_parquet.py` — round-trip тесты с registry
- `tests/unit/test_io/test_dict_io.py` — аналогично
- `notebooks/04_graph_model_playground.ipynb` — обновить примеры

---

## Шаг 7: Обновить exports и удалить deprecated code

### Файл: `gbp/core/__init__.py`

Убрать из `__all__`:
- `OperationCost`, `OperationCapacity`, `TransportCost`, `ResourceCost` — НЕ удалять из schemas, но убрать из верхнеуровневого экспорта если они больше не нужны пользователю
- `get_facility_attribute_specs`, `get_edge_attribute_specs`, `get_resource_attribute_specs` — это legacy specs, оставить если нужны для structural, иначе убрать
- Добавить `AttributeRegistry`, `RegisteredAttribute` если ещё не добавлены

### Файл: `gbp/core/attributes/defaults.py`

- `get_facility_attribute_specs()`, `get_edge_attribute_specs()`, `get_resource_attribute_specs()`, `get_all_default_specs()` — можно оставить как legacy/reference, но добавить docstring "DEPRECATED: use register_bike_sharing_defaults() + get_structural_attribute_specs() instead"
- Основной public API: `register_bike_sharing_defaults()` + `get_structural_attribute_specs()`

---

## Порядок выполнения

1. Шаг 1 (model.py) — удалить поля
2. Шаг 6 (тесты) — починить ВСЕ сломанные тесты сразу
3. Шаг 2 (build pipeline) — обновить
4. Шаг 3 (dataloader) — убрать двойную запись
5. Шаг 4 (I/O) — cleanup
6. Шаг 5 (factory) — обновить
7. Шаг 7 (exports) — cleanup

Запускать `pytest` после каждого шага.

---

## Верификация (после всех шагов)

```python
# 1. Нет фиксированных параметрических полей
raw = RawModelData(facilities=..., ...)
assert not hasattr(raw, 'operation_costs')  # поля нет

# 2. Единственный путь — через registry
raw.attributes.register(
    name="operation_cost", data=df,
    entity_type="facility", kind=AttributeKind.COST,
    grain=("facility_id", "operation_type", "commodity_category", "date"),
    value_column="cost_per_unit",
)
assert raw.attributes.get("operation_cost").data is df

# 3. table_summary показывает registry
summary = raw.table_summary()
assert "parameters (AttributeRegistry)" in summary
assert "operation_cost" in summary

# 4. Build pipeline работает через registry
resolved = build_model(raw)
assert "operation_cost" in resolved.attributes

# 5. Кастомный атрибут с нестандартным grain — работает
raw.attributes.register(
    name="custom_penalty",
    data=penalty_df,
    entity_type="facility",
    kind=AttributeKind.RATE,
    grain=("facility_id",),  # минимальный grain
    value_column="penalty",
)
resolved = build_model(raw)
assert "custom_penalty" in resolved.attributes

# 6. Все тесты проходят
# pytest tests/ -v  → 0 failures
```
