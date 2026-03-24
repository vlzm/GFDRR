# Design Doc: Динамическая система атрибутов

## 1. Контекст и проблема

### Что было задумано (graph_data_model.md, §9–10)

AttributeSpec — единое описание числового атрибута: имя, kind, grain, value_column, aggregation. Пользователь регистрирует произвольное количество кастомных атрибутов на любую сущность. `auto_group_attributes()` автоматически группирует по совместимости grain'ов. `plan_merges()` определяет порядок join'ов. `build_spines()` мёрджит в широкие DataFrame'ы.

Это generic engine, спроектированный для полной гибкости.

### Что реализовано сейчас

Движок (`core/attributes/`) реализован корректно и протестирован. Но он **отключён от pipeline**:

- `RawModelData` имеет **фиксированные поля** для параметрических таблиц: `operation_costs`, `transport_costs`, `resource_costs`, `edge_capacities`, `operation_capacities`, `commodity_sell_price_tiers`, `commodity_procurement_cost_tiers`.
- `defaults.py` содержит **захардкоженные** AttributeSpecs с фиксированными grain'ами.
- `build_model()` вызывает `get_all_default_specs()` — нет параметра для custom specs.
- `DataLoaderGraph._build_costs()` вручную создаёт DataFrame'ы, ничего не зная про AttributeSpec.

Результат: grain'ы зафиксированы. Нельзя добавить кастомный атрибут без изменения `core/model.py` и `defaults.py`. Гибкость, ради которой проектировался engine, не доступна пользователю.

### Цель

Сделать AttributeBuilder **центральным API** для добавления любых параметрических данных. Specs живут в модели, grain'ы кастомные, `defaults.py` — просто convenience-набор для bike-sharing.

---

## 2. Разделение: структурные vs параметрические таблицы

Ключевое архитектурное решение: не все таблицы одинаковы. Одни описывают **структуру графа** (что существует, как связано), другие — **числовые параметры** (стоимости, ёмкости, цены).

### Структурные таблицы (остаются фиксированными полями)

Эти таблицы определяют топологию и правила сети. У них стабильная схема, и потребители (optimizer, simulator) обращаются к ним по имени:

```
entity:          facilities, commodity_categories, resource_categories,
                 commodities, resources
temporal:        planning_horizon, planning_horizon_segments, periods
behavior:        facility_roles, facility_operations, facility_availability,
                 edge_rules
edge:            edges, edge_commodities, edge_commodity_capacities,
                 edge_vehicles
flow_data:       demand, supply, inventory_initial, inventory_in_transit
transformation:  transformations, transformation_inputs, transformation_outputs
resource:        resource_fleet, resource_commodity_compatibility,
                 resource_modal_compatibility, resource_availability
hierarchy:       facility_hierarchy_*, commodity_hierarchy_*
scenario:        scenarios, scenario_edge_rules, scenario_manual_edges,
                 scenario_parameter_overrides
```

Почему фиксированные: optimizer формулирует `flow conservation` через `demand`, `supply`, `edges`. Он должен знать, что таблица `demand` существует и имеет колонки `facility_id`, `commodity_category`, `quantity`. Это контракт.

### Параметрические таблицы (становятся динамическими)

Эти таблицы — числовые значения на определённом grain. Их набор, grain и даже имена зависят от домена:

```
Текущие фиксированные поля, которые уходят:
  operation_costs              → attribute "operation_cost"
  operation_capacities         → attribute "operation_capacity"
  transport_costs              → attribute "transport_cost"
  resource_costs               → attribute "resource_fixed_cost", "resource_maintenance_cost", ...
  edge_capacities              → attribute "edge_capacity"
  commodity_sell_price_tiers   → attribute "sell_price" (или tier-based pricing)
  commodity_procurement_cost_tiers → attribute "procurement_cost"
```

Почему динамические: в велошеринге operation_cost имеет grain `(facility_id, operation_type, commodity_category, date)`. В другом домене может быть `(facility_id, date)` — без operation_type. Или может появиться атрибут `fuel_surcharge` с grain `(source_id, target_id, modal_type, date)`, которого вообще нет в текущей схеме. Фиксированные поля не могут это выразить.

---

## 3. Новый API: регистрация атрибутов

### 3.1. Контейнер: RegisteredAttribute

Атрибут — это **spec + data**, неразрывная пара:

```python
@dataclass(frozen=True)
class RegisteredAttribute:
    """Attribute spec + its data, stored together."""
    spec: AttributeSpec
    data: pd.DataFrame
```

### 3.2. AttributeRegistry: хранилище атрибутов в модели

```python
class AttributeRegistry:
    """Registry of parametric attributes for a model.

    Central API for adding, validating, and retrieving
    parameter tables with their grain definitions.
    """

    def register(
        self,
        name: str,
        data: pd.DataFrame,
        *,
        entity_type: str,           # "facility", "edge", "resource"
        kind: AttributeKind,        # COST, REVENUE, RATE, CAPACITY, ADDITIONAL
        grain: tuple[str, ...],     # ("facility_id", "operation_type", "date")
        value_column: str,          # "cost_per_unit"
        aggregation: str = "mean",  # time resolution aggregation
        unit: str | None = None,
        nullable: bool = True,
        eav_filter: dict | None = None,
    ) -> None:
        """Register a parametric attribute with its data and grain.

        Validates:
        - grain contains entity_grain as subset
        - value_column exists in data
        - data columns cover grain columns
        - numeric values satisfy kind constraints (COST ≥ 0, CAPACITY > 0)

        Raises:
            ValueError: on validation failure
        """

    def get(self, name: str) -> RegisteredAttribute:
        """Get attribute by name. Raises KeyError if not found."""

    def get_by_entity(self, entity_type: str) -> list[RegisteredAttribute]:
        """All attributes for a given entity type."""

    def get_by_kind(self, kind: AttributeKind) -> list[RegisteredAttribute]:
        """All attributes of a given kind (e.g. all COSTs)."""

    @property
    def specs(self) -> list[AttributeSpec]:
        """All registered specs (for build pipeline)."""

    @property
    def names(self) -> list[str]:
        """All registered attribute names."""

    def to_dict(self) -> dict[str, pd.DataFrame]:
        """All attribute data as {name: DataFrame} for serialization."""

    def summary(self) -> str:
        """Human-readable summary of registered attributes."""
```

### 3.3. Что происходит внутри register()

```python
def register(self, name, data, *, entity_type, kind, grain, value_column, ...):
    # 1. Вычислить resolved_grain: заменить "date" → "period_id"
    resolved_grain = tuple(
        "period_id" if g == "date" else g for g in grain
    )

    # 2. Создать AttributeSpec (существующий класс, без изменений)
    spec = AttributeSpec(
        name=name,
        kind=kind,
        entity_type=entity_type,
        grain=grain,
        resolved_grain=resolved_grain,
        value_column=value_column,
        source_table=name,       # source_table = attribute name
        unit=unit,
        aggregation=aggregation,
        nullable=nullable,
        eav_filter=eav_filter,
    )
    # AttributeSpec.__post_init__ validates grain consistency

    # 3. Валидировать data: колонки покрывают grain + value_column
    required_cols = set(grain) | {value_column}
    missing = required_cols - set(data.columns)
    if missing:
        raise ValueError(f"Attribute {name!r}: data missing columns {missing}")

    # 4. Валидировать значения по kind
    _validate_numeric_series(spec, data[value_column])

    # 5. Сохранить
    self._attributes[name] = RegisteredAttribute(spec=spec, data=data)
```

### 3.4. Пример использования в dataloader

**Было (текущий код):**

```python
def _build_costs(self, temporal):
    # 60 строк ручной сборки DataFrame...
    cost_rows = []
    for d in horizon_dates:
        for _, r in costs.iterrows():
            cost_rows.append({
                "facility_id": str(r["station_id"]),
                "operation_type": "visit",
                "commodity_category": COMMODITY_CATEGORY,
                "date": d,
                "cost_per_unit": float(r["fixed_cost_per_visit"]),
                "cost_unit": "USD",
            })
    return {"operation_costs": pd.DataFrame(cost_rows)}
```

**Стало:**

```python
def _register_costs(self, registry: AttributeRegistry, temporal: dict):
    # Построить DataFrame (та же логика)
    visit_costs_df = self._build_visit_cost_df(temporal)

    # Зарегистрировать с grain
    registry.register(
        name="visit_cost",
        data=visit_costs_df,
        entity_type="facility",
        kind=AttributeKind.COST,
        grain=("facility_id", "operation_type", "commodity_category", "date"),
        value_column="cost_per_unit",
        aggregation="mean",
        unit="USD",
    )

    # Другой cost — другой grain
    handling_costs_df = self._build_handling_cost_df(temporal)
    registry.register(
        name="handling_cost",
        data=handling_costs_df,
        entity_type="facility",
        kind=AttributeKind.COST,
        grain=("facility_id", "commodity_category", "date"),  # без operation_type!
        value_column="cost_per_unit",
        aggregation="mean",
        unit="USD",
    )
```

Обрати внимание: `visit_cost` и `handling_cost` имеют **разные grain'ы**. Это именно та гибкость, которая была заложена в design, но не работала в реализации.

---

## 4. Изменения в RawModelData

### Что уходит (фиксированные параметрические поля)

```python
# УДАЛЯЮТСЯ из RawModelData:
operation_costs: pd.DataFrame | None = None
operation_capacities: pd.DataFrame | None = None
transport_costs: pd.DataFrame | None = None
resource_costs: pd.DataFrame | None = None
edge_capacities: pd.DataFrame | None = None
commodity_sell_price_tiers: pd.DataFrame | None = None
commodity_procurement_cost_tiers: pd.DataFrame | None = None
```

### Что приходит

```python
@dataclass
class RawModelData:
    # ── структурные таблицы (без изменений) ────────────────────
    facilities: pd.DataFrame
    commodity_categories: pd.DataFrame
    resource_categories: pd.DataFrame
    # ... все остальные структурные таблицы ...

    # ── параметрическая система (НОВОЕ) ────────────────────────
    attributes: AttributeRegistry = field(default_factory=AttributeRegistry)
```

### Группировка (_GROUPS)

Группа `parameters` больше не содержит фиксированные имена таблиц. Вместо этого:

```python
# Убираем из _GROUPS:
#   "parameters": ["operation_costs", "transport_costs", ...]

# Добавляем метод:
@property
def parameter_tables(self) -> dict[str, pd.DataFrame]:
    """All registered parametric attribute tables."""
    return self.attributes.to_dict()
```

### table_summary()

Секция `parameters` теперь динамическая:

```
  parameters (via AttributeRegistry)
  ──────────
    visit_cost: 960 rows [facility × operation_type × commodity × date] COST
    handling_cost: 480 rows [facility × commodity × date] COST
    operation_capacity: 8 rows [facility × operation_type × commodity] CAPACITY
    transport_cost: 180 rows [edge × resource × date] COST
    edge_capacity: — (not registered)
```

### Обратная совместимость

Для плавного перехода — **deprecation properties**:

```python
@property
def operation_costs(self) -> pd.DataFrame | None:
    """DEPRECATED: use attributes.get('operation_cost').data instead."""
    warnings.warn("operation_costs is deprecated, use attributes", DeprecationWarning)
    try:
        return self.attributes.get("operation_cost").data
    except KeyError:
        return None
```

Эти properties можно убрать через 1-2 milestone'а, когда весь код перейдёт на `attributes.get()`.

---

## 5. Изменения в build_model()

### Шаг 2: Time Resolution

Сейчас `resolve_all_time_varying()` знает конкретные имена таблиц:

```python
# СЕЙЧАС (hardcoded):
for table_name in ["operation_costs", "transport_costs", "edge_capacities", ...]:
    if getattr(raw, table_name) is not None:
        resolved_table = resolve_to_periods(...)
```

Станет:

```python
# ПОСЛЕ:
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
        resolved_registry.register(
            name=attr.name,
            data=resolved_data,
            entity_type=attr.entity_type,
            kind=attr.kind,
            grain=attr.resolved_grain,  # period_id вместо date
            value_column=attr.value_column,
            aggregation=attr.aggregation,
            unit=attr.unit,
            nullable=attr.nullable,
        )
```

Не нужно знать имена таблиц. Specs сами говорят, что time-varying, что агрегировать, какой grain.

### Шаг 8: Spine Assembly

Сейчас:

```python
specs = get_all_default_specs()  # hardcoded
```

Станет:

```python
specs = resolved.attributes.specs  # из модели
```

`AttributeBuilder.build_spines()` — без изменений. Он уже generic.

---

## 6. Изменения в ResolvedModelData

Аналогично RawModelData:

```python
@dataclass
class ResolvedModelData:
    # структурные таблицы (без изменений)
    ...

    # параметрическая система (НОВОЕ)
    attributes: AttributeRegistry = field(default_factory=AttributeRegistry)

    # generated таблицы (без изменений)
    edge_lead_time_resolved: pd.DataFrame | None = None
    transformation_resolved: pd.DataFrame | None = None
    fleet_capacity: pd.DataFrame | None = None

    # spines (без изменений — собираются из attributes.specs)
    facility_spines: dict[str, pd.DataFrame] | None = None
    edge_spines: dict[str, pd.DataFrame] | None = None
    resource_spines: dict[str, pd.DataFrame] | None = None
```

---

## 7. defaults.py: convenience, не конфигурация

`defaults.py` превращается из "единственного источника specs" в "helper для типичного bike-sharing сетапа":

```python
def register_bike_sharing_defaults(
    registry: AttributeRegistry,
    *,
    operation_costs: pd.DataFrame | None = None,
    transport_costs: pd.DataFrame | None = None,
    operation_capacities: pd.DataFrame | None = None,
    edge_capacities: pd.DataFrame | None = None,
    resource_costs: pd.DataFrame | None = None,
) -> None:
    """Register standard bike-sharing attributes with default grains.

    Convenience wrapper — registers known attributes with typical grains.
    Users can also call registry.register() directly for custom attributes.
    """
    if operation_costs is not None:
        registry.register(
            name="operation_cost",
            data=operation_costs,
            entity_type="facility",
            kind=AttributeKind.COST,
            grain=("facility_id", "operation_type", "commodity_category", "date"),
            value_column="cost_per_unit",
            aggregation="mean",
        )
    if operation_capacities is not None:
        registry.register(
            name="operation_capacity",
            data=operation_capacities,
            entity_type="facility",
            kind=AttributeKind.CAPACITY,
            grain=("facility_id", "operation_type", "commodity_category"),
            value_column="capacity",
            aggregation="min",
        )
    # ... остальные стандартные атрибуты
```

---

## 8. Serialization (I/O)

### Parquet

```
model_dir/
├── _metadata.json              # includes "attributes" section
├── facilities.parquet
├── edges.parquet
├── ...
├── attributes/
│   ├── _specs.json             # list of AttributeSpec as dicts
│   ├── visit_cost.parquet      # attribute data
│   ├── handling_cost.parquet
│   └── transport_cost.parquet
```

### Dict/JSON

```python
{
    "facilities": [...],
    "edges": [...],
    ...,
    "attributes": {
        "visit_cost": {
            "spec": {
                "name": "visit_cost",
                "kind": "cost",
                "entity_type": "facility",
                "grain": ["facility_id", "operation_type", "commodity_category", "date"],
                "value_column": "cost_per_unit",
                "aggregation": "mean"
            },
            "data": [
                {"facility_id": "s1", "operation_type": "visit", ...},
                ...
            ]
        }
    }
}
```

---

## 9. Пример: полный путь в новой архитектуре

```python
from gbp.core import RawModelData, AttributeRegistry, AttributeKind

# 1. Создаём модель со структурными таблицами
raw = RawModelData(
    facilities=facilities_df,
    commodity_categories=commodity_cats_df,
    resource_categories=resource_cats_df,
    planning_horizon=horizon_df,
    planning_horizon_segments=segments_df,
    periods=periods_df,
    facility_roles=roles_df,
    facility_operations=ops_df,
    edge_rules=rules_df,
    edges=edges_df,
    edge_commodities=edge_comm_df,
    demand=demand_df,
    inventory_initial=inv_df,
)

# 2. Регистрируем параметрические атрибуты
raw.attributes.register(
    name="operation_cost",
    data=operation_costs_df,
    entity_type="facility",
    kind=AttributeKind.COST,
    grain=("facility_id", "operation_type", "commodity_category", "date"),
    value_column="cost_per_unit",
    aggregation="mean",
)

raw.attributes.register(
    name="station_docking_capacity",
    data=docking_caps_df,
    entity_type="facility",
    kind=AttributeKind.CAPACITY,
    grain=("facility_id",),            # простой grain — только facility
    value_column="capacity",
    aggregation="min",
)

raw.attributes.register(
    name="transport_cost",
    data=transport_costs_df,
    entity_type="edge",
    kind=AttributeKind.COST,
    grain=("source_id", "target_id", "modal_type", "resource_category", "date"),
    value_column="cost_per_unit",
    aggregation="mean",
)

# 3. Кастомный атрибут, которого нет в defaults
raw.attributes.register(
    name="weather_penalty",
    data=weather_df,
    entity_type="edge",
    kind=AttributeKind.RATE,
    grain=("source_id", "target_id", "modal_type", "date"),
    value_column="penalty_factor",
    aggregation="mean",
)

# 4. Проверяем
print(raw.attributes.summary())
# operation_cost:           960 rows  facility  COST      [facility_id, operation_type, commodity_category, date]
# station_docking_capacity:   8 rows  facility  CAPACITY  [facility_id]
# transport_cost:           180 rows  edge      COST      [source_id, target_id, modal_type, resource_category, date]
# weather_penalty:           90 rows  edge      RATE      [source_id, target_id, modal_type, date]

# 5. Build — specs берутся из модели
from gbp.build import build_model
resolved = build_model(raw)

# Спайны содержат все атрибуты, сгруппированные по grain-совместимости
print(resolved.facility_spines.keys())
# dict_keys(["group_0", "group_1"])
# group_0: operation_cost (grain расширен operation_type × commodity × period)
# group_1: station_docking_capacity (только facility_id, без expansion)
```

---

## 10. Что НЕ меняется

- `core/attributes/spec.py` — AttributeSpec без изменений
- `core/attributes/grain_groups.py` — auto_group_attributes без изменений
- `core/attributes/merge_plan.py` — plan_merges без изменений
- `core/attributes/builder.py` — AttributeBuilder.build_spines() без изменений
- Все структурные таблицы и их Pydantic schemas
- `build_model()` шаги 1-7 (validation, edge building, lead times, etc.)
- I/O для структурных таблиц

---

## 11. Alternatives considered

### Alt 1: Оставить фиксированные поля + разрешить дополнительные

```python
class RawModelData:
    operation_costs: pd.DataFrame | None = None       # фиксированные
    transport_costs: pd.DataFrame | None = None
    extra_attributes: AttributeRegistry = ...          # кастомные
```

Отвергнуто: создаёт два пути для одного и того же (cost может быть и в фиксированном поле, и в registry). Пользователь не знает, где искать. Нарушает принцип единого API.

### Alt 2: Всё через registry, включая demand/supply/edges

Отвергнуто: demand, supply, edges — структурные. Optimizer обращается к ним по имени (`raw.demand`). Сделать их динамическими = потерять type safety и контракт с потребителями.

### Alt 3: EAV для всех параметров (одна таблица со всеми атрибутами)

Отвергнуто: теряем типизацию grain'ов, нельзя валидировать структуру каждого атрибута отдельно. Pivot-операции при каждом обращении.

---

## 12. Порядок реализации

### Milestone A: AttributeRegistry (новый код, ничего не ломает)

- Создать `core/attributes/registry.py` с `RegisteredAttribute` и `AttributeRegistry`
- Тесты: register, get, get_by_entity, get_by_kind, summary, validation
- Не трогать RawModelData, build_model, dataloader

### Milestone B: Интеграция в RawModelData

- Добавить поле `attributes: AttributeRegistry` в RawModelData и ResolvedModelData
- Deprecation properties для старых фиксированных полей
- Обновить `_GROUPS`, `table_summary()`, `populated_tables`
- Обновить parquet/dict serialization
- Тесты: существующие тесты продолжают работать через deprecation properties

### Milestone C: Перевод build_model()

- Шаг 2 (time resolution): итерирует по `raw.attributes.specs`
- Шаг 8 (spine assembly): берёт specs из `resolved.attributes`
- Тесты: build pipeline работает с attributes registry

### Milestone D: Перевод DataLoaderGraph

- `_build_costs()` → `_register_costs(registry)`
- `_build_flow_and_operations()`: operation_capacities → registry
- Удалить фиксированные параметрические поля из RawModelData
- Удалить deprecation properties
- Обновить DATA_JOURNEY.md

### Milestone E: Обновить make_raw_model() и defaults

- `defaults.py` → `register_bike_sharing_defaults()`
- `make_raw_model()` принимает optional AttributeRegistry или список атрибутов
- Тесты

---

## 13. Тесты ожидания (после всех milestone'ов)

```python
# Кастомный атрибут с нестандартным grain
raw.attributes.register(
    name="bike_weight_penalty",
    data=pd.DataFrame({
        "facility_id": ["s1", "s2"],
        "commodity_category": ["ebike", "ebike"],
        "penalty": [1.5, 1.5],
    }),
    entity_type="facility",
    kind=AttributeKind.RATE,
    grain=("facility_id", "commodity_category"),
    value_column="penalty",
)
assert "bike_weight_penalty" in raw.attributes.names
assert raw.attributes.get("bike_weight_penalty").spec.grain == (
    "facility_id", "commodity_category",
)

# Build pipeline использует кастомные specs
resolved = build_model(raw)
assert "bike_weight_penalty" in [s.name for s in resolved.attributes.specs]

# Спайны содержат кастомный атрибут
facility_spines = resolved.facility_spines
has_penalty = any("bike_weight_penalty" in df.columns for df in facility_spines.values())
assert has_penalty

# Optimizer собирает все costs через kind
all_costs = resolved.attributes.get_by_kind(AttributeKind.COST)
# → [operation_cost, transport_cost, ...] — независимо от имён
```
