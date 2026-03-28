# Attribute System: история о параметрах, которые не хотят быть колонками

*Storytelling-гайд по `docs/design/attribute_system.md`*

---

## Проблема

Представь, что ты описываешь стоимость операций на разных объектах сети. Для велошеринга стоимость ремонта зависит от: какой объект (facility_id), какая операция (operation_type), какой товар (commodity_category) и когда (date). Четыре измерения.

А стоимость хранения? Тут нет operation_type — зависит только от facility, commodity и даты. Три измерения.

А базовая ёмкость грузовика? Зависит только от resource_category. Одно измерение.

Если для каждого такого параметра заводить отдельную таблицу с фиксированными колонками — получится десятки таблиц с жёстко прошитой структурой. При переносе на новый домен (финансы вместо велошеринга) колонки будут другими. Модель сломается.

Attribute System решает эту проблему: вместо фиксированных колонок — **реестр**, где каждый параметр сам описывает свою структуру.

---

## Два типа таблиц: структурные и параметрические

Не все таблицы в модели одинаковые. Есть чёткое разделение.

### Структурные таблицы

Определяют **топологию** сети. Это контракт с потребителями — optimizer, simulator обращаются к ним по имени.

Facility, commodity_categories, resource_categories, edges, demand, supply, periods, transformations... Эти таблицы фиксированы. У demand всегда будут колонки facility_id, commodity_category, quantity. Это не меняется от домена к домену.

### Параметрические таблицы

**Числовые значения** на определённом уровне детализации (grain). Стоимости, ёмкости, скорости. Их набор, структура и даже количество — зависят от домена.

В велошеринге: operation_cost, transport_cost, resource_fixed_cost, resource_maintenance_cost. В другом домене их может быть больше или меньше, с другими grain'ами. Именно эти таблицы живут в `AttributeRegistry`, а не как фиксированные поля модели.

---

## AttributeSpec: паспорт параметра

Каждый параметр описывается через `AttributeSpec` — замороженный объект, который говорит:

- **name** — как называется ("operation_cost")
- **kind** — семантический тип (COST, CAPACITY, RATE, REVENUE, ADDITIONAL)
- **entity_type** — к какой сущности привязан ("facility", "edge", "resource")
- **grain** — на каком уровне детализации определён (["facility_id", "operation_type", "commodity_category", "date"])
- **resolved_grain** — grain после time resolution (date заменён на period_id)
- **value_column** — как называется колонка со значением ("cost_per_unit")
- **aggregation** — как агрегировать при time resolution ("mean" для стоимостей, "sum" для спроса)

Из этого "паспорта" система автоматически знает, как хранить, резолвить и собирать данные — без единой строчки hardcoded логики.

### AttributeKind — зачем это нужно

Kind — не просто метка. Он определяет:
- **Валидацию**: COST >= 0, CAPACITY > 0, RATE >= 0
- **Автоматический маппинг**: потребитель (optimizer) может сказать "дай мне все атрибуты с kind=COST" — и получить все стоимости, включая кастомные

Не нужно перечислять конкретные параметры — любой зарегистрированный атрибут участвует через свой kind.

---

## AttributeRegistry: центральный реестр

`AttributeRegistry` — это API для работы с параметрическими данными. Живёт на `RawModelData` и `ResolvedModelData`.

```python
registry.register(
    name="operation_cost",
    data=df,
    entity_type="facility",
    kind=AttributeKind.COST,
    grain=("facility_id", "operation_type", "commodity_category", "date"),
    value_column="cost_per_unit",
    aggregation="mean",
)
```

При регистрации происходит валидация: указанные колонки grain существуют в данных, value_column существует, значения удовлетворяют ограничениям kind. Нельзя зарегистрировать стоимость с отрицательными значениями.

Через реестр можно получить:
- Конкретный атрибут по имени
- Все атрибуты для entity type ("facility")
- Все атрибуты по kind (все COST'ы)
- Summary для отладки

---

## Как это работает в build pipeline

### Time Resolution

Когда build_model() собирает resolved модель, он проходит по всем зарегистрированным атрибутам. Если grain содержит "date" — атрибут time-varying. Для таких запускается resolution: date маппится на period_id, значения агрегируются внутри периода (mean для стоимостей, sum для объёмов).

```python
resolved_attrs = resolve_registry_attributes(raw.attributes, periods)
```

Никаких хардкодов "обработай operation_cost, потом transport_cost". Pipeline управляется спеками — каждый зарегистрированный атрибут обрабатывается автоматически.

### Spine Assembly

После resolution данные нужно собрать в удобный для потребителя формат. Здесь вступает в игру **spine** — широкая таблица, где все параметры для одного entity type собраны вместе.

Но есть проблема: если operation_cost зависит от (facility, operation_type, commodity, period), а storage_capacity — от (facility, commodity), то их нельзя просто merge'нуть в один DataFrame. Разные grain'ы дадут cross join, таблица взорвётся.

---

## Grain Groups: решение проблемы cross join

Это самая элегантная часть системы.

Атрибуты с совместимыми grain'ами (один вкладывается в другой) попадают в одну **группу**. Группы между собой не merge'атся.

Пример для facility:
- **Группа A**: grain = facility x commodity x period. Сюда попадут storage_capacity и throughput_rate.
- **Группа B**: grain = facility x operation_type x period. Сюда — handling_cost.
- **Группа C**: grain = facility. Только facility_type.

Группировка происходит автоматически через `auto_group_attributes()`. Алгоритм: для каждого атрибута ищем группу, чей grain является подмножеством или надмножеством grain'а атрибута. Если нашли — добавляем. Если нет — создаём новую.

Внутри каждой группы `plan_merges()` определяет оптимальный порядок join'ов: сначала "бесплатные" (grain уже покрыт), потом с минимальным expansion cost.

Результат: несколько compact spine DataFrames вместо одного раздутого. Потребитель получает `resolved.facility_spines`, `resolved.edge_spines`, `resolved.resource_spines` — словари с именованными группами.

---

## Как загрузчики используют систему

DataLoader (например, `DataLoaderGraph` для велошеринга) строит DataFrames из сырых данных и регистрирует их:

```python
registry = AttributeRegistry()
self._register_costs(registry, temporal)
self._register_resource_costs(registry, entities)
return RawModelData(..., attributes=registry)
```

Для стандартного велошеринга есть хелпер `register_bike_sharing_defaults()`, который регистрирует типичные атрибуты с типичными grain'ами. Для кастомных атрибутов — вызываешь `registry.register()` напрямую.

```python
# Кастомный атрибут с нестандартным grain
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

Weather_penalty автоматически пройдёт через time resolution, попадёт в edge spine и будет доступен потребителю. Никаких изменений в build pipeline.

---

## Сериализация

Registry сериализуется в обоих форматах:

**JSON** — spec как dict + data как список записей. Человекочитаемо.

**Parquet** — отдельная директория `attributes/` со спеками в `_specs.json` и данными в `.parquet` файлах. Эффективно.

Оба формата поддерживают round-trip fidelity — что записал, то и прочитал.

---

## Что не меняется при добавлении новых атрибутов

Вот список того, что стабильно:
- `AttributeSpec` — формат описания
- `auto_group_attributes()` — группировка по grain
- `plan_merges()` — порядок join'ов
- `AttributeBuilder.build_spines()` — сборка spine
- Все структурные таблицы и их Pydantic-схемы
- Шаги build_model(): validation, edge building, lead times, transformations, fleet capacity

Когда ты добавляешь новый атрибут — ты добавляешь только данные и spec. Всё остальное подхватывается автоматически.

---

## Главная идея

Attribute System — это о разделении **структуры** и **параметров**. Структура сети (какие объекты, как связаны, какие потоки) — фиксирована, это контракт. Параметры (стоимости, ёмкости, скорости) — динамические, определяются доменом.

Фиксированная структура + динамические параметры = модель, которая масштабируется на новые домены без переписывания кода.
