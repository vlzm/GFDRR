# Architecture Map

> Живая одностраничная карта проекта. Читать каждое утро перед работой (2 мин).
> Обновляется **только** когда меняется что-то из разделов «Центральная абстракция»,
> «Консумеры», «Инварианты», «Текущий фокус» или «Зоны риска».
>
> Это НЕ документация. Это протез рабочей памяти.

---

## Центральная абстракция

**`ResolvedModelData`** — плоский data bag из ~52 DataFrame-полей + `AttributeRegistry`.
Строится один раз из `RawModelData` через `build_model(raw)` и передаётся всем консумерам.

- Raw = **декларация** (что пользователь задал: правила, даты, ставки)
- Resolved = **материализация** (что получилось: рёбра, period_id, lead_time в периодах)

Всё. Если какой-то код строит что-то ещё поверх — это либо консумер, либо ошибка слоя.

Файл: [gbp/core/model.py](../gbp/core/model.py)
Группы полей (см. `_GROUPS`): `entity`, `temporal`, `behavior`, `edge`, `flow_data`,
`observations`, `transformation`, `resource`, `hierarchy`, `scenario` (+ `generated` у Resolved).

---

## Консумеры

| Консумер | Назначение | Вход | Выход |
|---|---|---|---|
| **Environment** (`gbp/consumers/simulator/`) | Пошаговая симуляция / digital twin | `ResolvedModelData` | `SimulationLog` (5 таблиц) |
| **Rebalancer** (`gbp/rebalancer/`) | VRP / PDP для ночной ребалансировки. **Будет переделан в `Task` внутри Environment** | `ResolvedModelData` + date | `PdpModel` dict |
| **Strategic Optimizer** (не существует) | LP/MILP на весь горизонт, one-shot | `ResolvedModelData` | план потоков |

**Правило:** один Resolved — много консумеров. Консумеры **не** мутируют Resolved и **не** знают, как он построен.

---

## Инварианты (НЕ нарушать)

1. **Nullable = LP-compatible.** Дискретные поля (`min_shipment`, `batch_size`) nullable. Null → LP, задано → MILP.
2. **Абсолютные единицы + resolution.** Время хранится в часах/датах, `period_id` рождается в build.
3. **Ортогональные измерения.** `FacilityType`, `OperationType`, `FacilityRole` — независимые оси. Role выводится из type + operations.
4. **Edge identity = `source_id × target_id × modal_type`.** Не забывать третью колонку.
5. **AttributeRegistry с явным grain.** Параметрика регистрируется с tuple-grain; grain определяет сборку в spine.
6. **Один Resolved на всех.** Environment, Optimizer, Analytics — одинаковый вход.
7. **Immutable state.** `SimulationState` — frozen; мутации только через `with_*` хелперы.

Пайплайн: **Raw → `build_model()` → Resolved → Consumer.** Больше ничего.

---

## Текущий фокус

> **Одна задача. Не список.** Если здесь больше одной строки — ты распыляешься.

**Rebalancer as Task** — первая реальная `Task` внутри Environment.
Начинается с design doc (следуя правилу «design doc → discussion → code»).

Существующий `gbp/rebalancer/` — прототип, переписывается под `Task`-протокол
([gbp/consumers/simulator/task.py](../gbp/consumers/simulator/task.py)).

Всё остальное из `PROJECT_STATE.md § Not Now` — **не трогаем**.

---

## Зоны риска (здесь меняется сразу в нескольких местах)

Когда правишь эти места — сделай паузу и проверь, не поехало ли в соседних файлах.

| Что меняешь | Где ломается |
|---|---|
| Добавить поле в `ResolvedModelData` | `model.py` + `_GROUPS` + `_SCHEMAS` + `from_raw` + соответствующая `schemas/*.py` + build step + консумер |
| Колонка в существующей таблице | её `schemas/*.py` + `columns.py` + build step, который её создаёт + все консумеры, которые её читают |
| Новая `Phase` | `phases.py`-регистрация + engine/config + тест + (опционально) `SimulatorView` |
| Новая Task | `task.py` — соответствие `DISPATCH_COLUMNS`, всё остальное — внутри Task |
| Новый компат-справочник (типа `resource_*_compatibility`) | schema + build validation + консумер-валидатор |

Если добавление поля требует правок в **5+ файлах** — это change amplification, подумай про абстракцию. Но **только после** того, как боль стала реальной, не заранее.

---

## Узкие места, про которые легко забыть

- **`resource_category_id` vs `resource_category`** — первое в справочнике, второе в `state.resources` и compat-таблицах. Двухступенчатый lookup.
- **`edge_lead_time_resolved`** — рождается в build, в Raw его нет. Консумер читает только из Resolved.
- **`period_id` ≠ `period_index`.** Первое — id в `periods`, второе — порядковый номер в симуляции.
- **`inventory_in_transit`** живёт в `SimulationState`, не в `ResolvedModelData`. Raw-поле `inventory_in_transit` — это начальное состояние, не живое.
- **`AttributeRegistry`** — параметрика (costs, capacities) НЕ в полях `ResolvedModelData`, а в `.attributes`. Доступ через `resolved.attributes.get(name).data` или `resolved.parameter_tables`.

---

## Процессные правила (для меня, не для архитектуры)

1. **Одна задача → одна ветка → один PR → один день.** Если не влезает — резать.
2. **Заморозка архитектуры на 2 недели** при любой новой фиче. Никаких views, протоколов, рефакторингов в том же PR.
3. **Сначала ноутбук в `notebooks/verify/`, потом код в `gbp/`.** Ноутбук — sandbox.
4. **После AI-сессии 15 мин читать дифф самому.** Без этого нет модели кода в голове.
5. **Дублирование > неправильная абстракция.** Три похожие строки — это ещё не повод для класса.

---

## Куда смотреть, если...

| Вопрос | Файл |
|---|---|
| Какие таблицы бывают | [gbp/core/model.py](../gbp/core/model.py) |
| Что в каждой таблице | [gbp/core/schemas/](../gbp/core/schemas/) |
| Как Raw превращается в Resolved | [gbp/build/pipeline.py](../gbp/build/pipeline.py) |
| Как движется симуляция | [gbp/consumers/simulator/engine.py](../gbp/consumers/simulator/engine.py) |
| Что сейчас делаем и что НЕ делаем | [PROJECT_STATE.md](../PROJECT_STATE.md) |
| Данные end-to-end | [docs/DATA_JOURNEY.md](DATA_JOURNEY.md) |
| Диаграммы архитектуры | [docs/architecture_diagrams.md](architecture_diagrams.md) |
