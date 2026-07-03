Разобрался в коде. Объясняю фазу `DockArrivals` подробно — как программисту, с разбором всех внутренних методов, которые она вызывает.

## Что делает фаза в целом

`DockArrivals` — это один шаг периода: **припарковать (пристыковать) велосипеды, которые приезжают в этот период**, и разослать те, что не поместились, на соседние станции.

Важно, что фаза запускается **дважды** за период, с разным значением `when`:

- `when="previous"` — стыкует велосипеды, которые выехали в **прошлые** периоды и приезжают сейчас. Запускается **до** формирования отправлений.
- `when="same"` — стыкует велосипеды, которые выехали и приехали **внутри этого же** периода. Запускается **после** формирования отправлений.

Порядок шагов периода: пристыковать ранние приезды → сформировать отправления → построить поездки → пристыковать приезды этого же периода.

Ключевая мысль из докстринга: при точном воспроизведении истории доки никогда не переполняются, поэтому лимит вместимости и перенаправление ничего не делают. Они включаются только когда трафик выше исторического.

## `__init__(self, when, schedule)`

```91:96:gbp/consumers/simulator/phases.py
    def __init__(self, when: str, schedule: Schedule | None = None) -> None:
        super().__init__(schedule)
        if when not in ("previous", "same"):
            raise ValueError(f"when must be 'previous' or 'same', got {when!r}")
        self.when = when
        self.name = f"dock_arrivals_{when}"
```

Проверяет, что `when` допустимый, сохраняет его и формирует имя фазы (`dock_arrivals_previous` или `dock_arrivals_same`). `schedule` (из базового класса) решает, в какие периоды фаза вообще запускается; по умолчанию — каждый период.

## `execute` — по секциям

Метод разбит на пять частей: **Reads → Mechanics → штамповка step_id → Writes → Check**. Разберу каждую.

### 1. Reads (строки 106–119) — что читаем

```108:119:gbp/consumers/simulator/phases.py
        t = period.period_id
        it = state.in_transit
        arrived_now = it["planned_end_period"] == t
        if self.when == "previous":
            due = it[arrived_now & (it["start_period"] < t)]
        else:
            due = it[arrived_now & (it["start_period"] == t)]
        if due.empty:
            return PhaseResult.empty(state)
        capacities = resolved.facilities_capacities_df
        inventory = state.state_inventory_df
        inventory_before = int(inventory["quantity"].sum())
```

- `it` — это `in_transit`: велосипеды, которые уже выехали (`departed`), но ещё не пристыковались. Это внутренняя проекция состояния, а не журнал.
- `arrived_now` — маска: у поездки плановый период приезда равен текущему `t`.
- `due` — велосипеды, которые надо стыковать именно сейчас. Разделение по `when` идёт через сравнение `start_period` с `t`: строго раньше (`previous`) или равно (`same`).
- Если стыковать некого — возвращается «пустой» результат (состояние не меняется).
- `inventory_before` — суммарный склад до фазы; понадобится в финальной проверке.

### 2. Mechanics (строки 121–162) — сама механика

Здесь фаза вызывает три функции из `mechanics.py`. Разберу их отдельно ниже, а сначала логику самой фазы.

```124:127:gbp/consumers/simulator/phases.py
        free = free_docks(inventory, capacities)
        docked, overflow = dock_up_to_capacity(due, free)
        new_flows = arrived_events(docked, t)
        inventory = adjust_inventory(inventory, dock_deltas(docked))
```

- `free_docks` считает, сколько свободных мест в доках на каждой станции.
- `dock_up_to_capacity` делит `due` на `docked` (поместились на своей плановой станции) и `overflow` (не поместились).
- `arrived_events(docked, t)` создаёт события «приехал» — журнальные записи о том, что велосипед пристыковался.
- `adjust_inventory(...)` прибавляет по +1 к складу на каждой станции, где велосипед пристыковался.

Дальше — обработка переполнения:

```129:162:gbp/consumers/simulator/phases.py
        if not overflow.empty:
            redirected, lost = plan_overflow_redirect(
                inventory, capacities, resolved.facilities_geo_df, overflow
            )
            ...
            if not redirected.empty:
                round_by_flow = redirected.set_index("flow_id")["phase_round"]
                bounce = redirected_events(redirected, t)
                bounce["phase_round"] = bounce["flow_id"].map(round_by_flow)
                continuation = redirect_continuation_events(redirected, t)
                continuation["phase_round"] = continuation["flow_id"].map(round_by_flow)
                new_flows = pd.concat([new_flows, bounce, continuation], ignore_index=True)
                inventory = adjust_inventory(
                    inventory, dock_deltas(redirected, "realized_target_id")
                )
            if not lost.empty:
                new_flows = pd.concat(
                    [new_flows, lost_events(lost, t, "dock_full")], ignore_index=True,
                )
```

`plan_overflow_redirect` возвращает две группы: `redirected` (велосипеды, которым нашли свободную станцию) и `lost` (для которых свободного дока нет нигде — сеть полностью забита).

**Главная идея тут — перенаправление это две дуги (arcs):**

1. `bounce` (событие `redirected`) — велосипед доехал до плановой станции B, но она полна, поэтому «отскочил» и **не** пристыковался там.
2. `continuation` (события `redirect_continuation_events`) — вторая дуга B→C: это `departed` + `arrived` в этом же периоде. Именно `arrived` в C стыкует велосипед (единственный +1 склада во всём перенаправлении).

Поэтому склад увеличивают только `docked` и `redirected` (по колонке `realized_target_id` — куда велосипед реально доехал). `lost` склад **не** трогает: велосипед уже покинул стартовую станцию при `departed`, а стыкуется теперь нигде.

`phase_round` переносится с плана перенаправления на события, чтобы позже раунды упорядочились как отдельные шаги (об этом ниже).

### 3. Штамповка `phase_rank` и `step_id` (строки 164–187)

```168:187:gbp/consumers/simulator/phases.py
        new_flows["phase_rank"] = DOCK_PREVIOUS_RANK if self.when == "previous" else DOCK_SAME_RANK
        ...
        if "phase_round" in new_flows.columns:
            rounds = new_flows["phase_round"].fillna(0).astype("int64")
        else:
            rounds = pd.Series(0, index=new_flows.index, dtype="int64")
        new_flows = new_flows.copy()
        new_flows["phase_round"] = rounds
        working = state
        step_ids = pd.Series(0, index=new_flows.index, dtype="int64")
        for r in sorted(rounds.unique()):
            sid, working = working.open_step()
            step_ids[rounds == r] = sid
        new_flows["step_id"] = step_ids
```

Три колонки задают строгий порядок событий в журнале:

- `phase_rank` — какая это фаза внутри периода. Значения из `flows.py`: `DOCK_PREVIOUS_RANK = 0`, `PERIOD_OWN_RANK = 1`, `DOCK_SAME_RANK = 2`. То есть ранние приезды идут до отправлений, приезды этого же периода — после.
- `phase_round` — номер раунда перенаправления. `0` — плановая стыковка (и dock-full-потери, которые стыкуются нигде), `1, 2, …` — раунды перенаправления.
- `step_id` — глобальный сквозной номер «шага склада». Фаза открывает **отдельный шаг на каждый раунд**: цикл идёт по отсортированным раундам и для каждого вызывает `working.open_step()`, который выдаёт следующий номер из счётчика в состоянии.

Почему номер берётся из счётчика, а не из колонок: так два разных упорядоченных изменения склада **никогда** не схлопнутся в один шаг, даже если у них совпадают `(period_id, phase_rank, phase_round)`. Это требование `Notations.md §0.1`.

### 4. Writes (строки 189–192) — что записываем

```191:192:gbp/consumers/simulator/phases.py
        in_transit = state.in_transit.drop(due.index)
        new_state = working.with_inventory(inventory).with_in_transit(in_transit)
```

- Пристыкованные велосипеды убираются из `in_transit` (они больше не в пути).
- `working` (у которого счётчик шагов уже сдвинут) получает новый склад и новый `in_transit`.

Обратите внимание: состояние **иммутабельно**. `with_inventory` и `with_in_transit` возвращают копию через `dataclasses.replace`, ничего не мутируя на месте.

### 5. Check (строки 194–200) — инварианты

```197:200:gbp/consumers/simulator/phases.py
        assert len(docked) + n_redirected + n_lost == len(due), "due flows not conserved"
        assert int(inventory["quantity"].sum()) - inventory_before == len(docked) + n_redirected, (
            "lost or redirected count moved inventory incorrectly"
        )
```

- Каждый прибывающий велосипед ровно один раз либо пристыковался, либо перенаправлен, либо потерян (сохранение количества).
- Склад вырос ровно на `docked + redirected`, но **не** на потерянные.

Возвращается `PhaseResult(new_state, new_flows)` — новое состояние и пакет новых событий (журнал дополняет уже движок, а не фаза).

## Внутренние методы механики

### `free_docks(inventory, capacities)`

```46:50:gbp/consumers/simulator/mechanics.py
    occupied = inventory.groupby("facility_id")["quantity"].sum()
    capacity = capacities.set_index("facility_id")["capacity"]
    idx = capacity.index.union(occupied.index)
    free = capacity.reindex(idx).fillna(0) - occupied.reindex(idx).fillna(0)
    return free.clip(lower=0).astype("int64")
```

Свободные места = вместимость − занято. Классические и электрические велосипеды делят одни и те же физические доки, поэтому «занято» — это суммарный склад по всем товарным категориям. Результат обрезается снизу нулём.

### `dock_up_to_capacity(due, free)`

```73:79:gbp/consumers/simulator/mechanics.py
    rank = due.groupby("planned_target_id").cumcount()
    capacity_here = due["planned_target_id"].map(free).fillna(0)
    fits = rank < capacity_here
    docked_n = due[fits].groupby("planned_target_id").size()
    assert (docked_n <= free.reindex(docked_n.index).fillna(0)).all(), "docked over capacity"
    return due[fits], due[~fits]
```

Делит поездки на «поместились / переполнение» **без Python-цикла**. Внутри каждой плановой станции строкам присваивается порядковый номер (`cumcount`), и первые `free` штук стыкуются, остальные — переполнение. Затем ассерт: ни одна станция не пристыковала больше, чем у неё свободных мест.

### `plan_overflow_redirect(inventory, capacities, geo, overflow)`

Это **решение, а не изменение состояния**: функция говорит, *куда* поедет каждый переполненный велосипед, а применение (склад, события) остаётся за фазой.

```164:188:gbp/consumers/simulator/mechanics.py
    while not remaining.empty:
        phase_round += 1
        free = free_docks(running, capacities)
        if not (free > 0).any():
            break
        target = _nearest_free_station(remaining["planned_target_id"], free, geo)
        candidate = remaining.assign(realized_target_id=target)
        candidate = candidate[candidate["realized_target_id"].notna()]
        if candidate.empty:
            break
        rank = candidate.groupby("realized_target_id").cumcount()
        fits = rank < candidate["realized_target_id"].map(free)
        docked = candidate[fits].assign(phase_round=phase_round)
        redirected_batches.append(docked)
        running = adjust_inventory(running, dock_deltas(docked, "realized_target_id"))
        remaining = candidate[~fits].drop(columns="realized_target_id")
```

Работает **раундами**, а не по одной строке:

1. Каждый раунд считает свободные доки на **локальной копии** склада (`running`).
2. Находит ближайшую свободную станцию для каждого ещё не пристыкованного велосипеда.
3. Стыкует до вместимости той станции (та же логика `cumcount < free`).
4. Обновляет локальный склад и повторяет с остатком.

Цикл останавливается, когда либо все пристыкованы, либо свободных доков нет нигде (тогда остаток — потери).

Зачем раунды: если в одном раунде велосипед занял место в C, следующий раунд должен это увидеть. `phase_round` (1, 2, …) записывает, в каком раунде пристыковался велосипед, чтобы порядок стыковок был согласован со складом. Локальная копия `running` наружу не выходит.

`_nearest_free_station` — вспомогательная функция: для каждой станции находит ближайшую **другую** станцию со свободным доком. Расстояние — квадрат евклидова по (lat, lng), считается через cross-join всех целей со всеми кандидатами и выбор минимума. Это упрощение уровня прототипа.

## Функции-события (что попадает в журнал)

- `arrived_events` — «велосипед пристыковался на своей плановой станции» (move 0, event 1). `realized_target_id = planned_target_id`. Это стыкующее событие (+1 склада).
- `redirected_events` — «отскок» от полной станции B (move 0, event 1). `realized_target_id = NA` — здесь велосипед **не** стыкуется, только фиксируется, что B была полна.
- `redirect_continuation_events` — вторая дуга: две строки на велосипед, `departed` (move 1, event 2) и `arrived` (move 1, event 3), обе в этом же периоде. Именно этот `arrived` стыкует велосипед в C. Move-1 `departed` — **не** пользовательское отправление, поэтому читатели, считающие отправления, фильтруют `move_id == 0`.
- `lost_events(..., "dock_full")` — «велосипед выехал, но не пристыковался нигде» (одна строка на flow, quantity=1). Склад не трогает.

## Итоговая картина

```
due (кого стыкуем)
  ├── dock_up_to_capacity
  │     ├── docked   → arrived_events, +1 склад
  │     └── overflow → plan_overflow_redirect
  │                      ├── redirected → redirected_events (отскок, NA)
  │                      │              + redirect_continuation_events (B→C, +1 склад в C)
  │                      └── lost       → lost_events("dock_full"), склад не трогает
  │
  └── все убираются из in_transit
```

Три уровня разделены чётко: `mechanics` считает решения на «голых» датафреймах и ничего не знает про состояние и журнал; `phases` применяет решения к живому состоянию и создаёт события; движок дописывает события в журнал. Зависимости идут в одну сторону: `journal ← state ← mechanics ← phases ← engine`.

Хочешь, чтобы я так же разобрал соседние фазы (`FormDeparturesPhase`, `FormPotentialTripsPhase`) — они вместе с этой составляют один период?