Фаза `DockArrivals` закрывает поездки, которые должны приехать в текущем периоде: ставит велосипеды в свободные доки, переправляет лишние на ближайшую станцию со свободным доком, а если доков нет нигде, пишет потерю `lost`.

Она запускается два раза за период: `DockArrivals("previous")` до новых отъездов и `DockArrivals("same")` после них.

```72:96:gbp/consumers/simulator/phases.py
class DockArrivals(Phase):
    """Dock the bikes that arrive this period, and redirect any that do not fit.
    ...
    def __init__(self, when: str, schedule: Schedule | None = None) -> None:
        super().__init__(schedule)
        if when not in ("previous", "same"):
            raise ValueError(f"when must be 'previous' or 'same', got {when!r}")
        self.when = when
        self.name = f"dock_arrivals_{when}"
```

## Общая Модель
Главные таблицы:

`state.in_transit` — поездки, которые уже уехали, но ещё не закрылись прибытием. Обычно это строки `departed`.

`state.state_inventory_df` — текущие велосипеды на станциях: `facility_id`, `commodity_category`, `quantity`.

`new_flows` — новые строки журнала событий. Фаза сама не добавляет их в общий журнал. Она возвращает `PhaseResult`, а движок потом делает `append_flows`.

`due` — строки из `in_transit`, которые должны приехать именно сейчас.

## Шаг 1: Выбрать Поездки, Которые Приехали Сейчас
```108:116:gbp/consumers/simulator/phases.py
t = period.period_id
it = state.in_transit
arrived_now = it["planned_end_period"] == t
if self.when == "previous":
    due = it[arrived_now & (it["start_period"] < t)]
else:
    due = it[arrived_now & (it["start_period"] == t)]
if due.empty:
    return PhaseResult.empty(state)
```

Здесь `t` — текущий период.

Если `when == "previous"`, фаза берёт поездки, которые стартовали раньше текущего периода и приехали сейчас. Они должны встать в док до новых отъездов, чтобы новые отъезды видели правильный инвентарь.

Если `when == "same"`, фаза берёт короткие поездки, которые стартовали и приехали в том же периоде. Они закрываются после формирования отъездов.

Если `due` пустой, фаза ничего не меняет.

## Шаг 2: Посчитать Свободные Доки
```124:127:gbp/consumers/simulator/phases.py
free = free_docks(inventory, capacities)
docked, overflow = dock_up_to_capacity(due, free)
new_flows = arrived_events(docked, t)
inventory = adjust_inventory(inventory, dock_deltas(docked))
```

`free_docks(inventory, capacities)` считает свободные места по станции:

```28:50:gbp/consumers/simulator/mechanics.py
def free_docks(inventory: pd.DataFrame, capacities: pd.DataFrame) -> pd.Series:
    """Free dock slots per facility: capacity minus bikes currently docked.
    ...
    occupied = inventory.groupby("facility_id")["quantity"].sum()
    capacity = capacities.set_index("facility_id")["capacity"]
    idx = capacity.index.union(occupied.index)
    free = capacity.reindex(idx).fillna(0) - occupied.reindex(idx).fillna(0)
    return free.clip(lower=0).astype("int64")
```

Важно: доки общие для всех типов велосипедов. Поэтому `occupied` суммирует `quantity` по `facility_id`, без разделения по `commodity_category`.

`dock_up_to_capacity(due, free)` делит прибывшие поездки на две группы:

```53:79:gbp/consumers/simulator/mechanics.py
def dock_up_to_capacity(due: pd.DataFrame, free: pd.Series) -> tuple[pd.DataFrame, pd.DataFrame]:
    ...
    rank = due.groupby("planned_target_id").cumcount()
    capacity_here = due["planned_target_id"].map(free).fillna(0)
    fits = rank < capacity_here
    ...
    return due[fits], due[~fits]
```

Для каждой станции назначения `planned_target_id` код нумерует прибывшие строки через `cumcount`: первая строка получает `0`, вторая `1`, и так дальше. Если номер строки меньше числа свободных доков, велосипед помещается. Остальные попадают в `overflow`.

`arrived_events(docked, t)` строит события `arrived` для тех, кто встал на свою плановую станцию. В этих событиях `realized_target_id == planned_target_id`.

`dock_deltas(docked)` строит изменения инвентаря: `+1` на станцию прибытия и тип велосипеда. Потом `adjust_inventory` применяет эти изменения к `inventory`.

## Шаг 3: Обработать Overflow
Если на плановой станции места не хватило, работает `plan_overflow_redirect`.

```129:153:gbp/consumers/simulator/phases.py
if not overflow.empty:
    redirected, lost = plan_overflow_redirect(
        inventory, capacities, resolved.facilities_geo_df, overflow
    )
    n_redirected, n_lost = len(redirected), len(lost)
    if not redirected.empty:
        ...
        inventory = adjust_inventory(
            inventory, dock_deltas(redirected, "realized_target_id")
        )
```

Обрати внимание: в `plan_overflow_redirect` передаётся уже обновлённый `inventory`, где обычные `docked` уже заняли доки. Это важно, потому что редирект должен видеть оставшиеся места после обычной стыковки.

`plan_overflow_redirect` делает несколько раундов:

```155:188:gbp/consumers/simulator/mechanics.py
redirected_batches = []
remaining = overflow
running = inventory
...
while not remaining.empty:
    phase_round += 1
    free = free_docks(running, capacities)
    if not (free > 0).any():
        break
    target = _nearest_free_station(remaining["planned_target_id"], free, geo)
    candidate = remaining.assign(realized_target_id=target)
    candidate = candidate[candidate["realized_target_id"].notna()]
    ...
    docked = candidate[fits].assign(phase_round=phase_round)
    redirected_batches.append(docked)
    running = adjust_inventory(running, dock_deltas(docked, "realized_target_id"))
    remaining = candidate[~fits].drop(columns="realized_target_id")
```

Смысл такой:

1. Берём все ещё не размещённые велосипеды `remaining`.
2. Считаем свободные доки по текущему локальному `running`.
3. Для каждого велосипеда ищем ближайшую другую станцию со свободным доком через `_nearest_free_station`.
4. Если несколько велосипедов выбрали одну и ту же станцию, туда помещаются только первые `free` строк.
5. Эти строки получают `realized_target_id` и `phase_round`.
6. Локальный `running` обновляется.
7. Остаток идёт в следующий раунд.

Если свободных доков нет нигде, остаток становится `lost`.

## Шаг 4: Записать События Redirect
Для редиректа пишутся не одно, а три события на поток.

```142:150:gbp/consumers/simulator/phases.py
round_by_flow = redirected.set_index("flow_id")["phase_round"]
bounce = redirected_events(redirected, t)
bounce["phase_round"] = bounce["flow_id"].map(round_by_flow)
continuation = redirect_continuation_events(redirected, t)
continuation["phase_round"] = continuation["flow_id"].map(round_by_flow)
new_flows = pd.concat(
    [new_flows, bounce, continuation],
    ignore_index=True,
)
```

`redirected_events` пишет `redirected`: велосипед доехал до плановой станции, но не встал в док, потому что доки полные.

`redirect_continuation_events` пишет вторую дугу поездки: `departed` от полной станции и `arrived` на найденную свободную станцию.

То есть редирект выглядит так:

`departed` уже был раньше → `redirected` на полной станции → `departed` для продолжения → `arrived` на новой станции.

Второй `departed` имеет `move_id == 1`, поэтому он не считается пользовательским отъездом и не уменьшает инвентарь. Единственное изменение инвентаря при редиректе — это `+1` на `realized_target_id`.

## Шаг 5: Записать Lost
```154:162:gbp/consumers/simulator/phases.py
if not lost.empty:
    new_flows = pd.concat(
        [new_flows, lost_events(lost, t, "dock_full")],
        ignore_index=True,
    )
```

`lost_events(lost, t, "dock_full")` пишет событие `lost` с причиной `dock_full`.

Инвентарь не меняется: велосипед уже уехал со станции в событии `departed`, но теперь не смог встать никуда.

## Шаг 6: Проставить Порядок Фазы И Шага
```168:187:gbp/consumers/simulator/phases.py
new_flows["phase_rank"] = DOCK_PREVIOUS_RANK if self.when == "previous" else DOCK_SAME_RANK
...
for r in sorted(rounds.unique()):
    sid, working = working.open_step()
    step_ids[rounds == r] = sid
new_flows["step_id"] = step_ids
```

`phase_rank` говорит, где событие стоит внутри периода:

`0` — `DockArrivals("previous")`.

`2` — `DockArrivals("same")`.

`phase_round` говорит, какой это раунд внутри фазы. Обычная стыковка получает `0`. Редиректы получают `1`, `2`, и так дальше.

`step_id` — глобальный номер шага изменения инвентаря. Он берётся через `state.open_step()`, а не вычисляется из колонок. Поэтому два разных раунда не могут случайно получить один и тот же шаг.

## Шаг 7: Вернуть Новый State
```189:201:gbp/consumers/simulator/phases.py
in_transit = state.in_transit.drop(due.index)
new_state = working.with_inventory(inventory).with_in_transit(in_transit)
...
return PhaseResult(new_state, new_flows)
```

Все строки `due` удаляются из `in_transit`: они уже либо встали в док, либо были перенаправлены и встали в док, либо потеряны.

Фаза возвращает:

`new_state` — новый инвентарь, новый `in_transit`, обновлённый счётчик `next_step_id`.

`new_flows` — события, которые движок потом добавит в журнал.

В конце стоят две проверки:

```194:199:gbp/consumers/simulator/phases.py
assert len(docked) + n_redirected + n_lost == len(due), "due flows not conserved"
assert int(inventory["quantity"].sum()) - inventory_before == len(docked) + n_redirected, (
    "lost or redirected count moved inventory incorrectly"
)
```

Первая проверяет, что каждый прибывший поток попал ровно в один исход: `docked`, `redirected` или `lost`.

Вторая проверяет инвентарь: он должен вырасти только на реально припаркованные велосипеды. Потерянные велосипеды инвентарь не увеличивают.