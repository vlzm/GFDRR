# Toy event tables for the seven single-bike scenarios

Each scenario follows **one bike** and shows the flow events it writes to the
journal (`gbp/model/flows.py`). The schema is `FLOW_EVENT_COLUMNS` (Notations.md
§0). The `event_type` here is a **richer split** of the canonical four outcomes
(see below) — it adds a time suffix and a redirect marker.

## Conventions

- **Stations:** `S` = source, `B` = planned target, `C` = realized target
  (the free station a redirect lands at). In a plain trip `B` is also where the
  bike docks; in a redirect the bike bounces off the full `B` and docks at `C`.
- **Periods are symbolic:** `t`, `t+k`, `t+k+m` (`k >= 1`, `m >= 1`), matching the
  scenario text. Read them as Int64 period ids.
- **Constant columns** on every user-trip row: `flow_type = user_trip`,
  `commodity_category = classic_bike`, `resource_id = NA` (resources are idle in
  the replay), `quantity = 1` (one bike after expansion).
- **`flow_id`** is the same on every event of one flow. A stockout has **no
  `flow_id`** (`NA`) — that demand never became a flow (Notations.md §3).
- **`NA`** marks a value that does not exist yet or never will.

## How the two ids place an event (Notations.md §0)

- `move_id` — arc index. A plain trip is one arc (`move_id = 0`). A redirect adds
  a second arc (`move_id = 1`): the continuation leg from the full `B` to `C`.
- `event_id` — event ordinal inside the trip. Row uniqueness is
  `(flow_id, event_id)`.
- A `departed` with `move_id == 0` is a **real user departure** (it is the `-1`
  to source inventory and the trip the OD model learns from). A `departed` with
  `move_id == 1` is a **redirect continuation leg** — pure transport, not demand,
  not outflow.

## The detailed `event_type` values

The canonical schema has four outcomes (`departed`, `arrived`, `redirected`,
`lost` — Notations.md §1). Here each is split by two extra facts:

- **Time suffix — relative to the flow's opening period `t`** (the period the user
  departed the source):
  - `_cur_period` — the event happens **in `t`** (the same period the bike left `S`).
  - `_prev_periods` — the event happens in a **period after `t`**; from the event's
    standpoint the source departure was in previous periods.
- **Redirect marker:** the infix `redirected` marks an event on a redirect's
  **second arc** (`move_id == 1`). The bounce itself keeps the stem `redirect`.

| Detailed `event_type` | Base | move_id | Docks? | reason | Meaning |
|---|---|---|---|---|---|
| `departed_cur_period` | departed | 0 | no (`-1`) | NA | User departure from `S`. Always in `t`, so always `cur_period`. |
| `departed_redirected_cur_period` | departed | 1 | no | NA | Continuation leg `B -> C` departs, flow opened **this** period. |
| `departed_redirected_prev_periods` | departed | 1 | no | NA | Continuation leg `B -> C` departs, flow opened in a **previous** period. |
| `redirect_cur_period` | redirected | 0 | no (bounce) | dock_full | Bounce off the full `B`, flow opened **this** period. |
| `redirect_prev_periods` | redirected | 0 | no (bounce) | dock_full | Bounce off the full `B`, flow opened in a **previous** period. |
| `arrived_cur_period` | arrived | 0 | **yes** (`+1`) | NA | Docks at the planned `B`, same period as departure. |
| `arrived_prev_periods` | arrived | 0 | **yes** (`+1`) | NA | Docks at the planned `B`, a later period than departure. |
| `arrived_redirected_cur_period` | arrived | 1 | **yes** (`+1`) | NA | Docks at `C` (after a redirect), flow opened **this** period. |
| `arrived_redirected_prev_periods` | arrived | 1 | **yes** (`+1`) | NA | Docks at `C` (after a redirect), flow opened in a **previous** period. |
| `lost` | lost | 0 | no | stockout / dock_full | A trip that did not happen / a bike that left the system. Stays one value. |

`realized_target_id` and `realized_end_period` are filled **only** on the row that
actually docks the bike (the terminal `arrived_*`); they are `NA` on every
`departed_*`, `redirect_*`, and `lost` row.

> These detailed values are a refinement of the four canonical `event_type`s; the
> current builders still emit the base four. The base outcome is always
> recoverable by dropping the suffix and the `redirected` marker.

---

## Scenario 1 — stockout at the source (period `t`)

The user wanted to depart from `S`, but there was no bike there. The demand never
became a flow: one `lost` event tagged `stockout`, no `flow_id`, no target.

| flow_id | move_id | event_id | period_id | flow_type | event_type | commodity_category | source_id | planned_target_id | realized_target_id | start_period | planned_end_period | realized_end_period | resource_id | quantity | reason |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| NA | 0 | 0 | t | user_trip | lost | classic_bike | S | NA | NA | NA | NA | NA | NA | 1 | stockout |

**Notes.** A stockout loss is aggregated per `(source_id, commodity_category)`, so
in a real run `quantity` is the count of lost trips and `flow_id`/target are `NA`.
For this single-bike toy `quantity = 1`. This is the source-side loss in
`demand = departed + lost(stockout)`.

---

## Scenario 2 — plain trip, dock at `B` same period (departs `t`, docks `t`)

`B` is both the planned and the realized target. One arc, two events. The arrival
is in `t`, the opening period, so it is `arrived_cur_period`.

| flow_id | move_id | event_id | period_id | flow_type | event_type | commodity_category | source_id | planned_target_id | realized_target_id | start_period | planned_end_period | realized_end_period | resource_id | quantity | reason |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| F1 | 0 | 0 | t | user_trip | departed_cur_period | classic_bike | S | B | NA | t | t | NA | NA | 1 | NA |
| F1 | 0 | 1 | t | user_trip | arrived_cur_period | classic_bike | S | B | B | t | t | t | NA | 1 | NA |

---

## Scenario 3 — plain trip, dock at `B` later (departs `t`, docks `t+k`)

Same as Scenario 2 but the ride spans `k` periods, so the arrival lands in a
period after `t` → `arrived_prev_periods`.

| flow_id | move_id | event_id | period_id | flow_type | event_type | commodity_category | source_id | planned_target_id | realized_target_id | start_period | planned_end_period | realized_end_period | resource_id | quantity | reason |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| F1 | 0 | 0 | t | user_trip | departed_cur_period | classic_bike | S | B | NA | t | t+k | NA | NA | 1 | NA |
| F1 | 0 | 1 | t+k | user_trip | arrived_prev_periods | classic_bike | S | B | B | t | t+k | t+k | NA | 1 | NA |

---

## Scenario 4 — redirect, everything in period `t`

Bike departs `S` in `t`, reaches the full `B` in `t` and bounces, then the
continuation leg `B -> C` departs and docks in `t`. The flow opens and closes in
`t`, so every event is a `*_cur_period`.

| flow_id | move_id | event_id | period_id | flow_type | event_type | commodity_category | source_id | planned_target_id | realized_target_id | start_period | planned_end_period | realized_end_period | resource_id | quantity | reason |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| F1 | 0 | 0 | t | user_trip | departed_cur_period | classic_bike | S | B | NA | t | t | NA | NA | 1 | NA |
| F1 | 0 | 1 | t | user_trip | redirect_cur_period | classic_bike | S | B | NA | t | t | t | NA | 1 | dock_full |
| F1 | 1 | 2 | t | user_trip | departed_redirected_cur_period | classic_bike | B | C | NA | t | t | NA | NA | 1 | NA |
| F1 | 1 | 3 | t | user_trip | arrived_redirected_cur_period | classic_bike | B | C | C | t | t | t | NA | 1 | NA |

**Notes.**
- On arc 1 the full station `B` becomes the `source_id` and `C` becomes the
  `planned_target_id`. The move-1 `departed_redirected_*` (event 2) is **not** a
  user departure; readers ignore it via `move_id == 0`.
- The whole redirect produces exactly one `+1`: the terminal `arrived_redirected_*`
  at `C`.

---

## Scenario 5 — redirect, second leg spans periods (bounce `t`, dock `C` at `t+k`)

First leg instant (bounce in `t`); the continuation leg `B -> C` departs in `t`
and docks in `t+k`. The flow still opened in `t`, so the bounce and the
continuation departure are `*_cur_period`, but the final docking lands after `t`
→ `arrived_redirected_prev_periods`.

| flow_id | move_id | event_id | period_id | flow_type | event_type | commodity_category | source_id | planned_target_id | realized_target_id | start_period | planned_end_period | realized_end_period | resource_id | quantity | reason |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| F1 | 0 | 0 | t | user_trip | departed_cur_period | classic_bike | S | B | NA | t | t | NA | NA | 1 | NA |
| F1 | 0 | 1 | t | user_trip | redirect_cur_period | classic_bike | S | B | NA | t | t | t | NA | 1 | dock_full |
| F1 | 1 | 2 | t | user_trip | departed_redirected_cur_period | classic_bike | B | C | NA | t | t+k | NA | NA | 1 | NA |
| F1 | 1 | 3 | t+k | user_trip | arrived_redirected_prev_periods | classic_bike | B | C | C | t | t+k | t+k | NA | 1 | NA |

**Notes.** The move-1 `departed_redirected_*` (event 2) has `realized_end_period =
NA` (it has not docked yet). This multi-period second arc goes **beyond** today's
`redirect_continuation_events`, which stamps both continuation rows into the bounce
period.

---

## Scenario 6 — redirect, first leg spans periods (bounce `t+k`, second leg instant)

Bike departs `S` in `t`, reaches the full `B` in `t+k` and bounces there, then the
continuation leg `B -> C` departs and docks in `t+k`. The flow opened in `t`, so
every event recorded in `t+k` is a `*_prev_periods`.

| flow_id | move_id | event_id | period_id | flow_type | event_type | commodity_category | source_id | planned_target_id | realized_target_id | start_period | planned_end_period | realized_end_period | resource_id | quantity | reason |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| F1 | 0 | 0 | t | user_trip | departed_cur_period | classic_bike | S | B | NA | t | t+k | NA | NA | 1 | NA |
| F1 | 0 | 1 | t+k | user_trip | redirect_prev_periods | classic_bike | S | B | NA | t | t+k | t+k | NA | 1 | dock_full |
| F1 | 1 | 2 | t+k | user_trip | departed_redirected_prev_periods | classic_bike | B | C | NA | t+k | t+k | NA | NA | 1 | NA |
| F1 | 1 | 3 | t+k | user_trip | arrived_redirected_prev_periods | classic_bike | B | C | C | t+k | t+k | t+k | NA | 1 | NA |

**Notes.** The user departure (event 0) still happens in `t`, so it stays
`departed_cur_period`. Everything from the bounce onward is in `t+k`, a period
after the opening, hence `*_prev_periods`. The continuation arc starts and ends in
`t+k`, so this scenario is producible by the current builders as written.

---

## Scenario 7 — redirect, both legs span periods (bounce `t+k`, dock `C` at `t+k+m`)

Bike departs `S` in `t`, reaches the full `B` in `t+k` and bounces, then the
continuation leg `B -> C` departs in `t+k` and docks in `t+k+m`. Only the opening
departure is in `t`; everything else is `*_prev_periods`.

| flow_id | move_id | event_id | period_id | flow_type | event_type | commodity_category | source_id | planned_target_id | realized_target_id | start_period | planned_end_period | realized_end_period | resource_id | quantity | reason |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| F1 | 0 | 0 | t | user_trip | departed_cur_period | classic_bike | S | B | NA | t | t+k | NA | NA | 1 | NA |
| F1 | 0 | 1 | t+k | user_trip | redirect_prev_periods | classic_bike | S | B | NA | t | t+k | t+k | NA | 1 | dock_full |
| F1 | 1 | 2 | t+k | user_trip | departed_redirected_prev_periods | classic_bike | B | C | NA | t+k | t+k+m | NA | NA | 1 | NA |
| F1 | 1 | 3 | t+k+m | user_trip | arrived_redirected_prev_periods | classic_bike | B | C | C | t+k | t+k+m | t+k+m | NA | 1 | NA |

**Notes.** The most general redirect: first arc takes `k` periods, continuation arc
takes `m` periods. Like Scenario 5, the multi-period continuation arc goes beyond
the current same-period `redirect_continuation_events`.

---

## Cross-scenario summary

| Scenario | Events | Terminal `event_type` | Redirect? | Spans periods |
|---|---|---|---|---|
| 1 | 1 | `lost` (stockout) | no | — |
| 2 | 2 | `arrived_cur_period` | no | no |
| 3 | 2 | `arrived_prev_periods` | no | first arc (`k`) |
| 4 | 4 | `arrived_redirected_cur_period` | yes | no |
| 5 | 4 | `arrived_redirected_prev_periods` | yes | second arc (`k`) |
| 6 | 4 | `arrived_redirected_prev_periods` | yes | first arc (`k`) |
| 7 | 4 | `arrived_redirected_prev_periods` | yes | both arcs (`k`, `m`) |

**The suffix rule in one line.** `cur_period` vs `prev_periods` is decided once per
event by comparing its `period_id` with the flow's opening period `t` (the user
departure from `S`): equal → `cur_period`, later → `prev_periods`. The user
departure is always `cur_period` by definition; only later events can be
`prev_periods`.

**Implementation note.** Scenarios 5 and 7 (a continuation leg that spans more than
one period) are not producible by today's `redirect_continuation_events`, which
forces the whole second arc into the bounce period. They are included because the
scenario list explores that timing; supporting them would mean letting the
continuation arc carry its own `start_period` / `planned_end_period` /
`realized_end_period` instead of collapsing them to `period_id`.
