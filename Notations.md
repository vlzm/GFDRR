# Notations — the canonical vocabulary

This file is the project's dictionary: **one concept, one word**. When code, a
docstring, or a chat answer needs to name something in this domain, it uses the
word listed here — not a synonym.

**Why this exists.** The same thing was being called by several names (`stock`
and `inventory`; `shortfall`, `lost`, and "the missing part"; `placed` and
`redirected`). Different words for one thing make the code harder to read and
impossible for a tool to check. This file fixes one word per concept so both
people and Claude can rely on it.

**Source of the vocabulary.** The flow journal (`gbp/model/journal.py`) is the
single source of truth for what happened in a run and the layer "the whole
system speaks." So the canonical word for any concept is the word the journal
uses. Everything below is anchored there.

**How to use it.**
- *Writing code:* use these words for variables, columns, functions, docstrings.
- *Talking to the user:* use these words (translated) instead of inventing new ones.
- *Reviewing code:* anything that names a listed concept with a listed "avoid"
  word is a drift to fix.
- *Missing a word?* Add the concept here first, then use it — never coin a
  synonym in passing.

---

## 1. The four flow outcomes

A *flow* is one bike's movement. Each flow ends in exactly one of four outcomes.
These are the journal's `event_type` values — the most important words here.

| Canonical | Meaning | Builder | Avoid |
|---|---|---|---|
| `departed` | A bike left its source station. Opens the flow. | `departed_events` | `dispatched`, `released` |
| `arrived` | A bike docked at its planned target. | `arrived_events` | — |
| `redirected` | A bike docked at a *different* station because the planned target was full. | `redirected_events` | `placed`, `rerouted` |
| `lost` | A trip that did not happen / a bike that left the system. | `lost_events` | `shortfall`, `missing`, `dropped`, `failed` |

*Dock* is the verb for landing a bike; both `arrived` and `redirected` dock a
bike. When you mean one specific outcome, use its event word, not `docked`.

The `reason` field says *why* a flow did not simply arrive:

- `stockout` — demand that never departed (no bike at the source). Always `lost`.
- `dock_full` — the planned target had no free dock. If another station had one,
  the outcome is `redirected`; if none did, it is `lost`.

So demand splits exactly into `departed + lost(stockout)`, and every departed
flow ends as `arrived`, `redirected`, or `lost(dock_full)`.

---

## 2. Core state

| Canonical | Meaning | Avoid |
|---|---|---|
| `inventory` | Bikes currently docked at stations (the amount on hand). Columns `facility_id`, `commodity_category`, `quantity`. | `stock`, `on-hand` |
| `in_transit` | Bikes that departed but have not yet docked (the working set). | "moving set", "moving bikes" |
| `demand` | The number of trips users wanted. `demand = departed + lost(stockout)`. | — |
| `supply` | Inventory in its "available to depart" role (`state_supply_df`). A *role view* of `inventory`, not a second word for it — do not use `supply` to mean the inventory table in general. | — |

`stock` is the main offender: the fundamental thing is `inventory`, so never
write `stock` / `stock_before` — write `inventory` / `inventory_before`.

---

## 3. Flow, trip, bike

Three levels of the same physical movement. Keep them distinct.

| Canonical | Level | Meaning |
|---|---|---|
| `trip` | demand / aggregate | A journey a user wants to take. Lives in counts and the OD matrix (`potential_trips`). |
| `flow` | journal | One trip recorded in the journal, identified by `flow_id`. The atomic journal unit. |
| `bike` | physical | One unit. After expansion each flow moves one bike (`quantity = 1`). |

Rule of thumb: aggregate counts and the OD matrix = `trip`; anything with a
`flow_id` = `flow`; the physical count = `bike`.

---

## 4. Facility and its roles in a trip

| Canonical | Meaning | Avoid |
|---|---|---|
| `facility` | A node in the network — a bike station. Identifier `facility_id`. | (see decision below) |
| `source` | The facility a trip leaves from. Column `source_id`. | `origin` (except OD matrix) |
| `target` | The facility a trip goes to. Columns `planned_target_id`, `realized_target_id`. | `destination` (except OD matrix) |
| `origin` / `destination` | Reserved for the **OD matrix** (Origin–Destination matrix) only, where O and D stand for exactly source and target. | using them anywhere else |

**Decision — `facility` vs `station`.** The schema column is `facility_id` and
the journal is the source of truth, so `facility` is canonical for identifiers
and code. "station" is the natural domain word and is fine in plain-English
prose, but is never an identifier. Collapsing the two fully would mean renaming
`facility_id` → `station_id` across the whole schema — a separate, larger
decision. Until then: `facility` in code, "station" only as prose.

---

## 5. Planned vs realized

The journal records both what was *intended* and what *actually happened*:

| Canonical | Meaning |
|---|---|
| `planned_*` | What was intended: `planned_target_id`, `planned_end_period`. |
| `realized_*` | What actually happened: `realized_target_id`, `realized_end_period`. |

`realized` as an **adjective** means "actual outcome vs the plan" — use it only
with this meaning. The verb `realize` (as in `realize_departures`) means "turn
wanted demand into actual departures, bounded by inventory."

Do **not** use `realized` as the name of the departure *count* — see §6.

---

## 6. Departures

| Canonical | Meaning | Avoid |
|---|---|---|
| `departed` | The count of bikes that left a source this period (matches the event type). | `realized` (as a column), `dispatched` |
| `departures` | The per-`(source, commodity)` table of departures. Its columns are `departed` and `lost`. | naming the table `realized` |

This mirrors the journal: a row of `departures` splits into `departed` + `lost`,
the same two event types the phase emits.

---

## 7. Time

| Canonical | Meaning |
|---|---|
| `period` / `period_id` | One step of the simulation clock. |
| `start_period` | The period a flow departed. |
| `planned_end_period` | The period a flow was expected to dock. |
| `realized_end_period` | The period a flow actually docked (NA if lost). |

---

## 8. Commodity

| Canonical | Meaning |
|---|---|
| `commodity_category` | The column: the bike type (classic / electric). |
| `commodity` | The short noun for the same thing in prose. |

Classic and electric bikes share the same physical docks but are counted per
`commodity_category`.

---

## Known drift to fix

None outstanding. The whole `gbp/` package was audited and brought to canon:

- `stock` (variables, `stock_keys`, and all prose) → `inventory` across the
  simulator (`phases.py`, `mechanics.py`, `state.py`, `validation.py`,
  `engine.py`), `journal.py`, and `loaders/`.
- the `realized` departure column/frame → `departed` / `departures`; the
  `realized` *adjective* (planned-vs-actual: `realized_target_id`,
  `realized_end_period`, "realized docking") stays — that is its canonical sense.
- `placed` / `placed_batches` → `redirected` / `redirected_batches`;
  `dispatched` → `departed`; `shortfall` → `lost` (loss) or `leftover` (rounding);
  "moving set" → "in-transit".
- `origin` / `destination` outside the OD matrix → `source` / `target` (the
  `origins` local in `_nearest_free_station`, the loss-side prose and the
  derived-observation docstrings in `journal.py` and `state.py`). The
  Origin–Destination matrix itself keeps `origin` / `destination`.

When new drift appears, list it here as a target for the audit skill.
