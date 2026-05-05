# Deep Interview Spec: Historical Bike Replay Pipeline

## Metadata
- Interview ID: hist-replay-2026-05-05
- Rounds: 4
- Final Ambiguity Score: 14%
- Type: brownfield
- Generated: 2026-05-05
- Threshold: 0.2 (20%)
- Status: PASSED
- Initial Context Summarized: no
- Source document: `docs/scenarios/algorithm_bike_simulation_historical.md`
- Form-factor reference: existing treatment example with `phases = [...]` + `Environment(EnvironmentConfig(phases=...))`

## Clarity Breakdown
| Dimension | Score | Weight | Weighted |
|---|---|---|---|
| Goal Clarity | 0.95 | 0.35 | 0.33 |
| Constraint Clarity | 0.75 | 0.25 | 0.19 |
| Success Criteria | 0.85 | 0.25 | 0.21 |
| Context Clarity | 0.85 | 0.15 | 0.13 |
| **Total Clarity** | | | **0.86** |
| **Ambiguity** | | | **0.14** |

---

## Goal

Implement Algorithm 1a (Historical Replay) from `docs/scenarios/algorithm_bike_simulation_historical.md` as a set of Phase objects pluggable into the existing `Environment(EnvironmentConfig(phases=[...])).run()` loop.

The defining property of the implementation: after running the pipeline on reference (mock) data, the resulting simulation logs match the historical observations exactly.

```
to_dataframes()["simulation_inventory_log"] == resolved.observed_inventory
to_dataframes()["simulation_flow_log"]      == resolved.observed_flow
```

---

## Constraints

1. **Extend, do not duplicate.** Existing phases B1, B2, B3 (`HistoricalLatentDemandPhase`, `HistoricalODStructurePhase`, `DeparturePhysicsPhase`) match the algorithm exactly and are reused as-is. Only B4, Phase A inner loop, and Phase C require new or extended code.
2. **`ObservedFlow` schema must be extended** with a per-trip duration. Either `arrival_date: dt.date` or `duration_hours: float` (one of the two; not both).
3. **`target_id` semantics is post-redirect.** The historical record carries the actual docking station, not the rider's intent. Therefore on the reference dataset capacity is by construction never violated and the new `OverflowRedirectPhase` does not fire — it exists as a safety-net for treatment scenarios with inflated demand.
4. **Multi-commodity contract:** capacity and redirect are per-`(facility_id, commodity_category)`. A working_bike redirected from a full station goes to the nearest station with free capacity *for working bikes*, not for any commodity.
5. **`DeparturePhysicsPhase` runs in `mode="strict"`** in the canonical pipeline (matches the treatment example).
6. **Same form-factor as the existing treatment example.** The user constructs a list of phases, passes it to `EnvironmentConfig`, runs `env.run()` (or the explicit step loop). No new entry points, no parallel "ReplayEnvironment".
7. **Brownfield rules from CLAUDE.md apply:** vectorized first, English-only code/docstrings, ruff line-length 100, mypy strict, Pydantic V2 strict. Core algorithms get skeletons + TODO comments rather than full implementations; boilerplate (schema, build pipeline plumbing, tests) is implemented in full.

---

## Non-Goals

- No `Replay*Phase` parallel stack. The existing `Historical*Phase` family stays in place.
- No stochastic Multinomial resampling in B4. Trips are replayed deterministically — one `in_transit` shipment per `observed_flow` row.
- No real GBFS feed integration in this task. Validation is on `DataLoaderMock` data.
- No optimizer/solver, API, UI, Docker, database changes (CLAUDE.md hard rule).
- No backwards-compatibility shim for the old `arrival_date`-less `ObservedFlow`. Schema gets bumped, all loaders updated.
- No new log table for trip status (`completed/redirected/in_transit/lost`). The four lifecycle states surface through existing tables: `simulation_flow_log`, `simulation_lost_demand_log`, the redirect-event channel emitted by `OverflowRedirectPhase`. A unified status column is a future-work item.

---

## Acceptance Criteria

### Schema and loaders
- [ ] `gbp/core/schemas/observations.py:ObservedFlow` gains a duration field (decision required at implementation time: `arrival_date` vs `duration_hours`; the planner picks one and documents the choice in the implementing PR).
- [ ] `DataLoaderMock` populates the new field for every generated trip.
- [ ] `DataLoaderGraph` (bike-share loader) populates the new field where source data carries it; deterministic fallback otherwise.
- [ ] `gbp/build/time_resolution.py` resolves the new field through the `date → period_id` pipeline so that `resolved.observed_flow` carries enough information for B4 to compute `arrival_period`.
- [ ] IO layer (`gbp/io/`) round-trips the new field through dict/parquet without loss.
- [ ] `RawModelData.validate()` and `ResolvedModelData.validate()` accept the new column.
- [ ] All existing tests touching `ObservedFlow` are updated.

### Phases
- [ ] `HistoricalTripSamplingPhase` is extended to read `τ_k` from `resolved.observed_flow` and set:
  ```
  arrival_period = period_index + ceil(tau_k / period_duration_hours)
  ```
  When `arrival_period == period_index`, the same-period semantics already implemented continue to apply. A `use_durations: bool = True` switch keeps the legacy zero-τ path available for callers that explicitly want it.
- [ ] **New** `OverflowRedirectPhase`:
  - placement: after `ArrivalsPhase` in the canonical pipeline;
  - input: `state.inventory`, `resolved.attributes["operation_capacity"]` filtered by `operation_type == "storage"`, `resolved.distance_matrix` (or `facilities[lat, lon]` Haversine fallback);
  - per `(facility_id, commodity_category)` with `quantity > capacity`: redirect the overflow to the nearest facility that has free capacity *for the same commodity*;
  - emits redirect events into a channel that the log layer routes to `simulation_flow_log` (with a marker phase_name) and/or a new short-name slot for redirected events;
  - on the reference dataset (post-redirect target_id), this phase is a no-op — that is the parity check.
- [ ] **New** `InvariantCheckPhase`:
  - placement: last in the period;
  - asserts `Σ_i state.inventory.quantity + |state.in_transit| == const` for the running scenario;
  - emits no events on success; raises a clear error or emits a diagnostic row on violation (planner picks the failure mode).

### Pipeline assembly
- [ ] The canonical historical replay pipeline is:
  ```python
  phases = [
      HistoricalLatentDemandPhase(),                       # B1
      HistoricalODStructurePhase(),                        # B2
      DeparturePhysicsPhase(mode="strict"),                # B3
      HistoricalTripSamplingPhase(use_durations=True),     # B4
      ArrivalsPhase(),                                     # Phase A (transfer half)
      OverflowRedirectPhase(),                             # Phase A (redirect half) — NEW
      InvariantCheckPhase(),                               # Phase C — NEW
  ]
  env = Environment(resolved, EnvironmentConfig(phases=phases, seed=42, scenario_id="historical_replay"))
  env.run()
  ```
- [ ] The pipeline composes with the treatment-style additions (`LatentDemandInflatorPhase`, `DispatchPhase(RebalancerTask, ...)`) without code changes — they are inserted at the right pipeline positions and the existing dependencies still hold.

### Verification notebook
- [ ] `notebooks/verify/12_historical_replay.ipynb` (next free index after `11_consumption_production_ops`).
- [ ] Cell A — baseline mock parity:
  - build `DataLoaderMock` data;
  - run the canonical pipeline;
  - assert `simulation_inventory_log` equals `resolved.observed_inventory` over the period grid (after the obvious `period_id`/`facility_id`/`commodity_category` alignment);
  - assert `simulation_flow_log` equals `resolved.observed_flow` over the same keys.
- [ ] Cell B — invariant constancy:
  - plot `Σ inventory + |in_transit|` per period; assert it is constant.
- [ ] Cell C — overflow safety net:
  - construct an artificial scenario where one station's capacity is intentionally undersized;
  - run the pipeline;
  - show that `OverflowRedirectPhase` records redirect events and the invariant still holds.
- [ ] Cell D — same-period vs cross-period τ_k:
  - construct one trip with τ inside the period and one with τ spanning two periods;
  - show the `in_transit` table evolution and the resulting `simulation_flow_log` rows.

### Tests
- [ ] Unit tests in `tests/unit/consumers/simulator/`:
  - `test_historical_trip_sampling_with_durations.py` — verifies `arrival_period` math for several τ values.
  - `test_overflow_redirect_phase.py` — verifies nearest-with-capacity selection per commodity, no-op on no overflow, deterministic tie-break.
  - `test_invariant_check_phase.py` — verifies pass / fail behaviour with constructed states.
- [ ] Build pipeline test for the extended `ObservedFlow` round trip through `time_resolution.py`.
- [ ] IO test for parquet round-trip of the extended schema.
- [ ] Existing simulator tests still green.

---

## Assumptions Exposed & Resolved

| Assumption | Challenge | Resolution |
|---|---|---|
| "Full reimplementation" implies a new `Replay*Phase` stack | Round 4 contrarian: three of six phases already match the doc verbatim | "Full reimplementation" is the description of *behaviour*, not of *new classes*. Extend two existing phases, add two new ones. |
| τ_k can be inferred from existing tables | Round 2: `ObservedFlow` schema has only `date` (start) | Extend `ObservedFlow` with `arrival_date` or `duration_hours`. |
| Capacity-aware redirect is essential for correctness on historical data | Round 3: requested parity `simulated_flow == observed_flow` | `target_id` is post-redirect → capacity is never violated on reference data → redirect phase is a safety net for treatment scenarios. |
| "Parity" means approximate / regression-safe | Round 3: explicit user clarification | "Parity" means exact equality of `simulation_inventory_log` to `observed_inventory` and `simulation_flow_log` to `observed_flow` after key alignment. |

---

## Technical Context (cited code)

- Environment loop: `gbp/consumers/simulator/engine.py:82-123` (`run`, `step`).
- Existing replay phases: `gbp/consumers/simulator/built_in_phases.py:261-330` (`HistoricalLatentDemandPhase`), `:333-403` (`HistoricalODStructurePhase`), `:406-514` (`DeparturePhysicsPhase`), `:517-590` (`HistoricalTripSamplingPhase`), `:593-668` (`ArrivalsPhase`), `:756-839` (`DockCapacityPhase` — close cousin of the new redirect phase, but does not redirect).
- Schema to extend: `gbp/core/schemas/observations.py:10-21` (`ObservedFlow`).
- Distance reference for nearest-search: `gbp/core/schemas/edge.py:74-87` (`DistanceMatrix`); usage example in `gbp/consumers/simulator/tasks/rebalancer.py:495-534` (`_build_edge_distance_map`, `_create_distance_matrix`, `_haversine_distance_m`).
- Capacity reference: `gbp/consumers/simulator/built_in_phases.py:798-839` (reading `resolved.attributes["operation_capacity"]`).
- State invariant container: `gbp/consumers/simulator/state.py:69-136` (`SimulationState`, immutable updates, `advance_period`).
- Log routing: `gbp/consumers/simulator/log.py:154-183` (`LOG_TABLES` registry — extend it if a new redirect short-name is introduced).

---

## Ontology (Key Entities)

| Entity | Type | Fields | Relationships |
|---|---|---|---|
| `Phase` | core protocol | `name`, `should_run`, `execute` | implements simulation step |
| `ObservedFlow` (extended) | core domain row | `source_id`, `target_id`, `commodity_category`, `date`, `arrival_date`/`duration_hours`, `quantity`, `modal_type`, `resource_id` | input to all replay phases |
| `SimulationState` | core domain | `inventory`, `in_transit`, `resources`, `intermediates` | mutated through phases |
| `transit buffer ℬ` | core concept | rows of `state.in_transit` with `arrival_period > current` | populated by B4, drained by Arrivals |
| `OverflowRedirectPhase` | new phase | nearest-search via `distance_matrix` | depends on `facilities`, `operation_capacity` |
| `InvariantCheckPhase` | new phase | per-period assertion | reads `state` only |
| `Parity check` | acceptance gate | exact equality of two log tables to two observation tables | drives every Cell A assertion in the verify notebook |

---

## Ontology Convergence

| Round | Entity Count | New | Changed | Stable | Stability Ratio |
|---|---|---|---|---|---|
| 1 | 8 | 8 | – | – | N/A |
| 2 | 8 | 0 | 1 (`ObservedFlow` enriched) | 7 | 100% |
| 3 | 9 | 1 (`Parity check`) | 0 | 8 | 100% |
| 4 | 9 | 0 | 0 | 9 | 100% |

The ontology converged after round 2. Subsequent rounds added refinements rather than new core entities.

---

## Pipeline Diagram (final)

```
┌──────────────────────────────────────┐
│ HistoricalLatentDemandPhase  (B1)    │  ← reused, no change
│ HistoricalODStructurePhase   (B2)    │  ← reused, no change
│ DeparturePhysicsPhase(strict) (B3)   │  ← reused, no change
│ HistoricalTripSamplingPhase  (B4*)   │  ← extended: read τ_k
│ ArrivalsPhase                (A1)    │  ← reused, no change
│ OverflowRedirectPhase        (A2 NEW)│  ← new: redirect on full
│ InvariantCheckPhase          (C  NEW)│  ← new: assert Σx + |B| const
└──────────────────────────────────────┘
                                          ↓
                            simulation_inventory_log == observed_inventory
                            simulation_flow_log      == observed_flow
```

---

## Interview Transcript

<details>
<summary>Full Q&A (4 rounds)</summary>

### Round 1 — Goal Clarity (weakest at 0.0)
**Q:** Какая часть алгоритма НОВАЯ, а какая уже закрыта существующими фазами? B1/B2/B3/B4 (без τ)/A (без редиректа) уже реализованы как `HistoricalLatentDemandPhase`, `HistoricalODStructurePhase`, `DeparturePhysicsPhase`, `HistoricalTripSamplingPhase`, `ArrivalsPhase`. Не закрыты: capacity-aware redirect, нетривиальная τ_k, единый status-лог.
**A:** Полная реимплементация по доку 1-в-1.
**Ambiguity after round:** 59% (Goal: 0.60, Constraints: 0.15, Criteria: 0.10, Context: 0.85)

### Round 2 — Constraint Clarity (weakest at 0.15)
**Q:** Откуда брать длительности τ_k для B4? Схема `ObservedFlow` несёт только дату старта. Это определяет, нужно ли расширять Pydantic-схему.
**A:** Расширить `ObservedFlow`: добавить `arrival_date` или `duration_hours`.
**Ambiguity after round:** 48%

### Round 3 — Success Criteria + clarification
**Q (initial):** Какая основная проверка должна доказывать корректность? Инвариант / парити с OrganicFlowPhase / редирект+lost метрики / репродукция бик-шер истории.
**Clarification by user:** «simulated inventory должен быть равен observed_inventory и simulated_flow должен быть равен observed_flow».

This unlocked a new Constraints sub-question:

**Q (follow-up):** Что такое `target_id` в `observed_flow`? Если изначальное намерение — наша редирект-фаза должна его пересчитать. Если фактическое докирование — то редирект в симуляции должен попадать в тот же `target_id`, иначе парити сломана.
**A:** Фактическое место докирования (post-redirect).
**Ambiguity after round:** 30% (Goal: 0.75, Constraints: 0.40, Criteria: 0.85, Context: 0.85)

### Round 4 — Contrarian challenge on Goal Clarity
**Q:** Что если «полная реимплементация 1-в-1» это описание поведения, а не инструкция писать `Replay*Phase` стек с нуля? Сущностные правки локализованы: B1/B2/B3 не трогаем, B4 расширить, +`OverflowRedirectPhase`, +`InvariantCheckPhase`.
**A:** Расширить, не дублировать (Recommended выбран).
**Ambiguity after round:** 14% — порог пройден.

</details>
