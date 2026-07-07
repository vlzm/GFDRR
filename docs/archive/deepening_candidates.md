# Deepening candidates — architecture review, 2026-07-06

This document lists the refactoring candidates found by the architecture review
on 2026-07-06. Each candidate turns a shallow module into a deep one: more
behaviour behind a smaller interface. The goal is testability and easier
navigation, not new features. Use this document to pick a candidate and start
implementation in a fresh session; every claim below was checked against the
code at the listed lines on the review date.

Status legend: every candidate starts as `proposed`. Update the status line
when work starts or a decision is made (`in progress`, `done`, `rejected — see
ADR`).

## Vocabulary used below

Five words describe every finding. They come from the review method, not from
the domain; domain words stay as in `Notations.md`.

- **module** — anything with an interface and an implementation: a function, a
  class, a file.
- **interface** — everything a caller must know to use the module correctly:
  the signature, plus required columns, call order, and invariants the caller
  must maintain.
- **deep / shallow** — a deep module hides a lot of behaviour behind a small
  interface. A shallow module has an interface almost as complex as its
  implementation.
- **seam** — the place where a module's interface lives; the place where
  behaviour can be changed without editing the callers.
- **locality** — knowledge and change concentrated in one place. The opposite:
  the same rule restated in several files, where the copies can drift apart.

The **deletion test** tells a working module from a pass-through: imagine
deleting the module. If the complexity disappears, the module was a
pass-through. If the same complexity reappears in every caller, the module was
doing real work.

## The candidates at a glance

| # | Candidate | Layer | Status |
|---|---|---|---|
| 1 | `apply_step_events` also maintains inventory and `in_transit` | simulator | done |
| 2 | Phases return events; the engine stamps the ordering | simulator | done |
| 3 | `plan_overflow_redirect` returns resolved outcomes | simulator | done |
| 4 | `flows_to_panel` — the panel becomes a journal read-model | model | done |
| 5 | Delete `get_flows_wide` and its `_join_*` helpers | loaders | done |
| 6 | One typed loader for the run artifact in the UI | app | done |
| 7 | One page module for `flow_totals` metrics + one diff helper | app | done |
| 8 | One arc-map module for the two map pages | app | done |

Candidates 1–3 are one storyline: the seam between phases and
`SimulationState`. Candidate 2 builds on candidate 1. Candidate 3 shrinks by
itself once candidate 1 is done. Do them in order 1 → 2 → 3.
Candidates 4–8 are independent of each other and of 1–3.

---

## 1. `apply_step_events` writes the journal but not the projections it implies

**Status:** done (2026-07-06). `apply_step_events` now applies the inventory
delta and the `in_transit` change implied by the events it writes, through two
new model-layer functions next to the predicates:
`inventory_deltas_from_events` and `in_transit_after_events`
(`gbp/model/flows.py`). Deleted: `departure_deltas_from_counts`,
`SimulationState.with_inventory`, `SimulationState.with_in_transit`, the three
reconciliation asserts. `adjust_inventory` and `dock_deltas` stay for the local
decision copies (the redirect's round loop, the rebalancing rounds). New unit
tests in `tests/test_scenarios.py` cover "events in, state out" directly.

**Files:**
- `gbp/consumers/simulator/state.py:203-239` — `apply_step_events`, the
  documented "single write path for a phase".
- `gbp/consumers/simulator/state.py:32-62` — `adjust_inventory`,
  `dock_deltas`, `departure_deltas_from_counts`.
- `gbp/model/flows.py:113-144` — `is_undocking`, `is_user_departure`,
  `is_docking`: the one place the `+1`/`-1` inventory rule is already written.
- Call sites that repeat the rule by hand: `gbp/consumers/simulator/phases.py`
  lines 119, 137, 154, 209-210, 229, 237;
  `gbp/consumers/simulator/rebalancing.py` lines 770-776 (an inline `groupby`
  builds the pickup `-1` side, because no shared helper exists), 794-795, 824.
- Reconciliation asserts that police the duplication: `phases.py:158-161`,
  `phases.py:240`, `rebalancing.py:801`.
- Run invariant I3: `gbp/consumers/simulator/validation.py:80,89-110`
  (`_check_projection_consistency`) — exists only to catch the live inventory
  diverging from the journal.

**Problem.** `apply_step_events` only appends events to `state_flows_df`.
Inventory is a second, parallel write every phase must perform itself: pick
the right delta builder, call `adjust_inventory`, then `with_inventory`. The
same goes for `in_transit`: phases remove docked rows with
`drop(due.index)` — a link through the positional row index of the frame, an
invariant stated nowhere (`phases.py:150-153`, `rebalancing.py:737-738`). The
`+1`/`-1` rule therefore lives twice: once in the `is_docking`/`is_undocking`
predicates, once spread over the delta builders in every phase. Three
hand-written asserts and the whole of invariant I3 exist only because the two
write paths can silently disagree.

**Solution.** Make `apply_step_events` apply the inventory delta implied by
the events it writes, using the existing `is_docking`/`is_undocking`
predicates, and maintain `in_transit` in the same call. A phase then builds
events and calls one method; it cannot desync the projections.

**Benefits.**
- Locality: the inventory rule exists in one place. The delta builders, the
  inline `groupby` in `rebalancing.py`, all `with_inventory` calls, and the
  three reconciliation asserts go away.
- Invariant I3 drops from load-bearing to a cheap safety check.
- Tests: a phase becomes testable as "events in, state out" without a full
  `Environment` run. Today no test runs a single phase on a single state;
  every phase is tested only end-to-end.

**Watch out for:** `in_transit` rows today are keyed by position, not by
`flow_id`. Moving its maintenance behind the seam likely means keying it by
`flow_id`; check `ApplyRebalancingPhase`, which filters `in_transit` by
`flow_type == "rebalance"` (`rebalancing.py:724-738`).

---

## 2. Phases return events; the engine stamps the ordering

**Status:** done (2026-07-06). Each phase class now declares its rank once
(`Phase.phase_rank`, a class attribute). A normal phase implements only
`build_events(state, resolved, period, config) -> events`; the base
`Phase.execute` writes them through `apply_step_events` with the declared
rank. The two rebalancing phases override `execute` because they also replace
`rebalance_plan`. The engine (`Environment.__init__`) refuses a phase list not
ordered by rank with `SimulatorConfigError`. `Phase.name` deleted (never
read). `phase_rank_by_timing` stays as the historical loader's rule, locked by
the existing oracle test.

**Files:**
- `gbp/consumers/simulator/phases.py:49-62` — the `Phase` base class.
- `gbp/consumers/simulator/scenario.py:34` — `canonical_phases()`: the phase
  list whose order drives execution.
- `gbp/model/flows.py:92-95` — the rank constants (`DOCK_PREVIOUS_RANK` … 
  `REBALANCE_RANK`).
- `gbp/model/flows.py:531-567` — `phase_rank_by_timing`, the historical
  loader's rule; its docstring admits the constants and the rule "must agree"
  and that only a scenario test locks the agreement.
- Rank stamping call sites: `phases.py:146,231`, `rebalancing.py:792`.

**Problem.** The declared interface of a phase is
`execute(state, resolved, period, config) -> state`. The real contract is
unwritten: import and stamp the right `phase_rank`, attach a `phase_round`
column for ordered batches, keep the projections in sync (candidate 1), and
stand in the phase list at a position that matches the stamped rank. The
order is encoded twice — list position hands out `step_id`, the rank constant
sorts the steps — and nothing checks the two agree. A caller who passes
`phases=` to `run_sized_scenario` in the wrong order gets a journal whose
`step_id` and `phase_rank` disagree, and only invariant I5 might catch it.
Dead ceremony: `Phase.name` is never read anywhere (checked by search);
`config` is used by two of the four phases.

**Solution.** A phase declares its rank once (a class attribute) and returns
the events it built. The engine (or the state) stamps `phase_rank`,
`phase_round`, and `step_id`, and asserts the phase list is ordered by rank.
`phase_rank_by_timing` stays as the historical loader's only rule.

**Benefits.**
- The phase contract shrinks to "build this period's events" — one deep seam
  instead of four duties.
- The phase order is written once; the hand-kept "constants must equal the
  timing rule" contract disappears from the docstring and into a check.
- A phase becomes unit-testable in isolation.

---

## 3. `plan_overflow_redirect` returns resolved outcomes

**Status:** done (2026-07-06). `plan_overflow_redirect` now returns one frame,
one row per overflow flow, with an `outcome` column (`"docked"` / `"riding"` /
`"lost"`) plus `realized_target_id`, `leg_end_period`, `phase_round`. The
phase-side re-dock disappeared with candidate 1; `DockArrivals.build_events`
now only maps outcomes to events (it no longer re-derives "docks now" from
`planned_end_period == t`). `tests/test_mechanics.py` pins the full outcome,
including the lost case.

**Files:**
- `gbp/consumers/simulator/mechanics.py:164-250` — `plan_overflow_redirect`:
  the round loop keeps a local `running` inventory copy to decide who fits,
  then throws it away.
- `gbp/consumers/simulator/phases.py:100-162` — `DockArrivals.execute`
  re-docks the same `legs_now` rows into the real inventory
  (`phases.py:137`), applying the docking rule a second time — keyed on
  `planned_target_id`, while the mechanics decided on `realized_target_id`.
  The two agree only because a redirect leg's planned target equals its
  realized target by construction.

**Problem.** Following one bounced bike takes about ten functions in four
files: the round loop, `_nearest_free_station`, `_leg_durations`,
`dock_up_to_capacity` (run twice, against two different column names), three
event builders stitched with `pd.concat`, and the manual `phase_round`
assignment. The docking arithmetic is done once to decide and once to apply.
Deletion test: neither copy can be removed today — the sign of a seam drawn
in the wrong place. A redirect bug has no single place to live.

**Solution.** `plan_overflow_redirect` returns the fully resolved outcome per
bike — docked here / rides a leg / lost, with the chosen station and leg end
period on the row. The phase only turns outcomes into events. With
candidate 1 in place, the phase-side re-dock disappears entirely.

**Benefits.**
- The docking rule runs once per bike.
- Understanding a redirect becomes one file read top to bottom.
- The mechanics test (`tests/test_mechanics.py`) then pins the full outcome,
  not a half-decision the phase must finish.

---

## 4. `flows_to_panel` — the panel becomes a journal read-model

**Status:** done (2026-07-06). `flows_to_panel(flows, initial_inventory)`
added to `gbp/model/flows.py` with the canonical `PANEL_KEYS` /
`PANEL_VALUES` lists; it owns the grid guard and the demand identity.
`build_panel` deleted; `build_run_tables` selects
`flows_to_panel(...)[PANEL_KEYS + PANEL_VALUES]`, so a `METRICS` entry the
model does not produce fails loudly at build time. The panel tests now run
against the model interface.

**Files:**
- `gbp/model/flows.py:698-785` — the existing read-models
  (`flows_to_departures`, `flows_to_arrivals`, `flows_to_redirects`,
  `flows_to_losses`, `flows_to_od_matrix`, `get_inventory_df`).
- `app/artifacts.py:162-203` — `build_panel` stitches them by hand.

**Problem.** There is no single "give me the per
`(period_id, facility_id, commodity_category)` state" call in the model
layer. So `build_panel` in the app layer must know: the group keys of each
read-model; that `flows_to_losses` lands a loss at a different facility
column per reason (`source_id` for `stockout`, `planned_target_id` for
`dock_full`); that `demand` is not a read-model but the sum
`departed + lost_demand`; and it must guard the period-grid coverage itself,
because the read-models return only pairs that had events. The identity
`demand = departed + lost(stockout)` is stated three times: in `build_panel`,
in `check_demand_split` (`gbp/model/flows.py`, see `check_flow_closure`
neighbourhood), and in `Notations.md` §2/§7 — with no shared home in code.

**Solution.** Add `flows_to_panel(flows, initial_inventory)` to
`gbp/model/flows.py` as the seventh read-model. It owns the grid guard and
the demand identity. `build_panel` becomes a thin wrapper (or is deleted and
`artifacts.py` calls the read-model directly).

**Benefits.**
- The demand identity and the "which marginal lands at which facility column"
  knowledge live once, next to the other read-models.
- The panel is testable at the model interface, not only through artifact
  building (`tests/test_app_artifacts.py`).
- The UI rule "the panel is a slice of one table" gets a model-layer anchor.

---

## 5. Delete `get_flows_wide` and its `_join_*` helpers

**Status:** done (2026-07-06). The notebook cell now calls
`flows_with_inventory` + `flows_with_measures`; `get_flows_wide`,
`_FLOW_FACILITY_ROLES` and the three `_join_*` helpers are deleted
(~146 lines). `docs/dataloader.md` documents the two-call replacement.

**Files:**
- `gbp/loaders/dataloader_graph.py:660-800` — `_FLOW_FACILITY_ROLES`,
  `_join_capacity`, `_join_geo`, `_join_inventory`, `get_flows_wide`.
- `gbp/model/flows.py:977` — `flows_with_inventory`, the canonical answer to
  the same question, built on `inventory_at_moments` (`flows.py:890`).
- `notebooks/test_pipeline.ipynb` — the only caller (one cell, verified by
  search: no callers in `gbp/`, `app/`, or `tests/`).

**Problem.** Two implementations answer "what was this facility's inventory
before/after this event" with different semantics. `flows_with_inventory`
reads inventory at the event's exact `step_id`. `_join_inventory`
(`dataloader_graph.py:704-725`) rebuilds it from per-period inventory and
fakes "before" with a `period_id + 1` shift — period-level, not step-level.
Only the canonical one is tested. The divergent one survives because one
notebook cell imports it.

**Solution.** Switch the notebook cell to `flows_with_inventory` +
`flows_with_measures`, then delete `get_flows_wide` and the three `_join_*`
helpers (~140 lines).

**Benefits.**
- One definition of inventory-at-an-event instead of two that disagree.
- The largest loader file shrinks; no reader can pick the wrong widening
  path again.

---

## 6. One typed loader for the run artifact in the UI

**Status:** done (2026-07-06). `ui_shared` is the one front door: typed
accessors `load_panel` / `load_arcs(run, flow_type=...)` / `load_flow_totals`
/ `load_facilities` / `load_meta`, plus `rebalancing_settings(meta)` for the
rebalancing block and `table_path` for the downloads page. The old-artifact
fallbacks (missing `flow_type` column) live inside `load_arcs`. The generic
`load_table` is private; `downloads.py` no longer builds file paths or calls
`artifacts.load_run_table`. Views import `PANEL_KEYS` / `PANEL_VALUES` from
`ui_shared`, not from `artifacts`.

**Files:**
- `app/ui_shared.py:57-78` — cached `load_table` / `load_meta`.
- `app/artifacts.py:546-555` — raw `load_run_table` / `load_run_meta`; the
  second front door.
- `app/views/downloads.py:34-41` — bypasses both and rebuilds the file path
  `f"{table}.parquet"` by hand.
- `app/views/trips_map.py:44-47`, `app/views/truck_trips.py:22-32` — each
  filters `flow_type` itself and carries an "older artifact has no
  `flow_type` column" branch.
- `app/ui_shared.py:110-114`, `app/views/truck_trips.py:103` — the
  `meta.get("rebalancing", {}).get("enabled", False)` chain, three copies.
- Views importing panel column vocabulary directly:
  `app/views/station_map.py:7`, `app/views/facility_detail.py:7`.

**Problem.** The run artifact folder is a real seam, but reading it has two
front doors plus one bypass, table names travel as bare strings through ~15
call sites, and schema knowledge (which columns a table carries, which
artifact versions lack a column, where the rebalancing flags sit in `meta`)
is restated inside individual pages.

**Solution.** Make `ui_shared` the single loader with typed accessors:
`load_panel(run)`, `load_arcs(run, flow_type=None)`, `load_flow_totals(run)`,
`load_facilities(run)`, `load_meta(run)` plus a small accessor for the
rebalancing block of `meta`. Old-artifact fallbacks live inside the loader.
`downloads.py` asks the loader for the file path instead of building it.

**Benefits.**
- A page knows only "ask for the table"; artifact schema changes become a
  one-file edit.
- The `flow_type` split and the version fallback exist once.
- Loader behaviour (cache, fallbacks) becomes directly testable.

---

## 7. One page module for `flow_totals` metrics + one diff helper

**Status:** done (2026-07-06). `ui_shared.FlowTotalsView` +
`flow_totals_page` render the shared four-block page; `costs.py` and
`distance_duration.py` are configs over it. `delta_b_minus_a` owns the
comparison direction and the `st.metric` sign at every difference tile
(KPI row included). `Metric` gained `flow_value` / `flow_agg`, so
`mean_duration_periods` is a first-class metric and `build_totals` computes
every total from the `METRICS` table — no hand-added keys. New unit tests
cover the diff helper, `arc_map_rows`, `rebalancing_settings` and the
totals coverage.

**Files:**
- `app/views/costs.py` and `app/views/distance_duration.py` — the same
  four-block page duplicated: load A/B frames, global-total branch with the
  `"Difference (B − A)"` metric, facility multiselect branch,
  `aggregate_flow_totals` → `level_line_chart` → data table.
- The B − A comparison with the sign trick for `st.metric`, four copies:
  `app/ui_shared.py:184-192` (KPI row), `app/views/station_map.py:57-97`,
  `app/views/costs.py:26-30`, `app/views/distance_duration.py`.
- Related: `app/artifacts.py:384-393` — `build_totals` hand-adds `cost`,
  `distance_km`, and `mean_duration_periods` outside the `METRICS` loop;
  `mean_duration_periods` is not a `Metric` at all, and
  `distance_duration.py:31-35` knows that exception by a string literal.

**Problem.** Two pages are one module written twice; the comparison
convention (direction B − A, sign string, red/green arrow trick) is
re-implemented at four sites; and the "chart column → totals key" mapping is
split between `build_totals` and one view. The deep helpers the pages rely on
(`aggregate_flow_totals`, `level_line_chart`, `panel_commodity_slice`) have
zero unit tests — they are exercised only by "the page renders".

**Solution.** One configurable page module in `ui_shared` (value column,
aggregation, axis title, unit format, totals key), one shared diff helper
owning direction and sign. Give `Metric` a totals field so
`mean_duration_periods` is a first-class metric and the view reads the key
from the `METRICS` table.

**Benefits.**
- A new `flow_totals` metric page is a config entry.
- The diff arithmetic and the aggregation helpers become directly testable.
- `METRICS` becomes what its docstring already claims: the single source for
  every metric, label, and total.

---

## 8. One arc-map module for the two map pages

**Status:** done (2026-07-06). `ui_shared.arc_map_rows(arcs, group_keys,
count_name)` owns the arc-row schema knowledge (endpoint coordinates ride on
every row); `arc_deck(rows, facilities, width_col, tooltip_html)` owns
pydeck's `[lng, lat]` order and the tooltip style. Both map pages keep only
their filter, colors, and tooltip text.

**Files:**
- `app/views/trips_map.py:41-100` and `app/views/truck_trips.py:22-88`.

**Problem.** Both pages shape `arcs.parquet` into deck rows with the same
`groupby` (sum `quantity`, take endpoint coordinates with `"first"`) and
build a pydeck `ArcLayer` with near-identical `_deck()` bodies. Only the
group key (`event_type` vs `resource_id` + periods) and the colour source
differ. The knowledge that endpoint coordinates ride on every arc row, and
pydeck's `[lng, lat]` ordering, live in two places.

**Solution.** `arc_map_rows(arcs, group_keys)` and
`arc_deck(rows, color_col, tooltip_html)` in `ui_shared`; the pages keep
their filter and colour choice.

**Benefits.**
- The arc-row schema knowledge exists once.
- A third arc map (for example, per-commodity) becomes a filter plus a
  colour, not a copied page.

---

## Small items (attach to whichever candidate touches the file)

- **Unchecked precondition in the inventory read-models.** `get_inventory_df`
  (`flows.py:785`), `inventory_at_moments` (`flows.py:890`),
  `flows_with_inventory` (`flows.py:977`) require a finalized journal (a
  `step_id` column) but do not check it; an unfinalized frame silently
  returns an empty result instead of an error. Add an explicit check at each
  entry.
- **`lost_events` is two contracts in one function** (`flows.py:336`): the
  `reason` argument selects two different required column sets
  (`stockout` — aggregated rows, no `flow_id`; `dock_full` — one row per
  flow with `move_id` and targets). Split into two functions sharing a
  private tail.
- **The arc/event numbering law** (`event_id = 2*move_id` opens,
  `2*move_id + 1` closes) is written inline in five builders in `flows.py`
  and inverted in `tests/invariants.py:57`. Hoist into helper functions in
  `flows.py`, used by builders and checker alike.
- **The inventory key list** `["facility_id", "commodity_category"]` is
  declared independently in `validation.py:30`, `rebalancing.py:156`, and
  inline in `state.py` and `mechanics.py`. Hoist one shared constant.
- **`flow_id` prefixes** (`"sim_"` in `mechanics.py:371`, `"rb_"` in
  `rebalancing.py:585`, `"hist_"` in the loader/tests) encode the
  "ids never collide" rule of `Notations.md` §10 as three scattered string
  literals with no single owner.

## What is already deep — do not touch

- `run_sized_scenario` (`gbp/consumers/simulator/scenario.py:52`) and
  `size_state_for_demand` (`gbp/consumers/simulator/sizing.py:40`): a small
  interface hiding the fixed size → run → validate order. Both consumers
  (the terminal runner and the notebook) share one path. Deletion test:
  the ordering knowledge would reappear in every caller.
- The "run a scenario and save the artifact" path: the Run page and the CLI
  both call `runner.run_scenario` (`app/runner.py:89`); there is no
  duplicated wiring. One minor leak: callers convert
  `period_len` to hours themselves before `build_meta`
  (`runner.py:191`); `build_meta` could take the `pd.Timedelta` and convert
  once.
- `Routes` (`gbp/routing.py`): well-tested, one owner for facility-pair
  distance and travel time. Known and accepted: the OSRM fallback to the
  straight-line answer is not observable through the interface, and redirect
  target *selection* uses `neighbor_distance_sq` while the reported arc
  distance comes from `Routes` — both are recorded decisions in
  `Notations.md` §13.
