# Architecture review — deepening candidates

Date: 2026-07-03. Branch at review time: `city_bike_mvp_accounting`.

> **Status: implemented on 2026-07-03** (same branch). All six candidates and the
> side findings are done, with two decisions taken during implementation:
>
> - **1a is intentionally NOT done**: the wide flow journal (`get_flows_wide`,
>   `slice_flows_wide`) is kept by the user's decision as the one-table view for
>   reading simulation outputs by eye. Its docstrings stay; `docs/design_documents/wide_panel.ru.md` stays.
> - **1d**: `potential_trips_df` / `build_potential_trips` were removed; the
>   engine's emptiness guard now reads `historical_demand_df`.
> - Candidate 2 kept the name `FormDeparturesPhase` for the merged phase;
>   `FormPotentialTripsPhase`, `intermediates` and `PhaseResult` are gone, and
>   `Phase.execute` now returns the new `SimulationState` directly.
> - Candidate 3 became `SimulationState.apply_step_events(new_flows, phase_rank)`:
>   one step per `phase_round`, all three ordering columns stamped, rows appended.
> - Candidate 6: `EnvironmentConfig.validate` now defaults to `True`;
>   `validate_run` takes the run's `demand_scale_factor` and `number_of_periods`
>   (I1 compares against the scaled demand of the periods the run actually
>   stepped); `Environment` refuses `number_of_periods` beyond the period grid.
>   The sizing run keeps `validate=False` (its own no-loss assert is the guard).

This document lists the results of a module-depth review of `gbp/`, `tests/`, and
`notebooks/test_pipeline.ipynb`. It is written to be self-contained: a new chat
session (or a new person) can start fixing from this file alone. Every claim
below was verified against the code at review time with exact file and line
references. Line numbers will drift as files change; the function names will not.

## Terms used in this document

- **Module** — anything with an interface and an implementation: a function, a
  class, or a package.
- **Interface** — everything a caller must know to use the module correctly:
  types, required call order, invariants, error modes, configuration. Not just
  the signature.
- **Deep / shallow** — a module is deep when a lot of behaviour sits behind a
  small interface. It is shallow when the caller must know almost as much as
  the implementation contains.
- **Seam** — the place where a module's interface lives; a place where
  behaviour can be changed without editing the code around it.
- **Locality** — the property that a change, a bug, or a piece of knowledge
  lives in one place instead of being spread across many callers.
- **Deletion test** — imagine deleting the module. If complexity disappears,
  the module was a pass-through. If the complexity reappears in N callers, the
  module was earning its keep.

## Overall verdict

The codebase is in good shape. `gbp/model/flows.py` is a genuinely deep module:
one column schema (`FLOW_EVENT_COLUMNS`) hides the builders, the ordering rules,
and all the read-models, and it has many real callers. The dependency direction
`journal <- state <- mechanics <- phases <- engine` holds. The six findings
below are places where the interface is nearly as complex as the implementation,
or where one piece of knowledge is written out in several modules at once.

## Constraints — do not re-open these decisions

1. **The notebook's data and graph-prep cells belong to the user.** Do not
   change their order or their variable names. Any fix that touches the
   notebook must fit into the existing cells.
2. **Resource tables are reserved.** `Notations.md` §5b declares the resource
   entity, its attributes (`resources_capacities_df`, `resources_rates_df`,
   `home_facility_id`) and the `resource_id` column canonical and reserved,
   even though trucks are idle in the replay. Do not delete them.
3. **The historical loader derives `step_id`; the simulator stamps it.** This
   dual mechanism is a documented design decision (`Notations.md` §0.1), not
   drift. Do not unify the two branches of `_assign_step_id`.
4. **CLAUDE.md scope rules apply**: no optimizer, no other domains, no API/UI/
   Docker/database. `notebooks/test_pipeline.ipynb` is the source of truth —
   code the canonical scenario does not use should be removed.
5. **`Notations.md` is the vocabulary.** Any new name introduced during these
   fixes must use the words already in `Notations.md`, or be added there first.

## How to verify a fix

```bash
uv pip install -e ".[dev]"    # gbp must be an editable install (see memory note)
ruff check gbp/ tests/
ruff format gbp/ tests/
mypy gbp/
pytest tests/
```

Then run `notebooks/test_pipeline.ipynb` top to bottom and check that the
base-replay equalities still hold (`simulated_* == historical_*`).

---

## Candidate 1 — remove dead surface (largest win, lowest risk)

This matches the current project phase: cleanup to the canonical scenario
minimum. Each item below was verified to have **zero callers** in `gbp/`,
`tests/`, and the notebook (grep at review time).

### 1a. The wide flow journal block — `gbp/loaders/dataloader_graph.py:555-762`

~207 lines, 27% of the file. Zero callers anywhere:

- `get_flows_wide` (line 642) and `slice_flows_wide` (line 722) — public, never called.
- Private helpers used only by them: `_join_capacity` (577), `_join_geo` (608),
  `_join_inventory` (618), `_haversine_km` (565), `_FLOW_FACILITY_ROLES` (560),
  `_EARTH_RADIUS_KM` (562).

**Caution:** there is a design document for this feature,
`docs/design_documents/wide_panel.ru.md`. The user must decide whether the wide
panel is still wanted. If it is removed, also remove or archive that document.

Extra benefit of removing it: `_join_inventory` (618-639) is a **second,
different definition** of "inventory before/after an event". It works on the
period axis (it shifts `period_id` by one), while the canonical definition
`inventory_at_moments` / `flows_with_inventory` in `flows.py` works on the step
axis (`step_id`). Deleting the wide panel deletes the duplicate definition.

### 1b. `Observations` dataclass and the phantom `observe` function

- `Observations` (`gbp/model/flows.py:975-1005`) is never instantiated, never
  imported, and not exported in `gbp/model/__init__.py`.
- Two docstrings reference a function `observe` that **does not exist
  anywhere**: `flows.py:10` and the comment at `dataloader_graph.py:459`.
- `Notations.md` §9 also says "(`observe` → `Observations` in `flows.py`)".

Fix: delete the dataclass, fix both docstrings, and update `Notations.md` §9 so
the dictionary stops pointing at a function that is not there.

### 1c. Small dead items

| Item | Location | Evidence |
|---|---|---|
| `departure_deltas` (trips-based) | `gbp/consumers/simulator/state.py:39` | 0 callers; phases use `departure_deltas_from_counts` (state.py:66) instead |
| `SimulationState.with_resources` | `state.py:209` | 0 callers |
| `EnvironmentConfig.seed` | `config.py:21` | never read anywhere (`RawModelData` has its own separate `seed` that IS used, `dataloader_raw.py:234`) |
| `facilities_required_capacities_df` | `dataloader_graph.py:474` | computed in every `ResolvedModelData.__init__`, never read by engine, phases, tests, or notebook |
| `Schedule.every_n_periods` | `state.py:96` | never called with n > 1 on any path; every phase uses the default `Schedule.every()` |

### 1d. `potential_trips_df` is built but not consumed by any phase

`ResolvedModelData` builds it (`dataloader_graph.py:496`), and the engine reads
it **only** in the emptiness guard (`engine.py:40`). No phase reads it — the
phases consume `historical_demand_df` and `historical_od_matrix_df` instead.
The module docstrings (`dataloader_graph.py:8`, `:412`) still claim the engine
consumes it. Decide: either remove it and re-point the engine guard, or keep it
as an inspection table and correct the two docstrings so a reader is not misled.

### 1e. Items that look dead but are NOT — keep them

- All `resources_*` / `facilities_costs_df` / rate tables: reserved by
  `Notations.md` §5b and possibly used by the user-owned notebook data cells.
- `redirect_neighbor_table` (`flows.py:869`): live, tested directly
  (`tests/test_scenarios.py:271-307`).

---

## Candidate 2 — merge `FormDeparturesPhase` and `FormPotentialTripsPhase`

**Files:** `gbp/consumers/simulator/phases.py:173-289`,
`gbp/consumers/simulator/state.py:220-222` (`with_intermediates`).

**Problem.** One inventory step (the departures step) spans two phases:

- `FormDeparturesPhase` opens the step (`phases.py:217-221`) and passes both
  the departure counts and the step number forward through the untyped
  `intermediates` dict with string keys `"departures"` and
  `"departures_step_id"` (`phases.py:223-224`).
- `FormPotentialTripsPhase` reads them back (`phases.py:264`, `282-284`) and
  enforces the invariant "the step was opened iff trips exist" with an `assert`
  — in a **different module** than the one that made the decision.
- The phase order in the notebook's phase list is load-bearing and unchecked:
  reorder the list and you get a silently empty result (`phases.py:265-266`)
  or an assert failure (`phases.py:283`). The engine does not validate the
  schedule.

**Deletion test:** the seam between the two phases is hypothetical — nothing is
ever inserted between "decide the departure counts" and "spread them over
targets with the OD matrix". Merging removes the `intermediates` hand-off, the
cross-phase step sharing, and one ordering requirement from the notebook
contract. After the merge, check whether `intermediates` /
`with_intermediates` / `advance_period`'s clearing of it are needed at all.

**Benefits.** Locality: the whole departures-step contract (open once, stamp
`PERIOD_OWN_RANK`, stockout `lost` events and `departed` events share one
`step_id`) lives in one `execute`. Tests: the merged module can be tested
through its interface — demand in, `departed` flows plus `lost(stockout)`
events out — without running the whole engine.

**Note.** The notebook builds the phase list in a cell
(`phases_canonical = [DockArrivals("previous"), FormDeparturesPhase(),
FormPotentialTripsPhase(), DockArrivals("same")]`). That cell must change to a
three-phase list. Confirm with the user first (notebook cells are user-owned).
`Notations.md` §0.1 text about "the period's own departures and stockout
losses" already describes the merged behaviour, so the dictionary needs no
change, but grep the docs for phase names (`docs/` mentions `FormDeparturesPhase`
in several design documents — those are historical records and can stay).

---

## Candidate 3 — one "apply a step to the journal" operation

**Files:** `gbp/consumers/simulator/phases.py:134-155` (DockArrivals),
`phases.py:217-234` (FormDeparturesPhase), `state.py:224-234` (`open_step`).

**Problem.** Every phase must repeat the same write sequence by hand: build
events with the `flows.py` builders → stamp `phase_rank` → stamp `phase_round`
→ call `open_step` per ordered batch → stamp `step_id` onto the rows → adjust
inventory → return `PhaseResult`. This sequence IS the interface of "write one
inventory step correctly", and it is spread across all callers. A future
rebalancer phase would have to reproduce all of it with no help from the types.
Today's evidence of the burden: `DockArrivals` builds a `step_by_round` dict by
looping over rounds (`phases.py:151-155`), and `FormDeparturesPhase` must
remember the guard "open only when the step will carry at least one event"
(`phases.py:217-221`).

**Suggested change.** Deepen `SimulationState` (or add a small journal-writer
module next to it) with one operation: a phase hands over event batches with
their `phase_rank` and `phase_round`, and behind the seam the state opens the
step numbers, stamps all three columns, and appends to `state_flows_df`.
The exact interface shape is open — design it in conversation before coding
(candidates: one method taking a list of (events, rank, round) batches, or an
accumulating writer object per phase execution).

**Benefits.** Leverage: any new phase gets correct step numbering for free.
Locality: the rule "two separately ordered batches never share a `step_id`"
lives and is enforced in one place. Tests: step numbering becomes directly
testable at the state interface; today it is covered only by end-to-end
scenario runs (`tests/test_scenarios.py:179`,
`test_stamped_step_id_matches_tuple_order`).

**Order note:** do candidate 2 first — merging the departures phases removes
the hardest case (a step shared across two phases) before this seam is built.

---

## Candidate 4 — deduplicate the docking rule and the distance metric

**Files:** `gbp/consumers/simulator/mechanics.py`, `gbp/model/flows.py`.

**Problem A — the "dock in row order up to free capacity" rule exists twice:**

- `dock_up_to_capacity` (`mechanics.py:53-79`): `rank = groupby(target).cumcount();
  fits = rank < free`.
- The round loop inside `plan_overflow_redirect` re-implements the same rule
  inline (`mechanics.py:199`):
  `fits = now.groupby("realized_target_id").cumcount() < now["realized_target_id"].map(free)`.

The difference is only the target column (`planned_target_id` vs
`realized_target_id`). One function with a target-column parameter (the same
pattern `dock_deltas` already uses) covers both sites.

**Problem B — the neighbour-ranking metric exists twice:**

- `mechanics._nearest_free_station` (`mechanics.py:82-123`) ranks candidate
  stations by squared Euclidean distance on (lat, lng) — this one **decides**
  where a redirected bike goes.
- `flows._squared_distances` (`flows.py:855-866`) re-implements the same metric
  so `redirect_neighbor_table` can **explain** that decision afterwards. Its
  docstring admits the coupling ("The same metric the redirect mechanics ranks
  neighbours by").

If the mechanics ever change the metric or the tie-break, the explainer
silently reports an order that is not the one the simulator used. Put the
metric in one importable function used by both. Where it lives needs care:
`flows.py` must not import from `mechanics.py` (dependency direction), so
either the metric lives in `flows.py` (model layer) and mechanics imports it,
or in a tiny shared helper below both.

**Benefits.** Locality: the docking rule and the metric each change in one
place. Tests: `plan_overflow_redirect`'s round loop and the largest-remainder
rounding in `form_potential_trips` (`mechanics.py:300-308`) are the two
subtlest pieces of mechanics, and today they are exercised **only** through
end-to-end scenario runs (`overflow`, `redirect_chain`,
`test_redirect_rounds_are_distinct_steps`). A single docking function gets its
own direct tests.

---

## Candidate 5 — name the "real user departure" predicate and the inventory delta rule

**Files:** `gbp/model/flows.py`, `gbp/loaders/dataloader_graph.py`, tests.

**Problem A.** The filter `(event_type == "departed") & (move_id == 0)` — the
"real user departure" concept of `Notations.md` §0 — is written out at ~9 sites:

- `flows.py:421` (`phase_rank_by_timing`), `:567` (`flows_to_departures`),
  `:600` (`flows_to_od_matrix`), `:657` (`get_inventory_df`),
  `:718` (`_inventory_deltas`), `:838` (`flows_with_inventory`)
- `dataloader_graph.py:253-255` (`build_potential_trips`)
- `tests/test_scenarios.py:106`, `:301`; related check in `tests/invariants.py:72`

**Problem B.** The inventory delta rule (+1 at `realized_target_id` for a
docking `arrived`, −1 at `source_id` for a move-0 `departed`, everything else
moves nothing) is written out three times inside `flows.py` alone:
`get_inventory_df` (656-669), `_inventory_deltas` (705-721),
`flows_with_inventory` (837-841).

**Suggested change.** Small named functions inside `flows.py` — an internal
seam, not a new module: a user-departure predicate, a docking predicate
(already half-exists as `DOCKING_EVENT_TYPES`), and one delta-building
function. Point all read-models at them. `get_inventory_df` can likely be
rebuilt on top of `_inventory_deltas` (per-period = per-step aggregated by
period), collapsing copies further — verify with the base-replay equality
tests.

**Benefits.** A future event type that moves inventory (rebalancing) is added
in one place instead of nine. The read-models and the tests then share one
definition by construction, not by discipline.

**Keep in mind:** the live-state side (`state.py` delta builders +
`adjust_inventory`) intentionally maintains inventory incrementally, and
invariant I3 (`validation.py:64`) exists precisely to catch drift between the
live projection and the journal recomputation. Do not collapse the live side
into the journal side without discussing it — the double bookkeeping is a
feature.

---

## Candidate 6 — the canonical run executes with invariants off, and run orchestration is manual

**Files:** `gbp/consumers/simulator/engine.py:72-75`, `config.py:23`,
`gbp/consumers/simulator/sizing.py:74-95`, `dataloader_graph.py:520-552`
(`attach_simulation`), `notebooks/test_pipeline.ipynb`.

**Problem.** `EnvironmentConfig.validate` defaults to `False` and **nothing
ever sets it to `True`** — the only occurrence is `validate=False` in
`tests/scenarios.py:138`. So the engine branch at `engine.py:72-75` never runs;
invariants I1-I5 execute in exactly one place, the direct call in
`tests/test_scenarios.py:46` (`test_run_invariants_hold`). The canonical
notebook run is protected only by the inline asserts in phases and mechanics.

On top of that, the notebook must obey several silent contracts, where a
mistake produces a wrong result instead of an error:

1. The two tables returned by `size_state_for_demand` must be hand-assigned
   back onto `graph_data` **before** `Environment` is constructed. Forget it →
   the run silently uses the un-sized base state.
2. `attach_simulation` must be called after the run, or all seven
   `simulated_*` slots stay `None` (`dataloader_graph.py:487-493`).
3. The sizing config and the run config are two separate objects; if
   `run_scale > sizing_scale` the run silently produces stockouts/dock-full
   (the notebook documents this in a comment, nothing checks it).
4. `number_of_periods` is a free integer; if it exceeds the period grid the run
   silently covers fewer periods, and `check_flow_closure` (I2) then measures
   "due by the horizon" against the truncated horizon.

**Suggested direction (needs a design conversation, not a mechanical fix):**
turn `validate=True` on for the canonical run (cost: one `finalize_flows` +
one `inventory_at_moments` pass at run end — measure it), and reduce the number
of hand steps between the sizing run and the real run. Constraint 1 applies:
the notebook cells are user-owned, so any new surface must fit the existing
cell structure.

---

## Side findings (not architecture)

- **`SettingWithCopyWarning` on every canonical run**:
  `dataloader_raw.py:243-244` assigns `self.stations_capacities_df["capacity"] = 100`
  onto a `[["station_id"]]` slice. One-line fix (`.assign(capacity=100)` or
  `.copy()`). The real GBFS capacity loader `get_stations_capacities`
  (`dataloader_raw.py:78`) is commented out at line 242 — the constant 100 is
  the current intended behaviour.
- **`sizing.py` couplings worth knowing before touching it**: it relies on
  `copy.copy(resolved)` being a shallow copy where only two attributes are
  swapped (`sizing.py:74`); it encodes the saturation arithmetic
  `(n_commodities + 1) * SATURATION_QUANTITY` (`sizing.py:83-86`) so saturated
  capacity clears saturated occupancy; and its "no lost/redirected in the
  sizing journal" assert (`sizing.py:94-95`) is the guard that the measurement
  is valid. `size_state_for_demand` itself has **no test** — only the notebook
  drives it. A direct test would be a cheap win.
- **Test-surface summary**: builders and `finalize_flows` are well tested
  directly (`tests/test_flows.py`, including negative tests);
  `tests/invariants.py::check_journal_well_formed` is a strong structural check
  of the finalized journal. All of `mechanics.py`, `sizing.py`, and the real
  `ResolvedModelData.__init__` are tested only end-to-end or not at all
  (`tests/scenarios.py:96` fakes the container with a `SimpleNamespace`).

## Suggested order of work

1. **Candidate 1** (deletions) — lowest risk, shrinks everything after it.
   Decide the wide-panel question with the user first (1a).
2. **Candidate 4 + 5** (deduplications) — local, mechanical, well covered by
   existing scenario tests.
3. **Candidate 2** (merge the two departures phases) — needs the user's OK for
   the notebook phase-list cell.
4. **Candidate 3** (apply-a-step operation) — after 2, since 2 removes the
   cross-phase step case.
5. **Candidate 6** (validate on + run orchestration) — needs a design
   conversation; smallest code change, most interface judgment.

After each step: `ruff check`, `mypy gbp/`, `pytest tests/`, and a full
notebook run checking `simulated_* == historical_*`.
