# Documentation audit — 2026-07-07

This file is a work list. It records every mismatch found between the
documents in `docs/` and the code, every coverage gap, and the proposed
folder restructuring. Each finding gives the doc location, the code evidence,
and the fix to make. Work through the checkboxes in the order of the
"Recommended fix order" section at the end. When everything is done, move
this file to `docs/archive/`.

How the audit was made: every active document was checked claim by claim
against the source code (function names, phase order, column names, quoted
code snippets, "who calls what" tables). Documents verified as clean are
listed at the end of section 1.

---

## 1. Mismatches with the code

### 1.1 `rebalancing.md` — fabricated check (most serious)

- [x] **Fix `docs/rebalancing.md:307-315`** ("What Execution Checks").
  The doc says: *"At the end of each period, `ApplyRebalancingPhase` checks
  that the inventory change matches the events it executed"* and quotes:

  ```python
  moved = int(inventory["quantity"].sum()) - inventory_before
  assert moved == docked_n - picked_n
  ```

  This code does not exist. `ApplyRebalancingPhase.execute`
  (`gbp/consumers/simulator/rebalancing.py:716-782`) has no such assert and
  no variables `inventory_before`, `moved`, `docked_n`, `picked_n`. The only
  asserts in `rebalancing.py` are at lines 581 and 599, both inside
  `assign_bikes_to_stops` (planning, not execution). The real inventory
  guarantee for rebalance flows comes from the run-level invariants I2/I4/I5
  in `validate_run` (`gbp/consumers/simulator/validation.py:38-87`) — which
  the doc correctly describes right after, at lines 325-328.
  **Fix:** delete the fabricated quote and rewrite the paragraph: execution
  itself asserts nothing; correctness is enforced by `validate_run` after
  the run, plus the two planning asserts in `assign_bikes_to_stops`.

### 1.2 `simulator.md` — same fabricated checks in the "Phase Checks" table

- [x] **Fix `docs/simulator.md:580-581`.** The table under "These checks run
  inside mechanics and phases" (line 572) has six rows. Four point at real
  asserts (`mechanics.py:92`, `:263`, `:295-296`, `phases.py:167`). Two do
  not:
  - `FormDeparturesPhase | Stockout losses do not move inventory.` — no
    assert exists in `FormDeparturesPhase.build_events`
    (`phases.py:209-249`). True by construction only: `lost` rows are
    neither docking nor undocking in `_event_deltas`.
  - `ApplyRebalancingPhase | Inventory change equals docked dropoffs minus
    pickups.` — no assert exists in `ApplyRebalancingPhase.execute`
    (`rebalancing.py:716-782`). True by construction only: inventory is
    derived from events via `inventory_deltas_from_events`.

  **Fix:** either remove these two rows, or move them to a separate note
  that says these properties hold by construction (the delta rule derives
  inventory from events), not by a runtime check.

### 1.3 `flow_journal.md` — behind the `_event_deltas` refactor

- [x] **Fix `docs/flow_journal.md:246-255`.** The inventory-delta snippet is
  shown under `def _inventory_deltas(flows):` and uses the variable `dep`.
  The current `_inventory_deltas` (`gbp/model/flows.py:1027-1035`) is a
  one-line delegator: `return _event_deltas(flows, ("step_id", "period_id"))`.
  The shown body now lives in `_event_deltas` (`flows.py:147-164`) and the
  variable is named `undock`, not `dep`.
  **Fix:** re-quote the real `_event_deltas` body and explain the delegation.
- [x] **Fix `docs/flow_journal.md:370`.** Claim: *"`_inventory_deltas` is the
  only place that says which event moves inventory and by how much."* The
  single place is now `_event_deltas` (`flows.py:147-164`); both the
  read-time `_inventory_deltas` (`flows.py:1027`) and the write-time
  `inventory_deltas_from_events` (`flows.py:167-191`, used by
  `SimulationState.apply_step_events`) delegate to it.
- [x] **Fix `docs/flow_journal.md:346`** ("Who Calls What" table). Claim:
  the loader calls *"`flows_with_measures` for the wide journal"*. False:
  `gbp/loaders/dataloader_graph.py` neither imports nor calls it; the only
  caller outside the model layer is `app/artifacts.py:686`. (dataloader.md
  lines 294-312 already states correctly that widening is model-layer.)
  **Fix:** remove that clause from the loader row.
- [x] **Fix `docs/flow_journal.md:236`.** Claim: each marginal read-model
  returns *"one row per `(period_id, facility_id, commodity_category)`"* —
  over-generalized. `flows_to_od_matrix` (`flows.py:855-867`) is keyed by
  `(source_id, planned_target_id, period_id, commodity_category)`; there is
  no `facility_id`. **Fix:** exclude the OD matrix from that sentence.
- [x] **Code-side aside:** the comment at `gbp/model/flows.py:1438` says
  "`validate_run` can collect I1-I4", but `validate_run` collects I1-I5
  (`validation.py:83-87`). The doc (flow_journal.md:338) is right; the code
  comment is stale. Fix the comment.

### 1.4 `app.md` — one stale claim, one half-true claim

- [x] **Fix `docs/app.md:170-171`.** Claim: *"`load_run_table` and
  `load_run_meta` are the raw file reads; only the `ui_shared` loader calls
  them."* False for `load_run_meta`: it is also called by `app/api.py:182`,
  `app/api.py:190` and by the runner CLI at `app/runner.py:261`. Only
  `load_run_table` is exclusive to `ui_shared` (`app/ui_shared.py:81`).
- [x] **Fix `docs/app.md:183-187`.** The loader description ("The cache key
  includes the file's modification time, so a rewritten artifact invalidates
  itself") is true only for the local backend. With `API_URL` set, the cache
  key is the constant `0.0` (`app/ui_shared.py:89-99`) because served
  artifacts are immutable. Say the mtime key is the local-backend rule and
  point to api.md for the HTTP rule.
- [x] **Fix `docs/app.md:172`** (minor). *"The Run scenario page calls
  `next_free_run_name` before it runs"* — true only for the local backend
  (`app/views/run_scenario.py:150`); with `API_URL` set the server resolves
  the name (`run_scenario.py:153-156`).

### 1.5 `dataloader.md` — minor drift

- [x] **Fix `docs/dataloader.md:253`.** Claim: *"`__init__` ends with an
  assert"*. The assert is at `gbp/loaders/dataloader_graph.py:717-721`, but
  `__init__` actually ends with `check_engine_tables(self)` at
  `dataloader_graph.py:727-731`. Say the assert is followed by the schema
  check (and see gap 2.4 below).
- [x] **Fix `docs/dataloader.md:58-60`.** Claim: *"`get_depots(rng, n)`
  synthesizes depots ... with random capacities and fixed costs."*
  `get_depots` (`dataloader_raw.py:165-173`) generates only
  `depot_id`/`lat`/`lng`; capacities come from `get_depots_capacities`
  (`:176-184`) and costs from `get_depots_costs` (`:187-191`).
- [x] **Fix `docs/dataloader.md:94`** (build-order table, minor). Doc order
  of attributes disagrees with `dataloader_graph.py:639-650` in three spots:
  code builds `resources_capacities_df` before `facilities_costs_df`;
  `period_len` before `t0` (`:653-654`); `routing_mode` before `routes`
  (`:687-688`).
- [x] **Fix `docs/dataloader.md:129`** (minor). The `get_historical_flows_df`
  snippet ends with `return finalize_flows(journal)`, but the real code
  (`dataloader_graph.py:217-225`) runs `check_journal_schema(flows)` and
  raises on violations before returning. Re-quote with the check.

### 1.6 `README.md` + `README_ru.md` — the API is missing entirely

- [x] **Fix `docs/README.md:91-101`** (and the same lines in README_ru.md —
  the pair is otherwise in sync). *"Five documents cover the run chain"*
  lists dataloader, simulator, rebalancing, flow_journal, app — api.md is
  not there. The repo map (`README.md:68-75`) describes `app/` without
  `api.py`, `api_client.py`, `ui_shared.py`. Meanwhile `architecture.md`
  draws the API as a fifth block and links api.md — the two documents are
  out of sync. **Fix:** add api.md as the sixth run-chain document and add
  the three files to the repo map, in both languages.
- [x] **Fix `docs/README.md:37`.** Install command is
  `uv pip install -e ".[dev,ui]"`, but `fastapi` lives only in the `api`
  extra (`pyproject.toml:43`). CLAUDE.md already says `.[dev,ui,api]`.
  Root `README.md:15-16` has the same problem (`.[ui]` / `.[dev,ui]`).
  Nothing in the documented install path lets a reader run `app/api.py`.
  Also consider adding the `uvicorn api:app --app-dir app` command to the
  "How to run" section.
- [x] **Root `README.md:64`** (nitpick). Example filename
  `202602-citibike-tripdata_1.csv`; the shipped file and the runner default
  (`app/runner.py:34`) are `202601-...`.
- [x] **Note, no action needed:** `data/osrm/` does not exist on disk until
  `scripts/osrm/*.sh` generate it. The layout descriptions in both READMEs
  and architecture.md are correct after setup; optionally mark it
  "(created by the OSRM setup)".

### 1.7 `deepening_candidates.md` — an executed plan that quotes deleted code

- [x] **Move `docs/deepening_candidates.md` to `docs/archive/`.** It is a
  dated review snapshot (2026-07-06); its glance table marks all 8
  candidates `done`, but its "Files"/"Problem" prose still describes the
  pre-refactor code. Symbols it names that no longer exist anywhere:
  `departure_deltas_from_counts`, `SimulationState.with_inventory`,
  `SimulationState.with_in_transit` (lines 71-88), `build_panel` at old
  `artifacts.py` lines (223, 240 — the read-model is now `flows_to_panel`,
  `gbp/model/flows.py:968`), `get_flows_wide`, `_FLOW_FACILITY_ROLES`,
  `_join_capacity`/`_join_geo`/`_join_inventory` (259-264), `Phase.name`
  (150-151 — phases now key off `phase_rank`). Read as a to-do list it is
  misleading; as history it is fine. No rewrite needed — archive it.

### Verified clean (no action)

- `docs/scenarios.md` — all 14 scenarios match `tests/test_docs_scenarios.py`
  one to one (exact test names, no orphan tests, columns and statuses match
  `gbp/model/journal_schema.py`). One optional nitpick: lines 33-37 say
  scenarios 10-14 inject `scripted_stops`, but scenario 14 injects
  `_failing_solver` instead — the doc corrects itself at lines 572-584.
- `docs/architecture.md` — every named module, arrow, interface, anchor,
  and link verified; nothing invented, nothing missing.
- `docs/osrm_setup.md` — paths, scripts, runner flags, `gbp/routing.py`
  interface, and docker-compose mount all check out.

---

## 2. Coverage gaps (fundamental things with no description)

### 2.1 api.md is a design document, not a reference of the running API

The user-visible symptom: "there is no API description, only how to build
it". The precise state: the endpoints section (`docs/api.md:58-148`) is
accurate — all six endpoints in `app/api.py` match it (paths, methods,
bodies, response shapes, 404 rules, status enum, disk fallback). The
problems are genre and navigation:

- [x] **Rewrite `docs/api.md` from future tense to present tense.** Title
  is "API design"; sections say "On a shared server this changes",
  "Streamlit becomes a client", "Two files change". All of it is already
  implemented and committed (`app/api.py`, `app/api_client.py`, two-backend
  loader in `app/ui_shared.py:69-152`, two-backend Run page in
  `app/views/run_scenario.py:49-193`). Keep the endpoint reference and the
  "Why it is built this way" section; turn "Changes in the Streamlit app"
  into a description of the two backends as they exist.
- [x] **Document `app/api_client.py`.** Its public functions — `api_url`,
  `list_runs`, `load_meta`, `load_table`, `start_run`, `run_status`
  (`app/api_client.py:24-75`) — are documented nowhere; api.md names the
  module once.
- [x] **Small endpoint-reference gaps while rewriting:**
  - `GET /runs/{run_name}` returns 404 for an unknown run
    (`app/api.py:189`) — stated for tables, not for this path.
  - `POST /runs` validates `run_name` against the pattern
    `^[A-Za-z0-9][A-Za-z0-9._-]*$` (`app/api.py:45`) — not in the doc.
  - api.md:63 says the tables endpoint "mirrors `load_run_table`", but
    `app/api.py:202-203` reads `path.read_bytes()` directly.
  - The dataset is built by the worker thread on first execution
    (`app/api.py:82-119`), not at `POST /runs` request time (api.md:52-54).

### 2.2 Runner CLI flags

- [x] `app.md` shows only two example invocations (lines 80-83). Five flags
  of `app/runner.py:204-240` appear in no document's runner section:
  `--sizing-scale`, `--trips-path`, `--truck-capacity`, `--routing`,
  `--osrm-url` (the last two are shown in osrm_setup.md only). Add a full
  flag list to app.md (or a short reference table in README).

### 2.3 `sizing_scale_factor` — the mechanism that creates stockouts

- [x] `simulator.md:170-183` ("How A Run Starts") describes sizing as if the
  state is sized to the same demand the run faces. `run_sized_scenario`
  (`gbp/consumers/simulator/scenario.py:57-111`) has two independent
  factors: the sizing run uses `sizing_scale_factor` (`scenario.py:108`),
  the real run uses `demand_scale_factor` (`scenario.py:133`). Sizing to a
  different demand than the run faces is what makes stockout/dock-full
  events appear at all. Plumbed through `app/runner.py:94,162,189,254`.
  Add this to simulator.md.

### 2.4 The pandera schema layer at the load boundary

- [x] `dataloader.md` never mentions load-time schema validation — the
  modules' main fail-fast mechanism: `TRIPS_SCHEMA`
  (`gbp/loaders/dataloader_raw.py:42-64`, enforced at `:133-138`); the six
  engine-table schemas and `ENGINE_TABLE_SCHEMAS`
  (`gbp/loaders/dataloader_graph.py:50-130`), enforced by
  `check_engine_tables` at `:727-743`; `check_journal_schema` on the
  historical journal at `:220-224`. Add a section.

### 2.5 The journal schema check runs on every run, not only in tests

- [x] `simulator.md:587-593` lists the run checks as exactly I1-I5 and frames
  journal-shape validation as test-only (lines 598-615). In fact
  `validate_run` runs `check_journal_schema(flows)` first
  (`gbp/consumers/simulator/validation.py:82`), enforcing dtypes, the legal
  `event_type`/`flow_type`/`reason` values, `move_id == event_id // 2`, and
  "`realized_target_id` set exactly on `arrived`"
  (`gbp/model/journal_schema.py:56-93,122-135`). Correct the framing.

### 2.6 Undocumented public functions of the journal library

- [x] `flow_journal.md` Code Map (lines 27-38) omits
  `inventory_deltas_from_events` (`flows.py:167-191`, the write-time delta
  rule used by `SimulationState.apply_step_events`) and
  `in_transit_after_events` (`flows.py:569-599`, maintains the in-transit
  working set across an event batch). Add both (and `_event_deltas` as the
  shared rule, per finding 1.3).

### 2.7 Rebalancing configuration guards

- [x] `rebalancing.md` never mentions that `PlanRebalancingPhase.execute`
  raises `SimulatorConfigError` (`rebalancing.py:674-683`) in three cases:
  no trucks, a truck with a null `home_facility_id`, a home depot absent
  from the facility tables. Run-aborting behavior; add it.
- [x] Minor: the `rebalance_plan` column list (`rebalancing.md:73-75`) omits
  `commodity_category`, `pickup_minute`, `dropoff_minute`
  (`PLAN_DTYPES`, `rebalancing.py:83-94`); the minutes are mentioned later
  (line 217), `commodity_category` never.

### 2.8 Secondary construction guards of the engine

- [x] `simulator.md:224-227` mentions only the phase-rank-order guard.
  Also raising `SimulatorConfigError`: `number_of_periods` larger than the
  period grid (`engine.py:53-57`); both `historical_demand_df` and
  `initial_inventory_df` empty (`engine.py:40-43`);
  `number_of_periods < 1` or `demand_scale_factor <= 0`
  (`config.py:27-32`). Minor; add a sentence.

### 2.9 The two-backend loader is described only in api.md

- [x] `app.md:9-11` calls the UI "a pure reader of saved artifacts" and
  describes only local file reads, but `app/ui_shared.py:69-152` branches
  on `api_client.api_url()` everywhere and `views/run_scenario.py:49-74`
  has a full server backend. This is documented in api.md, so it is a
  scoping decision: either add a short "two backends" note to app.md with
  a link to api.md, or state explicitly that app.md describes the local
  backend only.

---

## 3. Folder restructuring

### The problem

`docs/` mixes four genres in one flat list: explainers of the code
(architecture, dataloader, simulator, rebalancing, flow_journal, app, api,
scenarios), how-to guides (osrm_setup), documents about the way of working
(comprehension_levels, working-method — the latter says of itself "not
documentation of the code"), and plan snapshots (deepening_candidates;
api.md currently reads as one too).

### Proposed layout

```
docs/
  README.md, README_ru.md      # entry point and navigation — stays at the top
  explanation/                 # how the code works: architecture, dataloader,
                               #   simulator, rebalancing, flow_journal, app,
                               #   api, scenarios
  guides/                      # how to do something: osrm_setup
                               #   (+ a future "run on a server" guide)
  method/                      # about the way of working: comprehension_levels
                               #   (+ _ru), working-method (+ .ru)
  archive/                     # plan snapshots: existing archive
                               #   + deepening_candidates + this audit when done
```

### Paths that must be updated when files move

Code and tests reference docs by path; a move without these updates breaks
the test suite and the docstrings.

- `tests/test_docs_scenarios.py` — **reads `docs/scenarios.md` at test
  time** (asserts anchors exist, line 164) and names it in the module
  docstring. The file path constant must follow the move.
- `docs/api.md` referenced from: `app/api.py:1,165`,
  `app/api_client.py:1,39`, `app/views/run_scenario.py:3,153`,
  `app/ui_shared.py:67,94`, `tests/test_app_api.py:1,200`, `CLAUDE.md:16`.
- `docs/osrm_setup.md` referenced from: `gbp/routing.py:11`, root
  `README.md:65`, `docs/README.md:48`.
- `docs/README.md` referenced from root `README.md:7`.
- `docs/scenarios.md` also referenced from `tests/test_scenarios.py:90`
  (comment) and `docs/README.md`.
- Cross-links inside `docs/` (README ↔ architecture ↔ level-3 docs ↔
  scenarios; `../Notations.md` anchors) — every relative link must be
  re-checked after the move; links from files inside a subfolder to
  `Notations.md` become `../../Notations.md`.
- `.claude/skills/documentation-style/SKILL.md` targets `docs/**/*.md` —
  still matches after the move; no change needed.

Do the move as one commit: `git mv` + path updates + run `pytest` (the
docs-scenarios tests prove `scenarios.md` is still found).

---

## 4. Recommended fix order

1. **Facts first** (wrong statements are worse than missing ones):
   findings 1.1, 1.2 (fabricated checks), 1.3 (`_event_deltas` drift),
   1.4, 1.5.
2. **The API story:** rewrite api.md to present tense + document
   `api_client.py` (2.1), add the API to both READMEs and the install
   command (1.6), decide app.md's scoping note (2.9).
3. **Coverage gaps in the level-3 docs:** 2.2-2.8.
4. **Archive** `deepening_candidates.md` (1.7).
5. **Folder move** (section 3) — last, in one commit, with the path-update
   list above and a green `pytest`.
