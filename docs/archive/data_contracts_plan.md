# Data Contracts Plan

> **Status: implemented on 2026-07-07.** All five steps are in the code. One
> deviation from step 5: `Phase.build_events` was **not** made an `abc`
> abstract method. The two rebalancing phases (`PlanRebalancingPhase`,
> `ApplyRebalancingPhase`) override `execute` and never implement
> `build_events`, so an abstract method would forbid instantiating them; the
> base class keeps `raise NotImplementedError` and its docstring now states
> the contract. Found during implementation: the `rate` column of the widened
> journal was `int64` when the rates table carried integer prices;
> `flows_with_costs` now casts it to `float64`.

This document is an implementation plan. It says where to add explicit data
contracts — pandera schemas for tables and pydantic models for JSON — and
where not to add them. A contract here means: a written description of a
table's shape (columns, dtypes, allowed values) that is checked at the moment
the data crosses a module boundary, not at the end of the run.

The plan was produced by a review of the codebase on 2026-07-07. Every code
reference below was verified against the source at that date.

## Starting point

`pandera` and `pydantic` are already declared as dependencies
(`pyproject.toml:11-12`) but are never imported anywhere in `gbp/`, `app/`, or
`tests/`. Adding them costs nothing in dependencies. (`structlog`,
`pyproject.toml:13`, is also declared and unused; it is not part of this plan —
if it stays unused, remove it from the dependencies.)

The project already checks a lot — but late, at run end, not at the boundary
where the data enters:

- `FLOW_EVENT_COLUMNS` and `FLOW_EVENT_DTYPES` (`gbp/model/flows.py:41-83`)
  are a hand-written schema of the flow journal. The builders cast to these
  dtypes (`_typed_events`, `flows.py:197`), and `finalize_flows`
  (`flows.py:742`) casts again and projects to the column list.
- `check_journal_well_formed` (`tests/invariants.py:29`) checks the journal's
  structure by hand: exact column list, `quantity >= 1`, `event_type` in the
  legal set, `move_id == event_id // 2`, `realized_target_id` set only on
  `arrived`, per-flow event sequence. It lives in the test tree only.
- The run invariants I1–I5 (`gbp/consumers/simulator/validation.py:37`) check
  quantities at run end.
- `ResolvedModelData.__init__` ends with one hand-written `assert` comparing
  the initial inventory to the historical inventory at period 0
  (`gbp/loaders/dataloader_graph.py:597-613`).
- `save_run` checks only that the five table names are present
  (`app/artifacts.py:521-523`); the tables' columns are not checked.
- `meta.json` is a plain `dict[str, Any]`: `build_meta` builds it
  (`app/artifacts.py:379`), `load_run_meta` returns it unchecked
  (`app/artifacts.py:539`), and the UI indexes into it by string key
  (`app/views/home.py:33-41`, `app/ui_shared.py:198`).

## Rules for the whole plan

1. Check at boundaries, once per crossing. Never validate inside the
   per-period phase loop: the journal grows every period, so a per-phase check
   makes the run quadratic in time.
2. Each schema lives next to the data it describes: journal schema in
   `gbp/model/`, loader schemas in `gbp/loaders/`, artifact schemas in
   `app/artifacts.py`. No central `contracts/` package.
3. Build each schema from the constants that already exist
   (`FLOW_EVENT_DTYPES`, `FLOW_EVENT_COLUMNS`), so the column list is written
   once.
4. Validate with `lazy=True` so one check run reports all violations, matching
   the existing style of the invariants (a list of violations, empty = valid).
5. Use the vocabulary of `Notations.md` in schema names and error messages.

## Step 1 — pandera schema for the flow journal

The flow journal is the single source of truth (Notations.md §0); its schema
is the most valuable contract.

What to add:

- New module `gbp/model/journal_schema.py` with a pandera `DataFrameSchema`
  named `FLOW_EVENT_SCHEMA`. Columns and dtypes come from
  `FLOW_EVENT_DTYPES`; on top of that, the checks that today live as
  hand-written code in `check_journal_well_formed`
  (`tests/invariants.py:42-73`):
  - `quantity >= 1`;
  - `event_type` in `{"departed", "arrived", "redirected", "lost"}`;
  - `flow_type` in `{"user_trip", "rebalance"}` (Notations.md §0);
  - `reason` in `{"stockout", "dock_full"}` or NA;
  - table-level check `move_id == event_id // 2`;
  - table-level check: `realized_target_id` is set exactly on `arrived` rows;
  - column order equals `FLOW_EVENT_COLUMNS` (pandera `ordered=True`).
- A small wrapper `check_journal_schema(flows) -> list[str]` that runs the
  schema with `lazy=True` and converts pandera's error report into the
  project's list-of-violations form.

Where it runs (once per journal, not in `finalize_flows` itself —
`finalize_flows` is called from the `Environment.simulated_flows_df` property
on every access, `gbp/consumers/simulator/engine.py:75`):

- `validate_run` (`gbp/consumers/simulator/validation.py:67`): after
  `finalize_flows`, before I1–I5. A schema violation there is reported through
  the same violations list as the run invariants.
- `get_historical_flows_df` (`gbp/loaders/dataloader_graph.py:121`): validate
  the historical journal once at load time.

Import note: keep `journal_schema.py` out of `gbp/model/__init__.py` and
import it directly from the two call sites, so importing `gbp.model` does not
pay the pandera import cost.

Cleanup in the same step: `check_journal_well_formed` keeps only what pandera
cannot express row-wise — the per-flow event sequence (the
`LEGAL_FLOW_SHAPE` regex, event ids contiguous, `period_id` never decreasing
inside a flow, stockout losses aggregated and id-less) — and calls
`check_journal_schema` for the schema part instead of comparing columns by
hand.

## Step 2 — pydantic model for `meta.json`

The run artifact (Notations.md §12) is a boundary between processes: the
runner writes it, the Streamlit app reads it later, possibly with a different
code version. Today a missing key fails as a `KeyError` in the middle of
rendering a page.

What to add, in `app/artifacts.py`:

- A pydantic model `RunMeta` with the fields `build_meta` writes today
  (`app/artifacts.py:428-441`): `run_name`, `scenario_id`,
  `demand_scale_factor`, `sizing_scale_factor`, `number_of_periods`,
  `period_len_hours`, `routing_mode`, `t0`, `created_at`, `violations`,
  `rebalancing`, `totals`. Nested model for `rebalancing` (`enabled`, and when
  on also `truck_homes`, `truck_capacity_bikes`).
- `build_meta` constructs a `RunMeta` and returns it; `save_run` writes
  `meta.model_dump_json(indent=2)`.
- `load_run_meta` parses the file through `RunMeta.model_validate_json` and
  returns the model. An old artifact missing a field fails at load with a
  clear pydantic error naming the field.

Call sites to update (attribute access instead of string keys):
`app/views/home.py:33-41`, `app/views/distance_duration.py:15`,
`app/ui_shared.py:118`, `app/ui_shared.py:178-179`, `app/ui_shared.py:198`,
`app/ui_shared.py:574`, plus `app/runner.py` and the test fixtures that call
`build_meta` (`tests/test_app_artifacts.py`).

## Step 3 — pandera schemas at the loader boundary

The raw trip CSV is the only external data in the project — the classic
place for pandera. Today `load_trips_raw_df` (`gbp/loaders/dataloader_raw.py:16`)
fixes dtypes and drops rows with missing key fields, but nothing checks value
ranges; a bad CSV fails later, as an unrelated pandas error or an invariant
violation at run end.

What to add:

- In `gbp/loaders/dataloader_raw.py`: a schema for the loaded trips table —
  required columns, dtypes, `started_at <= ended_at`, coordinates within a
  loose bounding box around the service area. If the real CSV contains rows
  with `ended_at < started_at`, extend the loader's existing row cleanup to
  drop them (and keep the schema check strict) rather than weakening the
  schema.
- In `gbp/loaders/dataloader_graph.py`: schemas for the tables the engine and
  its phases read from `ResolvedModelData` (listed in its docstring,
  `dataloader_graph.py:492-495`): `periods_df`, `initial_inventory_df`,
  `historical_demand_df`, `historical_od_matrix_df`,
  `facilities_capacities_df`, `facilities_geo_df`. Typical checks:
  `quantity >= 0`, `capacity >= 0`, unique keys
  (`facility_id, commodity_category` for inventory; `period_id` for the
  grid), non-null ids.
- Validate once at the end of `ResolvedModelData.__init__`. The hand-written
  `assert` at `dataloader_graph.py:597-613` stays — it is a cross-table
  consistency check, not a shape check.
- `run_sized_scenario` replaces `initial_inventory_df` and
  `facilities_capacities_df` with the sized tables
  (`gbp/consumers/simulator/scenario.py:108-110`); validate these two against
  the same schemas right after sizing.

## Step 4 — pandera schemas for the run-artifact tables

`save_run` writes five tables (`RUN_TABLES`, `app/artifacts.py:30`) and
checks only their names. A table with a wrong column set is saved fine and
fails later, when a page tries to draw it.

What to add, in `app/artifacts.py`:

- One schema per table of `RUN_TABLES` (`flows`, `panel`, `arcs`,
  `flow_totals`, `facilities`), with the columns listed in Notations.md §12.
  The `flows` schema extends the journal schema from step 1 with the measure
  columns added by `flows_with_measures`.
- Validate in `save_run` before writing. Do not validate in
  `load_run_table`: reading is on every page load, and what was written was
  already checked.

## Step 5 — small typing contracts

- `EnvironmentConfig` (`gbp/consumers/simulator/config.py:12-25`): add a
  `__post_init__` with two checks — `number_of_periods >= 1`,
  `demand_scale_factor > 0`. Pydantic is not needed here.
- `routing_mode: str` → `Literal["haversine", "osrm"]`
  (`gbp/loaders/dataloader_graph.py:522` and `gbp/routing.py`).
- `DockArrivals(when: ...)` → `Literal["previous", "same"]`
  (`gbp/consumers/simulator/phases.py:87`).
- `Phase.build_events` (`gbp/consumers/simulator/phases.py:76`): mark the
  base class abstract with `abc` instead of `raise NotImplementedError`.

## What not to add

- `typing.Protocol`. The only place with several implementations of one
  interface is `Phase` (`gbp/consumers/simulator/phases.py:46`), and the base
  class already is the contract: it fixes `phase_rank` and `build_events`,
  and the engine checks the phase order at construction. A protocol would be
  a horizontal abstraction, against the project rule "vertical, not
  horizontal" (CLAUDE.md).
- `@pa.check_types` on the functions in `gbp/model/flows.py`. The
  intermediate event frames are incomplete on purpose: the ordering columns
  `phase_rank`, `phase_round`, `step_id` are stamped only in
  `SimulationState.apply_step_events` (`gbp/consumers/simulator/state.py:194`).
  A schema per intermediate shape means dozens of schemas and a validation
  cost inside the per-period loop.
- Validation inside `finalize_flows` or inside the phase loop — see rule 1.

## Order and verification

Implement in the step order above; each step is independent and can be a
separate commit. After each step:

```bash
ruff check gbp/ tests/ app/
mypy gbp/
pytest
python app/runner.py --run-name contracts_check   # one full run saves cleanly
streamlit run app/main.py                          # the saved run renders
```

The canonical scenario (`notebooks/test_pipeline.ipynb`) must run unchanged:
the contracts only reject data that is already wrong, they never change what a
valid run computes.

After the plan is done, add the new terms to `Notations.md` if any new
concept name appears (for example `RunMeta`), and move this file to
`docs/archive/`.
