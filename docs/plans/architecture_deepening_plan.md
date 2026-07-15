# Architecture deepening plan

Findings of an architecture review (2026-07-15, branch `city_bike_mvp_accounting`).
Each item is a refactor that hides more behaviour behind a smaller interface.
The goals are testability and locality: change, bugs, and knowledge concentrated
in one place instead of spread across callers.

Terms used below, defined once:

- A module is anything with an interface and an implementation: a function, a
  class, a package.
- The interface is everything a caller must know to use the module: types,
  invariants, call order, error modes, required config — not just the signature.
- A module is deep when a lot of behaviour sits behind a small interface. It is
  shallow when the caller must know almost as much as is written inside.
- A seam is the place where an interface lives: behaviour there can be replaced
  (for a test or a new implementation) without editing the code in place.

Line numbers are as of 2026-07-15; verify before editing.

## Constraints

Do not contradict the three recorded decisions in `docs/decisions/`:

1. The flow journal is the source of truth; read-models are pure functions of it.
2. The initial state is measured by a sizing run, not loaded.
3. A forecast run replaces only the demand table, in the one demand slot.

Do not touch the deep cores — they are fine as they are:

- `gbp/model/flows.py` (event schema, builders, read-models)
- `gbp/consumers/simulator/state.py` (`apply_step_events`)
- the `DemandModel` seam (4 families, nothing bypasses it)
- the `METRICS` table in `app/artifacts.py`
- `gbp/ml/features.py`, `gbp/ml/station_status.py`

Use the words from `Notations.md` for every domain concept.

## Candidates, ranked

Candidates 1–3 close one untested path — start scenario → validate → evaluate —
and give the largest joint win. Do them first, in order.

### 1. One typed run request for all four orchestrators

- **Files:** `app/runner.py:99-232` (`run_scenario`), `app/api.py:36-55`
  (`RunRequest`) and `:103-129` (`_worker_loop`), `app/backend.py:86-104` and
  `:154-178` (`run_and_wait` in both adapters), `app/evaluate.py:115-163`
  (`_ensure_run`), `app/artifacts.py:832-917` (`save_scenario_run`).
- **Problem:** the full parameter set of a run (`run_name`, both scale factors,
  `number_of_periods`, `demand_source`, `forecast_name`, the rebalancing block,
  truck fleet) is written out by hand four times. `evaluate._ensure_run` does
  not call `run_scenario` at all — it rebuilds the chain "apply forecast →
  sized run → save" itself. The rule "build the artifact from the data copy the
  run actually used (`data`, not `graph_data`)" exists only as a comment in
  `runner.py:218-219` plus discipline in each orchestrator;
  `test_app_runner.py::test_forecast_run_with_rebalancing_keeps_the_forecast`
  is a regression test for exactly this class of bug.
- **Solution:** one typed request object (dataclass or pydantic) that api,
  backend, and evaluate pass into `run_scenario` unchanged. Route the
  evaluation's replay-state forecast run through the same call — it only adds
  `sizing_data`. Make `save_scenario_run` take an explicit typed run context so
  "which data copy carries the run's real grid and t0" is a value, not a rule
  to remember.
- **Benefits:** the run recipe lives in one place; the four copies cannot
  drift; tests of the request cover every entry point at once.

### 2. Make the two-level evaluation testable

- **Files:** `app/evaluate.py` (348 lines): `evaluate_month:193-317`,
  `ensure_forecast:77-112`, `_ensure_run:115-163`, `_run_row:176-190`,
  `_panel_departed_mae:166-173`.
- **Problem:** the run-name grammar (`eval_<month>_<N>p_reference`,
  `eval_<month>_<N>p_<model>_forecast`), the "size on actual demand, run the
  forecast" rule, and the `comparison_<N>p.csv` layout are all inside one
  125-line function with no seam to replace the heavy simulator run. Zero tests
  cover the module's own logic — `test_evaluate.py` only exercises
  `restrict_demand_to_scenario`, a re-exported `gbp` function.
- **Solution:** extract the comparison bookkeeping (names, the reference-run
  rule, the comparison-row builder) into a small module that takes the run and
  load functions as parameters. Depends on candidate 1 for the run call.
- **Benefits:** the comparison logic gets millisecond-scale unit tests without
  the simulator; together with candidate 1 this covers the least-tested path
  in the project.

### 3. `validate_run` reads one object instead of loose scalars

- **Files:** `gbp/consumers/simulator/validation.py:42-104`,
  `gbp/consumers/simulator/engine.py:116-124`,
  `gbp/consumers/simulator/scenario.py:159-185`.
- **Problem:** to check invariant I1 the caller must pass the same
  `demand_scale_factor` and `number_of_periods` the `EnvironmentConfig` used —
  `validate_run` re-scales and re-filters the demand table itself
  (`validation.py:86-91`), in parallel with `FormDeparturesPhase`. There are
  two call sites, separated by the `validate=False` flag: `Environment.run`
  and `run_sized_scenario`. No test passes non-default scalars, and no test
  asserts that `RunInvariantError` is raised (`engine.py:124`,
  `scenario.py:184-185`).
- **Solution:** the run (or `ScenarioRun`) hands the validator the demand table
  it actually faced, or its config — one argument instead of scalars the
  caller must keep in sync. Collapse to one call site.
- **Benefits:** the validator's interface stops requiring knowledge of the
  engine's internals; the scaling and failure branches become directly
  testable.

### 4. A direct test seam for phase wiring

- **Files:** `gbp/consumers/simulator/phases.py:138-190`
  (`DockArrivals.build_events`),
  `gbp/consumers/simulator/rebalancing.py:657-708` and `:734-801`
  (`PlanRebalancingPhase.execute`, `ApplyRebalancingPhase.execute`).
- **Problem:** the pure mechanics (`dock_up_to_capacity`,
  `plan_overflow_redirect`, `target_inventory`, the VRP solver) have unit
  tests, but the risky part is the ordering: dock, then read inventory as it
  will stand (`inventory_after_events`), then plan redirects, with hand-set
  `phase_round` values; and in rebalancing a three-round sequence where
  `inventory_now()` is a closure over a mutable `batches` list
  (`rebalancing.py:753-760`). No test calls `build_events` or `execute`
  directly — coverage is only end-to-end through `run_scenario`.
- **Solution:** move each ordered sequence into a named function
  `(state, inputs, period) -> event batch` that a test can call without the
  engine and without OR-Tools.
- **Benefits:** ordering bugs get caught by unit tests in seconds, not by
  a full-month replay diff.

### 5. Declare the canonical phase order once

- **Files:** `gbp/consumers/simulator/scenario.py:41-43` (`canonical_phases`),
  `gbp/consumers/simulator/phases.py:120-136` (`DockArrivals.__init__`,
  `_due_arrivals`), `gbp/model/flows.py:652-689` (`phase_rank_by_timing`).
- **Problem:** the rule "dock-previous (0) < form-departures (1) < dock-same
  (2)" is encoded independently three times: the phase-list order, the
  `when` → rank mapping inside `DockArrivals`, and the timing rule in the
  historical loader. The sync mechanism is one oracle test
  (`test_stamped_step_id_matches_tuple_order`). A fourth phase means editing
  three sites correctly.
- **Solution:** one ordered declaration of phases from which both the engine's
  phase list and the historical ranks are derived; `phase_rank_by_timing`
  becomes a reader of the order, not a second author.
- **Benefits:** the phase order changes in one place; the oracle test turns
  from a safety net into a confirmation.

### 6. One shared tail for the three forecast builders

- **Files:** `gbp/ml/forecast.py:371-431` (`build_seasonal_naive_forecast`),
  `:458-536` (`build_model_forecast`), `:539-626` (`build_champion_forecast`).
- **Problem:** the three builders differ only in how the model and the history
  window are obtained (from a CSV, from training partitions, from the
  registry). The tail — forecast horizon grid → weather → `predict_horizon` →
  a ~10-field `ForecastMeta` → `save_forecast` — is copy-pasted three times
  and tested three times.
- **Solution:** one internal function owns the tail; the three public builders
  shrink to "resolve the model and the history window, then call it".
- **Benefits:** the forecast artifact contract is assembled in one place;
  a `ForecastMeta` change is one edit instead of three.

### 7. Comparison storage behind the `MlflowStore` seam

- **Files:** `gbp/ml/backtest.py:190-255` (`_log_comparison`,
  `latest_comparison`, `BacktestComparison`), `gbp/ml/pipeline.py:462-471`,
  `gbp/ml/registry.py:65-97`.
- **Problem:** `registry.py` is built to be the one module that knows MLflow,
  but `backtest.py` calls `mlflow.start_run`, `mlflow.search_runs`,
  `mlflow.artifacts.download_artifacts` directly, and `run_pipeline` knows the
  storage rule (run name `"comparison"`, file `"comparison.csv"`, match on
  `data_version`). The path "backtest in one command, promote in another" has
  no direct test.
- **Solution:** comparison read and write become methods on `MlflowStore`
  (`store.log_comparison` / `store.latest_comparison`); backtest and pipeline
  talk to the store, never to `mlflow.*`.
- **Benefits:** one seam again; comparison storage is testable through the
  store's interface without MLflow knowledge in callers.

### 8. One period-grid module

- **Files:** `gbp/ml/forecast.py:103-109` (`forecast_periods_from_meta`),
  `gbp/ml/data.py:43-55` (`month_period_grid`),
  `gbp/ml/monitoring.py:146-173` (`month_period_map`),
  `app/artifacts.py:907-909` (horizon facts recomputed into `RunMeta`).
- **Problem:** "how periods are numbered from a t0 with a period length" is
  stated four ways: built by `get_forecast_periods_df`, wrapped by
  `month_period_grid`, re-derived from meta by `forecast_periods_from_meta`,
  and re-implemented by `month_period_map`, which manually redoes the
  offset / `// DEFAULT_PERIOD_LEN` arithmetic to align two grids. No test pins
  that the four agree.
- **Solution:** one period-grid object that can build a grid from
  `(t0, n, period_len)` and align two grids by timestamp; the other functions
  become calls on it.
- **Benefits:** the numbering rule — the one the forecast run, monitoring, and
  training all meet on — lives in one place.

### 9. Stop threading parameters nobody reads on the way

- **Files:** `gbp/consumers/simulator/phases.py:81-95` (every phase takes the
  whole `EnvironmentConfig`), `gbp/consumers/simulator/config.py`;
  `gbp/ml/pipeline.py:248-284` (`step_backtest` forwards five overrides
  untouched) and ~43 call sites carrying `training_root`.
- **Problem:** every `build_events` accepts the full config but only
  `FormDeparturesPhase` and `PlanRebalancingPhase` read a field;
  `demand_scale_factor` passes through about six hops from `app/runner.py` to
  one multiplication. In `gbp/ml`, `training_root` / `raw` / `weather_df` /
  `store` play the same role: pure pass-throughs that exist so tests can point
  at `tmp_path`, re-declared in almost every signature.
- **Solution:** in the simulator, narrow the phase interface to the fields a
  phase reads (or bind `demand_scale_factor` into the demand table once at the
  run boundary). In `gbp/ml`, bundle the folder overrides into one small paths
  object created at the entry point, the same way `MlflowStore` is already
  passed as one object.
- **Benefits:** signatures stop lying about dependencies; adding a parameter
  is one edit to one object, not six signatures.

## Smaller findings

Each is a one-file fix, independent of the list above.

- **Model family dispatch stated twice.** `create_model` and `load_model` in
  `gbp/ml/models/__init__.py:30-83` are the same four-branch mapping written
  twice; the family list also appears in three argparse `choices`. One
  `{name: class}` mapping (values as import thunks to keep imports lazy)
  replaces both functions' bodies.
- **`data/` root computed three times.** `gbp/ml/data.py:31`,
  `gbp/loaders/download.py:99`, `app/artifacts.py:274` each resolve
  `DATA_DIR` with their own `parents[N]`. One `data_root()` that `raw_dir`,
  `ml_dir`, and `runs_root` derive from.
- **Backend seam: the `None` convention.** `DiskBackend.list_forecasts()`
  returns `list[str]`, `ApiBackend.list_forecasts()` returns `None`, and
  `app/views/run_scenario.py:56-73` must decode the three states (`None` /
  `[]` / names) — the page learns which adapter it holds. Return one small
  typed value (e.g. `can_list: bool` + `names`) from both adapters.
- **Trips cache block written twice.** The "processed parquet fresh → read it,
  else parse → schema-check → cache on success" block appears almost verbatim
  in `gbp/loaders/dataloader_raw.py:152-195` and
  `gbp/loaders/download.py:335-351`. Factor one cache-and-validate wrapper
  that takes the parse function.
- **`ScenarioInputs` conformance is one-sided.** The loader is checked by a
  never-called function `_supplies_scenario_inputs`
  (`gbp/loaders/dataloader_graph.py:966-972`); the second supplier — the test
  scenarios — is a `types.SimpleNamespace` (`tests/scenarios.py:103`) checked
  by nothing, so a missing field fails mid-run as `AttributeError`. Make the
  test supplier a real dataclass implementing the Protocol; drop the witness.
- **`truck_travel_minutes` repeats distance-over-speed.**
  `gbp/consumers/simulator/rebalancing.py:346-372` re-implements the
  haversine-over-speed formula that `Routes.duration_periods`
  (`gbp/routing.py:130-135`) already owns, plus a TODO for a car-profile
  `Routes` that has zero implementations today. Give `Routes` a
  speed-parameterised travel-time query; defer the car profile until a second
  implementation exists.

## Test gaps found along the way

Worth closing regardless of which candidates run:

- No test asserts `RunInvariantError` is raised (`engine.py:124`,
  `scenario.py:184-185`), and none passes non-default
  `demand_scale_factor` / `number_of_periods` to `validate_run`.
- `run_sized_scenario`'s `sizing_violations` raise
  (`scenario.py:145-150`) is untested.
- No test pins that the three `DATA_DIR` resolutions agree, or that the four
  period-grid functions agree.
- `refresh_dvc`'s real-folder guard (`pipeline.py:421`) is never taken under
  `tmp_path`.
