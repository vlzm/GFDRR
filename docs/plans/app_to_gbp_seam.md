# app → gbp seam — hand-off spec

This document is the decided seam between `app/` and `gbp/`. For every non-UI
computation that lives in `app/` today it gives a verdict — **move** into a
named `gbp` home, **stay** in `app/`, or **split** — and the exact contract
text the move forces. The rule "app draws, gbp computes" already exists
(Notations §12); this effort relocates the computation, it does not invent the
rule. No code is moved by this document: it decides, a later execution effort
applies it.

The decisions were reached over the tickets in
`.scratch/app-to-gbp-seam/`; this file collects them.

## Problem Statement

Several files under `app/` hold no Streamlit code yet do real domain work, and
they reach deep into `gbp`. `app/artifacts.py` builds, saves, and loads a run
artifact — a `gbp` concept named in Notations §12/§15/§16. `app/evaluate.py`
and `app/eval_comparison.py` drive `gbp.ml` end to end. `app/runner.py` holds
the run recipe (`RunRequest`) and the fleet defaults, domain policy about how a
scenario is configured. Because this code sits in `app/`, the run-artifact
contract and the run recipe are documented against `app/` paths, `gbp/artifacts`
and `gbp/consumers` code has to import back through `app/` at the type level
(the `artifacts`↔`runner` cycle), and the "gbp computes, app draws" boundary is
blurred: a reader cannot tell app's drawing code from app's computing code.

## Solution

Move each non-UI computation into a concrete `gbp` home, leaving `app/` as a
pure reader of run artifacts plus the FastAPI service and the CLI wrappers. The
run-artifact build/store lives in one new `gbp/artifacts.py`; the run recipe
and orchestration live in `gbp/consumers`; the evaluation cluster joins
`gbp/ml`. `app/` keeps only genuine draw + IO glue. The `gbp`-never-imports-`app`
boundary (true today) becomes the single seam the whole system is organised
around, and the canonical docs point at the real homes.

## The seam

There is one seam, and it already exists: the `gbp` package boundary. `app`
imports `gbp`; `gbp` never imports `app` (verified: no `import app` anywhere in
`gbp/`). Every verdict below moves code *across* this one seam or leaves it
where it is — no new seam is introduced. This is the highest seam available and
the fewest possible (one). Tests exercise the moved code through its new `gbp`
entry point; `app/` is tested as a reader.

## User Stories

1. As a developer reading `app/`, I want every file there to be either UI
   drawing or IO glue, so that I never have to guess whether a file computes or
   draws.
2. As a developer, I want the run-artifact builder in `gbp`, so that the thing
   the canonical docs call a `gbp` concept actually lives in `gbp`.
3. As a developer, I want the run-artifact contracts (`RunMeta`,
   `RUN_TABLE_SCHEMAS`, `METRICS`) next to the tables they describe, so that a
   schema change and its builder change sit in one module.
4. As a developer, I want `save_scenario_run` to take `RunRequest` from `gbp`,
   so that the `artifacts`↔`runner` import cycle disappears.
5. As a developer running the evaluation from the terminal, I want
   `python -m gbp.ml.evaluation --month ...`, so that the command matches every
   other `gbp.ml` entry point (`training`, `backtest`, `forecast`, `monitoring`).
6. As a developer, I want `panel_departed_mae` and the busy-share next to
   `forecast_metrics` and `busy_facility_ids` in `gbp/ml/metrics.py`, so that
   every evaluation metric is defined in one place.
7. As a developer, I want the run recipe (`RunRequest`) and the fleet/depot
   defaults in `gbp/consumers`, so that the CLI, the API, and the evaluation
   share one recipe and one fleet default, not three copies.
8. As a developer, I want `run_and_save` in `gbp/consumers`, so that its two
   callers (the evaluation and the API) reach it without importing `app/`.
9. As a Streamlit page author, I want the render-time reductions to stay in
   `app/`, so that a per-period, per-commodity, per-pair slice is computed at
   render, not baked into every artifact on disk.
10. As a Streamlit page author, I want `PANEL_VALUES` and `METRICS` imported
    from `gbp`, so that the panel's value columns have one definition the UI
    reads rather than redefines.
11. As a developer reading Notations §12/§16, I want the paths to name the real
    `gbp` homes, so that the dictionary does not send me to a moved file.
12. As a developer running the terminal commands in `CLAUDE.md`, I want the
    evaluation command to match its new module path, so that copy-paste works.
13. As an API maintainer, I want `app/api.py` to start runs through the `gbp`
    run path, so that the HTTP service and the Streamlit page call the same
    function.
14. As a developer, I want `DiskBackend` to stay in `app/` but delegate to
    `gbp.artifacts`, so that the disk-vs-HTTP swap the UI relies on keeps
    working while the real load/save logic lives in `gbp`.
15. As a developer, I want `app/` to stay a flat sys.path folder, so that the
    move does not also force a packaging change on the view and backend glue.
16. As a developer, I want each move to be a commit that keeps the tests green,
    so that the refactor is reviewable step by step.
17. As a test author, I want the moved code's tests to move with it and keep
    their assertions, so that the behaviour is proven identical before and after.

## Per-module verdict table

| Piece today | Verdict | Target `gbp` home |
|---|---|---|
| `app/artifacts.py` — table builders (`build_arcs`, `build_flow_totals`, `build_facilities`, `build_totals`, `build_run_tables`) | **move** | `gbp/artifacts.py` |
| `app/artifacts.py` — contracts (`RunMeta`, `RebalancingMeta`, `RUN_TABLE_SCHEMAS` + the per-table pandera schemas, `Metric`/`METRICS`, `RUN_TABLES`, `PANEL_VALUES`, `PANEL_FLOW_VALUES`) | **move** | `gbp/artifacts.py` |
| `app/artifacts.py` — save/load + paths (`save_run`, `save_scenario_run`, `load_run_table`, `load_run_meta`, `code_version`, `data_dir`, `runs_root`, `run_dir`, `table_path`, `meta_path`, `list_runs`, `next_free_run_name`) | **move** | `gbp/artifacts.py` |
| `app/eval_comparison.py` — `panel_departed_mae`, the busy-share aggregation | **move** | `gbp/ml/metrics.py` |
| `app/eval_comparison.py` — `EvalNames`, `ModelForecast`, `run_row`, `build_comparison` | **move** | `gbp/ml/evaluation.py` |
| `app/evaluate.py` — `evaluate_month`, `ensure_forecast`, `_ensure_run`, `actual_demand_table`, `evaluation_dir`, CLI | **move** | `gbp/ml/evaluation.py` (CLI: `python -m gbp.ml.evaluation`) |
| `app/runner.py` — `RunRequest`, fleet/depot defaults, `build_graph_data`, `run_scenario`, `run_and_save` | **move** | `gbp/consumers/run.py` |
| `app/runner.py` — the argparse `main()` | **stay** | `app/runner.py` (thin CLI over `gbp.consumers.run`) |
| `app/ui_shared.py` — `panel_commodity_slice`, `panel_slice`, `panel_slice_pair`, `aggregate_flow_totals`, `top_facilities`, `arc_map_rows` | **stay** | `app/ui_shared.py` (render-time helpers) |
| `app/backend.py` — `DiskBackend` | **stay (thinned)** | `app/backend.py`, delegating to `gbp.artifacts` |
| `app/api.py`, `app/api_client.py`, `app/main.py`, `app/views/*` | **stay** | `app/` (draw + IO glue) |

Notes on the verdicts:

- **01 — one module, not split.** The journal-reshaping builders stay with the
  contracts and the save/load in `gbp/artifacts.py`. They encode the on-disk
  *artifact table* shapes (arcs and flow_totals carry saved coordinates so the
  maps join nothing), which is an artifact concern, not a `gbp/model`
  read-model concern. Keeping build + schema + save in one module keeps it a
  deep module behind a small interface (`build_run_tables`, `save_scenario_run`,
  `load_run_meta`, `load_run_table`). A package `gbp/artifacts/` was considered
  and rejected as premature for 662 lines (CLAUDE.md minimalism).
- **03 — the whole path moves, the CLI stays.** `RunRequest`, the fleet/depot
  defaults, `build_graph_data`, `run_scenario`, and `run_and_save` are domain
  orchestration with no UI, so they move to `gbp/consumers/run.py`.
  `app/runner.py` keeps only the argparse `main()` that parses flags and calls
  `gbp.consumers.run.run_scenario`, so `python app/runner.py --help` keeps
  working. This move also breaks the cycle: `save_scenario_run` imports
  `RunRequest` from `gbp/consumers`, not from `app/runner`.
- **03 — one fleet default, not two.** `DEFAULT_TRUCK_HOMES`,
  `DEFAULT_N_DEPOTS`, `DEPOT_IDS`, `DEFAULT_TRUCK_CAPACITY_BIKES`,
  `DEFAULT_TRUCK_RATE` are used both by `RunRequest.resolved_truck_homes` and by
  `build_graph_data` (which sizes `RawModelData`). They become one typed source
  in `gbp/consumers/run.py`, read by both (CLAUDE.md "no repeated recipes").
- **04 — reductions stay.** Every reduction is a slice chosen at render:
  `panel_slice`/`panel_commodity_slice` by period + commodity, `panel_slice_pair`
  by the UI-chosen A/B pair, `aggregate_flow_totals` by a UI `level` and
  per-run meta timestamps, `top_facilities` by `n`, `arc_map_rows` by
  `group_keys`. The builder cannot precompute a per-period, per-commodity,
  per-pair table without a storage explosion, so Notations §12's rule is
  already satisfied. The reductions read `PANEL_VALUES`, which moves to `gbp`;
  importing a catalog is not "computing", so the rule still holds.

## Contract changes (Notations §12/§16, CLAUDE.md)

The moves rename paths in canonical text. The rules do not change — only the
paths and one command. "artifact builder" stays the canonical word for the
moved layer, now living in `gbp`.

**Notations §12 — Run artifacts.**

| Before | After |
|---|---|
| a folder `data/runs/<run_name>/` built by `app/artifacts.py` | a folder `data/runs/<run_name>/` built by `gbp/artifacts.py` |
| The full field list is the pydantic model `RunMeta` (`app/artifacts.py`). | The full field list is the pydantic model `RunMeta` (`gbp/artifacts.py`). |
| The contracts live in `app/artifacts.py`: `RunMeta` for `meta.json`, a pandera schema per table (`RUN_TABLE_SCHEMAS`), and `save_scenario_run` | The contracts live in `gbp/artifacts.py`: `RunMeta` for `meta.json`, a pandera schema per table (`RUN_TABLE_SCHEMAS`), and `save_scenario_run` |
| The `METRICS` table in `app/artifacts.py` describes each metric once | The `METRICS` table in `gbp/artifacts.py` describes each metric once |

No new table rows: ticket 04 added no artifact tables.

**Notations §16 — The run-artifact API.**

| Before | After |
|---|---|
| starts runs through the same `runner.run_scenario` the Run scenario page calls | starts runs through the same `run_scenario` (`gbp/consumers/run.py`) the Run scenario page calls |

The `API_URL` row (the `ui_shared.py` backend switch) is unchanged: the switch
stays app-side.

**CLAUDE.md — UI rule.**

| Before | After |
|---|---|
| must not compute anything the artifact builder (`app/artifacts.py`) can precompute | must not compute anything the artifact builder (`gbp/artifacts.py`) can precompute |

The rule itself is unchanged; ticket 04 kept the reductions in `app/` precisely
because the builder cannot precompute them.

**CLAUDE.md — Commands block.**

| Before | After |
|---|---|
| `python app/evaluate.py --month 202601  # two-level evaluation ...` | `python -m gbp.ml.evaluation --month 202601  # two-level evaluation ...` |

`python app/runner.py --help` is unchanged (the CLI stays in `app/runner.py`).

## Graduated fog decisions

- **app/ packaging — stays flat.** After `artifacts` and the run path leave,
  the only bare-name imports left in `app/` are genuine glue (`ui_shared`,
  `backend`, `api_client`, `views/*`), reached through the `sys.path` insert in
  `main.py`. Nothing forces `app/` to become an importable package. The moved
  code is reached as `from gbp import artifacts` and
  `from gbp.consumers.run import ...`.
- **DiskBackend — stays, thinned.** `backend.py`'s `DiskBackend` keeps its
  list/load/save methods but delegates each to `gbp.artifacts`. It is the
  app-side seam that lets `api_client` swap disk for HTTP, so it does not
  vanish; it becomes a pass-through.

## Testing Decisions

A good test here checks external behaviour, not the file a function lives in:
that a run artifact round-trips (`build_run_tables` → `save_run` →
`load_run_meta` / `load_run_table`) to the same tables and `meta.json`, and
that `evaluate_month` produces the same comparison rows. These assertions must
read identically before and after the move — that is the safety net for a pure
relocation.

- **Tests move with their code.** `tests/test_app_artifacts.py` follows the
  builder to a `gbp`-facing test (e.g. `tests/test_artifacts.py`);
  `tests/test_evaluate.py` and `tests/test_eval_comparison.py` follow the
  evaluation cluster (e.g. `tests/test_ml_evaluation.py`);
  `tests/test_app_runner.py`'s recipe assertions follow `RunRequest` to a
  `gbp/consumers` test. Only the imports change; the assertions do not.
- **Prior art.** The existing `tests/test_app_artifacts.py` (round-trip of the
  saved tables), `tests/test_evaluate.py`, `tests/test_eval_comparison.py`,
  `tests/test_app_runner.py`, and the `gbp.ml` test suite
  (`tests/test_ml_metrics`-style files) are the models to copy.
- **The seam test.** One test drives the whole run path through its new `gbp`
  entry point and asserts the saved artifact matches a golden `meta.json` /
  table set, proving the move changed no behaviour.
- **`app/` as a reader.** `tests/test_app_api.py` and the view-facing tests keep
  asserting that pages and the API read the moved tables through the thinned
  `DiskBackend` and the repointed imports.

## Suggested commit order

Smallest safe steps first; each commit keeps the suite green.

1. **Metrics.** Move `panel_departed_mae` and the busy-share into
   `gbp/ml/metrics.py`; repoint `app/eval_comparison.py`. Leaf move, no
   dependents relocate.
2. **Run path.** Move `RunRequest`, the fleet/depot defaults, `build_graph_data`,
   `run_scenario`, `run_and_save` into `gbp/consumers/run.py`. `app/runner.py`
   becomes the argparse CLI over it. `app/artifacts.py` still imports
   `RunRequest`, now from `gbp.consumers.run` (cycle broken).
3. **Artifact builder.** Move `app/artifacts.py` to `gbp/artifacts.py` whole
   (builders + contracts + save/load + paths). Repoint `app/ui_shared.py`,
   `app/backend.py`, `app/api.py`, `app/runner.py`, and the views to
   `from gbp import artifacts`; thin `DiskBackend` to delegate.
4. **Evaluation.** Move the comparison bookkeeping and `evaluate.py`
   orchestration into `gbp/ml/evaluation.py` (now `run_and_save` and the
   artifacts API both sit in `gbp`); delete `app/eval_comparison.py` and
   `app/evaluate.py`; add the `python -m gbp.ml.evaluation` CLI.
5. **UI import repoint.** Point `PANEL_VALUES` / `METRICS` in `app/ui_shared.py`
   at `gbp`; optionally move the `views/home.py` all-runs summary to a
   `gbp.artifacts` helper. No reduction moves.
6. **Docs.** Apply the §12/§16 and CLAUDE.md path changes above (docs last, so
   they describe the finished code).

## Out of Scope

- **The FastAPI service** (`app/api.py`, `app/api_client.py`) — stays app-side;
  it is not extracted into a `service/` layer.
- **`app/backend.py` core, `app/main.py`, `app/views/*`** — genuine draw + IO
  glue, stay in `app/`.
- **Any domain-agnostic "API/interface layer" over `gbp`** built for its own
  sake — forbidden by CLAUDE.md ("vertical, not horizontal"). This effort moves
  concrete domain code into concrete `gbp` modules, nothing more.
- **Behaviour changes.** Every move is a pure relocation; no run output,
  artifact table, or `meta.json` field changes.

## Further Notes

- The `gbp`-never-imports-`app` invariant must hold after every commit; a quick
  `grep -rn "import app" gbp/` is the guard.
- Exact target names inside `gbp/consumers` (`gbp/consumers/run.py` vs folding
  into `simulator/scenario.py`) and whether `gbp/artifacts.py` later grows into
  a package are left to the execution effort; the verdict fixes the module's
  responsibility, not its final line split.
