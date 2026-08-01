# Структурные события: развороты, сносы, переименования

## Коммиты с массовым удалением (>1000 удалённых строк)

Всего таких коммитов: 29

- `2026-02-23 #017` **-1301** / +857 (16 файлов, удалено файлов целиком: 3) — First iteration of refactoring
- `2026-02-24 #019` **-1783** / +249 (4 файлов, удалено файлов целиком: 2) — cleaned and prepared notes for graph loader
- `2026-03-08 #028` **-1800** / +2257 (17 файлов, удалено файлов целиком: 6) — feat(rebalancer): restructure rebalancer module with contracts and data loading enhancements
- `2026-03-22 #037` **-7289** / +874 (27 файлов, удалено файлов целиком: 14) — refactor(graph): restructure graph module and update documentation
- `2026-03-24 #045` **-1258** / +251 (9 файлов, удалено файлов целиком: 3) — refactor(docs): update project documentation and remove obsolete files
- `2026-03-27 #050` **-1007** / +1017 (19 файлов, удалено файлов целиком: 0) — Refactor documentation and code structure for GBP
- `2026-04-15 #064` **-5048** / +4611 (17 файлов, удалено файлов целиком: 0) — Refactor model loading and derivation process
- `2026-04-27 #075` **-1829** / +226 (11 файлов, удалено файлов целиком: 1) — Refactor inventory handling in DataLoaderMock and DataLoaderGraph
- `2026-05-01 #082` **-1883** / +40 (16 файлов, удалено файлов целиком: 11) — Refactor code structure and remove redundant sections for improved readability and maintainability
- `2026-05-09 #089` **-4069** / +36 (40 файлов, удалено файлов целиком: 36) — Remove outdated documentation files related to bike-sharing simulation and graph data model; update attribute system and environment design documentation for clarity and structure; add session state files for tracking simulation sessions.
- `2026-05-11 #091` **-25260** / +3198 (109 файлов, удалено файлов целиком: 102) — Add historical replay pipeline notebook for algorithm verification
- `2026-05-12 #095` **-4787** / +638 (41 файлов, удалено файлов целиком: 21) — Refactor loaders: Remove GenericSourceProtocol and validators.py
- `2026-05-15 #098` **-3243** / +0 (29 файлов, удалено файлов целиком: 29) — Remove obsolete state files and deep interview specification for the rebalancer task. This includes the deletion of the deep interview spec, autopilot state, deep interview state, mission state, and various session state files. These changes reflect the completion of the historical bike replay pipeline and the transition to the next phase of development.
- `2026-05-31 #103` **-15680** / +15635 (75 файлов, удалено файлов целиком: 1) —  to date
- `2026-06-02 #110` **-1713** / +187 (7 файлов, удалено файлов целиком: 6) — Refactor code structure for improved readability and maintainability
- `2026-06-03 #114` **-5163** / +443 (11 файлов, удалено файлов целиком: 10) — Remove integration and unit tests for the dispatch lifecycle and canonical scenario; add design document for the simulation engine environment.
- `2026-06-03 #115` **-9153** / +0 (46 файлов, удалено файлов целиком: 46) — Remove unused schemas and role derivation logic from the GBP core module
- `2026-06-05 #118` **-2521** / +1456 (18 файлов, удалено файлов целиком: 4) — Refactor simulation phases and update test pipeline
- `2026-06-07 #121` **-1574** / +480 (3 файлов, удалено файлов целиком: 1) — Refactor code structure for improved readability and maintainability
- `2026-06-12 #129` **-2000** / +2050 (9 файлов, удалено файлов целиком: 0) — up to date
- `2026-06-19 #137` **-7830** / +394 (5 файлов, удалено файлов целиком: 2) — Refactor code structure for improved readability and maintainability
- `2026-06-26 #151` **-1028** / +966 (2 файлов, удалено файлов целиком: 0) — Refactor code structure for improved readability and maintainability
- `2026-07-03 #158` **-2865** / +91 (5 файлов, удалено файлов целиком: 0) — Rename inventory_after to quantity_eop in _period_end_inventory_from_moments function and update related test to reflect this change
- `2026-07-06 #183` **-1779** / +946 (5 файлов, удалено файлов целиком: 2) — feat: consolidate and enhance rebalancing documentation
- `2026-07-06 #187` **-1794** / +1783 (30 файлов, удалено файлов целиком: 0) — feat: enhance documentation and refactor data loading functions
- `2026-07-13 #224` **-8309** / +1803 (53 файлов, удалено файлов целиком: 17) — Add documentation for new features and improve existing content
- `2026-07-16 #244` **-5065** / +410 (52 файлов, удалено файлов целиком: 0) — Refactor documentation in journal_schema.py and routing.py
- `2026-07-17 #258` **-2401** / +204 (5 файлов, удалено файлов целиком: 4) — chore: keep one overview, remove stray notebooks
- `2026-07-30 #270` **-4724** / +86 (33 файлов, удалено файлов целиком: 30) — Delete obsolete tickets and reports related to the Wayfinder Hugo site project, including decisions on non-site folders, GitHub Pages deployment research, and the deploy pipeline setup. Remove architecture review and model evaluation reports from January 2026, as they are no longer relevant to the current documentation structure.

## Жизнь и смерть каталогов

| каталог | первое касание | последнее | событий | жив сейчас |
|---|---|---|---|---|
| `gbp` | 2026-01-29 | 2026-07-30 | 914 | да |
| `notebooks` | 2026-02-01 | 2026-07-30 | 213 | да |
| `gbp/shared` | 2026-02-02 | 2026-03-08 | 25 | **НЕТ** |
| `gbp/rebalancer` | 2026-02-03 | 2026-05-01 | 57 | **НЕТ** |
| `gbp/graph` | 2026-03-01 | 2026-03-22 | 37 | **НЕТ** |
| `notebooks/utils` | 2026-03-08 | 2026-05-11 | 5 | **НЕТ** |
| `gbp/loaders` | 2026-03-08 | 2026-07-30 | 135 | **НЕТ** |
| `tests` | 2026-03-08 | 2026-07-30 | 378 | да |
| `gbp/loading` | 2026-03-22 | 2026-04-01 | 6 | **НЕТ** |
| `docs/diagrams` | 2026-03-22 | 2026-05-09 | 39 | **НЕТ** |
| `tests/fixtures` | 2026-03-22 | 2026-05-11 | 32 | **НЕТ** |
| `tests/integration` | 2026-03-22 | 2026-05-11 | 11 | **НЕТ** |
| `tests/unit` | 2026-03-22 | 2026-05-11 | 139 | **НЕТ** |
| `gbp/io` | 2026-03-22 | 2026-05-12 | 12 | **НЕТ** |
| `gbp/core` | 2026-03-22 | 2026-06-03 | 142 | **НЕТ** |
| `docs` | 2026-03-22 | 2026-07-30 | 789 | да |
| `docs/design` | 2026-03-24 | 2026-05-11 | 19 | **НЕТ** |
| `notebooks/verify` | 2026-03-24 | 2026-05-11 | 34 | **НЕТ** |
| `docs/_templates` | 2026-03-27 | 2026-05-09 | 4 | **НЕТ** |
| `docs/modules` | 2026-03-27 | 2026-05-09 | 14 | **НЕТ** |
| `.claude` | 2026-03-27 | 2026-07-30 | 64 | да |
| `.github` | 2026-03-27 | 2026-07-30 | 8 | да |
| `.github/workflows` | 2026-03-27 | 2026-07-30 | 8 | да |
| `docs/story_telling` | 2026-03-28 | 2026-05-09 | 8 | **НЕТ** |
| `.claude/skills` | 2026-03-28 | 2026-07-30 | 38 | да |
| `gbp/consumers` | 2026-03-28 | 2026-07-30 | 277 | да |
| `notebooks/understand` | 2026-03-31 | 2026-05-11 | 21 | **НЕТ** |
| `"docs` | 2026-04-18 | 2026-07-06 | 11 | **НЕТ** |
| `gbp/build` | 2026-04-20 | 2026-06-03 | 46 | **НЕТ** |
| `.omc/plans` | 2026-05-01 | 2026-05-15 | 6 | **НЕТ** |
| `.omc/specs` | 2026-05-01 | 2026-05-15 | 4 | **НЕТ** |
| `.omc` | 2026-05-01 | 2026-06-02 | 107 | **НЕТ** |
| `.omc/state` | 2026-05-01 | 2026-06-02 | 49 | **НЕТ** |
| `.omc/sessions` | 2026-05-02 | 2026-06-02 | 38 | **НЕТ** |
| `docs/scenarios` | 2026-05-05 | 2026-07-16 | 4 | **НЕТ** |
| `"docs/deprecated` | 2026-05-11 | 2026-06-03 | 6 | **НЕТ** |
| `.claude/rules` | 2026-05-11 | 2026-06-07 | 11 | **НЕТ** |
| `docs/design_documents` | 2026-05-11 | 2026-06-25 | 16 | **НЕТ** |
| `notebooks/old` | 2026-06-03 | 2026-06-03 | 4 | **НЕТ** |
| `gbp/model` | 2026-06-08 | 2026-07-30 | 52 | да |
| `other_staff` | 2026-07-03 | 2026-07-12 | 6 | **НЕТ** |
| `scripts` | 2026-07-04 | 2026-07-24 | 4 | да |
| `app` | 2026-07-04 | 2026-07-30 | 151 | да |
| `app/views` | 2026-07-04 | 2026-07-30 | 50 | да |
| `docs/archive` | 2026-07-06 | 2026-07-24 | 34 | **НЕТ** |
| `docs/explanation` | 2026-07-07 | 2026-07-15 | 41 | **НЕТ** |
| `docs/method` | 2026-07-07 | 2026-07-15 | 6 | да |
| `data` | 2026-07-10 | 2026-07-11 | 8 | да |
| `data/ml` | 2026-07-10 | 2026-07-11 | 4 | да |
| `docs/plans` | 2026-07-10 | 2026-07-30 | 62 | да |
| `gbp/ml` | 2026-07-10 | 2026-07-30 | 105 | да |
| `docs/reports` | 2026-07-11 | 2026-07-30 | 6 | **НЕТ** |
| `docs/how-to` | 2026-07-13 | 2026-07-16 | 8 | **НЕТ** |
| `docs/getting-started` | 2026-07-15 | 2026-07-15 | 7 | **НЕТ** |
| `docs/key-components` | 2026-07-15 | 2026-07-17 | 26 | **НЕТ** |
| `.scratch` | 2026-07-16 | 2026-07-16 | 12 | да |
| `.scratch/app-to-gbp-seam` | 2026-07-16 | 2026-07-16 | 12 | да |
| `docs/interview` | 2026-07-17 | 2026-07-30 | 4 | **НЕТ** |
| `docs/site` | 2026-07-24 | 2026-07-30 | 336 | да |
| `domains` | 2026-07-30 | 2026-07-30 | 25 | да |
| `domains/citybike` | 2026-07-30 | 2026-07-30 | 24 | да |

## Переименования (миграции путей)

Всего: 101

**2026-02-23** — 2 переименований
  - `gbp/shared/graph_model.py` → `gbp/shared/old/graph_model.py`
  - `gbp/shared/graph_model_mock.py` → `gbp/shared/old/graph_model_mock.py`
**2026-03-08** — 1 переименований
  - `gbp/shared/dataloader_mock.py` → `gbp/loaders/dataloader_mock.py`
**2026-03-24** — 3 переименований
  - `docs/ATTRIBUTE_SYSTEM_DESIGN.md` → `docs/design/attribute_system.md`
  - `docs/graph_data_model.md` → `docs/design/graph_data_model.md`
  - `docs/REFACTORING_SPEC.md` → `docs/design/refactoring.md`
**2026-04-01** — 2 переименований
  - `gbp/loading/csv_loader.py` → `gbp/loaders/csv_loader.py`
  - `gbp/loading/validators.py` → `gbp/loaders/validators.py`
**2026-05-11** — 3 переименований
  - `"docs/philosophy/\321\203\321\200\320\276\320\262\320\275\320\270_\320\277\320\276\320\275\320\270\320\274\320\260\320\275\320\270\321\217_\320\272\320\276\320\264\320\260.md"` → `"docs/deprecated/architecture/\321\203\321\200\320\276\320\262\320\275\320\270_\320\277\320\276\320\275\320\270\320\274\320\260\320\275\320\270\321\217_\320\272\320\276\320\264\320\260.md"`
  - `"docs/philosophy/\321\203\321\200\320\276\320\262\320\275\320\270_\320\277\320\276\320\275\320\270\320\274\320\260\320\275\320\270\321\217_\320\272\320\276\320\264\320\260_\320\264\320\270\320\260\320\263\321\200\320\260\320\274\320\274\321\213.md"` → `"docs/deprecated/architecture/\321\203\321\200\320\276\320\262\320\275\320\270_\320\277\320\276\320\275\320\270\320\274\320\260\320\275\320\270\321\217_\320\272\320\276\320\264\320\260_\320\264\320\270\320\260\320\263\321\200\320\260\320\274\320\274\321\213.md"`
  - `notebooks/verify/12_historical_replay.ipynb` → `notebooks/workbook_mock.ipynb`
**2026-05-12** — 1 переименований
  - `docs/design_documents/graph_data_model.md` → `docs/deprecated/graph_data_model.md`
**2026-06-03** — 3 переименований
  - `notebooks/canonical_scenario.ipynb` → `notebooks/old/canonical_scenario.ipynb`
  - `notebooks/test_or_tools.ipynb` → `notebooks/old/test_or_tools.ipynb`
  - `docs/design_documents/environment.md` → `docs/design_documents/environment_old.md`
**2026-06-05** — 3 переименований
  - `notebooks/engine.py` → `gbp/consumers/simulator/engine.py`
  - `notebooks/state.py` → `gbp/consumers/simulator/state.py`
  - `notebooks/dataloader_raw.py` → `gbp/loaders/dataloader_raw.py`
**2026-06-08** — 1 переименований
  - `gbp/consumers/simulator/journal.py` → `gbp/model/journal.py`
**2026-06-25** — 2 переименований
  - `gbp/model/journal.py` → `gbp/model/flows.py`
  - `tests/test_journal.py` → `tests/test_flows.py`
**2026-07-06** — 13 переименований
  - `docs/architecture_review.md` → `docs/archive/architecture_review.md`
  - `docs/architecture_review_2026-07-04.md` → `docs/archive/architecture_review_2026-07-04.md`
  - `docs/deployment_plan_ru.md` → `docs/archive/deployment_plan_ru.md`
  - `docs/step_id_stamped_plan.md` → `docs/archive/step_id_stamped_plan.md`
  - `docs/step_id_unification_plan.md` → `docs/archive/step_id_unification_plan.md`
  - `docs/step_id_unification_plan_ru.md` → `docs/archive/step_id_unification_plan_ru.md`
  - `docs/scenario_event_tables.md` → `docs/archive/scenario_event_tables.md`
  - `docs/scenario_step_id_tables_ru.md` → `docs/archive/scenario_step_id_tables_ru.md`
  - `docs/design_documents/move_id_event_id_plan.md` → `docs/archive/move_id_event_id_plan.md`
  - `docs/design_documents/transaction_model.ru.md` → `docs/archive/transaction_model.ru.md`
  - `docs/trip_distance_and_duration.md` → `docs/archive/trip_distance_and_duration.md`
  - `docs/trip_distance_and_duration_ru.md` → `docs/archive/trip_distance_and_duration_ru.md`
  - …и ещё 1
**2026-07-07** — 14 переименований
  - `docs/deepening_candidates.md` → `docs/archive/deepening_candidates.md`
  - `docs/api.md` → `docs/explanation/api.md`
  - `docs/app.md` → `docs/explanation/app.md`
  - `docs/architecture.md` → `docs/explanation/architecture.md`
  - `docs/dataloader.md` → `docs/explanation/dataloader.md`
  - `docs/flow_journal.md` → `docs/explanation/flow_journal.md`
  - `docs/rebalancing.md` → `docs/explanation/rebalancing.md`
  - `docs/scenarios.md` → `docs/explanation/scenarios.md`
  - `docs/simulator.md` → `docs/explanation/simulator.md`
  - `docs/osrm_setup.md` → `docs/guides/osrm_setup.md`
  - `docs/comprehension_levels.md` → `docs/method/comprehension_levels.md`
  - `docs/comprehension_levels_ru.md` → `docs/method/comprehension_levels_ru.md`
  - …и ещё 2
**2026-07-15** — 11 переименований
  - `docs/first_run.md` → `docs/getting-started/quickstart.md`
  - `docs/guides/osrm_setup.md` → `docs/how-to/set-up-osrm.md`
  - `docs/explanation/dataloader.md` → `docs/key-components/data-model.md`
  - `docs/explanation/flow_journal.md` → `docs/key-components/flow-journal.md`
  - `docs/explanation/ml.md` → `docs/key-components/ml-toolkit.md`
  - `docs/explanation/rebalancing.md` → `docs/key-components/rebalancing.md`
  - `docs/explanation/simulator.md` → `docs/key-components/simulation-engine.md`
  - `docs/explanation/app.md` → `docs/key-components/visualization.md`
  - `docs/explanation/scenarios.md` → `docs/key-components/worked-examples.md`
  - `docs/explanation/api.md` → `docs/reference/api.md`
  - `docs/explanation/architecture.md` → `docs/key-components/overview.md`
**2026-07-16** — 3 переименований
  - `app/artifacts.py` → `gbp/artifacts.py`
  - `app/evaluate.py` → `gbp/ml/evaluation.py`
  - `tests/test_eval_comparison.py` → `tests/test_ml_evaluation.py`
**2026-07-24** — 22 переименований
  - `docs/site/static/images/ui_overview.png` → `docs/site/assets/images/ui_overview.png`
  - `docs/key-components/data-model.md` → `docs/site/content/docs/architecture/data-model.md`
  - `docs/key-components/flow-journal.md` → `docs/site/content/docs/architecture/flow-journal.md`
  - `docs/key-components/ml-toolkit.md` → `docs/site/content/docs/architecture/ml-toolkit.md`
  - `docs/key-components/overview.md` → `docs/site/content/docs/architecture/overview.md`
  - `docs/key-components/rebalancing.md` → `docs/site/content/docs/architecture/rebalancing.md`
  - `docs/key-components/simulation-engine.md` → `docs/site/content/docs/architecture/simulation-engine.md`
  - `docs/key-components/visualization.md` → `docs/site/content/docs/architecture/visualization.md`
  - `docs/key-components/worked-examples.md` → `docs/site/content/docs/architecture/worked-examples.md`
  - `docs/decisions/forecast-replaces-only-demand.md` → `docs/site/content/docs/decisions/forecast-replaces-only-demand.md`
  - `docs/decisions/journal-as-source-of-truth.md` → `docs/site/content/docs/decisions/journal-as-source-of-truth.md`
  - `docs/decisions/sizing-run.md` → `docs/site/content/docs/decisions/sizing-run.md`
  - …и ещё 10
**2026-07-30** — 17 переименований
  - `gbp/loaders/dataloader_raw.py` → `domains/citybike/loaders/dataloader_raw.py`
  - `gbp/loaders/download.py` → `domains/citybike/loaders/download.py`
  - `gbp/loaders/dataloader_graph.py` → `gbp/model/dataloader_graph.py`
  - `gbp/ml/data.py` → `domains/citybike/ml/data.py`
  - `gbp/ml/features.py` → `domains/citybike/ml/features.py`
  - `gbp/ml/forecast.py` → `domains/citybike/ml/forecast.py`
  - `gbp/ml/models/boosting.py` → `domains/citybike/ml/models/boosting.py`
  - `gbp/ml/models/graph.py` → `domains/citybike/ml/models/graph.py`
  - `gbp/ml/models/sarimax.py` → `domains/citybike/ml/models/sarimax.py`
  - `gbp/ml/models/seasonal_naive.py` → `domains/citybike/ml/models/seasonal_naive.py`
  - `gbp/ml/backtest.py` → `domains/citybike/ml/ops/backtest.py`
  - `gbp/ml/evaluation.py` → `domains/citybike/ml/ops/evaluation.py`
  - …и ещё 5

## Воскрешения (файл удалили, потом вернули)

- `tests/conftest.py` — рождён 2026-03-08, воскрешений: 1 (2026-06-24)
- `gbp/consumers/simulator/config.py` — рождён 2026-03-28, воскрешений: 1 (2026-06-17)
- `gbp/consumers/simulator/phases.py` — рождён 2026-03-28, воскрешений: 1 (2026-06-05)
- `.omc/state/last-tool-error.json` — рождён 2026-05-01, воскрешений: 1 (2026-05-03)
- `.omc/state/subagent-tracking.json` — рождён 2026-05-01, воскрешений: 1 (2026-05-03)
- `.omc/project-memory.json` — рождён 2026-05-02, воскрешений: 1 (2026-05-29)
- `AGENTS.md` — рождён 2026-05-13, воскрешений: 1 (2026-07-03)
- `docs/architecture.md` — рождён 2026-07-07, воскрешений: 1 (2026-07-16)

## Самые переписываемые файлы (по числу коммитов, где менялись)

| файл | коммитов | жив сейчас |
|---|---|---|
| `gbp/loaders/dataloader_graph.py` | 60 | **НЕТ** |
| `Notations.md` | 54 | да |
| `notebooks/test_pipeline.ipynb` | 41 | да |
| `gbp/consumers/simulator/phases.py` | 40 | да |
| `CLAUDE.md` | 39 | да |
| `gbp/consumers/simulator/engine.py` | 28 | да |
| `pyproject.toml` | 27 | да |
| `gbp/consumers/simulator/state.py` | 26 | да |
| `gbp/model/__init__.py` | 21 | да |
| `gbp/model/flows.py` | 20 | да |
| `app/runner.py` | 20 | да |
| `gbp/consumers/simulator/mechanics.py` | 19 | да |
| `tests/test_scenarios.py` | 19 | да |
| `app/artifacts.py` | 18 | **НЕТ** |
| `PROJECT_STATE.md` | 17 | **НЕТ** |
| `notebooks/workbook.ipynb` | 17 | **НЕТ** |
| `gbp/consumers/simulator/validation.py` | 17 | да |
| `README.md` | 15 | да |
| `gbp/loaders/__init__.py` | 15 | **НЕТ** |
| `gbp/loaders/protocols.py` | 15 | **НЕТ** |
| `gbp/core/model.py` | 15 | **НЕТ** |
| `gbp/consumers/simulator/__init__.py` | 15 | да |
| `app/ui_shared.py` | 15 | да |
| `docs/README.md` | 15 | **НЕТ** |
| `app/views/run_scenario.py` | 14 | да |
| `tests/test_app_artifacts.py` | 14 | да |
| `docs/README_ru.md` | 14 | **НЕТ** |
| `gbp/consumers/simulator/built_in_phases.py` | 13 | **НЕТ** |
| `gbp/loaders/dataloader_raw.py` | 13 | **НЕТ** |
| `tests/scenarios.py` | 13 | да |
| `app/api.py` | 13 | да |
| `gbp/ml/forecast.py` | 13 | **НЕТ** |
| `notebooks/00_EDA.ipynb` | 12 | **НЕТ** |
| `tests/test_graph_loader.py` | 12 | **НЕТ** |
| `.claude/settings.local.json` | 12 | да |
| `gbp/consumers/simulator/scenario.py` | 12 | да |
| `gbp/consumers/simulator/rebalancing.py` | 12 | да |
| `gbp/consumers/simulator/config.py` | 11 | да |
| `gbp/ml/backtest.py` | 11 | **НЕТ** |
| `app/evaluate.py` | 11 | **НЕТ** |
