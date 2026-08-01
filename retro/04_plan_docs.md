# Документы намерений: планы, дизайны, правила

Каждый документ — это записанное намерение. Дата смерти и срок жизни показывают, сколько прожила идея.

Всего документов: 146. Удалено: 100. Живо сейчас: 46.

Из удалённых прожили неделю или меньше: **34**.

## Удалённые документы (по дате рождения)

| документ | создан | удалён | прожил, дн. | строк | о чём |
|---|---|---|---|---|---|
| `SPEC.md` | 2026-01-29 | 2026-03-24 | **54** | 604 | End-to-end pipeline: from raw data ingestion to rebalancing route visualization, using a modular monolith architecture with a focus on minimalism and performanc |
| `SPEC_reference.md` | 2026-01-29 | 2026-03-08 | **38** | 992 | A modular platform for graph-based optimization problems. The platform transforms arbitrary data into graph structures and applies optimization algorithms (reba |
| `docs/architecture_diagrams.md` | 2026-03-22 | 2026-05-09 | **48** | 352 | This document provides a progressive visual guide to the graph-based data model implemented in `gbp/core`, `gbp/build`, `gbp/loaders`, and `gbp/io`. Diagrams ar |
| `docs/design/graph_data_model.md` | 2026-03-22 | 2026-05-11 | **50** | 1964 | Данный документ описывает универсальную графовую модель данных для задач сетевых потоков. Платформа позволяет моделировать перемещение commodity (велосипеды в ш |
| `docs/repo_struct.md` | 2026-03-22 | 2026-03-24 | **2** | 122 | ``` |
| `docs/design/refactoring.md` | 2026-03-22 | 2026-05-09 | **48** | 173 | **Status: ALL STEPS DONE.** This document records what was done and why. |
| `docs/DATA_JOURNEY.md` | 2026-03-22 | 2026-05-09 | **48** | 447 | Этот документ проводит через **полный путь данных** на конкретном примере велошеринга. Не абстрактная архитектура, а конкретные строки, конкретные числа, конкре |
| `docs/design/attribute_system.md` | 2026-03-22 | 2026-05-09 | **48** | 276 | **Status: IMPLEMENTED.** This document describes the current architecture as built. |
| `docs/CLEANUP_PARAMETRIC_FIELDS.md` | 2026-03-22 | 2026-03-24 | **2** | 398 | `AttributeRegistry` реализован и интегрирован: `registry.py` работает, `RawModelData.attributes` существует, `build_model()` резолвит атрибуты из registry, `Dat |
| `PROJECT_STATE.md` | 2026-03-24 | 2026-06-05 | **73** | 70 | > Last updated: 2026-05-12 |
| `docs/ideas.md` | 2026-03-24 | 2026-05-09 | **46** | 0 |  |
| `PROJECT.md` | 2026-03-24 | 2026-07-13 | **111** | 31 | A bike-sharing simulation platform built vertically on the Citi Bike domain. The core is **Environment**: bikes move through a network of stations across time p |
| `docs/design/environment_design.md` | 2026-03-26 | 2026-05-11 | **46** | 1198 | > Status: READY FOR IMPLEMENTATION |
| `docs/index.md` | 2026-03-27 | 2026-05-09 | **43** | 140 | A universal graph-based logistics platform for network flow problems, |
| `.claude/skills/update-docs/SKILL.md` | 2026-03-28 | 2026-06-07 | **71** | 31 | name: update-docs |
| `docs/story_telling/01_graph_data_model.md` | 2026-03-28 | 2026-05-09 | **42** | 192 | *Storytelling-гайд по `docs/design/graph_data_model.md`* |
| `docs/story_telling/02_attribute_system.md` | 2026-03-28 | 2026-05-09 | **42** | 188 | *Storytelling-гайд по `docs/design/attribute_system.md`* |
| `docs/story_telling/03_environment.md` | 2026-03-28 | 2026-05-09 | **42** | 200 | *Storytelling-гайд по `docs/design/environment_design.md`* |
| `.claude/skills/understand-code/SKILL.md` | 2026-03-31 | 2026-07-16 | **107** | 184 | name: understand-code |
| `docs/temp_plan.md` | 2026-03-31 | 2026-03-31 | **0** | 233 | Сырые данные поездок (`df_trips`) и телеметрии (`df_telemetry_ts`) живут только на source и никак не связаны с графовой моделью. Нужно сделать их **полноценными |
| `docs/design/observations_design.md` | 2026-03-31 | 2026-05-09 | **39** | 435 | > Status: READY FOR IMPLEMENTATION |
| `.claude/skills/ousterhout-review/SKILL.md` | 2026-04-15 | 2026-07-16 | **92** | 294 | name: ousterhout-review |
| `docs/ARCHITECTURE_MAP.md` | 2026-04-15 | 2026-05-09 | **24** | 113 | > Живая одностраничная карта проекта. Читать каждое утро перед работой (2 мин). |
| `.claude/notes/architecture_deepening_plan.md` | 2026-04-30 | 2026-06-07 | **38** | 154 | > Session in progress. Survived `/compact` / `/clear`. Read this first before continuing. |
| `.omc/plans/open-questions.md` | 2026-05-01 | 2026-05-15 | **14** | 58 | Tracks open questions, deferred decisions, and spec interpretations across all |
| `.omc/plans/rebalancer-pdp-task.md` | 2026-05-01 | 2026-05-15 | **14** | 1140 | - Plan ID: rebalancer-pdp-task |
| `.omc/specs/deep-interview-rebalancer.md` | 2026-05-01 | 2026-05-15 | **14** | 318 | - Interview ID: rebalancer-2026-05-01 |
| `.omc/plans/historical-replay-pipeline.md` | 2026-05-05 | 2026-05-15 | **10** | 473 | **Source spec:** `.omc/specs/deep-interview-historical-replay.md` |
| `.omc/specs/deep-interview-historical-replay.md` | 2026-05-05 | 2026-05-15 | **10** | 235 | - Interview ID: hist-replay-2026-05-05 |
| `docs/scenarios/algorithm_bike_simulation_historical.md` | 2026-05-05 | 2026-05-09 | **4** | 142 | Before we can predict the future, we need to prove we can reproduce the past. This algorithm takes a dataset of real bike trips — every ride that actually happe |
| `docs/design_documents/environment_old.md` | 2026-05-11 | 2026-06-19 | **39** | 443 | > Status: IMPLEMENTED |
| `docs/deprecated/graph_data_model.md` | 2026-05-11 | 2026-06-03 | **23** | 1961 | This document describes a universal graph-based data model for network flow problems. The platform enables modeling the movement of commodities (bikes in sharin |
| `.claude/rules/code-style/code-style.md` | 2026-05-11 | 2026-06-07 | **27** | 14 | - **Vectorization first.** No `for` loops over data — use pandas/NumPy operations. |
| `.claude/rules/epistemic-rules/epistemic-rules.md` | 2026-05-11 | 2026-06-07 | **27** | 19 | These rules apply when writing documentation, storytelling guides, summary tables, or any text that claims one entity is derived from another. |
| `.claude/rules/post-task-process/post-task-process.md` | 2026-05-11 | 2026-05-12 | **1** | 16 | Read this after completing a code task. |
| `.claude/rules/post-task-process/SKILL.md` | 2026-05-12 | 2026-06-07 | **26** | 15 | Read this after completing a code task. |
| `docs/cleanup_plan.md` | 2026-05-12 | 2026-05-15 | **3** | 284 | > Date: 2026-05-12 |
| `docs/architecture_deepening.md` | 2026-05-12 | 2026-05-15 | **3** | 295 | Architectural review of the codebase as of 2026-05-12. |
| `.claude/skills/phase-diagram/SKILL.md` | 2026-06-11 | 2026-07-30 | **49** | 152 | name: phase-diagram |
| `.claude/skills/phase-diagram/example.md` | 2026-06-11 | 2026-07-30 | **49** | 61 | The approved diagram for |
| `docs/design_documents/loss_logging.md` | 2026-06-14 | 2026-06-19 | **5** | 219 | > Status: IMPLEMENTED (steps 1-4 + 5a/5b/5c done; verified end-to-end -- see §10) |
| `.claude/skills/check-notations/SKILL.md` | 2026-06-16 | 2026-07-30 | **44** | 122 | name: check-notations |
| `docs/archive/move_id_event_id_plan.md` | 2026-06-23 | 2026-07-13 | **20** | 313 | Status: **approved, not yet implemented.** This document is a self-contained |
| `docs/archive/transaction_model.ru.md` | 2026-06-25 | 2026-07-13 | **18** | 500 | Статус: **концептуальный документ, не задача на реализацию.** Он даёт имя и один |
| `docs/archive/wide_panel.ru.md` | 2026-06-25 | 2026-07-13 | **18** | 175 | Статус: **план на реализацию.** Документ описывает, что строим, что НЕ строим и |
| `docs/archive/scenario_event_tables.md` | 2026-06-29 | 2026-07-13 | **14** | 222 | Each scenario follows **one bike** and shows the flow events it writes to the |
| `docs/archive/scenario_step_id_tables_ru.md` | 2026-06-29 | 2026-07-13 | **14** | 348 | Цель этого документа — показать на примерах таблиц, как для одного велосипеда |
| `docs/archive/step_id_unification_plan.md` | 2026-06-29 | 2026-07-13 | **14** | 212 | > **Update (2026-06-29).** This plan is done, and the design has since moved one |
| `docs/archive/step_id_unification_plan_ru.md` | 2026-06-29 | 2026-07-13 | **14** | 244 | > **Обновление (2026-06-29).** План выполнен, и с тех пор архитектура шагнула |
| `docs/archive/step_id_stamped_plan.md` | 2026-06-30 | 2026-07-13 | **13** | 282 | Today `step_id` is **computed after the run** from the tuple |
| `docs/archive/architecture_review.md` | 2026-07-03 | 2026-07-13 | **10** | 390 | Date: 2026-07-03. Branch at review time: `city_bike_mvp_accounting`. |
| `other_staff/claude_code_opus_answer.md` | 2026-07-03 | 2026-07-12 | **9** | 176 | Разбор по коду `gbp/consumers/simulator/phases.py:72–201`. Вызываемые методы — из `mechanics.py`, `state.py` и `gbp/model/flows.py`. |
| `other_staff/cursor_gpt_answer.md` | 2026-07-03 | 2026-07-12 | **9** | 227 | Фаза `DockArrivals` закрывает поездки, которые должны приехать в текущем периоде: ставит велосипеды в свободные доки, переправляет лишние на ближайшую станцию с |
| `other_staff/cursor_opus_answer.md` | 2026-07-03 | 2026-07-12 | **9** | 246 | Разобрался в коде. Объясняю фазу `DockArrivals` подробно — как программисту, с разбором всех внутренних методов, которые она вызывает. |
| `docs/archive/trip_distance_and_duration.md` | 2026-07-03 | 2026-07-13 | **10** | 108 | How the codebase computes trip length in time and in space. |
| `docs/archive/trip_distance_and_duration_ru.md` | 2026-07-03 | 2026-07-13 | **10** | 108 | Как в коде считаются время и расстояние поездки. |
| `docs/archive/architecture_review_2026-07-04.md` | 2026-07-04 | 2026-07-13 | **9** | 479 | Дата: 2026-07-04. Ветка на момент проверки: `city_bike_mvp_accounting`. |
| `docs/archive/deployment_plan_ru.md` | 2026-07-05 | 2026-07-13 | **8** | 479 | Цель: текущее рабочее ядро (симулятор `gbp/` и Streamlit-приложение `app/`) |
| `docs/documentation_plan_ru.md` | 2026-07-06 | 2026-07-06 | **0** | 282 | > Этот файл — рабочий план. Он написан, чтобы начать работу в новом чате без |
| `docs/README.md` | 2026-07-06 | 2026-07-24 | **18** | 105 | A framework for problems on flow graphs — networks where commodities move |
| `docs/README_ru.md` | 2026-07-06 | 2026-07-24 | **18** | 114 | Фреймворк для задач на потоковых графах — сетях, где перемещаемые единицы |
| `docs/simulator_clear.md` | 2026-07-06 | 2026-07-06 | **0** | 662 | This document explains how one simulation run works. |
| `.claude/skills/documentation-style/SKILL.md` | 2026-07-06 | 2026-07-15 | **9** | 94 | name: documentation-style |
| `docs/rebalancing_clear.md` | 2026-07-06 | 2026-07-06 | **0** | 349 | This document explains how the simulator moves bikes by truck at night. The |
| `docs/archive/deepening_candidates.md` | 2026-07-06 | 2026-07-13 | **7** | 448 | This document lists the refactoring candidates found by the architecture review |
| `docs/archive/data_contracts_plan.md` | 2026-07-07 | 2026-07-13 | **6** | 234 | > **Status: implemented on 2026-07-07.** All five steps are in the code. One |
| `docs/archive/docs_audit_2026-07-07.md` | 2026-07-07 | 2026-07-13 | **6** | 363 | This file is a work list. It records every mismatch found between the |
| `docs/plans/ml_demand_forecast_plan.md` | 2026-07-10 | 2026-07-30 | **20** | 401 | This document is the plan for the next project phase: demand forecasting. |
| `docs/plans/ml_cloud_phase_plan.md` | 2026-07-10 | 2026-07-30 | **20** | 127 | This document records the decisions about the Azure cloud phase of the ML |
| `docs/reports/model_evaluation_202601.md` | 2026-07-11 | 2026-07-30 | **19** | 130 | This report is the result of phase 5 of the demand-forecasting plan |
| `docs/reports/architecture_review_202607_ru.md` | 2026-07-11 | 2026-07-30 | **19** | 486 | Дата обзора: 2026-07-11. Статус: только предложения — код не менялся. |
| `docs/concepts.md` | 2026-07-13 | 2026-07-15 | **2** | 88 | Every other page of this documentation assumes five words: period, demand, |
| `docs/plans/docs_rework_plan_ru.md` | 2026-07-13 | 2026-07-15 | **2** | 200 | Этот документ — план работ по документации репозитория. Цель: новый человек |
| `docs/plans/docs_maro_diataxis_plan_ru.md` | 2026-07-15 | 2026-07-30 | **15** | 332 | Рабочий план автора, по-русски (как и предыдущий план документации — |
| `docs/plans/architecture_deepening_plan.md` | 2026-07-15 | 2026-07-30 | **15** | 261 | Findings of an architecture review (2026-07-15, branch `city_bike_mvp_accounting`). |
| `docs/plans/app_to_gbp_seam.md` | 2026-07-16 | 2026-07-30 | **14** | 265 | This document is the decided seam between `app/` and `gbp/`. For every non-UI |
| `.claude/skills/write-explanation/SKILL.md` | 2026-07-16 | 2026-07-30 | **14** | 93 | name: write-explanation |
| `.claude/skills/write-explanation/references/explanation-diataxis.md` | 2026-07-16 | 2026-07-30 | **14** | 186 | Explanation is a discursive treatment of a subject that permits *reflection*. |
| `explanation-diataxis-ru.md` | 2026-07-16 | 2026-07-16 | **0** | 118 | Объяснение — это рассуждающее изложение темы, допускающее *осмысление*. Объяснение **ориентировано на понимание**. |
| `docs/archive/overview.md` | 2026-07-16 | 2026-07-24 | **8** | 108 | ```mermaid |
| `docs/plans/interview_portfolio_plan.md` | 2026-07-17 | 2026-07-30 | **13** | 209 | Goal: make the project ready to present at an applied-scientist interview — with a |
| `docs/interview/presentation_45min.md` | 2026-07-17 | 2026-07-30 | **13** | 400 | This is a speaking script, not slides. Read it aloud to rehearse. Every |
| `docs/interview/presentation_45min_ru.md` | 2026-07-17 | 2026-07-30 | **13** | 426 | Русская копия файла `presentation_45min.md`. Само интервью идёт на |
| `docs/plans/wayfinder-hugo-site/map.md` | 2026-07-24 | 2026-07-30 | **6** | 138 | label: wayfinder:map |
| `docs/plans/wayfinder-hugo-site/research/github-pages-deploy.md` | 2026-07-24 | 2026-07-30 | **6** | 203 | Resolves ticket T8. All claims verified against primary sources on 2026-07-24. |
| `docs/plans/wayfinder-hugo-site/research/notebook-rendering.md` | 2026-07-24 | 2026-07-30 | **6** | 94 | Resolves ticket T5. Researched 2026-07-24. |
| `docs/plans/wayfinder-hugo-site/tickets/T1-stand-up-the-hugo-site-skeleton.md` | 2026-07-24 | 2026-07-30 | **6** | 60 | id: T1 |
| `docs/plans/wayfinder-hugo-site/tickets/T10-move-the-docs-pages-into-the-site.md` | 2026-07-24 | 2026-07-30 | **6** | 74 | id: T10 |
| `docs/plans/wayfinder-hugo-site/tickets/T2-choose-the-section-structure.md` | 2026-07-24 | 2026-07-30 | **6** | 48 | id: T2 |
| `docs/plans/wayfinder-hugo-site/tickets/T3-design-the-landing-page.md` | 2026-07-24 | 2026-07-30 | **6** | 47 | id: T3 |
| `docs/plans/wayfinder-hugo-site/tickets/T4-decide-where-notations-lives.md` | 2026-07-24 | 2026-07-30 | **6** | 54 | id: T4 |
| `docs/plans/wayfinder-hugo-site/tickets/T5-research-notebook-rendering-options.md` | 2026-07-24 | 2026-07-30 | **6** | 35 | id: T5 |
| `docs/plans/wayfinder-hugo-site/tickets/T6-decide-how-notebooks-appear-on-the-site.md` | 2026-07-24 | 2026-07-30 | **6** | 60 | id: T6 |
| `docs/plans/wayfinder-hugo-site/tickets/T7-decide-the-fate-of-non-site-folders.md` | 2026-07-24 | 2026-07-30 | **6** | 49 | id: T7 |
| `docs/plans/wayfinder-hugo-site/tickets/T8-research-github-pages-deploy.md` | 2026-07-24 | 2026-07-30 | **6** | 35 | id: T8 |
| `docs/plans/wayfinder-hugo-site/tickets/T9-set-up-the-deploy-pipeline.md` | 2026-07-24 | 2026-07-30 | **6** | 66 | id: T9 |
| `docs/plans/wayfinder-hugo-site/tickets/T11-rewrite-the-root-readme.md` | 2026-07-24 | 2026-07-30 | **6** | 50 | id: T11 |
| `docs/plans/wayfinder-hugo-site/tickets/T12-delete-the-retired-docs-leftovers.md` | 2026-07-24 | 2026-07-30 | **6** | 37 | id: T12 |
| `docs/plans/refactor_domain_agnostic_gbp.md` | 2026-07-30 | 2026-07-30 | **0** | 83 | Черновик для обсуждения. Фиксирует цель, целевое дерево репозитория и открытые |
| `docs/plans/split_gbp_ml.md` | 2026-07-30 | 2026-07-30 | **0** | 246 | Written 2026-07-30, right after `gbp/loaders` moved to `domains/citybike/loaders`. |

## Живые документы

| документ | создан | правок | строк | о чём |
|---|---|---|---|---|
| `README.md` | 2026-01-29 | 30 | 64 | A **flow graph** is a network where some commodity moves between facilities. |
| `CLAUDE.md` | 2026-03-24 | 78 | 35 | A framework for problems on flow graphs — networks where commodities move between facilities; the first and so far only scenario is the Citi Bike bike-sharing s |
| `.claude/skills/improve-codebase-architecture/DEEPENING.md` | 2026-04-30 | 4 | 39 | How to deepen a cluster of shallow modules safely, given its dependencies. Assumes the vocabulary in [LANGUAGE.md](LANGUAGE.md) — **module**, **interface**, **s |
| `.claude/skills/improve-codebase-architecture/INTERFACE-DESIGN.md` | 2026-04-30 | 4 | 44 | When the user wants to explore alternative interfaces for a chosen deepening candidate, use this parallel sub-agent pattern. Based on "Design It Twice" (Ousterh |
| `.claude/skills/improve-codebase-architecture/LANGUAGE.md` | 2026-04-30 | 2 | 53 | Shared vocabulary for every suggestion this skill makes. Use these terms exactly — don't substitute "component," "service," "API," or "boundary." Consistent lan |
| `.claude/skills/improve-codebase-architecture/SKILL.md` | 2026-04-30 | 6 | 71 | name: improve-codebase-architecture |
| `.claude/skills/create-dockstrings/SKILL.md` | 2026-05-11 | 8 | 79 | name: create-dockstrings |
| `AGENTS.md` | 2026-05-13 | 12 | 31 | Citi Bike Simulation Platform — vertical bike-sharing simulation built on the Citi Bike domain. Current phase: **codebase cleanup to canonical scenario minimum* |
| `Notations.md` | 2026-06-16 | 108 | 565 | A framework for problems on flow graphs — networks where commodities move |
| `docs/method/working-method.md` | 2026-06-19 | 3 | 68 | > A reminder for how to work through a hard task without getting lost in my own head. |
| `docs/method/working-method.ru.md` | 2026-06-19 | 3 | 71 | > Напоминание, как проходить сложную задачу, не теряясь в собственной голове. |
| `docs/site/content/docs/how-to/set-up-osrm.md` | 2026-07-04 | 3 | 183 | title: "Set up OSRM" |
| `docs/site/content/docs/architecture/worked-examples.md` | 2026-07-06 | 3 | 614 | title: "Worked examples" |
| `docs/site/content/docs/architecture/simulation-engine.md` | 2026-07-06 | 7 | 194 | title: "The simulation engine" |
| `docs/site/content/docs/architecture/rebalancing.md` | 2026-07-06 | 5 | 111 | title: "Rebalancing" |
| `docs/method/comprehension_levels.md` | 2026-07-06 | 4 | 365 | > Russian version: [`comprehension_levels_ru.md`](comprehension_levels_ru.md). |
| `docs/method/comprehension_levels_ru.md` | 2026-07-06 | 4 | 371 | > Каноническая версия — английская: [`comprehension_levels.md`](comprehension_levels.md). |
| `docs/site/content/docs/architecture/visualization.md` | 2026-07-06 | 8 | 155 | title: "Visualization" |
| `docs/site/content/docs/architecture/data-model.md` | 2026-07-06 | 6 | 236 | title: "The data model" |
| `docs/site/content/docs/architecture/flow-journal.md` | 2026-07-06 | 6 | 157 | title: "The flow journal" |
| `docs/site/content/docs/reference/api.md` | 2026-07-07 | 4 | 124 | title: "The run-artifact API" |
| `docs/site/content/docs/architecture/overview.md` | 2026-07-07 | 4 | 0 |  |
| `docs/site/content/docs/architecture/ml-toolkit.md` | 2026-07-13 | 6 | 147 | title: "The ML toolkit" |
| `docs/site/content/docs/decisions/forecast-replaces-only-demand.md` | 2026-07-13 | 3 | 39 | title: "A forecast run replaces only the demand" |
| `docs/site/content/docs/decisions/journal-as-source-of-truth.md` | 2026-07-13 | 2 | 35 | title: "The flow journal is the source of truth" |
| `docs/site/content/docs/decisions/sizing-run.md` | 2026-07-13 | 3 | 43 | title: "The initial state is measured by a sizing run" |
| `docs/site/content/docs/getting-started/quickstart.md` | 2026-07-13 | 3 | 285 | title: "First run" |
| `docs/site/content/docs/how-to/add-a-table-to-the-run-artifact.md` | 2026-07-13 | 3 | 67 | title: "Add a table to the run artifact" |
| `docs/site/content/docs/how-to/change-the-demand.md` | 2026-07-13 | 4 | 61 | title: "Change the demand" |
| `docs/site/content/docs/how-to/debug-an-invariant-violation.md` | 2026-07-13 | 2 | 96 | title: "Debug an invariant violation" |
| `docs/site/content/docs/how-to/run-on-a-forecast.md` | 2026-07-13 | 4 | 72 | title: "Run on a forecast" |
| `docs/site/content/docs/getting-started/installation.md` | 2026-07-15 | 4 | 81 | title: "Installation" |
| `docs/site/content/docs/getting-started/ui.md` | 2026-07-15 | 3 | 82 | title: "First look at the web interface" |
| `docs/site/content/docs/scenario/citibike.md` | 2026-07-15 | 5 | 294 | title: "Citi Bike in New York City" |
| `.scratch/app-to-gbp-seam/execution/01-move-evaluation-metrics.md` | 2026-07-16 | 2 | 20 | **What to build:** the two ad-hoc evaluation metrics that live in |
| `.scratch/app-to-gbp-seam/execution/02-move-run-path.md` | 2026-07-16 | 2 | 28 | **What to build:** the whole "configure → run → save" path moves out of |
| `.scratch/app-to-gbp-seam/execution/03-move-artifact-builder.md` | 2026-07-16 | 2 | 29 | **What to build:** the whole run-artifact builder moves from `app/artifacts.py` |
| `.scratch/app-to-gbp-seam/execution/04-move-evaluation-cluster.md` | 2026-07-16 | 2 | 24 | **What to build:** the two-level evaluation moves fully into `gbp`. The |
| `.scratch/app-to-gbp-seam/execution/05-update-canonical-docs.md` | 2026-07-16 | 2 | 30 | **What to build:** the canonical text now names the moved homes. The paths |
| `.scratch/app-to-gbp-seam/issues/01-artifact-builder-home.md` | 2026-07-16 | 2 | 50 | Type: grilling |
| `.scratch/app-to-gbp-seam/issues/02-evaluation-cluster.md` | 2026-07-16 | 2 | 43 | Type: grilling |
| `.scratch/app-to-gbp-seam/issues/03-run-recipe.md` | 2026-07-16 | 2 | 44 | Type: grilling |
| `.scratch/app-to-gbp-seam/issues/04-ui-shared-reductions.md` | 2026-07-16 | 2 | 51 | Type: grilling |
| `.scratch/app-to-gbp-seam/issues/05-canonical-contract-rewrite.md` | 2026-07-16 | 2 | 41 | Type: grilling |
| `.scratch/app-to-gbp-seam/issues/06-assemble-seam-spec.md` | 2026-07-16 | 2 | 34 | Type: task |
| `.scratch/app-to-gbp-seam/map.md` | 2026-07-16 | 2 | 91 | <!-- wayfinder:map --> |
