# Interview Portfolio Plan

Goal: make the project ready to present at an applied-scientist interview — with a
rehearsed story, a research notebook, a live demo link, and a clean repository.
The code itself is ~85% ready; this plan covers the missing 15%, which is mostly
artifacts and narrative, not code.

How to use this file: each phase is independent enough to start in a fresh chat.
Give the assistant this file and the phase number. Phases are ordered by priority,
but Phase 1 (cleanup) and Phase 4 (deploy) can run in any order relative to the rest.

## Context (facts established 2026-07-17)

State of the repository at planning time:

- Simulator core is complete: `gbp/consumers/simulator/` (engine, phases, mechanics,
  OR-Tools VRP rebalancing), journal engine in `gbp/model/flows.py`. Zero TODOs,
  one abstract `NotImplementedError` (by design).
- ML toolkit is complete: four model families behind one `DemandModel` interface
  (`SeasonalNaiveModel`, `SarimaxTotalModel`, `LightGbmModel`, `GraphSageModel`),
  rolling-origin backtest, MLflow registry with champion/challenger aliases, DVC,
  retraining pipeline, two-level evaluation through the simulator, drift monitoring
  (Evidently). Plan `docs/plans/ml_demand_forecast_plan.md` — all phases done.
- Tests: 30 files, 283 test functions. CI (GitHub Actions): ruff, mypy, pytest.
- UI: Streamlit app with 10 pages (`app/`). API: FastAPI over run artifacts (`app/api.py`).
- Docker: `Dockerfile` (Streamlit on 8501) + `docker-compose.yml` (app + OSRM).
- **Nothing deployed to Azure.** Cloud exists only as a plan (`docs/plans/ml_cloud_phase_plan.md`).
- Notebooks are smoke-style (little markdown). **No research/EDA narrative notebook.**
- `docs/key-components/` holds 7 parallel drafts of the same overview document.
- Key existing material to reuse: `docs/reports/model_evaluation_202601.md`
  (four-family evaluation), `gbp/ml/station_status.py` (censored demand),
  `gbp/ml/backtest.py`, `docs/decisions/` (three ADRs).

The central selling point of the project, to keep in front of every artifact this
plan produces: **forecasts are judged by operational cost through the simulator,
not only by forecast metrics**. "Model A wins on MAE, model B is cheaper to
operate" is the headline result.

Positioning rule for all artifacts: the project is a **general platform for
flow-graph problems**, and every artifact states its current state in the same
breath: today exactly one domain is implemented — the Citi Bike bike-sharing
system in New York, done end to end — and the platform grows by adding domains
later. The order is fixed: platform idea first, then "one domain implemented so
far, more to come", then the whole story is told through the Citi Bike domain
only. Do not drop the platform idea, and do not tell the story of a second
domain that does not exist yet.

---

## Phase 1 — Repository cleanup (quick wins, ~half a day)

A stranger's first five minutes with the repo should not hit drafts and stray files.

Tasks:

1. In `docs/key-components/`: pick ONE overview from the 7 drafts
   (`overview_*.md`), name it `overview.md`, delete the rest. The older copy in
   `docs/archive/overview.md` stays archived or is deleted. Note: `overview.md`
   is currently deleted in git status (branch `city_bike_mvp_accounting`) — the
   chosen draft replaces it.
2. Delete `notebooks/Untitled*` and `notebooks/__pycache__`.
3. Decide the fate of `notebooks/check_pipeline.ipynb`, `build_wild_inventory.ipynb`,
   `flat_env.ipynb`: per CLAUDE.md, everything must serve one of the two canonical
   runs. Either delete them or move the still-useful parts into scripts/tests.
4. README pass, reader = potential employer with 5 minutes:
   - one paragraph: what the system is and the central idea (cost-based model
     evaluation through the simulator);
   - one architecture picture or short diagram;
   - the three user roles and their interfaces (see Phase 3, "interfaces" slide);
   - quickstart that actually works;
   - link to the live demo (after Phase 4) and to the research notebook (after Phase 2).
5. Commit the cleanup separately from any content work.

Done when: `docs/key-components/` has exactly one overview; `notebooks/` has only
notebooks that serve the canonical runs (plus the research notebook from Phase 2);
README reads well top-to-bottom for a first-time visitor.

## Phase 2 — Research notebook (the main missing artifact, ~2–4 days)

Create `notebooks/research_demand_forecast.ipynb` — a narrative notebook that
shows how the demand model was chosen. This is the artifact an applied-scientist
interviewer will ask for. All material already exists in the codebase; the work
is assembling and narrating, not new research.

It serves the forecast run (it justifies the champion model behind
`forecast_pipeline.ipynb`), so it fits the "two canonical runs" rule.

Structure (each section: markdown first, then code; write for a reader who has
never seen the repo):

1. **Problem statement.** What is being forecast (station-hour demand), why
   (the simulator needs a demand table for future periods), and how success is
   measured (two levels: forecast metrics AND simulated operational cost).
2. **EDA.** Seasonality (hour-of-day, day-of-week, month), spatial demand
   distribution across stations, busy vs quiet stations, weather effect.
   Use existing loaders (`gbp/loaders/`) and features (`gbp/ml/features.py`).
3. **Censored demand.** The research nugget: when a station is empty, demand is
   observed as zero. Show real examples from `gbp/ml/station_status.py` outputs
   (stockout periods), explain the correction and its effect on the training table.
4. **Feature engineering.** Calendar, weather, history features — what and why
   (source: `gbp/ml/features.py`).
5. **Model families and backtest.** The four families, why these four (a
   complexity ladder: naive seasonal → classical → gradient boosting → GNN),
   rolling-origin backtest design and how leakage is excluded
   (source: `gbp/ml/backtest.py`, `gbp/ml/metrics.py`). Comparison table + plots.
6. **Two-level evaluation.** The headline: run the simulator on each model's
   forecast, compare operational metrics (redirects, lost trips, rebalancing
   cost) vs forecast metrics. Reuse `gbp/ml/evaluation.py` and the numbers from
   `docs/reports/model_evaluation_202601.md`.
7. **Champion choice and limitations.** Why the current champion, what would be
   tried next.

Rules: notebook calls existing `gbp` code — no logic reimplemented in cells; runs
top to bottom on the data months already used in the repo; plots follow one clean
style. Add it to the docs-testing setup if `test_docs_*` conventions allow.

Done when: the notebook runs end to end, reads as a story without the reader
opening any source file, and is linked from the README.

## Phase 3 — Presentation script (~2 days, no code)

Create `docs/interview/` with three files. Language: English (interviews are in
English). These are speaking scripts, not slides — write them as text to rehearse
aloud.

1. `presentation_45min.md` — the full story:
   - Problem (5 min): bike-sharing operations, stockouts/overflows, rebalancing
     trucks; the question "how do we test a decision before spending money?"
   - Digital twin (10 min): exact historical replay, event journal as source of
     truth (use ADR `docs/decisions/journal-as-source-of-truth`), invariants,
     redirect mechanics, VRP rebalancing.
   - Demand forecasting (10 min): censored demand, feature engineering, four
     model families, rolling-origin backtest.
   - The key idea (10 min): two-level evaluation — forecast error vs operational
     cost; the "model A wins MAE, model B wins cost" slide.
   - Operations loop (5 min): retraining pipeline, MLflow registry,
     champion/challenger promotion, drift monitoring.
   - Demo + limitations (5 min): Streamlit walk-through; self-stated
     "what I would do next" list.
2. `presentation_5min.md` — the short version (most interviews start here):
   problem → digital twin → cost-based evaluation idea → one result → stack.
3. `qa_prep.md` — answers to hard questions, each answered in 3–5 sentences:
   - Why GraphSAGE and not something simpler? (answer includes: the simpler
     models are in the comparison, and the backtest decides)
   - How is data leakage excluded in the backtest?
   - Why a simulator and not an analytical queueing model?
   - How is censored demand handled and what does it change?
   - Why only one domain? (answer: depth over breadth; designed so the
     generalization path is clear, not built speculatively)
   - What is NOT in production, honestly? (local-first MLOps loop; cloud is
     designed in `ml_cloud_phase_plan.md`, minimal deploy live — after Phase 4)
   - What would you change in the code today? (turn the perfectionism list into
     a prepared "known limitations" answer — state it proactively)

Also add a `docs/interview/interfaces.md` one-pager (or a section in the 45-min
script): three user roles and their entry points —
analyst → Streamlit (10 pages); data scientist → CLI
(`python -m gbp.ml.pipeline / forecast / evaluation / monitoring`) + MLflow UI +
two canonical notebooks; integrator → FastAPI run-artifact API.

Done when: all three scripts exist and the 45-min one has been read aloud at
least once with a timer (rehearsal itself is user work, not assistant work).

## Phase 4 — Minimal Azure deploy (~1–2 days)

Goal: a live URL for the portfolio site. Minimal scope — the Streamlit app in a
container, reading pre-built run artifacts baked into the image or mounted from
storage. NOT in scope: cloud retraining, Databricks, scheduled jobs (that stays
a written plan in `ml_cloud_phase_plan.md`).

Tasks:

1. Decide artifact strategy: bake `data/runs/<run_name>/` for 1–2 showcase runs
   into the image (simplest) vs Azure Blob storage mount. Start with baking.
2. Check the existing `Dockerfile` builds and serves the app with baked data;
   trim image size if needed (no torch/ml extras for the UI-only image).
3. Deploy to Azure Container Apps (or App Service): container registry (ACR),
   `az containerapp up` or a small bicep/CLI script committed to `scripts/` or
   `infra/`; scale-to-zero to keep cost near zero.
4. OSRM: the demo does not need live routing — confirm the UI pages degrade
   gracefully without the OSRM service, or bake precomputed routes.
5. Add the URL to README and the portfolio site. Optional: basic auth or a
   simple access token if the app should not be fully public.
6. Document the deploy in `docs/how-to/deploy-demo.md` (commands, how to update).

Done when: a public URL shows the app with at least the compare, station map,
costs, and model monitoring pages working; deploy is reproducible from the
how-to doc.

## Phase 5 — Final pass (~half a day)

1. Re-read README + overview + research notebook in one sitting as "the
   employer's 15 minutes"; fix rough edges.
2. Check the story is consistent everywhere: same central idea (cost-based
   evaluation), same positioning (general platform; one implemented domain —
   Citi Bike — end to end; expansion later), same role/interface list in
   README, overview, and presentation.
3. Run the full local check: `ruff check`, `mypy gbp/`, `pytest`; confirm CI is
   green on the branch; merge to `main`.
4. Rehearse the 45-min and 5-min scripts with a timer (user).

---

## Priorities if time is short

1. Phase 3 (script) — directly reduces interview anxiety, needs no code.
2. Phase 2 (research notebook) — the biggest artifact gap for the role.
3. Phase 4 (live demo link).
4. Phases 1 and 5 (cleanup and polish).
