# 02 — Move the evaluation cluster into gbp/ml

Type: grilling
Status: resolved
Blocked by: —

## Decision

`panel_departed_mae` and the busy-share go to `gbp/ml/metrics.py`. The
comparison bookkeeping (`EvalNames`, `ModelForecast`, `build_comparison`,
`run_row`) and the `evaluate.py` orchestration move to a new
`gbp/ml/evaluation.py`, CLI `python -m gbp.ml.evaluation`. `app/eval_comparison.py`
and `app/evaluate.py` are deleted; no app shell stays. See
`docs/plans/app_to_gbp_seam.md`.

## Question

`app/eval_comparison.py` (167 lines) and `app/evaluate.py` (216 lines) contain
no Streamlit at all, yet reach deep into `gbp.ml`. Decide their new home and
interface in `gbp/ml`.

The clearest "not UI" cluster. What is there today:
- `eval_comparison.py`: `panel_departed_mae` (`:90`) — per-station-hour MAE
  between two run panels; the `lost_demand` aggregation + `lost_demand_busy_
  share` inside `build_comparison` (`:150`). It already imports
  `forecast_metrics` / `busy_facility_ids` from `gbp.ml.metrics`. Also
  `EvalNames` (run/file naming) and `build_comparison` (assembles the
  comparison DataFrame).
- `evaluate.py`: `evaluate_month`, `ensure_forecast`, `_ensure_run`,
  `actual_demand_table` (`:31`) — orchestration that drives `gbp.ml.forecast`,
  `gbp.ml.training`, `gbp.ml.data`, and the loader demand helpers, then saves
  artifacts. Has an `argparse` CLI (`python app/evaluate.py --month ...`).

Decisions to reach:
1. Where do the two ad-hoc metrics (`panel_departed_mae`, the busy-share)
   go — into `gbp/ml/metrics.py` next to their siblings?
2. Does the orchestration become a `gbp/ml/evaluation.py` module, and does the
   CLI entry move with it (`python -m gbp.ml.evaluation`)?
3. What thin shell stays in `app/` — the page trigger, or nothing?
4. What is the interface `app` calls after the move?

Consult: `/grilling`, `codebase-design`. Do not edit code — record the target
module(s) and the interface.
