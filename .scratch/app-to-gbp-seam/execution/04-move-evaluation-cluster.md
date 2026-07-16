# 04 — Move the evaluation cluster into gbp/ml/evaluation.py

**What to build:** the two-level evaluation moves fully into `gbp`. The
comparison bookkeeping (`EvalNames`, `ModelForecast`, `run_row`,
`build_comparison`) and the orchestration from `app/evaluate.py`
(`evaluate_month`, `ensure_forecast`, `_ensure_run`, `actual_demand_table`,
`evaluation_dir`) become one new module `gbp/ml/evaluation.py`. Its callables
(`run_and_save`, the artifacts load/list) now all live in `gbp` after tickets 02
and 03, so the module imports no `app`. The terminal entry point becomes
`python -m gbp.ml.evaluation --month ...`, matching the other `gbp.ml` commands.
`app/eval_comparison.py` and `app/evaluate.py` are deleted — no app shell stays.
`evaluate_month` for a held-out month produces the same comparison table.

**Blocked by:** 01 (the metrics it calls), 02 (`run_and_save`), 03 (the
artifacts load/list it reads).

**Status:** ready-for-agent

- [ ] `gbp/ml/evaluation.py` holds the comparison bookkeeping and the `evaluate_month` orchestration.
- [ ] The CLI runs as `python -m gbp.ml.evaluation --month <YYYYMM>`.
- [ ] `app/eval_comparison.py` and `app/evaluate.py` are deleted; nothing under `app/` imports them.
- [ ] The module imports no `app`; `grep -rn "import app" gbp/` is empty.
- [ ] `evaluate_month` for a held-out month writes the same `comparison.csv` rows as before.
- [ ] The evaluation tests point at `gbp.ml.evaluation`, assertions unchanged; the suite is green.
