# 01 — Move the two evaluation metrics into gbp/ml/metrics.py

**What to build:** the two ad-hoc evaluation metrics that live in
`app/eval_comparison.py` today — `panel_departed_mae` (per-station-hour mean
absolute difference of `departed` between two run panels) and the busy-share
aggregation (`lost_demand` on busy facilities over total `lost_demand`) — are
defined in `gbp/ml/metrics.py`, next to `forecast_metrics` and
`busy_facility_ids` they already lean on. Every evaluation metric is then
defined in one place. The comparison code calls them from `gbp.ml.metrics`, and
the evaluation produces the same comparison rows as before.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] `panel_departed_mae` lives in `gbp/ml/metrics.py` and is imported from there by the comparison code.
- [ ] The busy-share (`lost_demand_busy_share`) is a named function in `gbp/ml/metrics.py`, not an inline expression.
- [ ] No metric math stays inlined in `app/eval_comparison.py`.
- [ ] The comparison table for a held-out month is byte-for-byte the same as before the move.
- [ ] The metric tests move next to the other `gbp.ml.metrics` tests, assertions unchanged; the suite is green (`ruff`, `mypy gbp/`, tests).
