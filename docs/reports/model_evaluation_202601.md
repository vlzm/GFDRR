# Model evaluation through the simulator — January 2026

This report is the result of phase 5 of the demand-forecasting plan
(`docs/plans/ml_demand_forecast_plan.md`): the two-level evaluation
(Notations.md §17) of the four model families on the held-out month
**January 2026 (202601)**, over its **first week (168 hourly periods,
Jan 1–7)**. Level 1 is forecast error against the actual departure counts.
Level 2 runs the simulator on each forecast with the physics held fixed and
compares the run totals against the reference run on the actual demand. The
report ends with the model the platform should use and why.

Everything below was produced by one command:

```bash
python app/evaluate.py --month 202601 --periods 168
```

## How the runs were built

- Every model trained on the training partitions 202502–202512 — the same
  window as the backtest split that tests 202601. Each forecast is a saved
  forecast artifact (`data/ml/forecasts/<model>_202601/`) covering the
  whole month; the evaluation reads its first 168 hours.
- One physical picture for every run. The state — initial inventory and
  dock capacities — is sized once from the **actual** demand of the window:
  66,895 bikes and 128,537 docks. Every model's forecast runs against this
  same state (a replay-state forecast run, Notations.md §11), so any
  difference in run totals comes from the forecast alone.
- The scenario (stations, OD matrix) comes from the canonical trip CSV
  (`202601-citibike-tripdata_1.csv`). Every demand table — the actual one
  included — was first cut to the scenario's OD coverage
  (`restrict_demand_to_scenario` in `app/evaluate.py`); the cut removed
  2.6% of the actual demand and 3–17% of each forecast. After the cut the
  actual demand of the week is 424,191 trips.
- Five runs, each 168 periods: the reference run on the actual demand, and
  one replay-state forecast run per model. Every run passed the run
  invariants (`violations: []`).

The run artifacts are `data/runs/eval_202601_168p_*`; the full comparison
table is `data/ml/evaluation/202601/comparison_168p.csv`.

## Level 1 — forecast error

Error of the whole-bike forecast tables the runs consumed, after the cut,
over the station-hours where either side is positive. "Busy" stations are
the smallest set that produced half of the week's departures.

| model | MAE | MAE busy | Poisson deviance | forecast total | vs actual |
|---|---|---|---|---|---|
| seasonal_naive | 1.450 | 2.096 | 6.743 | 518,108 | +22.1% |
| sarimax | 1.297 | **1.705** | 9.660 | 304,018 | −28.3% |
| lightgbm | **1.246** | 1.720 | **5.193** | 501,531 | +18.2% |
| graphsage | 1.908 | 2.562 | 27.517 | 235,631 | −44.5% |

LightGBM is the most accurate per row. No model gets the week's volume
right: seasonal naive and LightGBM overshoot it (their history windows are
dominated by busier weeks), SARIMAX and GraphSage undershoot it. This week
is a hard one — it starts with the New Year holiday and ramps up from
27,000 trips on January 1 to 94,000 on January 7.

Note on the phase-4 backtest: on the fractional forecasts over the full
grid (zero rows included) GraphSage had the *best* MAE of the four
(0.478 vs 0.545 for LightGBM). That number rewarded predicting near-zero
everywhere. The tables here show what that bias does.

## Level 2 — run totals against the reference

Each forecast ran through the state sized for the actual demand. The
reference run is the target: departures equal to the actual demand, zero
lost, zero redirected. A forecast whose run lands close to those totals
would let the simulator behave as it does on history.

| run | demand | departed | vs reference | lost | redirected | cost, $ | vs reference |
|---|---|---|---|---|---|---|---|
| reference (actual) | 424,191 | 424,191 | — | 0 | 0 | 301,003 | — |
| seasonal_naive | 518,108 | 475,255 | +12.0% | 42,853 | 30,178 | 344,691 | +14.5% |
| sarimax | 304,018 | 284,737 | −32.9% | 19,281 | 12,636 | 208,055 | −30.9% |
| lightgbm | 501,531 | 455,043 | **+7.3%** | 46,488 | 26,911 | 331,415 | **+10.1%** |
| graphsage | 235,631 | 152,482 | −64.1% | 83,149 | 10,335 | 114,252 | −62.0% |

How to read the rows. Overpredicting models (naive, LightGBM) send more
trips than the state can serve: the extra demand ends as `lost_demand`
(stockouts at station-hours the real week never used) and `redirected`
(arrivals into docks the real week filled differently). Underpredicting
models (SARIMAX, GraphSage) lose little to friction but simply do not send
the trips: SARIMAX misses a third of the reference departures, GraphSage
two thirds — and GraphSage still loses 35% of the trips it does send
(83,149 of 235,631; 93% of those losses at busy stations), so its demand
sits at the wrong stations too.

LightGBM lands closest to the reference on both departures (+7.3%) and
cost (+10.1%). Seasonal naive is the same picture with worse numbers.

## The decision

**Use LightGBM for forecast runs.** On this window it is closest to the
reference on departures and cost, and it wins level 1 on the overall MAE
and the Poisson deviance (on busy-station MAE it is within 1% of SARIMAX's
best value). Its one systematic flaw is volume: +18–22% overprediction,
which against the fixed state turns into 9% of its trips lost and another
5% redirected. The next step is to correct this bias — calibrate the
predicted totals or rework how training months are weighted — and rerun
this evaluation; the totals should tighten further.

SARIMAX is not a substitute: its January **month** total is nearly right
(−4.5% in the backtest), but its within-month shape is wrong — it
underpredicts the post-holiday week by 28% and misses a third of the
reference departures. A city-total model split by fixed hour-of-week
shares cannot track shape; per-row it also loses to LightGBM.

Seasonal naive remains the baseline; it is beaten by LightGBM on every
number above. GraphSage is rejected: the worst error, the worst volume,
and the largest gap to the reference — the research answer is that spatial
structure did not add accuracy over boosting on this data.

## Limitations

- The window is one week, and an unusual one (New Year plus the ramp-up
  after it). The same command without `--periods` reruns the evaluation on
  the full month; artifacts and the comparison file are kept apart by name.
- The OD matrix (where trips go, how long they take) comes from the same
  January the runs simulate, mapped by hour of week. All models share it,
  so the comparison is fair, but a real deployment would map it from
  earlier months.
- The weather features used the month's published weather — a perfect
  weather forecast, as in the backtest.
- The cut to the scenario's OD coverage removed 2.6% of the actual demand;
  results describe the covered universe.
- Observed departures are censored demand (Notations.md §17): the actual
  counts are a lower bound wherever stations stood empty.
