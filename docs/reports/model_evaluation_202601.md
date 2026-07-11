# Model evaluation through the simulator — January 2026

This report is the result of phase 5 of the demand-forecasting plan
(`docs/plans/ml_demand_forecast_plan.md`): the two-level evaluation
(Notations.md §17) of the four model families on the held-out month
**January 2026 (202601)**. Level 1 is forecast error against the month's
actual departure counts. Level 2 runs the simulator on each forecast and on
the actual demand and compares the decisions: the sized state (initial
inventory and dock capacities) and the run totals. The report ends with the
model the platform should use and why.

Everything below was produced by one command:

```bash
python app/evaluate.py --month 202601
```

## How the runs were built

- Every model trained on the training partitions 202502–202512 — the same
  window as the backtest split that tests 202601. Each forecast is a saved
  forecast artifact (`data/ml/forecasts/<model>_202601/`), 744 hourly
  periods from 2026-01-01 00:00.
- The scenario (stations, OD matrix) comes from the canonical trip CSV
  (`202601-citibike-tripdata_1.csv`). A forecast can name stations or
  station-hours this scenario has never seen, so every demand table — the
  actual one included — was first cut to the scenario's OD coverage
  (`restrict_demand_to_scenario` in `app/evaluate.py`). The cut removed
  8.01% of the actual demand and 3–17% of each forecast; all runs face the
  same universe, so their totals compare. After the cut the actual demand
  is 1,697,807 trips.
- Three run kinds per Notations.md §11, nine runs total, each 744 periods:
  one **reference run** (actual demand, state sized on itself), and per
  model one **forecast run** (forecast demand, state sized on itself) and
  one **forecast-sized run** (actual demand against the state sized on the
  forecast). Every run passed the run invariants (`violations: []`).

The run artifacts are `data/runs/eval_202601_*`; the full comparison table
is `data/ml/evaluation/202601/comparison.csv`.

## Level 1 — forecast error

Error of the whole-bike forecast tables the runs consumed, after the cut,
over the station-hours where either side is positive. "Busy" stations are
the smallest set that produced half of the month's departures.

| model | MAE | MAE busy | Poisson deviance | forecast total | vs actual |
|---|---|---|---|---|---|
| seasonal_naive | 1.554 | 2.287 | 5.674 | 2,310,464 | +36.1% |
| sarimax | 1.439 | 2.008 | 7.331 | 1,621,567 | −4.5% |
| lightgbm | **1.328** | **1.861** | **4.220** | 2,234,627 | +31.6% |
| graphsage | 1.995 | 2.569 | 27.878 | 860,592 | −49.3% |

LightGBM is the most accurate per row. SARIMAX is the only model whose
monthly total is close to the truth. Seasonal naive and LightGBM overshoot
the total by about a third — their history windows are dominated by warmer,
busier months. GraphSage predicts less than half of the real volume.

Note on the phase-4 backtest: on the fractional forecasts over the full
grid (zero rows included) GraphSage had the *best* MAE of the four
(0.478 vs 0.545 for LightGBM). That number rewarded predicting near-zero
everywhere. The tables above and the runs below show what that bias does.

## Level 2 — decisions

The forecast run shows the plan a forecast leads to: how large a system the
platform would size and pay for.

| run | demand | cost, $ | fleet, bikes | dock capacity |
|---|---|---|---|---|
| reference (actual) | 1,697,807 | 1,217,573 | 203,052 | 396,604 |
| seasonal_naive | +36.1% | +36.4% | +58.3% | +58.3% |
| sarimax | −4.5% | −3.3% | +29.8% | +30.3% |
| lightgbm | +31.6% | +33.0% | +54.4% | +54.1% |
| graphsage | −49.3% | −48.1% | +26.3% | +29.6% |

The forecast-sized run shows what that plan costs when the real January
demand arrives. Losses are stockouts; redirected are arrivals whose planned
dock was full and who docked at the nearest station with space.

| model | lost demand | lost share | redirected | lost at busy stations |
|---|---|---|---|---|
| seasonal_naive | 39,188 | 2.3% | 31,943 | 34% |
| sarimax | 41,674 | 2.5% | 47,289 | 27% |
| lightgbm | 42,772 | 2.5% | 38,445 | 41% |
| graphsage | 377,802 | 22.3% | 69,481 | 29% |

Two results matter.

First, the three viable models lose almost the same share of real demand
(2.3–2.5%) but need very different fleets to do it. SARIMAX serves January
with 263,488 bikes; seasonal naive needs 321,407 — 58,000 more bikes to
avoid 2,500 more lost trips. Overprediction does not improve service,
because the losses sit at specific station-hours the forecasts miss, mostly
at quiet stations (busy stations take only 27–41% of the losses while
producing half the demand).

Second, GraphSage's underprediction breaks the decisions: the state sized
on it loses
377,802 trips — 22% of the real demand — even though its fleet is *larger*
than the reference minimum (+26%). It predicts volume at the wrong places,
so bikes stand where demand does not come. This answers the research
question the plan posed for GraphSage: on this data, spatial structure did
not add accuracy over boosting, and its error profile is the worst kind for
decisions. A negative answer, as the plan allowed.

## The decision

**Use SARIMAX for forecast runs now.** It is the only model whose monthly
volume is right (−4.5%), its plan is the cheapest of the four (fleet +30%
over the true minimum instead of +54–58%), and its realized service equals
the others (2.5% lost). Its per-row error is second best.

**LightGBM stays the main candidate.** It wins level 1 outright (best MAE
and Poisson deviance, best on busy stations) but overshoots the monthly
volume by a third, which inflates the whole plan. The next step is fixing
its scale bias — calibrating the predicted monthly total or reworking how
training months are weighted — and rerunning this evaluation; if the totals
come right, it should replace SARIMAX as champion.

Seasonal naive remains the baseline; it is beaten on both levels by
SARIMAX. GraphSage is rejected for the platform.

## Limitations

- The OD matrix (where trips go, how long they take) comes from the same
  January the runs simulate, mapped by hour of week. All models share it,
  so the comparison is fair, but the absolute totals are better than a real
  deployment, where the OD matrix would come from earlier months.
- The weather features used the month's published weather — a perfect
  weather forecast, as in the backtest.
- The cut to the scenario's OD coverage removed 8% of the actual demand;
  results describe the covered universe.
- Observed departures are censored demand (Notations.md §17): the actual
  counts are a lower bound wherever stations stood empty.
