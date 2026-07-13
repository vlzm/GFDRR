# Demand forecasting (the ML part)

This document describes everything under `gbp/ml/`: how raw trip files become
a training table, which feature columns the models read, the four model
families, how a trained model becomes a saved forecast, how models are judged
(the backtest and the two-level evaluation), how retraining and promotion
work, and how model quality is watched after the forecasts are made.

The reason this code exists is one table. The simulator can run on a
**forecast demand table** — a table in the shape of `HISTORICAL_DEMAND_SCHEMA`
(`period_id`, `facility_id`, `commodity_category`, `quantity`) whose
`quantity` comes from a model instead of history. The simulator reads it
exactly as it reads historical demand; that is the whole integration
contract. Everything in `gbp/ml/` serves the production of that table and the
measurement of its quality.

The canonical vocabulary is [Notations.md §17](../../Notations.md#17-demand-forecasting-the-model-around-the-simulator);
the plan is [ml_demand_forecast_plan.md](../plans/ml_demand_forecast_plan.md).

## The Code Map

| Module | Role |
|---|---|
| `gbp/ml/data.py` | Downloads the monthly trip CSVs and the daily weather; loads trip files of both published schemas into the one trips schema. |
| `gbp/ml/station_status.py` | Downloads the archived station-status dumps and computes the censoring mark `stockout_share`. |
| `gbp/ml/training.py` | Builds the training table, one parquet partition per month, under `data/ml/training/`. |
| `gbp/ml/features.py` | The one feature builder. Both the training table and the forecast input get their feature columns from here. |
| `gbp/ml/models/` | The four model families behind the one interface `DemandModel`: `seasonal_naive`, `sarimax`, `lightgbm`, `graphsage`. |
| `gbp/ml/metrics.py` | Forecast-vs-actual error measures: MAE and Poisson deviance, overall and split by station traffic. |
| `gbp/ml/backtest.py` | Rolling-origin backtest over the training partitions, logged to MLflow. |
| `gbp/ml/forecast.py` | The forecast builder: the forecast input, the one rounding rule, and the saved forecast artifact. |
| `gbp/ml/registry.py` | The MLflow store, the model registry, and the `champion` alias. |
| `gbp/ml/pipeline.py` | The retraining pipeline: `download → build-table → train → backtest → promote`. |
| `gbp/ml/monitoring.py` | Scores saved forecasts against actual months, computes the degraded mark, builds the drift report. |
| `app/evaluate.py` | The two-level evaluation: runs the simulator on actual vs forecast demand for one held-out month. |

## The Data Map

Everything lives under `data/` (Notations.md §15):

| Path | Contents |
|---|---|
| `data/raw/` | The downloaded source files, exactly as published: trip CSVs (`202601-citibike-tripdata_1.csv`, `JC-202601-...`), one weather CSV per year (`weather-central-park-<year>.csv`), and the station-status dumps (`<YYYYMM>-citi-bike-nyc-stats.parquet`). Code never edits this folder. |
| `data/ml/training/` | The training table: one parquet partition per month (`202601.parquet`). |
| `data/ml/forecasts/<forecast_name>/` | One forecast artifact: `demand.parquet` (the forecast demand table) plus `meta.json`. |
| `data/ml/mlflow/` | The MLflow store: experiment runs and the model registry in `mlflow.db` (SQLite), logged files under `artifacts/`. |
| `data/ml/pipeline_log.csv` | One row per retraining-pipeline run: the promote-or-keep decision and its reason. |
| `data/ml/monitoring/` | The metrics table `metrics.parquet` and the drift reports (`drift_<month>.html` / `.json`). |
| `data/ml/evaluation/<month>/` | The two-level evaluation output: `comparison.csv`, one row per run. |

Two folders are versioned with DVC: `data/raw/` and `data/ml/training/`. DVC
writes each folder's checksum into a small text file next to it (`data/raw.dvc`,
`data/ml/training.dvc`), and git versions those files. The **data version** of
a training run is the git commit that last touched the `.dvc` files, with
`-dirty` appended while they have uncommitted changes (`data_version` in
`gbp/ml/backtest.py`).

## The Tables Carried Through

Four tables pass through the whole process. Each is defined once here.

- **Training table** — what a model learns from. One row per
  `(period_id, facility_id, commodity_category)` with the observed departure
  count in `quantity`, the feature columns, and the censoring mark
  `stockout_share`. Zero-count rows are kept: a station-hour with no
  departures is a real observation, not a gap. Schema:
  `TRAINING_TABLE_SCHEMA` in `gbp/ml/training.py`.
- **Forecast input** — what a model predicts from. One row per
  `(period, facility, commodity)` of the forecast horizon, with the same
  feature columns appended by the same functions. Built by `forecast_input`
  in `gbp/ml/forecast.py`.
- **Fractional demand** — a model's raw prediction: demand-shaped rows whose
  `quantity` is a non-negative float, not yet whole bikes.
- **Forecast demand table** — the fractional demand rounded to whole bikes by
  the one rounding rule (`round_forecast_demand`), positive rows only. This
  is the table the simulator reads.

A **period** is one hour (`DEFAULT_PERIOD_LEN`). A month's hours are numbered
on the **month period grid**: `period_id` 0, 1, 2, … from the month's first
hour (`month_period_grid` in `gbp/ml/data.py`). A forecast numbers its own
periods 0, 1, 2, … from its `t0` — a forecast run is its own scenario with
its own clock.

## Step 1: Raw Data

### Trips

`python -m gbp.ml.data --months 202502 202503` downloads the published
monthly zips from the Citi Bike bucket (`https://s3.amazonaws.com/tripdata`)
into `data/raw/` and unpacks their CSVs. The key names in the bucket are not
uniform, so `download_months` lists the bucket by prefix and takes what
exists. The Jersey City zip of the same month is downloaded together with
the city one. Months before 2024 exist only inside yearly bundles; the
downloader raises a clear error for them, and the bundle is unpacked by hand.
A month whose CSVs are already on disk is skipped whole.

The published columns changed in February 2021, so there are two schemas.
`load_trips_any_schema` (`gbp/ml/data.py`) loads a file of either era into
the one trips schema (`TRIPS_SCHEMA`): new files go through
`load_trips_raw_df` unchanged; old files are renamed and filled
(`starttime` → `started_at`, `usertype` → `member_casual`, every old trip
becomes a `classic_bike`, `ride_id` becomes null). Both paths drop unusable
rows through the shared cleaning step (`clean_trips`) and cache the parsed
result as parquet in `data/processed/`.

### Weather

The weather features come from one station: NOAA GHCN-Daily, Central Park
(`USW00094728`). `load_weather_daily` returns one row per date with
`temperature_max_c`, `temperature_min_c`, `precipitation_mm`. The data is
cached as one CSV per calendar year in `data/raw/`; a past year's file never
changes, and the current year's file is downloaded again when it does not
yet reach the asked dates (NOAA publishes with a few days' delay). A date the
station has not reported is simply absent, and the join leaves NaN.

### Station status (for the censoring mark)

CityBikes archives the public station feed and publishes monthly dumps, one
row per status change per station (`bikes` = bikes available, `timestamp` in
UTC). Dumps for New York start at 2024-11; earlier months get no mark.
`download_status_months` (`gbp/ml/station_status.py`) fetches the dumps into
`data/raw/`; a month the archive does not have is noted and skipped — the
mark is best-effort, a missing dump is not an error.

## Step 2: The Training Table

`python -m gbp.ml.training --months 202502 202503 202504` is one command:
download what is missing, then build the partitions. It is idempotent —
a month already in `data/raw/` is not downloaded again, a partition already
in `data/ml/training/` is not rebuilt (`--force` rebuilds).

One month becomes one partition, built by `build_month_partition`
(`gbp/ml/training.py`) in four moves:

1. **Count departures** (`departure_counts`). Keep the trips that started
   inside the month, number the month's hours on the month period grid, and
   count each trip into the hour and station it started at. The result is a
   full grid: every hour of the month × every station seen in the month's
   trips (as a start or an end point) × every bike type seen. Hours with no
   departures hold `quantity` 0. A published file sometimes carries a few
   trips of the neighbor month; the month filter keeps them out.
2. **Append the feature columns** (`build_features`, next section). The
   history is read from the earlier partitions on disk, so months are built
   oldest-first; the oldest month has no history and its history features
   stay NaN.
3. **Merge the `stockout_share` mark** (next section). Facilities the feed
   covers get 0.0 where no zero-bike time was recorded; facilities without a
   match stay NaN — unknown, not zero.
4. **Check the schema** (`TRAINING_TABLE_SCHEMA`) and write
   `data/ml/training/<YYYYMM>.parquet`.

The observed departure count here equals what the simulator's own read-model
gives: counting trips by start hour matches building the historical flow
journal and counting its `departed` events. A test in
`tests/test_ml_training.py` holds the two together.

## The Feature Columns

All feature columns are built in one module, `gbp/ml/features.py`. The
training-table builder and the forecast input builder both call it; neither
keeps its own copy. This closes the classic failure called train/serve skew:
the model seeing one definition of a feature in training and another in
production. For the same station-day the training table and the forecast
input hold identical feature values; the test
`test_training_table_and_forecast_input_agree_for_the_same_station_day`
proves it.

The columns (`FEATURE_COLUMNS`), in three groups:

| Group | Columns | Source |
|---|---|---|
| Calendar | `hour_of_day` (0–23), `day_of_week` (0 = Monday), `month` (1–12), `is_holiday` (US public holidays) | `start_timestamp` alone |
| Weather | `temperature_max_c`, `temperature_min_c`, `precipitation_mm` | the daily Central Park table, joined by calendar date — every hour of a day gets that day's values |
| History | `quantity_lag_1w` (the count at the same hour one week earlier), `quantity_mean_4w` (the mean of the same hour over the past `LAG_WEEKS` = 4 weeks), `facility_mean` (the station's mean count over the window), `facility_hour_of_week_mean` (the station's mean count at this hour of week) | the history window |

The **history window** is the departure counts of the `HISTORY_WEEKS` = 8
weeks right before the rows being built (`clip_history_window`). **Hour of
week** is `weekday * 24 + hour`, 0..167, 0 = Monday 00:00.

The one information rule: every history feature of a row is computed from
counts strictly before that row — never from the row itself, never from
counts after it. The guard `_require_history_before_targets` raises when the
history overlaps the target rows. The training table of a month is built as
if that month were being forecast: its features read only the months before
it. This is what makes the backtest honest — a model never sees the answer
inside its own inputs.

Missing values are meaningful. A history value whose source hours are not
observed stays NaN — missing, never zero. On a forecast horizon,
`quantity_lag_1w` of a row more than one week past `t0` points at an hour
inside the horizon itself, so it stays NaN. Weather of a date NOAA has not
published stays NaN. A model family must therefore accept NaN inputs
(LightGBM does natively; GraphSage imputes and adds missing flags).

## Censored Demand: the `stockout_share` Mark

The training target is the observed departure count, and the platform
assumes observed departures ≈ demand. The assumption is biased low exactly
where the system fails: an hour a station stood with no bikes records zero
departures no matter how many people wanted one. This is **censored demand**.

Where the archived station-status dumps exist, each training row carries the
mark `stockout_share`: the share of its hour (0..1) the station had zero
bikes available. `gbp/ml/station_status.py` computes it:

- Dump timestamps are UTC and are converted to New York wall-clock time; one
  local month also needs the next month's dump, because the last local
  evening lies in the next UTC month.
- The feed writes only changes, so between two records a station's count is
  carried forward. Zero-bike stretches are cut at hour borders and summed
  per hour (`stockout_shares`).
- The feed's stations are matched to the trips' `facility_id` by
  coordinates: each facility takes the nearest feed station within
  `MATCH_DISTANCE_M` = 100 meters. A facility without a match keeps NaN.
- `bikes` counts every bike type, so the mark is per station — the same
  value for every `commodity_category` of the facility.

The mark is **not** a feature: future stockouts are unknown at prediction
time, so it never appears in `FEATURE_COLUMNS`. It is a training weight: the
LightGBM and GraphSage models weight each row by `1 - stockout_share` — an
hour the station was empty half the time counts half as much, an hour with
no snapshot (NaN) counts fully.

## The Model Families

A **model family** is one way to forecast demand behind the one interface
`DemandModel` (`gbp/ml/models/base.py`):

- `fit(training_table)` learns from the training table;
- `predict(feature_table)` takes a forecast input and returns fractional
  demand — one row per `(period_id, facility_id, commodity_category)` with a
  non-negative float `quantity` (the helper `fractional_demand` clips
  negatives and sorts);
- `save(folder)` / `load(folder)` write and read a fitted model as files, so
  a backtest run can log the model to MLflow and a later step can load it
  back without refitting;
- `params()` returns the settings that define the instance, for experiment
  logs.

`predict` returns fractional values on purpose: the rounding to whole bikes
happens once, in the forecast builder, and the backtest metrics read the
fractional values — rounding is a simulator constraint, not a model
property.

The forecast builder and the backtest construct families only through the
factory `create_model(name)` (`gbp/ml/models/__init__.py`); family internals
never leak past the interface. Imports inside the factory are lazy, so a
family's library (torch, lightgbm) loads only when that family is used.

### `seasonal_naive` — the baseline

The forecast for a station at a given hour is the mean demand at the same
hour of week over the history window. That number is already a feature
column — `facility_hour_of_week_mean` — so the model reads the column
instead of computing its own copy, and `fit` stores nothing. A pair absent
from the history window has a NaN mean; the model predicts 0 for it. Every
other family must beat this model.

### `sarimax` — the classical baseline

Two parts, and only the first is SARIMAX (`gbp/ml/models/sarimax.py`):

1. **The level.** Sum the training table into total departures per day for
   the whole city and fit SARIMAX on that series (default order `(1, 0, 1)`,
   weekly seasonal part `(1, 1, 1, 7)`). Daily totals keep the series short,
   so the fit takes seconds; SARIMAX on hourly data with a 168-hour season
   would take hours.
2. **The split.** Each forecast day's total is divided over that day's
   `(period, facility, commodity)` rows proportionally to
   `facility_hour_of_week_mean` — the same hour-of-week mean the seasonal
   naive predicts.

So the split of a day over stations and hours is the naive one; what SARIMAX
adds is the day-level total (trend and recent weeks). The horizon must start
after the training days: the model only forecasts forward.

### `lightgbm` — the expected main model

One gradient-boosting model over all stations
(`gbp/ml/models/boosting.py`). Its inputs are the `FEATURE_COLUMNS` plus the
station and the bike type themselves as categorical features — one global
model, with the station as a feature. Default settings: Poisson objective
(the target is a count, so predictions are already positive),
`num_boost_round` 300, `learning_rate` 0.05, `num_leaves` 63,
`min_data_in_leaf` 50, `feature_fraction` 0.9, `bagging_fraction` 0.8.
LightGBM handles the NaN values the history and weather features carry.
Rows are weighted by `1 - stockout_share`.

The category lists of `facility_id` and `commodity_category` are fixed at
`fit` and stored on the model; `predict` re-encodes its input with the same
lists, so the codes never depend on which stations appear in the forecast
input. A station unseen in training becomes NaN, and the model falls back to
its other features for it.

### `graphsage` — the research model

The question this model answers: does spatial structure add accuracy over
boosting? A negative answer is a valid result (`gbp/ml/models/graph.py`).

Nodes are stations; edges come from OD flows: `station_graph_edges` counts
the trips between every pair of stations. When no `edges_df` is passed in,
`fit` counts the edges from the raw trip files of its last training month.
The counts become edge weights, made symmetric and cut to the
`max_neighbors` = 32 strongest neighbors per station; the adjacency is
row-normalized, so a neighbor's influence is its share of the station's
traffic.

The network is plain torch, no graph library: two layers of
`relu(W_self·x + W_neigh·(A·x))` where `A` is the sparse weighted adjacency,
then a linear head that outputs the log of the expected departure count
(Poisson loss). One training sample is one `(period, commodity)` pair — the
node feature matrix holds every station's values at that hour. This works
because the training table and the forecast input are full grids. Features
are z-scored with the training mean and deviation; a NaN becomes 0 after
z-scoring, and each nullable column gets a 0/1 missing flag so the model can
tell a real value from an imputed one. The bike type enters as a one-hot
column. Rows are weighted by `1 - stockout_share`.

Training reads only the last `train_window_months` = 3 months of the
training table: the history features already carry the longer memory, and
more months add cost much faster than signal.

One platform note: on macOS, torch and lightgbm each bring their own OpenMP
runtime, and one process cannot run both. The fix is pinned at the top of
`graph.py`: import lightgbm first, and `torch.set_num_threads(1)`.

## From a Prediction to a Forecast Artifact

### The one rounding rule

The engine moves whole bikes, so a forecast demand table must hold whole
numbers. Rounding each row on its own would drift the totals: a thousand
stations forecast at 0.4 would round to zero demand. `round_forecast_demand`
(`gbp/ml/forecast.py`) decides the rule once: within each
`(period_id, commodity_category)` group, round the group total to the
nearest whole number, give every row the whole part of its own value, and
hand the remaining bikes one each to the rows with the largest fractional
parts (ties broken by `facility_id`, so the result is deterministic). This
is the largest-remainder method — the same rule `form_potential_trips` uses
to split a source's departures over targets. Two properties follow: the
group total is exact to the nearest bike, and no demand is invented — a row
with fractional part zero is never rounded up. Rows that end at zero are
dropped; like the historical demand marginal, the table lists only positive
demand.

### The one prediction recipe

`predict_horizon` (`gbp/ml/forecast.py`) is the recipe every forecast demand
table is built by: take the history window before the horizon, build the
forecast input (`forecast_input` — every facility × commodity of the history
window crossed with every horizon period, features appended), let the model
predict, round to whole bikes. Without an explicit `history_df` the history
is read from the training partitions before the horizon.

`naive_month_prediction` forecasts one calendar month with the seasonal
naive through this same recipe. It is the shared baseline both quality
measures rely on: the backtest divides every family's error by its error,
and the monitoring alert compares against it.

### The forecast artifact

One saved forecast is the folder `data/ml/forecasts/<forecast_name>/` with
`demand.parquet` and `meta.json`. `meta.json` follows the pydantic model
`ForecastMeta`: the model name and version, the history window
(`history_start`, `history_end`, `inputs`), and the horizon (`t0`,
`horizon_periods`, `period_len_hours` — enough to rebuild the forecast
period grid). `save_forecast` checks the demand table against
`HISTORICAL_DEMAND_SCHEMA` before writing — a wrong shape fails here, not
inside a simulator run — and writes `meta.json` last, so a folder with a
`meta.json` is always a complete artifact.

Three builders save an artifact:

- `build_seasonal_naive_forecast` — the phase-1 path: from one trip CSV's
  demand and period grid, seasonal naive only.
- `build_model_forecast` — from the training partitions, any family by
  name, fitted on the spot; the horizon starts right after the last training
  month.
- `build_champion_forecast` — the platform's path: the fitted champion
  resolved from the model registry by its alias, no refitting, never a file
  path.

```bash
python -m gbp.ml.forecast --model lightgbm --months 202502 ... 202512 \
    --forecast-name lightgbm_202601 --horizon-periods 744
python -m gbp.ml.forecast --champion --forecast-name champion_202602 --horizon-periods 744
```

Weather for a true future horizon does not exist yet. The builders use
whatever NOAA has published for the horizon dates; unpublished dates leave
the weather features NaN, and a fully unreachable weather source is skipped
with a note. In a backtest, the held-out month's actual weather is joined
instead — it plays the role of a perfect weather forecast, so real future
forecasts will be somewhat worse than backtest scores.

The forecast run itself (loading a forecast by name, cutting it to the
scenario, sizing the state, running) is a simulator topic — see
[Notations.md §11](../../Notations.md#11-run-kinds) and
`notebooks/forecast_pipeline.ipynb`.

## Level-1 Evaluation: Metrics and the Backtest

### The error measures

`gbp/ml/metrics.py` scores fractional demand against actual counts row by
row. Alignment first: `align_forecast` joins the two tables on the demand
keys with an outer join and fills missing sides with 0 — a station the model
never predicted counts with prediction 0, a predicted station with no actual
trips counts with actual 0. Two measures:

- **MAE** — mean absolute error, in bikes per station-hour.
- **Poisson deviance** — the count-data error: it punishes underprediction
  of busy hours harder than MAE. Predictions are floored at `POISSON_EPS`
  before the logarithm, so predicting exactly zero for an hour that had
  departures gives a large finite penalty instead of an infinite one.

Both are reported overall and split by station traffic: the busy stations
(the smallest set that together produced half of the month's departures)
against the quiet rest. Thousands of near-empty stations would otherwise
hide bad predictions on the stations that matter. The totals
(`actual_total`, `predicted_total`) are reported too, so a scale error is
visible at a glance.

### The backtest

The **backtest** (`python -m gbp.ml.backtest`) is rolling-origin validation:
train on months `1..k`, forecast month `k+1`, move the split forward, at
least 3 splits (`backtest_splits`; the months must be consecutive). Rows
from the future never appear in training — there are no random splits.

For every split and every family the run is the same four steps
(`run_backtest`):

1. stack the training partitions of the split's training months;
2. build the forecast input for the held-out month (`month_forecast_input`)
   — history from the partitions right before it, weather the month's
   actual weather;
3. `fit` on the training table, `predict` on the forecast input;
4. score the fractional demand against the month's actual counts and log
   everything to MLflow.

The loop is split-major: the training table and the forecast input of a
split are built once and every model reuses them.

Every `(model, split)` pair is one MLflow run in the `demand-backtest`
experiment: parameters (model settings, training months, the data version),
the split's metrics, and the fitted model files as artifacts. One extra run
named `comparison` holds the cross-model table: mean metrics per family plus
`mae_over_naive` and `poisson_deviance_over_naive` — the family's mean error
divided by the shared naive baseline's (below 1.0 beats the baseline). The
pipeline's promote step reads this saved table back through
`latest_comparison`.

```bash
mlflow ui --backend-store-uri sqlite:///data/ml/mlflow/mlflow.db
```

## Level-2 Evaluation: Through the Simulator

Level-1 error says how far the numbers are from the actual counts. It does
not say what those errors cost in the system — a small MAE concentrated on
the morning peak of a busy station can hurt more than a large MAE spread
over quiet nights. The **two-level evaluation**
(`python app/evaluate.py --month 202601`) answers that with simulator runs.

For one held-out month, over the same period grid, the same OD matrix, and
the same demand universe:

- one **reference run** — the month's actual demand against the state sized
  on itself; its sized state (initial inventory and dock capacities) is the
  replay state every other run reuses;
- per model, one **replay-state forecast run** — the model's forecast demand
  against that same state. The state has no slack beyond what the actual
  demand needed, so overprediction shows up as `lost_demand` and
  `redirected`, and underprediction as departures below the reference.

Every demand table is first cut to what the scenario can run
(`restrict_demand_to_scenario`) — the same cut for the actual and every
forecast, so all runs face the same universe and their totals compare. Every
run is a normal run artifact under `data/runs/` (named `eval_<month>_...`),
and the comparison lands in `data/ml/evaluation/<month>/comparison.csv`.

## The Model Registry and the Champion

The **MLflow store** is one folder, default `data/ml/mlflow/`: the runs and
the registry in `mlflow.db` (SQLite), the logged files under `artifacts/`.
In code it is one object, `MlflowStore` (`gbp/ml/registry.py`): an entry
point creates it once and passes it on; every method first points MLflow's
process-wide state at the store's folder, so no caller has to remember a
"configure first" rule.

The **registry** is the list of trained model versions the store keeps. One
registered model, `demand-model`, holds every version regardless of family.
A **model version** is a fitted model plus what defines it — three tags:
`model_family`, `train_months`, `data_version`. Its files are the folder the
model's `save` wrote, logged to a training run in the `demand-training`
experiment. Two pipeline runs over the same family, months, and data version
reuse the same version instead of registering a twin (`find_version`).

The alias `champion` marks the version the platform currently uses.
`build_champion_forecast` resolves the model through
`MlflowStore.resolve_champion` — never by a file path. Promotion moves the
alias (`promote_to_champion`); the version it left keeps the tag
`role: challenger`. The retraining pipeline is the registry's only writer.

## The Retraining Pipeline

`python -m gbp.ml.pipeline` runs five idempotent steps in one fixed order
(`gbp/ml/pipeline.py`); `--steps` runs any subset in that order:

1. **download** — the trigger lives here: ask the bucket for published
   months missing from `data/raw/` (`published_missing_months`) and fetch
   them, trips plus status dumps. A scheduler (cron, later a cloud job) only
   has to start the pipeline.
2. **build-table** — build the missing training partitions, oldest first.
   After download or build-table changed the tracked folders, `refresh_dvc`
   runs `dvc add` so the data version reflects the new months.
3. **train** — fit the candidate family (default `lightgbm`, `--model`
   changes it) on every partition on disk and register it as a model
   version. A registered version with the same family, months, and data
   version is reused, not registered twice.
4. **backtest** — the phase-4 backtest over the candidate's family and the
   champion's family, on the same splits. When the candidate is already the
   champion, the backtest is skipped.
5. **promote** — the rule, not a manual choice (`promote_decision`): no
   champion yet → the candidate is promoted; the candidate already is the
   champion → nothing changes; otherwise the candidate becomes champion only
   when its mean backtest MAE (`PRIMARY_METRIC`) is at least as good as the
   champion's over the same splits. An equal score promotes — same recipe,
   newer data version wins. The loser stays a challenger.

Either way one row goes to the **pipeline log**
(`data/ml/pipeline_log.csv`): when it ran, the data version, the candidate
and champion versions with their scores, `promoted` yes or no, and the
reason in words. The log is the audit trail of every promote-or-keep
decision.

Rerunning the whole pipeline with no new data changes nothing: nothing to
download, nothing to build, the train step reuses the registered version,
and when that version is already the champion the backtest is skipped and
the log row says so.

## Monitoring

Monitoring watches model quality after the forecasts are made
(`gbp/ml/monitoring.py`). Every saved forecast names its model version in
its `meta.json` — that record is the base of all monitoring.

```bash
python -m gbp.ml.monitoring --month 202602
```

### Scoring a new actual month

When a new actual month arrives (its training partition is on disk),
`score_month` joins the month against every saved forecast whose horizon
touches it and appends one row per forecast to the **monitoring metrics
table** `data/ml/monitoring/metrics.parquet`: `mae`, `poisson_deviance`,
`bias` (the mean of predicted − actual; positive means the model predicts
too much), and `naive_mae` — the seasonal naive baseline over the same rows.
Only the overlap is scored: the actual counts are cut to the month hours the
forecast covers, so a one-week forecast is not punished with zeros for the
rest of the month (`month_period_map` lines the two period numberings up by
wall-clock time).

Unlike the backtest, which scores fractional demand, monitoring scores the
saved forecast demand table — whole bikes, the numbers the simulator
actually consumed. Scoring the same (forecast, month) pair again replaces
its row, so the step is idempotent.

### The alert: degraded months

`metric_history` aggregates the metrics table per model version and month
and computes `rolling_mae` — the mean of the version's MAE over its last
`ROLLING_MONTHS` = 3 scored months. A month is marked **degraded** when the
rolling MAE is worse than the seasonal naive MAE of that month. A month with
no baseline is never marked. The "Model monitoring" page only reads this
mark; it computes nothing.

### The drift report

The drift report answers a different question: does the new month still look
like the data the champion was trained on? Drift means the feature
distributions moved. `drift_report` compares the month's weather and
demand-history columns (`DRIFT_COLUMNS`) against the champion's training
months (the monitored month itself is left out of the reference), with
Evidently, on a row sample from both sides (`DRIFT_SAMPLE_ROWS` = 200 000
per side). The calendar columns are not checked on purpose: their
distributions are set by the calendar, not by the riders — one monitored
month holds exactly one `month` value, so that column would be flagged every
time and the flag would mean nothing.

Per column the measure is the Wasserstein distance in units of the reference
standard deviation; the column has drifted when the distance is at or above
`DRIFT_NUM_THRESHOLD` = 0.25 (Evidently's default of 0.1 is crossed easily
by ordinary month-to-month variation). Two files go to
`data/ml/monitoring/`: the full HTML report (`drift_<month>.html`) and a
small JSON summary (`drift_<month>.json`) the monitoring page lists.

## What Is Checked

- `TRIPS_SCHEMA` — every loaded trip file, of either era.
- `TRAINING_TABLE_SCHEMA` — every partition before it is written; the keys
  `(period_id, facility_id, commodity_category)` must be unique.
- `_require_history_before_targets` — the feature builder rejects history
  that overlaps the target rows, so a feature can never read the row it
  describes.
- `HISTORICAL_DEMAND_SCHEMA` — every forecast demand table in
  `save_forecast`, before anything is written.
- `ForecastMeta` — `meta.json` is validated back on every `load_forecast`.
- Tests hold the seams together: the training table and the forecast input
  agree on feature values for the same station-day
  (`tests/test_ml_features.py`), and counting trips by start hour equals
  counting the historical journal's `departed` events
  (`tests/test_ml_training.py`).

## Why It Is Built This Way

**One feature module for both paths.** The alternative — the forecast
builder computing its own features — invites train/serve skew, the bug where
training and prediction silently disagree on a definition. Here the two
paths share the same functions, and a test pins the agreement.

**Features strictly before targets, splits only forward.** Any feature that
reads its own row or later rows makes backtest scores better than real
scores, and the error is invisible until production. The guard raises
instead.

**Zero rows kept in the training table.** A station-hour with no departures
is an observation of zero demand. Dropping it would teach the model that
quiet hours do not exist and bias every mean upward.

**Models predict fractions; rounding happens once.** Whole bikes are a
simulator constraint, not a model property. Keeping predictions fractional
lets the metrics see the real model output, and the largest-remainder rule
keeps per-period totals exact where rounding does happen.

**`stockout_share` is a weight, not a feature.** Future stockouts are
unknown at prediction time, so a model that trained on the mark as an input
could not be given it at prediction time. As a weight it only reduces the
influence of hours where the observed count understates demand.

**The seasonal naive is the bar everywhere.** The backtest reports every
family's error relative to it, and the monitoring alert fires when a version
falls behind it. A model that cannot beat the hour-of-week mean adds
complexity without value.

**The champion is an alias, not a path.** Resolving the production model
through the registry means promotion is one recorded operation, every
version keeps its identity tags, and no code hard-wires a file location.

**Every pipeline step is idempotent.** A scheduler can start the pipeline
blindly: with no new data, nothing changes and the log says why. Failures
can be retried without cleanup.

**Monitoring scores what the simulator consumed.** The saved forecast demand
table — whole bikes — is what the platform acted on, so that is what gets
compared to reality; the fractional backtest scores answer a different,
model-side question.
