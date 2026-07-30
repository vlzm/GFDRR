---
title: "Run on a forecast"
weight: 2
---

# How to run the simulator on a forecast

Task: run a scenario whose demand table comes from a saved forecast instead
of history — a forecast run
([Notations.md §11](../reference/notations.md#11-run-kinds)).

## 1. Pick a forecast

Each saved forecast is a folder `data/ml/forecasts/<forecast_name>/` with
`demand.parquet` (the forecast demand table) and `meta.json` (the model and
the horizon):

```bash
ls data/ml/forecasts/
```

## 2. Build one if none fits

The fastest builder needs no trained model: the seasonal naive from one trip
CSV. Its horizon starts right after the CSV's history ends and covers 168
one-hour periods (one week) by default:

```bash
python -m domains.citybike.ml.forecast --trips-path data/raw/202601-citibike-tripdata_1.csv \
    --forecast-name my_naive
```

Once the retraining pipeline (`python -m domains.citybike.ml.ops.pipeline`) has trained and
promoted a champion, forecast with it instead:

```bash
python -m domains.citybike.ml.forecast --champion --forecast-name champion_w1
```

## 3. Run on it

```bash
python app/runner.py --run-name forecast_demo --demand-source forecast \
    --forecast-name my_naive --periods 168
```

What happens before the run (`run_scenario` in `app/runner.py`): the forecast
is loaded by name; rows the scenario cannot run — stations or station-hours
the trip CSV has never seen, so no OD rows exist for them — are cut first
(`restrict_demand_to_scenario`); then `apply_forecast_demand` puts the
forecast in place of the historical demand. The period grid becomes the
forecast horizon, and the OD matrix is mapped onto it by hour of week.
Everything after that is the same sized-run path a history run takes.

## Expected result

`data/runs/forecast_demo/meta.json` records the source:

```json
"demand_source": "forecast",
"forecast_name": "my_naive",
"forecast_dropped_share": 0.02
```

`forecast_dropped_share` is the share of the forecast demand cut in step 3;
the runner also prints it while running when it is above zero. Period 0 of
this run is the first hour of the forecast horizon, not of the historical
month.

The canonical forecast run lives in `notebooks/forecast_pipeline.ipynb`; the
model side (training, backtest, evaluation) is described in
[ml-toolkit.md](../architecture/ml-toolkit.md).
