---
title: "The forecast notebook"
weight: 3
---

# Forecast pipeline

The forecast run of the canonical scenario (Notations.md §11), next to the base
replay in `test_pipeline.ipynb`. The steps, in order:

1. Load one month of trips and resolve the graph tables (same as the base replay).
2. Build a seasonal naive forecast for the week right after the history ends and
   save it as a forecast artifact under `data/ml/forecasts/`.
3. Put the forecast in place of the historical demand: the period grid becomes
   the forecast horizon, and the OD matrix is mapped onto it by hour of week.
4. Size the state for the forecast demand, run, and check the invariants I1-I5.
5. Compare the simulated departures against the forecast demand — in a clean
   run they match row by row.


```python
import pandas as pd

from gbp.loaders.dataloader_raw import RawModelData
from gbp.loaders.dataloader_graph import (
    ResolvedModelData,
    apply_forecast_demand,
    attach_simulation,
)
from gbp.consumers.simulator import run_sized_scenario
from gbp.ml import forecast
```


```python
ubuntu_path = "/mnt/outer/Documents/vlzm/GFDRR_ubuntu/GFDRR/data/raw/202602-citibike-tripdata_1.csv"
mac_path = "/Users/vladislav/Documents/vlzm/GFDRR/data/raw/202601-citibike-tripdata_1.csv"
trips_path = mac_path

# Raw model data: read the trip CSV, derive raw entities.
raw_data = RawModelData(
    trips_path=trips_path,
    seed=42,
    n_depots=10,
    depot_capacity=9000,
    n_trucks=5,
    truck_capacity_bikes=20,
    truck_rate=50.0,
    electric_bike_rate=5,
    classic_bike_rate=3,
)
graph_data = ResolvedModelData(raw_data, period_len=pd.Timedelta(hours=1))
```


```python
# Seasonal naive forecast: for each station, commodity and hour of week, the
# mean demand over the historical weeks. The fractional means are rounded to
# whole bikes by the largest-remainder rule (round_forecast_demand — the
# platform's one rounding rule). The horizon starts where the history ends.
forecast_name = "seasonal_naive_w1"
horizon_periods = 168  # one week of one-hour periods

folder = forecast.build_seasonal_naive_forecast(
    graph_data.historical_demand_df,
    graph_data.periods_df,
    forecast_name=forecast_name,
    horizon_periods=horizon_periods,
    inputs=[trips_path.split("/")[-1]],
)
forecast_demand_df, forecast_meta = forecast.load_forecast(forecast_name)
print(f"Saved {folder}")
print(f"Horizon: {forecast_meta.horizon_periods} periods from {forecast_meta.t0}")
print(f"Forecast demand rows: {len(forecast_demand_df)}, bikes: {forecast_demand_df['quantity'].sum()}")
forecast_demand_df.head()
```


```python
# The forecast run (Notations.md §11). apply_forecast_demand returns a shallow
# copy of graph_data: the demand table is the forecast, the period grid is the
# horizon (period ids restart at 0), and the OD matrix is the historical one
# pooled by hour of week. graph_data itself is not modified.
forecast_data = apply_forecast_demand(
    graph_data, forecast_demand_df, forecast.forecast_periods_from_meta(forecast_meta)
)

number_of_periods = 168

# One call owns the whole order: size the state against the forecast demand,
# run the demand against it, and check the run invariants I1-I5.
forecast_run = run_sized_scenario(
    forecast_data,
    scenario_id="forecast_run",
    number_of_periods=number_of_periods,
)
print(f"Invariant violations: {forecast_run.violations}")

# Later cells read the sized state off forecast_data, so store it there, then
# wire the finished run into the container's simulated_* slots.
forecast_data.initial_inventory_df = forecast_run.initial_inventory_df
forecast_data.facilities_capacities_df = forecast_run.facilities_capacities_df
attach_simulation(forecast_data, forecast_run.simulated_flows_df)
simulated_flows_df = forecast_data.simulated_flows_df
simulated_flows_df['event_type'].value_counts()
```


```python
# In a clean forecast run no demand is lost, so the simulated departures equal
# the forecast demand per (period, facility, commodity).
comparison = forecast_demand_df.merge(
    forecast_data.simulated_departures_df,
    on=["period_id", "facility_id", "commodity_category"],
    how="outer",
    suffixes=("_forecast", "_departed"),
).fillna(0)
mismatches = comparison[comparison["quantity_forecast"] != comparison["quantity_departed"]]
print(f"Rows: {len(comparison)}, mismatches: {len(mismatches)}")

per_period = comparison.groupby("period_id")[["quantity_forecast", "quantity_departed"]].sum()
per_period.head(24)
```
