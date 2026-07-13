"""Demand forecasting: the model around the simulator (Notations.md §17).

The package turns Citi Bike trip history into a forecast demand table --
``period_id, facility_id, commodity_category, quantity`` -- the same shape as
historical demand, so the simulator runs a forecast through the same run
chain (``apply_forecast_demand`` in the loaders puts it in the one demand
slot the engine reads).

The process, one module per step:

- ``data`` holds the forecasting data helpers: the ``data/ml/`` root, the
  month period grid, and the weather download; ``station_status`` downloads
  the station-status snapshots. The trip CSVs themselves come from
  ``gbp/loaders/download.py``, shared with the base replay;
- ``training`` builds the training table, one parquet partition per month
  under ``data/ml/training/``, with the feature columns from ``features``;
- ``models`` holds the four ``DemandModel`` families (seasonal naive,
  SARIMAX, LightGBM, GraphSage) behind one factory, ``create_model``;
- ``forecast`` builds the forecast input, rounds fractional demand to whole
  bikes, and saves each forecast as an artifact under ``data/ml/forecasts/``;
- ``metrics`` and ``backtest`` score the families with a rolling-origin
  backtest, logged to the MLflow store in ``registry``;
- ``pipeline`` is the retraining pipeline (download -> build-table -> train
  -> backtest -> promote), moving the ``champion`` alias in the registry;
- ``monitoring`` scores saved forecasts once a month's actuals arrive and
  builds the drift reports.

Each module's docstring carries its own details; the map of the subsystem is
``docs/explanation/ml.md``.
"""
