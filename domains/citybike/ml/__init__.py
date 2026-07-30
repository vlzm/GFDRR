"""Demand forecasting for Citi Bike: the training table, the model families, the operations.

Importing anything under this package registers the four model families with
``gbp.ml.model``, so ``create_model("lightgbm")`` works from anywhere. Only
the class names are registered here — each family's module (and its heavy
dependency: LightGBM, statsmodels, torch) is imported when that family is
first asked for.
"""

from gbp.ml.model import register_model

register_model("seasonal_naive", "domains.citybike.ml.models.seasonal_naive:SeasonalNaiveModel")
register_model("sarimax", "domains.citybike.ml.models.sarimax:SarimaxTotalModel")
register_model("lightgbm", "domains.citybike.ml.models.boosting:LightGbmModel")
register_model("graphsage", "domains.citybike.ml.models.graph:GraphSageModel")
