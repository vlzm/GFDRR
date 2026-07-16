"""The model families of the forecasting phase, behind one interface."""

from __future__ import annotations

import pathlib

from gbp.ml.models.base import DemandModel

MODEL_FAMILIES = ("seasonal_naive", "sarimax", "lightgbm", "graphsage")


def create_model(name: str, **params: object) -> DemandModel:
    """Build one unfitted model of the given family."""
    if name == "seasonal_naive":
        from gbp.ml.models.seasonal_naive import SeasonalNaiveModel

        return SeasonalNaiveModel(**params)
    if name == "sarimax":
        from gbp.ml.models.sarimax import SarimaxTotalModel

        return SarimaxTotalModel(**params)  # type: ignore[arg-type]
    if name == "lightgbm":
        from gbp.ml.models.boosting import LightGbmModel

        return LightGbmModel(**params)  # type: ignore[arg-type]
    if name == "graphsage":
        from gbp.ml.models.graph import GraphSageModel

        return GraphSageModel(**params)  # type: ignore[arg-type]
    raise ValueError(f"unknown model family {name!r}; known: {', '.join(MODEL_FAMILIES)}")


def load_model(name: str, folder: pathlib.Path) -> DemandModel:
    """Read a fitted model of the given family from a folder ``save`` wrote."""
    if name == "seasonal_naive":
        from gbp.ml.models.seasonal_naive import SeasonalNaiveModel

        return SeasonalNaiveModel.load(folder)
    if name == "sarimax":
        from gbp.ml.models.sarimax import SarimaxTotalModel

        return SarimaxTotalModel.load(folder)
    if name == "lightgbm":
        from gbp.ml.models.boosting import LightGbmModel

        return LightGbmModel.load(folder)
    if name == "graphsage":
        from gbp.ml.models.graph import GraphSageModel

        return GraphSageModel.load(folder)
    raise ValueError(f"unknown model family {name!r}; known: {', '.join(MODEL_FAMILIES)}")


__all__ = ["MODEL_FAMILIES", "DemandModel", "create_model", "load_model"]
