"""The model families of the forecasting phase, behind one interface.

One file per family, every family implements ``DemandModel``
(``gbp/ml/models/base.py``): ``fit(training_table)`` and
``predict(feature_table)`` returning fractional demand. The forecast
builder and the backtest create models only through :func:`create_model`,
so they never see a family's internals.

The families, in the plan's order of effort:

- ``seasonal_naive`` — the hour-of-week mean; the baseline every other
  family must beat.
- ``sarimax`` — SARIMAX on the daily city total, split by the hour-of-week
  means; the classical baseline.
- ``lightgbm`` — one gradient-boosting model over all stations; the
  expected main model.
- ``graphsage`` — GraphSage on the station graph; the research model.

Imports are inside :func:`create_model` on purpose: importing this package
stays cheap, and a family's library (torch, lightgbm) loads only when that
family is used.
"""

from __future__ import annotations

import pathlib

from gbp.ml.models.base import DemandModel

MODEL_FAMILIES = ("seasonal_naive", "sarimax", "lightgbm", "graphsage")


def create_model(
    name: str,
    *,
    train_months: list[str] | None = None,
    raw: pathlib.Path | None = None,
    **params: object,
) -> DemandModel:
    """Build one unfitted model of the given family.

    ``train_months`` and ``raw`` matter only for ``graphsage``, whose graph
    edges are counted from the raw trip files of the last training month —
    the other families ignore them. ``params`` go to the family's
    constructor unchanged.
    """
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
        from gbp.ml.models.graph import GraphSageModel, station_graph_edges

        if "edges_df" not in params:
            if not train_months:
                raise ValueError(
                    "graphsage needs train_months (its graph edges are counted "
                    "from the last training month) or an explicit edges_df"
                )
            params["edges_df"] = station_graph_edges([max(train_months)], raw)
        return GraphSageModel(**params)  # type: ignore[arg-type]
    raise ValueError(f"unknown model family {name!r}; known: {', '.join(MODEL_FAMILIES)}")


__all__ = ["MODEL_FAMILIES", "DemandModel", "create_model"]
