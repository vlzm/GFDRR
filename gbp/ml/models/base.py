"""The one model interface every model family implements."""

from __future__ import annotations

import abc
import pathlib
from typing import ClassVar, Self

import numpy as np
import pandas as pd

#: The key columns of the fractional demand a model predicts.
DEMAND_KEYS = ["period_id", "facility_id", "commodity_category"]


class DemandModel(abc.ABC):
    """One model family behind the ``fit`` / ``predict`` interface."""

    #: The family name — the key in ``MODEL_FAMILIES`` and the
    #: ``model_name`` a forecast artifact records.
    name: ClassVar[str]

    @abc.abstractmethod
    def fit(self, training_table: pd.DataFrame) -> None:
        """Learn from the training table (``TRAINING_TABLE_SCHEMA``)."""

    @abc.abstractmethod
    def predict(self, feature_table: pd.DataFrame) -> pd.DataFrame:
        """Forecast the rows of a forecast input; return fractional demand."""

    @abc.abstractmethod
    def save(self, folder: pathlib.Path) -> None:
        """Write the fitted model into ``folder`` (created if missing)."""

    @classmethod
    @abc.abstractmethod
    def load(cls, folder: pathlib.Path) -> Self:
        """Read a model saved by ``save``."""

    def params(self) -> dict[str, object]:
        """Return the settings that define this model instance, for experiment logs."""
        return {}


def fractional_demand(
    feature_table: pd.DataFrame, quantity: pd.Series | np.ndarray
) -> pd.DataFrame:
    """Wrap predicted quantities into the fractional demand shape."""
    out = feature_table[DEMAND_KEYS].copy()
    for column in ["facility_id", "commodity_category"]:
        if isinstance(out[column].dtype, pd.CategoricalDtype):
            out[column] = out[column].astype(object)
    out["quantity"] = np.clip(np.asarray(quantity, dtype="float64"), 0.0, None)
    return out.sort_values(DEMAND_KEYS).reset_index(drop=True)
