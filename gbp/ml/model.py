"""The one model interface every model family implements, and the family registry."""

from __future__ import annotations

import abc
import importlib
import pathlib
from typing import ClassVar, Self

import numpy as np
import pandas as pd

#: The key columns of the fractional demand a model predicts.
DEMAND_KEYS = ["period_id", "facility_id", "commodity_category"]


class DemandModel(abc.ABC):
    """One model family behind the ``fit`` / ``predict`` interface."""

    #: The family name — the key it is registered under and the
    #: ``model_name`` a forecast artifact records.
    name: ClassVar[str]

    @abc.abstractmethod
    def fit(self, training_table: pd.DataFrame) -> None:
        """Learn from the training table the domain builds."""

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


# ---------------------------------------------------------------------------
# The family registry
# ---------------------------------------------------------------------------
#: Family name -> the class that implements it, written as
#: ``"module.path:ClassName"``. A domain fills this at import time by calling
#: :func:`register_model`; the module is imported only when the family is
#: first asked for, so registering a heavy family costs nothing.
_FAMILIES: dict[str, str] = {}


def register_model(name: str, target: str) -> None:
    """Register a model family; ``target`` names its class as ``"module.path:ClassName"``."""
    _FAMILIES[name] = target


def model_families() -> tuple[str, ...]:
    """Return the registered family names, in the order they were registered."""
    return tuple(_FAMILIES)


def model_class(name: str) -> type[DemandModel]:
    """Return a registered family's class, importing its module on first use."""
    if name not in _FAMILIES:
        known = ", ".join(_FAMILIES) or "none — the domain package was never imported"
        raise ValueError(f"unknown model family {name!r}; registered: {known}")
    module_path, _, class_name = _FAMILIES[name].partition(":")
    found = getattr(importlib.import_module(module_path), class_name)
    if not (isinstance(found, type) and issubclass(found, DemandModel)):
        raise TypeError(f"{_FAMILIES[name]} is registered as {name!r} but is not a DemandModel")
    return found


def create_model(name: str, **params: object) -> DemandModel:
    """Build one unfitted model of the given family."""
    return model_class(name)(**params)


def load_model(name: str, folder: pathlib.Path) -> DemandModel:
    """Read a fitted model of the given family from a folder ``save`` wrote."""
    return model_class(name).load(folder)
