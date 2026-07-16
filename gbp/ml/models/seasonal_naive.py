"""Seasonal naive behind the model interface."""

from __future__ import annotations

import json
import pathlib
from typing import ClassVar, Self

import pandas as pd

from gbp.ml.models.base import DemandModel, fractional_demand


class SeasonalNaiveModel(DemandModel):
    """The baseline: predict the hour-of-week mean of the history window."""

    name: ClassVar[str] = "seasonal_naive"

    def fit(self, training_table: pd.DataFrame) -> None:
        """Store nothing — the prediction is a feature column, not a fit."""

    def predict(self, feature_table: pd.DataFrame) -> pd.DataFrame:
        """Return ``facility_hour_of_week_mean`` as the quantity, NaN as 0."""
        return fractional_demand(
            feature_table, feature_table["facility_hour_of_week_mean"].fillna(0.0)
        )

    def save(self, folder: pathlib.Path) -> None:
        """Write a small marker file — the model has no learned state."""
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "model.json").write_text(json.dumps({"name": self.name}))

    @classmethod
    def load(cls, folder: pathlib.Path) -> Self:
        """Rebuild the (stateless) model from a saved folder."""
        saved = json.loads((folder / "model.json").read_text())
        if saved["name"] != cls.name:
            raise ValueError(f"folder holds a {saved['name']!r} model, not {cls.name!r}")
        return cls()
