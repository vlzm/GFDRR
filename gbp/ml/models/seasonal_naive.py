"""Seasonal naive (Notations.md §17) behind the model interface.

The forecast for a station at a given hour is the mean demand at the same
hour of week over the history window. That number is already a feature
column: ``facility_hour_of_week_mean``, built by the one feature module
(``gbp/ml/features.py``) from the departure counts of the ``HISTORY_WEEKS``
weeks before the horizon, zero weeks counted in. So the model reads that
column instead of computing its own copy — the definition cannot drift from
the feature the other models see.

``fit`` therefore stores nothing. A pair ``(facility, commodity)`` absent
from the history window has a NaN mean; the model predicts 0 for it — no
history means no forecast demand, exactly as in phase 1.

This is the baseline every other model family must beat (plan, phase 4).
"""

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
