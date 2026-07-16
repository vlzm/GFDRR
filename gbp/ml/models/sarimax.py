"""SARIMAX on the city-level total series — the classical baseline."""

from __future__ import annotations

import json
import pathlib
from typing import Any, ClassVar, Self

import pandas as pd
from statsmodels.iolib.smpickle import load_pickle
from statsmodels.tsa.statespace.sarimax import SARIMAX

from gbp.ml.models.base import DemandModel, fractional_demand


class SarimaxTotalModel(DemandModel):
    """SARIMAX daily city total, split by the hour-of-week means."""

    name: ClassVar[str] = "sarimax"

    def __init__(
        self,
        order: tuple[int, int, int] = (1, 0, 1),
        seasonal_order: tuple[int, int, int, int] = (1, 1, 1, 7),
    ) -> None:
        """Remember the SARIMAX orders; nothing is fitted yet."""
        self.order = tuple(order)
        self.seasonal_order = tuple(seasonal_order)
        self._result: Any = None
        self._train_end: pd.Timestamp | None = None

    def params(self) -> dict[str, object]:
        """Return the SARIMAX orders, for experiment logs."""
        return {"order": str(self.order), "seasonal_order": str(self.seasonal_order)}

    def fit(self, training_table: pd.DataFrame) -> None:
        """Fit SARIMAX on the daily city totals of the training table."""
        days = training_table["start_timestamp"].dt.normalize()
        daily = training_table.groupby(days, observed=True)["quantity"].sum().sort_index()
        daily = daily.asfreq("D", fill_value=0).astype("float64")
        model = SARIMAX(
            daily,
            order=self.order,
            seasonal_order=self.seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        self._result = model.fit(disp=False)
        self._train_end = daily.index.max()

    def predict(self, feature_table: pd.DataFrame) -> pd.DataFrame:
        """Forecast daily totals, then split each day by the hour-of-week means."""
        if self._result is None or self._train_end is None:
            raise ValueError("fit the model before predicting")
        days = feature_table["start_timestamp"].dt.normalize()
        if days.min() <= self._train_end:
            raise ValueError(
                f"the horizon starts at {days.min().date()} but the training days reach "
                f"{self._train_end.date()}; the model only forecasts forward"
            )
        steps = int((days.max() - self._train_end).days)
        day_total = self._result.get_forecast(steps).predicted_mean.clip(lower=0.0)

        weight = feature_table["facility_hour_of_week_mean"].fillna(0.0)
        day_weight = weight.groupby(days).transform("sum")
        share = (weight / day_weight).where(day_weight > 0, 0.0)
        return fractional_demand(feature_table, days.map(day_total) * share)

    def save(self, folder: pathlib.Path) -> None:
        """Write the fitted SARIMAX results and the orders into ``folder``."""
        if self._result is None or self._train_end is None:
            raise ValueError("fit the model before saving")
        folder.mkdir(parents=True, exist_ok=True)
        self._result.save(str(folder / "sarimax_results.pkl"))
        (folder / "model.json").write_text(
            json.dumps(
                {
                    "name": self.name,
                    "order": list(self.order),
                    "seasonal_order": list(self.seasonal_order),
                    "train_end": self._train_end.isoformat(),
                }
            )
        )

    @classmethod
    def load(cls, folder: pathlib.Path) -> Self:
        """Read a model saved by ``save``."""
        saved = json.loads((folder / "model.json").read_text())
        model = cls(order=tuple(saved["order"]), seasonal_order=tuple(saved["seasonal_order"]))
        model._result = load_pickle(str(folder / "sarimax_results.pkl"))
        model._train_end = pd.Timestamp(saved["train_end"])
        return model
