"""LightGBM trained on all stations at once — the expected main model."""

from __future__ import annotations

import json
import pathlib
from typing import ClassVar, Self

import lightgbm as lgb
import pandas as pd

from gbp.ml.features import FEATURE_COLUMNS
from gbp.ml.models.base import DemandModel, fractional_demand

#: Fixed training settings; instance parameters override them.
_DEFAULT_PARAMS: dict[str, object] = {
    "objective": "poisson",
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_data_in_leaf": 50,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "verbosity": -1,
}

#: The station and the bike type enter the model as categorical features.
_CATEGORICAL = ["facility_id", "commodity_category"]


class LightGbmModel(DemandModel):
    """One global LightGBM over all stations, Poisson objective."""

    name: ClassVar[str] = "lightgbm"

    def __init__(self, num_boost_round: int = 300, **lgb_params: object) -> None:
        """Store the boosting settings; ``lgb_params`` override the defaults."""
        self.num_boost_round = num_boost_round
        self.lgb_params: dict[str, object] = {**_DEFAULT_PARAMS, **lgb_params}
        self._booster: lgb.Booster | None = None
        self._categories: dict[str, list[str]] = {}

    def params(self) -> dict[str, object]:
        """Return the boosting settings, for experiment logs."""
        return {"num_boost_round": self.num_boost_round, **self.lgb_params}

    def _matrix(self, table: pd.DataFrame) -> pd.DataFrame:
        """Build the input matrix: float32 features + fixed-code categoricals."""
        matrix = pd.DataFrame(index=table.index)
        for column in FEATURE_COLUMNS:
            matrix[column] = table[column].astype("float32")
        for column in _CATEGORICAL:
            matrix[column] = pd.Categorical(table[column], categories=self._categories[column])
        return matrix

    def fit(self, training_table: pd.DataFrame) -> None:
        """Train on every row; weight rows down by their stockout share."""
        self._categories = {
            column: sorted(str(value) for value in training_table[column].unique())
            for column in _CATEGORICAL
        }
        weight = None
        if "stockout_share" in training_table.columns:
            weight = 1.0 - training_table["stockout_share"].fillna(0.0).astype("float64")
        dataset = lgb.Dataset(
            self._matrix(training_table),
            label=training_table["quantity"].astype("float32"),
            weight=weight,
            categorical_feature=_CATEGORICAL,
            free_raw_data=True,
        )
        self._booster = lgb.train(self.lgb_params, dataset, num_boost_round=self.num_boost_round)

    def predict(self, feature_table: pd.DataFrame) -> pd.DataFrame:
        """Predict the fractional demand for the rows of a forecast input."""
        if self._booster is None:
            raise ValueError("fit the model before predicting")
        predicted = self._booster.predict(self._matrix(feature_table))
        return fractional_demand(feature_table, predicted)

    def save(self, folder: pathlib.Path) -> None:
        """Write the booster and the encoding into ``folder``."""
        if self._booster is None:
            raise ValueError("fit the model before saving")
        folder.mkdir(parents=True, exist_ok=True)
        self._booster.save_model(str(folder / "booster.txt"))
        (folder / "model.json").write_text(
            json.dumps(
                {
                    "name": self.name,
                    "num_boost_round": self.num_boost_round,
                    "lgb_params": self.lgb_params,
                    "categories": self._categories,
                }
            )
        )

    @classmethod
    def load(cls, folder: pathlib.Path) -> Self:
        """Read a model saved by ``save``."""
        saved = json.loads((folder / "model.json").read_text())
        model = cls(num_boost_round=saved["num_boost_round"], **saved["lgb_params"])
        model._booster = lgb.Booster(model_file=str(folder / "booster.txt"))
        model._categories = saved["categories"]
        return model
