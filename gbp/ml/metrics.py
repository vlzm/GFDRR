"""Forecast-vs-actual metrics: MAE and Poisson deviance, overall and split by station traffic."""

from __future__ import annotations

import numpy as np
import pandas as pd

#: The floor under a prediction inside the Poisson deviance logarithm.
POISSON_EPS = 1e-6

#: The join keys of the row-level comparison.
DEMAND_KEYS = ["period_id", "facility_id", "commodity_category"]


def align_forecast(actual_df: pd.DataFrame, predicted_df: pd.DataFrame) -> pd.DataFrame:
    """Join actual and predicted quantities on the demand keys, missing → 0."""
    actual = actual_df[DEMAND_KEYS + ["quantity"]].rename(columns={"quantity": "actual"})
    predicted = predicted_df[DEMAND_KEYS + ["quantity"]].rename(columns={"quantity": "predicted"})
    out = actual.merge(predicted, on=DEMAND_KEYS, how="outer")
    out["actual"] = out["actual"].fillna(0.0).astype("float64")
    out["predicted"] = out["predicted"].fillna(0.0).astype("float64")
    return out


def mean_absolute_error(actual: np.ndarray, predicted: np.ndarray) -> float:
    """Mean of ``|predicted - actual|``."""
    return float(np.mean(np.abs(predicted - actual)))


def mean_poisson_deviance(actual: np.ndarray, predicted: np.ndarray) -> float:
    """Mean Poisson deviance, predictions floored at ``POISSON_EPS``."""
    mu = np.maximum(predicted, POISSON_EPS)
    y = actual
    with np.errstate(divide="ignore", invalid="ignore"):
        log_term = np.where(y > 0, y * np.log(np.maximum(y, POISSON_EPS) / mu), 0.0)
    return float(np.mean(2.0 * (log_term - (y - mu))))


def busy_facility_ids(actual_df: pd.DataFrame) -> set[str]:
    """Return the busiest stations that together made half of all departures."""
    totals = (
        actual_df.groupby("facility_id", observed=True)["quantity"]
        .sum()
        .sort_values(ascending=False)
    )
    covered_before = totals.cumsum() - totals
    return set(totals.index[covered_before < totals.sum() / 2.0])


def forecast_metrics(actual_df: pd.DataFrame, predicted_df: pd.DataFrame) -> dict[str, float]:
    """Score one forecast against one actual month (MAE and Poisson deviance, overall and split)."""
    aligned = align_forecast(actual_df, predicted_df)
    busy = aligned["facility_id"].isin(busy_facility_ids(actual_df))
    out = {
        "mae": mean_absolute_error(aligned["actual"].values, aligned["predicted"].values),
        "poisson_deviance": mean_poisson_deviance(
            aligned["actual"].values, aligned["predicted"].values
        ),
        "actual_total": float(aligned["actual"].sum()),
        "predicted_total": float(aligned["predicted"].sum()),
    }
    for label, mask in [("busy", busy), ("quiet", ~busy)]:
        part = aligned[mask]
        out[f"mae_{label}"] = mean_absolute_error(part["actual"].values, part["predicted"].values)
        out[f"poisson_deviance_{label}"] = mean_poisson_deviance(
            part["actual"].values, part["predicted"].values
        )
    return out
