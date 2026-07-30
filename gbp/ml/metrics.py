"""Forecast-vs-actual metrics: MAE and Poisson deviance, overall and split by station traffic."""

from __future__ import annotations

import numpy as np
import pandas as pd

from gbp.ml.model import DEMAND_KEYS

#: The floor under a prediction inside the Poisson deviance logarithm.
POISSON_EPS = 1e-6


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


def panel_departed_mae(panel_df: pd.DataFrame, reference_panel_df: pd.DataFrame) -> float:
    """Mean |departed difference| per station-hour between two run panels."""
    keys = ["facility_id", "period_id"]
    joined = reference_panel_df[keys + ["departed"]].merge(
        panel_df[keys + ["departed"]], on=keys, how="outer", suffixes=("_reference", "")
    )
    joined = joined.fillna({"departed_reference": 0, "departed": 0})
    return float((joined["departed"] - joined["departed_reference"]).abs().mean())


def lost_demand_busy_share(panel_df: pd.DataFrame, busy_ids: set[str]) -> float:
    """Share of a run's lost demand that fell on the busy stations (0.0 when nothing was lost)."""
    lost = panel_df.groupby("facility_id", observed=True)["lost_demand"].sum()
    total = float(lost.sum())
    if not total:
        return 0.0
    return float(lost[lost.index.isin(busy_ids)].sum() / total)


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
