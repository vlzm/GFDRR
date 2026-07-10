"""Forecast-vs-actual metrics (plan, phase 4).

The backtest scores a model by comparing its fractional demand against the
actual departure counts of the held-out month, row by row — one row per
``(period_id, facility_id, commodity_category)``. Two error measures:

- MAE — the mean absolute error, in bikes per station-hour;
- Poisson deviance — the count-data error: it punishes underprediction of
  busy hours harder than MAE and is the natural score for the Poisson
  objective the LightGBM model trains with. Predictions are floored at
  ``POISSON_EPS`` before the logarithm, so predicting exactly zero for an
  hour that had departures gives a large finite penalty instead of an
  infinite one.

Both are reported overall and split by station traffic: the busy stations
(the smallest set that together produced half of the month's departures)
against the quiet rest. Thousands of near-empty stations would otherwise
hide bad predictions on the stations that matter — a model cannot win on
the split metrics by being right about zeros.

Alignment: the two tables are joined on the demand keys with an outer join
and missing sides filled with 0 — a station the model never predicted
counts with prediction 0, a predicted station with no actual trips counts
with actual 0.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

#: The floor under a prediction inside the Poisson deviance logarithm.
POISSON_EPS = 1e-6

#: The join keys of the row-level comparison.
DEMAND_KEYS = ["period_id", "facility_id", "commodity_category"]


def align_forecast(actual_df: pd.DataFrame, predicted_df: pd.DataFrame) -> pd.DataFrame:
    """Join actual and predicted quantities on the demand keys, missing → 0.

    Returns one row per key present in either table, with the columns
    ``actual`` and ``predicted``.
    """
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
    """Mean Poisson deviance, predictions floored at ``POISSON_EPS``.

    Per row the deviance is ``2 * (y * log(y / mu) - (y - mu))`` with the
    ``y * log(y / mu)`` term read as 0 when ``y`` is 0.
    """
    mu = np.maximum(predicted, POISSON_EPS)
    y = actual
    with np.errstate(divide="ignore", invalid="ignore"):
        log_term = np.where(y > 0, y * np.log(np.maximum(y, POISSON_EPS) / mu), 0.0)
    return float(np.mean(2.0 * (log_term - (y - mu))))


def busy_facility_ids(actual_df: pd.DataFrame) -> set[str]:
    """Return the busiest stations that together made half of all departures.

    Stations are ranked by their total actual departures; the set grows from
    the top until it covers half of the total, including the station that
    crosses the half mark.
    """
    totals = (
        actual_df.groupby("facility_id", observed=True)["quantity"]
        .sum()
        .sort_values(ascending=False)
    )
    covered_before = totals.cumsum() - totals
    return set(totals.index[covered_before < totals.sum() / 2.0])


def forecast_metrics(actual_df: pd.DataFrame, predicted_df: pd.DataFrame) -> dict[str, float]:
    """Score one forecast against one actual month.

    Returns MAE and Poisson deviance — overall and split into busy and
    quiet stations — plus the two totals, so a scale error is visible at a
    glance.
    """
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
