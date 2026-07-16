"""One feature builder for the training table and the forecast input."""

from __future__ import annotations

import holidays
import pandas as pd
import pandera.pandas as pa

from gbp.loaders.dataloader_graph import hour_of_week

#: The history the history features read: the departure counts of the
#: HISTORY_WEEKS weeks right before the target rows. Both the training-table
#: builder and the forecast input builder use exactly this window.
HISTORY_WEEKS = 8

#: How many weekly lags the rolling mean averages over. The column name
#: ``quantity_mean_4w`` spells this number — rename it if the number changes.
LAG_WEEKS = 4

_WEEK = pd.Timedelta(weeks=1)

CALENDAR_FEATURES = ["hour_of_day", "day_of_week", "month", "is_holiday"]
WEATHER_FEATURES = ["temperature_max_c", "temperature_min_c", "precipitation_mm"]
HISTORY_FEATURES = [
    "quantity_lag_1w",
    "quantity_mean_4w",
    "facility_mean",
    "facility_hour_of_week_mean",
]

#: Every feature column with its check; the training-table schema merges
#: this into its own columns. History and weather values are nullable — a
#: missing value means "not observed", never "zero".
FEATURE_SCHEMA_COLUMNS: dict[str, pa.Column] = {
    "hour_of_day": pa.Column("int64", pa.Check.in_range(0, 23), nullable=False),
    "day_of_week": pa.Column("int64", pa.Check.in_range(0, 6), nullable=False),
    "month": pa.Column("int64", pa.Check.in_range(1, 12), nullable=False),
    "is_holiday": pa.Column("bool", nullable=False),
    "temperature_max_c": pa.Column("float64", nullable=True),
    "temperature_min_c": pa.Column("float64", nullable=True),
    "precipitation_mm": pa.Column("float64", pa.Check.ge(0), nullable=True),
    "quantity_lag_1w": pa.Column("float64", pa.Check.ge(0), nullable=True),
    "quantity_mean_4w": pa.Column("float64", pa.Check.ge(0), nullable=True),
    "facility_mean": pa.Column("float64", pa.Check.ge(0), nullable=True),
    "facility_hour_of_week_mean": pa.Column("float64", pa.Check.ge(0), nullable=True),
}

FEATURE_COLUMNS: list[str] = list(FEATURE_SCHEMA_COLUMNS)


def clip_history_window(history_df: pd.DataFrame, t0: pd.Timestamp) -> pd.DataFrame:
    """Keep the counts inside the history window: the ``HISTORY_WEEKS`` weeks before ``t0``."""
    inside = (history_df["start_timestamp"] >= t0 - HISTORY_WEEKS * _WEEK) & (
        history_df["start_timestamp"] < t0
    )
    return history_df[inside]


def _us_holiday_days(days: pd.Series) -> set[pd.Timestamp]:
    """Return the US public holidays inside the years that ``days`` spans."""
    if days.empty:
        return set()
    years = range(int(days.dt.year.min()), int(days.dt.year.max()) + 1)
    return {pd.Timestamp(day) for day in holidays.country_holidays("US", years=years)}


def add_calendar_features(table: pd.DataFrame) -> pd.DataFrame:
    """Add the calendar columns, computed from ``start_timestamp`` alone."""
    out = table.copy()
    ts = out["start_timestamp"]
    out["hour_of_day"] = ts.dt.hour.astype("int64")
    out["day_of_week"] = ts.dt.dayofweek.astype("int64")
    out["month"] = ts.dt.month.astype("int64")
    days = ts.dt.normalize()
    out["is_holiday"] = days.isin(_us_holiday_days(days)).astype(bool)
    return out


def add_weather_features(table: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    """Join the daily weather onto the rows by calendar date."""
    missing = [c for c in ["date", *WEATHER_FEATURES] if c not in weather_df.columns]
    if missing:
        raise ValueError(f"weather table lacks columns: {missing}")
    days = weather_df[["date", *WEATHER_FEATURES]].copy()
    days["date"] = pd.to_datetime(days["date"]).dt.normalize()
    if days["date"].duplicated().any():
        raise ValueError("weather table must hold one row per date")

    out = table.copy()
    out["_date"] = out["start_timestamp"].dt.normalize()
    out = out.merge(days, left_on="_date", right_on="date", how="left")
    out = out.drop(columns=["_date", "date"])
    for column in WEATHER_FEATURES:
        out[column] = out[column].astype("float64")
    return out


def add_history_features(table: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
    """Add the history columns, computed from ``history_df`` only."""
    keys = ["facility_id", "commodity_category"]
    hist = history_df[[*keys, "start_timestamp", "quantity"]]
    if hist.duplicated([*keys, "start_timestamp"]).any():
        raise ValueError("history holds more than one count per station-hour")
    out = table.copy()

    # The count at the same hour k weeks earlier, k = 1..LAG_WEEKS: shifting
    # the history forward by k weeks and joining on the timestamp is a
    # lookup at (target time - k weeks).
    lag_columns = []
    for weeks_back in range(1, LAG_WEEKS + 1):
        column = "quantity_lag_1w" if weeks_back == 1 else f"_quantity_lag_{weeks_back}w"
        lag_columns.append(column)
        lag = hist.assign(start_timestamp=hist["start_timestamp"] + weeks_back * _WEEK)
        lag = lag.rename(columns={"quantity": column})
        out = out.merge(lag, on=[*keys, "start_timestamp"], how="left")
    out["quantity_mean_4w"] = out[lag_columns].mean(axis=1)
    out = out.drop(columns=lag_columns[1:])

    facility_mean = (
        hist.groupby(keys, as_index=False)["quantity"]
        .mean()
        .rename(columns={"quantity": "facility_mean"})
    )
    out = out.merge(facility_mean, on=keys, how="left")

    by_hour = hist.assign(_hour_of_week=hour_of_week(hist["start_timestamp"]))
    hour_mean = (
        by_hour.groupby([*keys, "_hour_of_week"], as_index=False)["quantity"]
        .mean()
        .rename(columns={"quantity": "facility_hour_of_week_mean"})
    )
    out["_hour_of_week"] = hour_of_week(out["start_timestamp"])
    out = out.merge(hour_mean, on=[*keys, "_hour_of_week"], how="left")
    out = out.drop(columns="_hour_of_week")

    for column in HISTORY_FEATURES:
        out[column] = out[column].astype("float64")
    return out


def _require_history_before_targets(targets_df: pd.DataFrame, history_df: pd.DataFrame) -> None:
    """Reject history that overlaps the targets — rows would leak into their own features."""
    if history_df.empty or targets_df.empty:
        return
    history_end = history_df["start_timestamp"].max()
    targets_start = targets_df["start_timestamp"].min()
    if history_end >= targets_start:
        raise ValueError(
            f"history reaches {history_end} but the target rows start at {targets_start}; "
            "features must be built from counts strictly before the rows they describe"
        )


def build_features(
    targets_df: pd.DataFrame,
    history_df: pd.DataFrame,
    weather_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Append every feature column to the target rows."""
    _require_history_before_targets(targets_df, history_df)
    history = clip_history_window(history_df, targets_df["start_timestamp"].min())
    out = add_calendar_features(targets_df)
    if weather_df is None:
        weather_df = pd.DataFrame(
            {
                "date": pd.Series(dtype="datetime64[ns]"),
                **{column: pd.Series(dtype="float64") for column in WEATHER_FEATURES},
            }
        )
    out = add_weather_features(out, weather_df)
    return add_history_features(out, history)
