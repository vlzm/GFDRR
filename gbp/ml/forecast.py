"""Build and save forecast demand tables (Notations.md §17).

A model's whole job is to produce a forecast demand table: a table in
``HISTORICAL_DEMAND_SCHEMA`` shape whose ``quantity`` comes from a model, for
periods that have no history yet. The models themselves live behind the one
interface in ``gbp/ml/models/``; this module is the forecast builder around
them. It holds the one rounding rule that turns fractional forecasts into
whole bikes, the forecast input (:func:`forecast_input` — the feature table a
model predicts from, built by the shared feature module), and the forecast
artifact — the folder ``data/ml/forecasts/<forecast_name>/`` with
``demand.parquet`` and ``meta.json`` that a forecast run (Notations.md §11)
is loaded from.

Two builders save an artifact:

- :func:`build_seasonal_naive_forecast` — the phase-1 path: from one trip
  CSV's demand and period grid, seasonal naive only.
- :func:`build_model_forecast` — from the training partitions, any model
  family by name.

Terminal use (one command each; wrapped here for width)::

    python -m gbp.ml.forecast --trips-path data/raw/202601-citibike-tripdata_1.csv
        --forecast-name seasonal_naive_w1 --horizon-periods 168

    python -m gbp.ml.forecast --model lightgbm --months 202502 ... 202512
        --forecast-name lightgbm_202601 --horizon-periods 744
"""

from __future__ import annotations

import argparse
import datetime
import pathlib
from collections.abc import Callable

import numpy as np
import pandas as pd
import pydantic

from gbp.loaders.dataloader_graph import (
    DEFAULT_PERIOD_LEN,
    HISTORICAL_DEMAND_SCHEMA,
    get_forecast_periods_df,
)
from gbp.ml.data import load_weather_daily, ml_dir, month_bounds, normalize_month
from gbp.ml.features import HISTORY_WEEKS, build_features, clip_history_window
from gbp.ml.models import create_model
from gbp.model.journal_schema import schema_violations


def forecasts_root() -> pathlib.Path:
    """Folder that holds all forecast artifacts: ``<ml dir>/forecasts``."""
    return ml_dir() / "forecasts"


def forecast_dir(forecast_name: str, root: pathlib.Path | None = None) -> pathlib.Path:
    """Folder of one forecast artifact."""
    return (root or forecasts_root()) / forecast_name


def list_forecasts(root: pathlib.Path | None = None) -> list[str]:
    """Names of every saved forecast (folders with a ``meta.json``), sorted."""
    base = root or forecasts_root()
    if not base.exists():
        return []
    return sorted(p.name for p in base.iterdir() if (p / "meta.json").exists())


class ForecastMeta(pydantic.BaseModel):
    """The ``meta.json`` contract of a forecast artifact.

    Written next to ``demand.parquet`` so a forecast names what produced it
    (model name and version), what it was built from (``inputs``, the history
    window), and the horizon it covers (``t0``, ``horizon_periods``,
    ``period_len_hours`` — enough to rebuild the forecast period grid with
    :func:`forecast_periods_from_meta`).
    """

    forecast_name: str
    model_name: str
    model_version: str
    created_at: str
    #: Wall-clock start of forecast period 0 (right after the history ends).
    t0: str
    horizon_periods: int
    period_len_hours: float
    #: The history window the model was built from.
    history_start: str
    history_end: str
    #: File names of the raw source files the history came from.
    inputs: list[str]


def forecast_periods_from_meta(meta: ForecastMeta) -> pd.DataFrame:
    """Rebuild the forecast period grid a saved forecast was built for."""
    return get_forecast_periods_df(
        pd.Timestamp(meta.t0),
        meta.horizon_periods,
        pd.Timedelta(hours=meta.period_len_hours),
    )


def counts_from_demand(demand_df: pd.DataFrame, periods_df: pd.DataFrame) -> pd.DataFrame:
    """Turn the historical demand marginal into a zero-filled counts grid.

    The demand marginal lists only positive rows, but the feature builder
    reads departure counts in training-table shape, where a station-hour
    with no departures is a real zero observation. So the grid crosses every
    period of ``periods_df`` with every ``(facility, commodity)`` seen in
    the demand, takes each row's quantity from the demand, and fills the
    rest with 0.

    Raises ``ValueError`` when the demand names a period the grid does not
    have — such a row would silently vanish otherwise.

    Returns
    -------
    pandas.DataFrame
        ``start_timestamp``, ``facility_id``, ``commodity_category``,
        ``quantity`` — one row per period × facility × commodity.
    """
    known = demand_df["period_id"].isin(set(periods_df["period_id"]))
    if not known.all():
        missing = demand_df.loc[~known, "period_id"].unique()[:5].tolist()
        raise ValueError(f"demand has periods outside the period grid: {missing}")
    grid = pd.MultiIndex.from_product(
        [
            periods_df["period_id"],
            sorted(demand_df["facility_id"].unique()),
            sorted(demand_df["commodity_category"].unique()),
        ],
        names=["period_id", "facility_id", "commodity_category"],
    ).to_frame(index=False)
    out = grid.merge(demand_df, on=["period_id", "facility_id", "commodity_category"], how="left")
    out["quantity"] = out["quantity"].fillna(0).astype("int64")
    out = out.merge(periods_df[["period_id", "start_timestamp"]], on="period_id")
    return out[["start_timestamp", "facility_id", "commodity_category", "quantity"]]


def round_forecast_demand(demand_df: pd.DataFrame) -> pd.DataFrame:
    """Round fractional forecast quantities to whole bikes — the one rounding rule.

    The engine moves whole bikes, so a forecast demand table must hold whole
    numbers. Rounding each row on its own drifts the totals: a thousand
    stations forecast at 0.4 would round to zero demand. The rule, decided
    once here (plan, phase 1): within each ``(period_id, commodity_category)``
    group, round the group total to the nearest whole number, give every row
    the whole part of its own value, and hand the remaining bikes one each to
    the rows with the largest fractional parts (ties broken by
    ``facility_id``, so the result is deterministic). This is the
    largest-remainder method — the same rule ``form_potential_trips`` uses to
    split a source's departures over targets.

    Two properties follow. The group total is exact: the rounded table demands
    as many bikes per period and commodity as the fractional one, to the
    nearest bike. And no demand is invented: a row with fractional part zero
    is never rounded up.

    Rows that end at zero are dropped — like the historical demand marginal,
    the table lists only positive demand.

    Parameters
    ----------
    demand_df : pandas.DataFrame
        Fractional demand: ``period_id``, ``facility_id``,
        ``commodity_category``, ``quantity``.

    Returns
    -------
    pandas.DataFrame
        The same table with whole-bike ``quantity`` values, positive rows only.
    """
    m = demand_df.copy()
    m["base"] = np.floor(m["quantity"]).astype("int64")
    m["remainder"] = m["quantity"] - m["base"]
    m = m.sort_values(
        ["period_id", "commodity_category", "remainder", "facility_id"],
        ascending=[True, True, False, True],
        kind="stable",
    )
    grp = m.groupby(["period_id", "commodity_category"])
    leftover = grp["quantity"].transform("sum").round() - grp["base"].transform("sum")
    m["quantity"] = m["base"] + (grp.cumcount() < leftover).astype("int64")
    m = m[m["quantity"] > 0]
    return (
        m[["period_id", "facility_id", "commodity_category", "quantity"]]
        .sort_values(["period_id", "facility_id", "commodity_category"])
        .reset_index(drop=True)
    )


def forecast_input(
    history_df: pd.DataFrame,
    forecast_periods_df: pd.DataFrame,
    weather_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build the feature table a model predicts from (plan, phase 3).

    One row per ``(period, facility, commodity)`` of the forecast horizon:
    every facility × commodity of the history window crossed with every
    horizon period, with the feature columns appended by the same functions
    that build the training table (``gbp/ml/features.py``) — never a copy.
    For the same station-day this table and the training table hold
    identical feature values; a test in ``tests/test_ml_features.py``
    proves it.

    Parameters
    ----------
    history_df : pandas.DataFrame
        Departure counts in training-table shape (zero rows kept), for
        example read from the training partitions. Only the history window
        (the ``HISTORY_WEEKS`` weeks right before the horizon) is used.
    forecast_periods_df : pandas.DataFrame
        The forecast period grid (:func:`get_forecast_periods_df`).
    weather_df : pandas.DataFrame, optional
        Daily weather covering the horizon dates. In a backtest this is the
        actual weather of the held-out month — a perfect weather forecast.
        A true future horizon has no published weather; without a supplied
        weather forecast the weather columns stay NaN.

    Returns
    -------
    pandas.DataFrame
        ``period_id``, ``start_timestamp``, ``facility_id``,
        ``commodity_category`` plus the ``FEATURE_COLUMNS``.
    """
    t0 = forecast_periods_df["start_timestamp"].iloc[0]
    history = clip_history_window(history_df, t0)
    if history.empty:
        raise ValueError(f"no departure counts in the {HISTORY_WEEKS}-week window before {t0}")
    grid = pd.MultiIndex.from_product(
        [
            forecast_periods_df["period_id"],
            sorted(history["facility_id"].unique()),
            sorted(history["commodity_category"].unique()),
        ],
        names=["period_id", "facility_id", "commodity_category"],
    ).to_frame(index=False)
    grid = grid.merge(forecast_periods_df[["period_id", "start_timestamp"]], on="period_id")
    grid = grid[["period_id", "start_timestamp", "facility_id", "commodity_category"]]
    return build_features(grid, history, weather_df)


def save_forecast(
    demand_df: pd.DataFrame, meta: ForecastMeta, root: pathlib.Path | None = None
) -> pathlib.Path:
    """Write one forecast artifact to ``<forecasts root>/<forecast_name>/``.

    The demand table is checked against ``HISTORICAL_DEMAND_SCHEMA`` before
    anything is written — the integration contract says the simulator reads a
    forecast exactly as it reads historical demand, so a wrong shape fails
    here, not inside a run. ``meta.json`` is written last, so a folder with a
    ``meta.json`` is always a complete artifact (``list_forecasts`` keys on
    that file).
    """
    violations = schema_violations(HISTORICAL_DEMAND_SCHEMA, demand_df)
    if violations:
        raise ValueError(
            "forecast demand table breaks the demand schema:\n" + "\n".join(violations)
        )
    folder = forecast_dir(meta.forecast_name, root)
    folder.mkdir(parents=True, exist_ok=True)
    demand_df.to_parquet(folder / "demand.parquet", index=False)
    (folder / "meta.json").write_text(meta.model_dump_json(indent=2))
    return folder


def load_forecast(
    forecast_name: str, root: pathlib.Path | None = None
) -> tuple[pd.DataFrame, ForecastMeta]:
    """Read a saved forecast: the demand table and its validated ``meta.json``."""
    folder = forecast_dir(forecast_name, root)
    if not (folder / "meta.json").exists():
        raise FileNotFoundError(
            f"unknown forecast {forecast_name!r}; saved forecasts: {list_forecasts(root)}"
        )
    meta = ForecastMeta.model_validate_json((folder / "meta.json").read_text())
    demand_df = pd.read_parquet(folder / "demand.parquet")
    return demand_df, meta


def build_seasonal_naive_forecast(
    demand_df: pd.DataFrame,
    periods_df: pd.DataFrame,
    *,
    forecast_name: str,
    horizon_periods: int,
    inputs: list[str],
    root: pathlib.Path | None = None,
) -> pathlib.Path:
    """Build a seasonal naive forecast for the periods right after history and save it.

    The phase-1 builder: history is a demand marginal plus its period grid
    (what one trip CSV gives), the model is the seasonal naive. The path is
    the interface one — the demand becomes a zero-filled counts grid
    (:func:`counts_from_demand`), the counts become a forecast input
    (:func:`forecast_input`), and the model predicts from it, so the mean
    reads the history window like every history feature. The forecast
    horizon starts where the history ends (the last period's
    ``end_timestamp``) and runs for ``horizon_periods`` periods of the same
    length, numbered from 0. The fractional values are rounded to whole
    bikes with :func:`round_forecast_demand`.

    Parameters
    ----------
    demand_df : pandas.DataFrame
        The historical demand marginal the model averages over.
    periods_df : pandas.DataFrame
        The historical period grid.
    forecast_name : str
        Folder name of the forecast artifact.
    horizon_periods : int
        How many periods the forecast covers.
    inputs : list of str
        File names of the raw source files the history came from.
    root : pathlib.Path, optional
        Forecasts root override (defaults to :func:`forecasts_root`).

    Returns
    -------
    pathlib.Path
        The saved forecast artifact folder.
    """
    t0 = periods_df["end_timestamp"].iloc[-1]
    period_len = periods_df["end_timestamp"].iloc[0] - periods_df["start_timestamp"].iloc[0]
    forecast_periods_df = get_forecast_periods_df(t0, horizon_periods, period_len)
    counts = counts_from_demand(demand_df, periods_df)
    model = create_model("seasonal_naive")
    model.fit(counts)
    fractional = model.predict(forecast_input(counts, forecast_periods_df))
    forecast_demand_df = round_forecast_demand(fractional)
    meta = ForecastMeta(
        forecast_name=forecast_name,
        model_name=model.name,
        model_version="1",
        created_at=datetime.datetime.now().isoformat(timespec="seconds"),
        t0=pd.Timestamp(t0).isoformat(),
        horizon_periods=horizon_periods,
        period_len_hours=period_len / pd.Timedelta(hours=1),
        history_start=pd.Timestamp(periods_df["start_timestamp"].iloc[0]).isoformat(),
        history_end=pd.Timestamp(periods_df["end_timestamp"].iloc[-1]).isoformat(),
        inputs=list(inputs),
    )
    return save_forecast(forecast_demand_df, meta, root)


def build_model_forecast(
    model_name: str,
    train_months: list[str],
    *,
    horizon_periods: int,
    forecast_name: str,
    root: pathlib.Path | None = None,
    training_root: pathlib.Path | None = None,
    raw: pathlib.Path | None = None,
    weather_df: pd.DataFrame | None = None,
    log: Callable[[str], None] = print,
    **model_params: object,
) -> pathlib.Path:
    """Fit one model family on the training partitions and save its forecast.

    The interface path: ``create_model`` builds the family by name, ``fit``
    reads the stacked partitions of ``train_months``, and the horizon starts
    right where the last training month ends. The weather for the horizon is
    whatever NOAA has published for those dates; unpublished dates leave the
    weather features NaN, and a fully unreachable weather file is skipped
    with a note — the models accept missing values.

    Parameters
    ----------
    model_name : str
        A model family name (``MODEL_FAMILIES`` in ``gbp/ml/models``).
    train_months : list of str
        The months whose partitions the model trains on, as ``YYYYMM``.
    horizon_periods : int
        How many one-hour periods the forecast covers.
    forecast_name : str
        Folder name of the forecast artifact.
    root, training_root, raw : pathlib.Path, optional
        Folder overrides for the forecast artifacts, the training
        partitions, and the raw files.
    weather_df : pandas.DataFrame, optional
        Daily weather for the horizon dates; without it the published
        weather is loaded (and missing dates stay NaN).
    log : callable, optional
        Where progress notes go (default: ``print``).
    **model_params
        Extra settings for the family's constructor.

    Returns
    -------
    pathlib.Path
        The saved forecast artifact folder.
    """
    # Imported here, not at the top: gbp.ml.training is the partition
    # builder, and only this builder needs it.
    from gbp.ml.training import load_history_counts, load_training_table

    months = sorted(normalize_month(m) for m in train_months)
    log(f"Training {model_name} on {months[0]}..{months[-1]} ...")
    train_table = load_training_table(months, training_root)
    model = create_model(model_name, train_months=months, raw=raw, **model_params)
    model.fit(train_table)

    t0 = month_bounds(months[-1])[1]
    forecast_periods_df = get_forecast_periods_df(t0, horizon_periods, DEFAULT_PERIOD_LEN)
    horizon_end = forecast_periods_df["end_timestamp"].iloc[-1]
    if weather_df is None:
        try:
            weather_df = load_weather_daily(t0, horizon_end - pd.Timedelta(days=1), raw)
        except OSError as error:
            log(f"weather unavailable, features stay NaN: {error}")
            weather_df = None
    history = load_history_counts(t0.strftime("%Y%m"), training_root)
    log(f"Predicting {horizon_periods} periods from {t0} ...")
    fractional = model.predict(forecast_input(history, forecast_periods_df, weather_df))
    forecast_demand_df = round_forecast_demand(fractional)
    meta = ForecastMeta(
        forecast_name=forecast_name,
        model_name=model.name,
        model_version="1",
        created_at=datetime.datetime.now().isoformat(timespec="seconds"),
        t0=pd.Timestamp(t0).isoformat(),
        horizon_periods=horizon_periods,
        period_len_hours=DEFAULT_PERIOD_LEN / pd.Timedelta(hours=1),
        history_start=pd.Timestamp(month_bounds(months[0])[0]).isoformat(),
        history_end=pd.Timestamp(t0).isoformat(),
        inputs=[f"{month}.parquet" for month in months],
    )
    return save_forecast(forecast_demand_df, meta, root)


def main() -> None:
    """Terminal entry point: build one forecast artifact.

    Two modes: ``--trips-path`` builds the phase-1 seasonal naive from one
    raw trip CSV; ``--months`` (with ``--model``) trains any model family on
    the training partitions.
    """
    parser = argparse.ArgumentParser(description="Build a forecast demand table and save it.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--trips-path", help="raw trip CSV path (seasonal naive only)")
    source.add_argument(
        "--months", nargs="+", help="training months, as YYYYMM or YYYY-MM (any --model)"
    )
    parser.add_argument(
        "--model", default="seasonal_naive", help="model family name (default: seasonal_naive)"
    )
    parser.add_argument("--forecast-name", required=True, help="forecast folder name")
    parser.add_argument(
        "--horizon-periods", type=int, default=168, help="periods to forecast (default: one week)"
    )
    parser.add_argument(
        "--period-len-hours",
        type=float,
        default=1.0,
        help="wall-clock length of one period (--trips-path mode only)",
    )
    args = parser.parse_args()

    if args.months:
        folder = build_model_forecast(
            args.model,
            args.months,
            horizon_periods=args.horizon_periods,
            forecast_name=args.forecast_name,
        )
        _, meta = load_forecast(args.forecast_name)
        print(f"Saved {folder}")
        print(f"Horizon: {meta.horizon_periods} periods from {meta.t0}")
        return

    if args.model != "seasonal_naive":
        raise SystemExit("--trips-path builds the seasonal naive only; use --months instead")

    # The heavy loader imports live here so `import gbp.ml.forecast` stays light.
    from gbp.loaders.dataloader_graph import get_historical_flows_df, get_periods_df
    from gbp.loaders.dataloader_raw import load_trips_raw_df
    from gbp.model import flows_to_departures

    print("Loading the trips and deriving the historical demand ...")
    period_len = pd.Timedelta(hours=args.period_len_hours)
    trips_df = load_trips_raw_df(args.trips_path)
    t0 = trips_df["started_at"].min().floor("h")
    periods_df = get_periods_df(trips_df, t0, period_len)
    demand_df = flows_to_departures(get_historical_flows_df(trips_df, t0, period_len))

    print(f"Building the seasonal naive forecast for {args.horizon_periods} periods ...")
    folder = build_seasonal_naive_forecast(
        demand_df,
        periods_df,
        forecast_name=args.forecast_name,
        horizon_periods=args.horizon_periods,
        inputs=[pathlib.Path(args.trips_path).name],
    )
    _, meta = load_forecast(args.forecast_name)
    print(f"Saved {folder}")
    print(f"Horizon: {meta.horizon_periods} periods from {meta.t0}")


if __name__ == "__main__":
    main()
