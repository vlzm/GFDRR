"""Build and save forecast demand tables (Notations.md §17).

A model's whole job is to produce a forecast demand table: a table in
``HISTORICAL_DEMAND_SCHEMA`` shape whose ``quantity`` comes from a model, for
periods that have no history yet. The models themselves live behind the one
interface in ``gbp/ml/models/``; this module is the forecast builder around
them. It holds the one rounding rule that turns fractional forecasts into
whole bikes, the forecast input (:func:`forecast_input` — the feature table a
model predicts from, built by the shared feature module), the one
horizon-prediction recipe (:func:`predict_horizon` — history window →
forecast input → prediction → whole bikes), and the forecast artifact — the
folder ``data/ml/forecasts/<forecast_name>/`` with ``demand.parquet`` and
``meta.json`` that a forecast run (Notations.md §11) is loaded from.

Three builders save an artifact:

- :func:`build_seasonal_naive_forecast` — the phase-1 path: from one trip
  CSV's demand and period grid, seasonal naive only.
- :func:`build_model_forecast` — from the training partitions, any model
  family by name, fitted on the spot.
- :func:`build_champion_forecast` — the platform's path (plan, phase 6):
  the fitted champion resolved from the model registry by its alias
  (Notations.md §17), never by a file path.

Terminal use (one command each; wrapped here for width)::

    python -m gbp.ml.forecast --trips-path data/raw/202601-citibike-tripdata_1.csv
        --forecast-name seasonal_naive_w1 --horizon-periods 168

    python -m gbp.ml.forecast --model lightgbm --months 202502 ... 202512
        --forecast-name lightgbm_202601 --horizon-periods 744

    python -m gbp.ml.forecast --champion --forecast-name champion_202602
        --horizon-periods 744
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
from gbp.ml.data import (
    load_weather_daily,
    ml_dir,
    month_bounds,
    month_period_grid,
    normalize_month,
)
from gbp.ml.features import HISTORY_WEEKS, build_features, clip_history_window
from gbp.ml.models import DemandModel, create_model
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


def predict_horizon(
    model: DemandModel,
    forecast_periods_df: pd.DataFrame,
    *,
    history_df: pd.DataFrame | None = None,
    training_root: pathlib.Path | None = None,
    weather_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Predict one forecast horizon with a fitted model, in whole bikes.

    The one recipe every forecast demand table is built by: take the history
    window before the horizon, build the forecast input
    (:func:`forecast_input`), let the model predict, and round the fractional
    values to whole bikes (:func:`round_forecast_demand`). The forecast
    builders in this module and the shared naive baseline
    (:func:`naive_month_prediction`) call this function instead of
    assembling the steps themselves.

    Parameters
    ----------
    model : DemandModel
        A fitted model — only its ``predict`` is called.
    forecast_periods_df : pandas.DataFrame
        The forecast period grid (:func:`get_forecast_periods_df`, or
        ``month_period_grid`` for a full month).
    history_df : pandas.DataFrame, optional
        Departure counts in training-table shape. Without it the history is
        read from the training partitions before the horizon
        (``load_history_counts``); the horizon must then start at a month's
        first hour, because the partitions are monthly.
    training_root : pathlib.Path, optional
        Training partitions folder override; used only when ``history_df``
        is not given.
    weather_df : pandas.DataFrame, optional
        Daily weather covering the horizon dates; without it the weather
        features stay NaN.

    Returns
    -------
    pandas.DataFrame
        A forecast demand table: whole-bike quantities, positive rows only.
    """
    if history_df is None:
        # Imported here, not at the top: gbp.ml.training is the partition
        # builder, and only this default needs it.
        from gbp.ml.training import load_history_counts

        t0 = forecast_periods_df["start_timestamp"].iloc[0]
        history_df = load_history_counts(t0.strftime("%Y%m"), training_root)
    fractional = model.predict(forecast_input(history_df, forecast_periods_df, weather_df))
    return round_forecast_demand(fractional)


def naive_month_prediction(
    month: str, training_root: pathlib.Path | None = None
) -> pd.DataFrame | None:
    """Forecast one calendar month with the seasonal naive — the shared baseline.

    The one naive forecast both quality measures rely on: the backtest
    divides every family's error by its error (``mae_over_naive``), and the
    monitoring alert compares a model version's rolling MAE against it
    (``naive_mae``). Built like any forecast: the family comes from the
    factory, and the prediction goes through the one recipe
    :func:`predict_horizon` on the month's period grid, with the history
    read from the training partitions before the month. Weather is not
    read — the seasonal naive does not use it. Returns None when no earlier
    partition exists.
    """
    # Imported here, not at the top: gbp.ml.training is the partition
    # builder, and only the partition-backed helpers need it.
    from gbp.ml.training import load_history_counts

    history = load_history_counts(month, training_root)
    if history.empty:
        return None
    return predict_horizon(
        create_model("seasonal_naive"), month_period_grid(month), history_df=history
    )


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
    (:func:`counts_from_demand`), and the prediction goes through the one
    recipe :func:`predict_horizon`, so the mean reads the history window
    like every history feature and the fractional values are rounded to
    whole bikes. The forecast horizon starts where the history ends (the
    last period's ``end_timestamp``) and runs for ``horizon_periods``
    periods of the same length, numbered from 0.

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
    forecast_demand_df = predict_horizon(model, forecast_periods_df, history_df=counts)
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


def _horizon_weather(
    t0: pd.Timestamp,
    horizon_end: pd.Timestamp,
    raw: pathlib.Path | None,
    weather_df: pd.DataFrame | None,
    log: Callable[[str], None],
) -> pd.DataFrame | None:
    """Return the horizon's published daily weather, or None when unreachable.

    A true future horizon has no published weather; whatever NOAA has for the
    horizon dates is used, unpublished dates leave the weather features NaN,
    and a fully unreachable weather source is skipped with a note — the
    models accept missing values. A caller-supplied ``weather_df`` is
    returned unchanged.
    """
    if weather_df is not None:
        return weather_df
    try:
        return load_weather_daily(t0, horizon_end - pd.Timedelta(days=1), raw)
    except OSError as error:
        log(f"weather unavailable, features stay NaN: {error}")
        return None


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
        partitions, and the raw files (``raw`` feeds the horizon weather).
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
    from gbp.ml.training import load_training_table

    months = sorted(normalize_month(m) for m in train_months)
    log(f"Training {model_name} on {months[0]}..{months[-1]} ...")
    train_table = load_training_table(months, training_root)
    model = create_model(model_name, **model_params)
    model.fit(train_table)

    t0 = month_bounds(months[-1])[1]
    forecast_periods_df = get_forecast_periods_df(t0, horizon_periods, DEFAULT_PERIOD_LEN)
    horizon_end = forecast_periods_df["end_timestamp"].iloc[-1]
    weather_df = _horizon_weather(t0, horizon_end, raw, weather_df, log)
    log(f"Predicting {horizon_periods} periods from {t0} ...")
    forecast_demand_df = predict_horizon(
        model, forecast_periods_df, training_root=training_root, weather_df=weather_df
    )
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


def build_champion_forecast(
    *,
    horizon_periods: int,
    forecast_name: str,
    t0_month: str | None = None,
    root: pathlib.Path | None = None,
    training_root: pathlib.Path | None = None,
    raw: pathlib.Path | None = None,
    tracking_dir: pathlib.Path | None = None,
    weather_df: pd.DataFrame | None = None,
    log: Callable[[str], None] = print,
) -> pathlib.Path:
    """Build a forecast with the champion, resolved from the registry by its alias.

    The platform's forecast path (plan, phase 6): the model is the fitted
    version the ``champion`` alias points at (``resolve_champion`` in
    ``gbp/ml/registry.py``) — no refitting, no file paths. The saved
    ``meta.json`` records the family as ``model_name`` and the registry
    version number as ``model_version``; the version's own training months
    live on its registry tags. The ``history_*`` fields and ``inputs`` name
    what the forecast input read: the training partitions of the
    ``HISTORY_WEEKS`` window before the horizon.

    Parameters
    ----------
    horizon_periods : int
        How many one-hour periods the forecast covers.
    forecast_name : str
        Folder name of the forecast artifact.
    t0_month : str, optional
        The month the horizon starts at, as ``YYYYMM`` — the forecast then
        starts at that month's first hour. Default: right after the newest
        training partition on disk.
    root, training_root, raw, tracking_dir : pathlib.Path, optional
        Folder overrides for the forecast artifacts, the training
        partitions, the raw files, and the MLflow store.
    weather_df : pandas.DataFrame, optional
        Daily weather for the horizon dates; without it the published
        weather is loaded (and missing dates stay NaN).
    log : callable, optional
        Where progress notes go (default: ``print``).

    Returns
    -------
    pathlib.Path
        The saved forecast artifact folder.
    """
    # Imported here, not at the top: the registry drags in MLflow and the
    # training module is the partition builder — only this builder needs them.
    from gbp.ml.registry import resolve_champion
    from gbp.ml.training import history_months, partition_path, training_dir

    model, version = resolve_champion(tracking_dir)
    log(f"Champion: {model.name} version {version.version}")

    if t0_month is None:
        partitions = sorted((training_root or training_dir()).glob("*.parquet"))
        if not partitions:
            raise FileNotFoundError(
                "no training partitions; build them first (python -m gbp.ml.training)"
            )
        t0_month = partitions[-1].stem
        t0 = month_bounds(t0_month)[1]
        t0_month = t0.strftime("%Y%m")
    else:
        t0 = month_bounds(t0_month)[0]

    forecast_periods_df = get_forecast_periods_df(t0, horizon_periods, DEFAULT_PERIOD_LEN)
    horizon_end = forecast_periods_df["end_timestamp"].iloc[-1]
    weather_df = _horizon_weather(t0, horizon_end, raw, weather_df, log)
    log(f"Predicting {horizon_periods} periods from {t0} ...")
    forecast_demand_df = predict_horizon(
        model, forecast_periods_df, training_root=training_root, weather_df=weather_df
    )
    window = [m for m in history_months(t0_month) if partition_path(m, training_root).exists()]
    meta = ForecastMeta(
        forecast_name=forecast_name,
        model_name=model.name,
        model_version=str(version.version),
        created_at=datetime.datetime.now().isoformat(timespec="seconds"),
        t0=pd.Timestamp(t0).isoformat(),
        horizon_periods=horizon_periods,
        period_len_hours=DEFAULT_PERIOD_LEN / pd.Timedelta(hours=1),
        history_start=pd.Timestamp(month_bounds(window[0])[0] if window else t0).isoformat(),
        history_end=pd.Timestamp(t0).isoformat(),
        inputs=[f"{month}.parquet" for month in window],
    )
    return save_forecast(forecast_demand_df, meta, root)


def main() -> None:
    """Terminal entry point: build one forecast artifact.

    Three modes: ``--trips-path`` builds the phase-1 seasonal naive from one
    raw trip CSV; ``--months`` (with ``--model``) trains any model family on
    the training partitions; ``--champion`` predicts with the fitted version
    the registry's champion alias points at.
    """
    parser = argparse.ArgumentParser(description="Build a forecast demand table and save it.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--trips-path", help="raw trip CSV path (seasonal naive only)")
    source.add_argument(
        "--months", nargs="+", help="training months, as YYYYMM or YYYY-MM (any --model)"
    )
    source.add_argument(
        "--champion",
        action="store_true",
        help="predict with the registry champion, no refitting",
    )
    parser.add_argument(
        "--model", default="seasonal_naive", help="model family name (default: seasonal_naive)"
    )
    parser.add_argument("--forecast-name", required=True, help="forecast folder name")
    parser.add_argument(
        "--horizon-periods", type=int, default=168, help="periods to forecast (default: one week)"
    )
    parser.add_argument(
        "--t0-month",
        default=None,
        help="month the horizon starts at, as YYYYMM (--champion mode only; "
        "default: right after the newest training partition)",
    )
    parser.add_argument(
        "--period-len-hours",
        type=float,
        default=1.0,
        help="wall-clock length of one period (--trips-path mode only)",
    )
    args = parser.parse_args()

    if args.champion:
        folder = build_champion_forecast(
            horizon_periods=args.horizon_periods,
            forecast_name=args.forecast_name,
            t0_month=args.t0_month,
        )
        _, meta = load_forecast(args.forecast_name)
        print(f"Saved {folder}")
        print(f"Model: {meta.model_name} version {meta.model_version}")
        print(f"Horizon: {meta.horizon_periods} periods from {meta.t0}")
        return

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
