"""Build and save forecast demand tables."""

from __future__ import annotations

import argparse
import datetime
import pathlib
from collections.abc import Callable

import numpy as np
import pandas as pd
import pydantic

from domains.citybike.loaders.download import month_bounds, normalize_month
from gbp.ml.data import MlPaths, load_weather_daily, ml_dir, month_period_grid
from gbp.ml.features import HISTORY_WEEKS, build_features, clip_history_window
from gbp.ml.models import DemandModel, create_model
from gbp.model.dataloader_graph import (
    DEFAULT_PERIOD_LEN,
    HISTORICAL_DEMAND_SCHEMA,
    PeriodGrid,
    get_forecast_periods_df,
)
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
    """The meta.json contract of a forecast artifact."""

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

    @property
    def grid(self) -> PeriodGrid:
        """The forecast horizon this meta describes, as a PeriodGrid."""
        return PeriodGrid(
            pd.Timestamp(self.t0),
            self.horizon_periods,
            pd.Timedelta(hours=self.period_len_hours),
        )


def forecast_periods_from_meta(meta: ForecastMeta) -> pd.DataFrame:
    """Rebuild the forecast period grid a saved forecast was built for."""
    return meta.grid.frame()


def counts_from_demand(demand_df: pd.DataFrame, periods_df: pd.DataFrame) -> pd.DataFrame:
    """Turn the historical demand marginal into a zero-filled counts grid."""
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
    """Round a fractional forecast to whole bikes (largest-remainder, group totals stay exact)."""
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
    """Build the feature table a model predicts from, over the forecast horizon."""
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
    """Predict one forecast horizon with a fitted model, in whole bikes."""
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
    """Forecast one calendar month with the seasonal naive; None if no earlier partition exists."""
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
    """Write one forecast artifact, after checking the demand table against the schema."""
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


def _finish_forecast(
    model: DemandModel,
    *,
    t0: pd.Timestamp,
    horizon_periods: int,
    period_len: pd.Timedelta,
    forecast_name: str,
    model_version: str,
    history_start: pd.Timestamp,
    history_end: pd.Timestamp,
    inputs: list[str],
    root: pathlib.Path | None,
    history_df: pd.DataFrame | None = None,
    training_root: pathlib.Path | None = None,
    weather_df: pd.DataFrame | None = None,
) -> pathlib.Path:
    """Predict the horizon and save the artifact — the tail every builder ends with."""
    forecast_periods_df = get_forecast_periods_df(t0, horizon_periods, period_len)
    forecast_demand_df = predict_horizon(
        model,
        forecast_periods_df,
        history_df=history_df,
        training_root=training_root,
        weather_df=weather_df,
    )
    meta = ForecastMeta(
        forecast_name=forecast_name,
        model_name=model.name,
        model_version=model_version,
        created_at=datetime.datetime.now().isoformat(timespec="seconds"),
        t0=pd.Timestamp(t0).isoformat(),
        horizon_periods=horizon_periods,
        period_len_hours=period_len / pd.Timedelta(hours=1),
        history_start=pd.Timestamp(history_start).isoformat(),
        history_end=pd.Timestamp(history_end).isoformat(),
        inputs=list(inputs),
    )
    return save_forecast(forecast_demand_df, meta, root)


def build_seasonal_naive_forecast(
    demand_df: pd.DataFrame,
    periods_df: pd.DataFrame,
    *,
    forecast_name: str,
    horizon_periods: int,
    inputs: list[str],
    root: pathlib.Path | None = None,
) -> pathlib.Path:
    """Build and save a seasonal naive forecast for the periods right after history."""
    t0 = periods_df["end_timestamp"].iloc[-1]
    period_len = periods_df["end_timestamp"].iloc[0] - periods_df["start_timestamp"].iloc[0]
    counts = counts_from_demand(demand_df, periods_df)
    model = create_model("seasonal_naive")
    model.fit(counts)
    return _finish_forecast(
        model,
        t0=t0,
        horizon_periods=horizon_periods,
        period_len=period_len,
        forecast_name=forecast_name,
        model_version="1",
        history_start=periods_df["start_timestamp"].iloc[0],
        history_end=periods_df["end_timestamp"].iloc[-1],
        inputs=inputs,
        root=root,
        history_df=counts,
    )


def _horizon_weather(
    t0: pd.Timestamp,
    horizon_end: pd.Timestamp,
    raw: pathlib.Path | None,
    weather_df: pd.DataFrame | None,
    log: Callable[[str], None],
) -> pd.DataFrame | None:
    """Return the horizon's published daily weather, or None when unreachable."""
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
    paths: MlPaths | None = None,
    weather_df: pd.DataFrame | None = None,
    log: Callable[[str], None] = print,
    **model_params: object,
) -> pathlib.Path:
    """Fit one model family on the training partitions and save its forecast."""
    # Imported here, not at the top: gbp.ml.training is the partition
    # builder, and only this builder needs it.
    from gbp.ml.training import load_training_table

    paths = paths or MlPaths.resolve()
    months = sorted(normalize_month(m) for m in train_months)
    log(f"Training {model_name} on {months[0]}..{months[-1]} ...")
    train_table = load_training_table(months, paths.training)
    model = create_model(model_name, **model_params)
    model.fit(train_table)

    t0 = month_bounds(months[-1])[1]
    horizon_end = t0 + horizon_periods * DEFAULT_PERIOD_LEN
    weather_df = _horizon_weather(t0, horizon_end, paths.raw, weather_df, log)
    log(f"Predicting {horizon_periods} periods from {t0} ...")
    return _finish_forecast(
        model,
        t0=t0,
        horizon_periods=horizon_periods,
        period_len=DEFAULT_PERIOD_LEN,
        forecast_name=forecast_name,
        model_version="1",
        history_start=month_bounds(months[0])[0],
        history_end=t0,
        inputs=[f"{month}.parquet" for month in months],
        root=paths.forecasts,
        training_root=paths.training,
        weather_df=weather_df,
    )


def build_champion_forecast(
    *,
    horizon_periods: int,
    forecast_name: str,
    t0_month: str | None = None,
    paths: MlPaths | None = None,
    weather_df: pd.DataFrame | None = None,
    log: Callable[[str], None] = print,
) -> pathlib.Path:
    """Build a forecast with the champion, resolved from the registry by its alias."""
    # Imported here, not at the top: the registry drags in MLflow and the
    # training module is the partition builder — only this builder needs them.
    from gbp.ml.registry import MlflowStore
    from gbp.ml.training import history_months, partition_path

    paths = paths or MlPaths.resolve()
    model, version = MlflowStore(paths.tracking).resolve_champion()
    log(f"Champion: {model.name} version {version.version}")

    if t0_month is None:
        partitions = sorted(paths.training.glob("*.parquet"))
        if not partitions:
            raise FileNotFoundError(
                "no training partitions; build them first (python -m gbp.ml.training)"
            )
        t0_month = partitions[-1].stem
        t0 = month_bounds(t0_month)[1]
        t0_month = t0.strftime("%Y%m")
    else:
        t0 = month_bounds(t0_month)[0]

    horizon_end = t0 + horizon_periods * DEFAULT_PERIOD_LEN
    weather_df = _horizon_weather(t0, horizon_end, paths.raw, weather_df, log)
    log(f"Predicting {horizon_periods} periods from {t0} ...")
    window = [m for m in history_months(t0_month) if partition_path(m, paths.training).exists()]
    return _finish_forecast(
        model,
        t0=t0,
        horizon_periods=horizon_periods,
        period_len=DEFAULT_PERIOD_LEN,
        forecast_name=forecast_name,
        model_version=str(version.version),
        history_start=month_bounds(window[0])[0] if window else t0,
        history_end=t0,
        inputs=[f"{month}.parquet" for month in window],
        root=paths.forecasts,
        training_root=paths.training,
        weather_df=weather_df,
    )


def main() -> None:
    """Terminal entry point: build one forecast artifact."""
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
    from domains.citybike.loaders.dataloader_graph import (
        get_historical_flows_df,
        get_periods_df,
    )
    from domains.citybike.loaders.dataloader_raw import load_trips_raw_df
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
