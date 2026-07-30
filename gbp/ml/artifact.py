"""The forecast artifact: its ``meta.json`` contract, the reader, and the writer.

A forecast artifact is a folder with two files — ``demand.parquet``, a table
in the shape of the historical demand, and ``meta.json``, the horizon it was
built for. Reading one and running a scenario on it is the whole integration
contract between a demand model and the simulator; how the model was built is
the domain's business.
"""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pydantic

from gbp.artifacts import data_dir
from gbp.model.dataloader_graph import (
    HISTORICAL_DEMAND_SCHEMA,
    PeriodGrid,
    ResolvedModelData,
    apply_forecast_demand,
    restrict_demand_to_scenario,
)
from gbp.model.journal_schema import schema_violations


def ml_dir() -> pathlib.Path:
    """Root of the forecasting data: ``<data dir>/ml`` (honors the ``DATA_DIR`` switch)."""
    return data_dir() / "ml"


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
    """Round a fractional forecast to whole units (largest-remainder, group totals stay exact)."""
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


def apply_saved_forecast(
    resolved: ResolvedModelData,
    forecast_name: str,
    root: pathlib.Path | None = None,
) -> tuple[ResolvedModelData, float]:
    """Return a copy of ``resolved`` that runs on a saved forecast, plus the dropped share."""
    forecast_demand_df, meta = load_forecast(forecast_name, root)
    forecast_periods_df = forecast_periods_from_meta(meta)
    forecast_demand_df, dropped_share = restrict_demand_to_scenario(
        forecast_demand_df, resolved, forecast_periods_df
    )
    out = apply_forecast_demand(resolved, forecast_demand_df, forecast_periods_df)
    return out, dropped_share
