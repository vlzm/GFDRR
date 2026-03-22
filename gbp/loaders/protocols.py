"""Structural protocols for the loaders package.

GenericSourceProtocol  — minimal interface for any data source.
BikeShareSourceProtocol — bike-sharing specific: stations, depots, trips, etc.
DataSourceProtocol     — backward-compatible alias for BikeShareSourceProtocol.
GraphLoaderProtocol    — what any graph loader must expose.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import pandas as pd

if TYPE_CHECKING:
    from gbp.core.model import RawModelData, ResolvedModelData


# ---------------------------------------------------------------------------
# Generic (aspirational — for future domain-agnostic loaders)
# ---------------------------------------------------------------------------

class GenericSourceProtocol(Protocol):
    """Minimal interface: any data source that can produce DataFrames."""

    def load_data(self) -> None: ...

    def get_dataframes(self) -> dict[str, pd.DataFrame]: ...


# ---------------------------------------------------------------------------
# Bike-sharing domain
# ---------------------------------------------------------------------------

class BikeShareSourceProtocol(Protocol):
    """Bike-sharing data source — stations, depots, trips, telemetry."""

    df_stations: pd.DataFrame
    df_depots: pd.DataFrame
    df_resources: pd.DataFrame
    timestamps: pd.DatetimeIndex
    df_inventory_ts: pd.DataFrame
    df_telemetry_ts: pd.DataFrame
    df_trips: pd.DataFrame
    df_station_costs: pd.DataFrame
    df_truck_rates: pd.DataFrame

    def load_data(self) -> None: ...


DataSourceProtocol = BikeShareSourceProtocol
"""Backward-compatible alias — existing code importing DataSourceProtocol keeps working."""


# ---------------------------------------------------------------------------
# Graph loader
# ---------------------------------------------------------------------------

class GraphLoaderProtocol(Protocol):
    """Builds ``gbp.core`` model tables from a data source."""

    @property
    def raw(self) -> RawModelData: ...

    @property
    def resolved(self) -> ResolvedModelData: ...

    @property
    def source(self) -> BikeShareSourceProtocol: ...

    @property
    def available_dates(self) -> pd.DatetimeIndex: ...

    def load_data(self) -> None: ...
