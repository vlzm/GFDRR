"""Structural protocols for the loaders package.

DataSourceProtocol — what any raw data source must expose.
GraphLoaderProtocol — what any graph loader must expose.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import pandas as pd

if TYPE_CHECKING:
    from gbp.core.model import RawModelData, ResolvedModelData


class DataSourceProtocol(Protocol):
    """Raw (possibly temporal) data source — mock, CSV, DB, API, etc."""

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


class GraphLoaderProtocol(Protocol):
    """Builds ``gbp.core`` model tables from a data source."""

    @property
    def raw(self) -> RawModelData: ...

    @property
    def resolved(self) -> ResolvedModelData: ...

    @property
    def source(self) -> DataSourceProtocol: ...

    @property
    def available_dates(self) -> pd.DatetimeIndex: ...

    def load_data(self) -> None: ...
