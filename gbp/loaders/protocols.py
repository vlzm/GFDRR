"""Structural protocols for the loaders package.

DataSourceProtocol — what any raw data source must expose.
GraphLoaderProtocol — what any graph loader must expose.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import pandas as pd

if TYPE_CHECKING:
    from gbp.loaders.dataloader_graph import RebalancerGraphSnapshot


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
    """Temporal model loader: core tables + rebalancer-oriented snapshots."""

    @property
    def available_dates(self) -> pd.DatetimeIndex: ...

    def load_data(self) -> None: ...

    def rebalancer_snapshot(self, date: pd.Timestamp) -> RebalancerGraphSnapshot: ...
