"""The Citi Bike run path: build the scenario data, put the truck fleet on it, run it.

Everything the framework cannot know lives here: which raw files a run reads,
how many depots and trucks the synthetic fleet has, and what a truck costs.
The framework's ``gbp.consumers.run`` takes it from there -- it sizes the
state, runs the phases, and saves the artifact, without ever hearing the word
"truck".
"""

from __future__ import annotations

import pathlib
from collections.abc import Callable

import pandas as pd

from domains.citybike.loaders import RawModelData, apply_truck_fleet, build_resolved
from gbp import artifacts
from gbp.consumers import run as gbp_run
from gbp.model.dataloader_graph import ResolvedModelData
from gbp.routing import DEFAULT_OSRM_URL, RoutingMode

DEFAULT_TRIPS_PATH = str(artifacts.data_dir() / "raw" / "202601-citibike-tripdata_1.csv")

# The synthetic depot and truck fleet (see domains/citybike/loaders/dataloader_raw.py).
DEFAULT_N_DEPOTS = 10
DEPOT_IDS = [f"depot_{i + 1}" for i in range(DEFAULT_N_DEPOTS)]
DEFAULT_TRUCK_HOMES = ["depot_1"] * 5
DEFAULT_TRUCK_CAPACITY_BIKES = 20
DEFAULT_TRUCK_RATE = 50.0


class RunRequest(gbp_run.RunRequest):
    """The framework's run recipe plus the Citi Bike truck fleet."""

    truck_homes: list[str] | None = None
    truck_capacity_bikes: int = DEFAULT_TRUCK_CAPACITY_BIKES

    def resolved_truck_homes(self) -> list[str]:
        """Home depot per truck the run uses: the request's list, or the default fleet."""
        if self.truck_homes is not None:
            return list(self.truck_homes)
        return list(DEFAULT_TRUCK_HOMES)

    def rebalancing_meta(self) -> dict[str, object]:
        """Add the fleet the run used to the framework's rebalancing block."""
        meta = super().rebalancing_meta()
        if self.rebalancing:
            meta["truck_homes"] = self.resolved_truck_homes()
            meta["truck_capacity_bikes"] = self.truck_capacity_bikes
        return meta


def build_graph_data(
    trips_path: str = DEFAULT_TRIPS_PATH,
    period_len_hours: float = 1.0,
    routing_mode: RoutingMode = "haversine",
    osrm_url: str = DEFAULT_OSRM_URL,
) -> ResolvedModelData:
    """Load the raw sources and resolve the graph tables (the heavy step)."""
    raw = RawModelData(
        trips_path=trips_path,
        seed=42,
        n_depots=DEFAULT_N_DEPOTS,
        depot_capacity=9000,
        n_trucks=len(DEFAULT_TRUCK_HOMES),
        truck_capacity_bikes=DEFAULT_TRUCK_CAPACITY_BIKES,
        truck_rate=DEFAULT_TRUCK_RATE,
        electric_bike_rate=5,
        classic_bike_rate=3,
    )
    return build_resolved(
        raw,
        period_len=pd.Timedelta(hours=period_len_hours),
        routing_mode=routing_mode,
        osrm_url=osrm_url,
    )


def run_scenario(
    graph_data: ResolvedModelData,
    request: RunRequest,
    *,
    root: pathlib.Path | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> pathlib.Path:
    """Put the requested truck fleet on ``graph_data``, then run and save it.

    The fleet swap rebuilds the resource tables only, so it commutes with the
    forecast substitution the framework does next (which rewrites the demand
    side); the run sees both.
    """
    data = graph_data
    if request.rebalancing:
        homes = request.resolved_truck_homes()
        if on_progress is not None:
            on_progress(f"Applying the truck fleet: {len(homes)} trucks")
        data = apply_truck_fleet(data, homes, request.truck_capacity_bikes, DEFAULT_TRUCK_RATE)
    return gbp_run.run_scenario(data, request, root=root, on_progress=on_progress)
