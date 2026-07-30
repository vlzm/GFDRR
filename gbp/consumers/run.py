"""The run path: the recipe, the fleet defaults, and configure → run → save."""

from __future__ import annotations

import pathlib
from collections.abc import Callable
from typing import Literal

import pandas as pd
import pydantic

from domains.citybike.loaders import RawModelData, apply_truck_fleet, build_resolved
from gbp import artifacts
from gbp.consumers.simulator import (
    RebalancingParams,
    canonical_phases,
    rebalancing_phases,
    run_sized_scenario,
)
from gbp.ml.artifact import apply_saved_forecast
from gbp.model.dataloader_graph import ResolvedModelData
from gbp.routing import DEFAULT_OSRM_URL, RoutingMode

DEFAULT_TRIPS_PATH = str(artifacts.data_dir() / "raw" / "202601-citibike-tripdata_1.csv")
DEFAULT_NUMBER_OF_PERIODS = 50

#: Where a run's demand table can come from (Notations.md §11).
DEMAND_SOURCES = ("history", "forecast")

# The synthetic depot and truck fleet (see domains/citybike/loaders/dataloader_raw.py).
DEFAULT_N_DEPOTS = 10
DEPOT_IDS = [f"depot_{i + 1}" for i in range(DEFAULT_N_DEPOTS)]
DEFAULT_TRUCK_HOMES = ["depot_1"] * 5
DEFAULT_TRUCK_CAPACITY_BIKES = 20
DEFAULT_TRUCK_RATE = 50.0


class RunRequest(pydantic.BaseModel):
    """The full recipe of one run: every parameter that says what to run."""

    run_name: str = pydantic.Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    demand_scale_factor: float = 1.0
    sizing_scale_factor: float = 1.0
    number_of_periods: int = DEFAULT_NUMBER_OF_PERIODS
    demand_source: Literal["history", "forecast"] = "history"
    #: A saved forecast (``data/ml/forecasts/``); required when
    #: ``demand_source="forecast"``.
    forecast_name: str | None = None
    rebalancing: bool = False
    truck_homes: list[str] | None = None
    truck_capacity_bikes: int = DEFAULT_TRUCK_CAPACITY_BIKES

    @pydantic.model_validator(mode="after")
    def _forecast_needs_a_name(self) -> RunRequest:
        """Require a forecast name when the demand source is a forecast."""
        if self.demand_source == "forecast" and not self.forecast_name:
            raise ValueError("demand_source='forecast' needs a forecast_name")
        return self

    def resolved_truck_homes(self) -> list[str]:
        """Home depot per truck the run uses: the request's list, or the default fleet."""
        if self.truck_homes is not None:
            return list(self.truck_homes)
        return list(DEFAULT_TRUCK_HOMES)

    def rebalancing_meta(self) -> dict[str, object]:
        """Build the run's rebalancing block for ``meta.json``."""
        meta: dict[str, object] = {"enabled": self.rebalancing}
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
    """Resolve the run's demand from ``graph_data``, then run and save it."""

    def progress(message: str) -> None:
        if on_progress is not None:
            on_progress(message)

    data: ResolvedModelData = graph_data
    forecast_dropped_share: float | None = None
    if request.demand_source == "forecast":
        assert request.forecast_name is not None  # RunRequest guarantees this
        progress(
            f"Loading forecast {request.forecast_name} and mapping the OD matrix onto its horizon"
        )
        data, forecast_dropped_share = apply_saved_forecast(data, request.forecast_name)
        if forecast_dropped_share > 0:
            progress(
                f"Cut {forecast_dropped_share:.2%} of the forecast demand: rows the "
                "scenario has no OD rows for (unknown stations or station-hours)"
            )

    return run_and_save(
        data,
        request,
        forecast_dropped_share=forecast_dropped_share,
        root=root,
        on_progress=on_progress,
    )


def run_and_save(
    data: ResolvedModelData,
    request: RunRequest,
    *,
    sizing_data: ResolvedModelData | None = None,
    forecast_dropped_share: float | None = None,
    root: pathlib.Path | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> pathlib.Path:
    """Apply the fleet, size and run the scenario on ``data``, save the artifact."""

    def progress(message: str) -> None:
        if on_progress is not None:
            on_progress(message)

    phases = None
    if request.rebalancing:
        homes = request.resolved_truck_homes()
        progress(f"Applying the truck fleet: {len(homes)} trucks")
        data = apply_truck_fleet(data, homes, request.truck_capacity_bikes, DEFAULT_TRUCK_RATE)
        phases = canonical_phases() + rebalancing_phases(RebalancingParams())

    progress("Sizing the state, running the simulation, checking the invariants I1-I5")
    result = run_sized_scenario(
        data,
        scenario_id=request.run_name,
        demand_scale_factor=request.demand_scale_factor,
        sizing_scale_factor=request.sizing_scale_factor,
        number_of_periods=request.number_of_periods,
        phases=phases,
        validate=False,
        sizing_data=sizing_data,
    )

    progress("Building and saving the run artifact")
    return artifacts.save_scenario_run(
        result,
        data,
        request,
        forecast_dropped_share=forecast_dropped_share,
        root=root,
    )
