"""Run one scenario end to end and save its run artifact."""

from __future__ import annotations

import argparse
import pathlib
from collections.abc import Callable
from typing import Literal

import artifacts
import pandas as pd
import pydantic

from gbp.consumers.simulator import (
    RebalancingParams,
    canonical_phases,
    rebalancing_phases,
    run_sized_scenario,
)
from gbp.loaders.dataloader_graph import (
    ResolvedModelData,
    apply_saved_forecast,
    apply_truck_fleet,
)
from gbp.loaders.dataloader_raw import RawModelData
from gbp.logging import configure_logging
from gbp.routing import DEFAULT_OSRM_URL, ROUTING_MODES

DEFAULT_TRIPS_PATH = str(artifacts.data_dir() / "raw" / "202601-citibike-tripdata_1.csv")
DEFAULT_NUMBER_OF_PERIODS = 50

#: Where a run's demand table can come from (Notations.md §11).
DEMAND_SOURCES = ("history", "forecast")

# The synthetic depot and truck fleet (see gbp/loaders/dataloader_raw.py).
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
    routing_mode: str = "haversine",
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
    return ResolvedModelData(
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


def main() -> None:
    """Terminal entry point: parse arguments, run one scenario, save it."""
    configure_logging()
    parser = argparse.ArgumentParser(description="Run one scenario and save its run artifact.")
    parser.add_argument("--run-name", required=True, help="artifact folder name")
    parser.add_argument("--demand-scale", type=float, default=1.0, help="demand multiplier")
    parser.add_argument(
        "--sizing-scale", type=float, default=1.0, help="demand the state is sized for"
    )
    parser.add_argument(
        "--periods", type=int, default=DEFAULT_NUMBER_OF_PERIODS, help="periods to step"
    )
    parser.add_argument("--trips-path", default=DEFAULT_TRIPS_PATH, help="raw trip CSV path")
    parser.add_argument(
        "--demand-source",
        choices=DEMAND_SOURCES,
        default="history",
        help="where the demand table comes from: the historical replay or a saved forecast",
    )
    parser.add_argument(
        "--forecast-name",
        default=None,
        help="saved forecast to run on (a folder under data/ml/forecasts/); "
        "required with --demand-source forecast",
    )
    parser.add_argument(
        "--rebalancing",
        action="store_true",
        help="run with the overnight rebalancing phases (trucks move bikes at night)",
    )
    parser.add_argument(
        "--truck-homes",
        default=None,
        help=(
            "home depot per truck, comma-separated (the list length is the fleet size), "
            "e.g. depot_1,depot_1,depot_3; default: 5 trucks at depot_1"
        ),
    )
    parser.add_argument(
        "--truck-capacity",
        type=int,
        default=DEFAULT_TRUCK_CAPACITY_BIKES,
        help="bikes one truck can carry",
    )
    parser.add_argument(
        "--routing",
        choices=ROUTING_MODES,
        default="haversine",
        help="distance/travel-time mode: haversine formula or a local OSRM server",
    )
    parser.add_argument(
        "--osrm-url", default=DEFAULT_OSRM_URL, help="OSRM server URL (for --routing osrm)"
    )
    args = parser.parse_args()

    print("Loading raw data and resolving the graph tables ...")
    graph_data = build_graph_data(
        args.trips_path, routing_mode=args.routing, osrm_url=args.osrm_url
    )
    truck_homes = None
    if args.truck_homes:
        truck_homes = [home.strip() for home in args.truck_homes.split(",") if home.strip()]
    request = RunRequest(
        run_name=args.run_name,
        demand_scale_factor=args.demand_scale,
        sizing_scale_factor=args.sizing_scale,
        number_of_periods=args.periods,
        demand_source=args.demand_source,
        forecast_name=args.forecast_name,
        rebalancing=args.rebalancing,
        truck_homes=truck_homes,
        truck_capacity_bikes=args.truck_capacity,
    )
    folder = run_scenario(graph_data, request, on_progress=print)
    meta = artifacts.load_run_meta(args.run_name)
    print(f"Saved {folder}")
    print(f"Invariant violations: {len(meta.violations)}")
    print(f"Totals: {meta.totals}")


if __name__ == "__main__":
    main()
