"""Terminal entry point: parse flags, run one scenario through the gbp run path, save it."""

from __future__ import annotations

import argparse

from gbp import artifacts
from gbp.consumers.run import (
    DEFAULT_NUMBER_OF_PERIODS,
    DEFAULT_TRIPS_PATH,
    DEFAULT_TRUCK_CAPACITY_BIKES,
    DEMAND_SOURCES,
    RunRequest,
    build_graph_data,
    run_scenario,
)
from gbp.logging import configure_logging
from gbp.routing import DEFAULT_OSRM_URL, ROUTING_MODES


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
