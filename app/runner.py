"""Run one scenario end to end and save its run artifact.

The pipeline is the canonical one from ``notebooks/test_pipeline.ipynb``:
load the raw data, resolve the graph tables, size the initial inventory and
dock capacities against the sizing demand, run the scaled demand, validate
the run invariants, and save every table the UI reads (Notations.md §12).

Terminal use::

    python app/runner.py --run-name demand_x2 --demand-scale 2.0 --periods 50
    python app/runner.py --run-name with_trucks --rebalancing \
        --truck-homes depot_1,depot_1,depot_3
    python app/runner.py --run-name forecast_demo --demand-source forecast \
        --forecast-name seasonal_naive_w1
"""

from __future__ import annotations

import argparse
import pathlib
from collections.abc import Callable

import artifacts
import pandas as pd

from gbp.consumers.simulator import (
    RebalancingParams,
    canonical_phases,
    rebalancing_phases,
    run_sized_scenario,
)
from gbp.loaders.dataloader_graph import (
    ResolvedModelData,
    apply_forecast_demand,
    apply_truck_fleet,
    restrict_demand_to_scenario,
)
from gbp.loaders.dataloader_raw import RawModelData
from gbp.logging import configure_logging
from gbp.ml import forecast
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


def build_graph_data(
    trips_path: str = DEFAULT_TRIPS_PATH,
    period_len_hours: float = 1.0,
    routing_mode: str = "haversine",
    osrm_url: str = DEFAULT_OSRM_URL,
) -> ResolvedModelData:
    """Load the raw sources and resolve the graph tables (the heavy step).

    Parameters
    ----------
    trips_path : str, optional
        Path to the raw Citi Bike trip CSV.
    period_len_hours : float, optional
        Wall-clock length of one period, in hours.
    routing_mode : {"haversine", "osrm"}, optional
        How distances and travel times between facilities are measured
        (see :mod:`gbp.routing`). ``"osrm"`` needs a running OSRM server.
    osrm_url : str, optional
        Base URL of the OSRM server. Only read when ``routing_mode="osrm"``.

    Returns
    -------
    ResolvedModelData
        The resolved scenario data, ready for :func:`run_scenario`.
    """
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
    *,
    run_name: str,
    demand_scale_factor: float,
    sizing_scale_factor: float = 1.0,
    number_of_periods: int = DEFAULT_NUMBER_OF_PERIODS,
    demand_source: str = "history",
    forecast_name: str | None = None,
    rebalancing: bool = False,
    truck_homes: list[str] | None = None,
    truck_capacity_bikes: int = DEFAULT_TRUCK_CAPACITY_BIKES,
    root: pathlib.Path | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> pathlib.Path:
    """Size, run, validate, and save one scenario as a run artifact.

    The state (initial inventory and dock capacities) is sized against
    ``sizing_scale_factor``; the run itself faces ``demand_scale_factor``.
    Equal values give a clean, no-loss run; a larger run scale makes the
    limits take effect (stockout and dock-full events appear).

    With ``demand_source="forecast"`` the run is a forecast run
    (Notations.md §11): the named forecast is loaded from
    ``data/ml/forecasts/`` and put in place of the historical demand with
    :func:`apply_forecast_demand` — the period grid becomes the forecast
    horizon and the OD matrix is mapped onto it by hour of week. Everything
    after that is the same sized-run path.

    Parameters
    ----------
    graph_data : ResolvedModelData
        The resolved scenario data. Not modified: the sized state stays inside
        :func:`run_sized_scenario`, and the truck fleet is applied to a
        shallow copy. The Run page shares one cached ``graph_data`` across
        runs, so this must hold.
    run_name : str
        Name of the artifact folder (and of the run in the UI).
    demand_scale_factor : float
        Demand multiplier the run faces.
    sizing_scale_factor : float, optional
        Demand multiplier the state is sized to survive with no loss.
    number_of_periods : int, optional
        How many periods to step.
    demand_source : {"history", "forecast"}, optional
        Where the demand table comes from. Default ``"history"``.
    forecast_name : str, optional
        Name of the saved forecast to run on. Required when
        ``demand_source="forecast"``.
    rebalancing : bool, optional
        When True, run with the two overnight-rebalancing phases
        (Notations.md §14) and the truck fleet below. Default False: the
        canonical three phases only, trucks stay idle.
    truck_homes : list of str, optional
        Home depot per truck, one entry per truck (the list length is the
        fleet size). Only read when ``rebalancing`` is True. Default:
        ``DEFAULT_TRUCK_HOMES`` (5 trucks at ``depot_1``).
    truck_capacity_bikes : int, optional
        Bikes one truck can carry. Only read when ``rebalancing`` is True.
    root : pathlib.Path, optional
        Runs root override.
    on_progress : callable, optional
        Called with a short message before each stage (for UI status boxes).

    Returns
    -------
    pathlib.Path
        The saved artifact folder.
    """

    def progress(message: str) -> None:
        if on_progress is not None:
            on_progress(message)

    if demand_source not in DEMAND_SOURCES:
        raise ValueError(f"demand_source must be one of {DEMAND_SOURCES}, got {demand_source!r}")
    if demand_source == "forecast" and not forecast_name:
        raise ValueError("demand_source='forecast' needs a forecast_name")

    homes = list(truck_homes) if truck_homes is not None else list(DEFAULT_TRUCK_HOMES)
    data = graph_data
    phases = None
    forecast_dropped_share: float | None = None
    if demand_source == "forecast":
        assert forecast_name is not None
        progress(f"Loading forecast {forecast_name} and mapping the OD matrix onto its horizon")
        forecast_demand_df, forecast_meta = forecast.load_forecast(forecast_name)
        forecast_periods_df = forecast.forecast_periods_from_meta(forecast_meta)
        # A forecast can name stations or station-hours the scenario's trip
        # CSV has never seen; those rows have no OD rows to run on, so they
        # are cut first, like the evaluation does (app/evaluate.py).
        forecast_demand_df, forecast_dropped_share = restrict_demand_to_scenario(
            forecast_demand_df, data, forecast_periods_df
        )
        if forecast_dropped_share > 0:
            progress(
                f"Cut {forecast_dropped_share:.2%} of the forecast demand: rows the "
                "scenario has no OD rows for (unknown stations or station-hours)"
            )
        data = apply_forecast_demand(data, forecast_demand_df, forecast_periods_df)
    if rebalancing:
        progress(f"Applying the truck fleet: {len(homes)} trucks")
        data = apply_truck_fleet(data, homes, truck_capacity_bikes, DEFAULT_TRUCK_RATE)
        phases = canonical_phases() + rebalancing_phases(RebalancingParams())

    progress("Sizing the state, running the simulation, checking the invariants I1-I5")
    result = run_sized_scenario(
        data,
        scenario_id=run_name,
        demand_scale_factor=demand_scale_factor,
        sizing_scale_factor=sizing_scale_factor,
        number_of_periods=number_of_periods,
        phases=phases,
        # The UI records the violation list in meta.json instead of failing
        # on a raised error.
        validate=False,
    )

    progress("Building and saving the run artifact")
    rebalancing_meta: dict = {"enabled": rebalancing}
    if rebalancing:
        rebalancing_meta["truck_homes"] = homes
        rebalancing_meta["truck_capacity_bikes"] = truck_capacity_bikes
    # A forecast run's grid and t0 are the forecast horizon's, so the artifact
    # is built from `data` (the copy the run actually used), not `graph_data`.
    return artifacts.save_scenario_run(
        result,
        data,
        run_name=run_name,
        number_of_periods=number_of_periods,
        demand_scale_factor=demand_scale_factor,
        sizing_scale_factor=sizing_scale_factor,
        rebalancing=rebalancing_meta,
        demand_source=demand_source,
        forecast_name=forecast_name,
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
    folder = run_scenario(
        graph_data,
        run_name=args.run_name,
        demand_scale_factor=args.demand_scale,
        sizing_scale_factor=args.sizing_scale,
        number_of_periods=args.periods,
        demand_source=args.demand_source,
        forecast_name=args.forecast_name,
        rebalancing=args.rebalancing,
        truck_homes=truck_homes,
        truck_capacity_bikes=args.truck_capacity,
        on_progress=print,
    )
    meta = artifacts.load_run_meta(args.run_name)
    print(f"Saved {folder}")
    print(f"Invariant violations: {len(meta.violations)}")
    print(f"Totals: {meta.totals}")


if __name__ == "__main__":
    main()
