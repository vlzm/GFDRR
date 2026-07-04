"""Run one scenario end to end and save its run artifact.

The pipeline is the canonical one from ``notebooks/test_pipeline.ipynb``:
load the raw data, resolve the graph tables, size the initial inventory and
dock capacities against the sizing demand, run the scaled demand, validate
the run invariants, and save every table the UI reads (Notations.md §12).

Terminal use::

    python app/runner.py --run-name demand_x2 --demand-scale 2.0 --periods 50
"""

from __future__ import annotations

import argparse
import pathlib
from collections.abc import Callable

import artifacts
import pandas as pd

from gbp.consumers.simulator import run_sized_scenario
from gbp.loaders.dataloader_graph import ResolvedModelData
from gbp.loaders.dataloader_raw import RawModelData
from gbp.routing import DEFAULT_OSRM_URL, ROUTING_MODES

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

DEFAULT_TRIPS_PATH = str(_REPO_ROOT / "data" / "raw" / "202602-citibike-tripdata_1.csv")
DEFAULT_NUMBER_OF_PERIODS = 50


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
        n_depots=10,
        depot_capacity=9000,
        n_trucks=5,
        truck_capacity_bikes=20,
        truck_rate=50.0,
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
    root: pathlib.Path | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> pathlib.Path:
    """Size, run, validate, and save one scenario as a run artifact.

    The state (initial inventory and dock capacities) is sized against
    ``sizing_scale_factor``; the run itself faces ``demand_scale_factor``.
    Equal values give a clean, no-loss run; a larger run scale makes the
    limits take effect (stockout and dock-full events appear).

    Parameters
    ----------
    graph_data : ResolvedModelData
        The resolved scenario data. Not modified: the sized state stays inside
        :func:`run_sized_scenario` and is saved into the artifact. The Run page
        shares one cached ``graph_data`` across runs, so this must hold.
    run_name : str
        Name of the artifact folder (and of the run in the UI).
    demand_scale_factor : float
        Demand multiplier the run faces.
    sizing_scale_factor : float, optional
        Demand multiplier the state is sized to survive with no loss.
    number_of_periods : int, optional
        How many periods to step.
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

    progress("Sizing the state, running the simulation, checking the invariants I1-I5")
    result = run_sized_scenario(
        graph_data,
        scenario_id=run_name,
        demand_scale_factor=demand_scale_factor,
        sizing_scale_factor=sizing_scale_factor,
        number_of_periods=number_of_periods,
        # The UI records the violation list in meta.json instead of failing
        # on a raised error.
        validate=False,
    )

    progress("Building and saving the run artifact")
    tables = artifacts.build_run_tables(
        result.simulated_flows_df,
        initial_inventory=result.initial_inventory_df,
        facilities=graph_data.facilities_df,
        facilities_geo=graph_data.facilities_geo_df,
        facilities_capacities=result.facilities_capacities_df,
        rates=graph_data.commodities_categories_rates_df,
        period_len=graph_data.period_len,
        routes=graph_data.routes,
    )
    meta = artifacts.build_meta(
        tables,
        run_name=run_name,
        demand_scale_factor=demand_scale_factor,
        sizing_scale_factor=sizing_scale_factor,
        number_of_periods=number_of_periods,
        period_len_hours=graph_data.period_len / pd.Timedelta(hours=1),
        routing_mode=graph_data.routing_mode,
        t0=graph_data.t0,
        violations=result.violations,
    )
    return artifacts.save_run(run_name, tables, meta, root)


def main() -> None:
    """Terminal entry point: parse arguments, run one scenario, save it."""
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
    folder = run_scenario(
        graph_data,
        run_name=args.run_name,
        demand_scale_factor=args.demand_scale,
        sizing_scale_factor=args.sizing_scale,
        number_of_periods=args.periods,
        on_progress=print,
    )
    meta = artifacts.load_run_meta(args.run_name)
    print(f"Saved {folder}")
    print(f"Invariant violations: {len(meta['violations'])}")
    print(f"Totals: {meta['totals']}")


if __name__ == "__main__":
    main()
