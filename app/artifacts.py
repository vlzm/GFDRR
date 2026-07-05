"""Build, save, and load run artifacts (Notations.md §12).

A run artifact is one folder under the runs root (``data/runs/<run_name>/``)
holding everything the UI needs to draw a finished run: the priced journal,
the facility period panel, the arcs, the flow totals, the facility attributes,
and ``meta.json``. The UI only reads these files; it never runs a simulation
and never recomputes what this module can precompute.
"""

from __future__ import annotations

import dataclasses
import datetime
import json
import os
import pathlib
from typing import Any

import pandas as pd

from gbp.model import (
    flows_to_arrivals,
    flows_to_departures,
    flows_to_losses,
    flows_to_redirects,
    flows_with_measures,
    get_inventory_df,
    is_docking,
)
from gbp.routing import Routes

#: The parquet tables a run artifact holds, by file stem.
RUN_TABLES = ("flows", "panel", "arcs", "flow_totals", "facilities")

#: The panel's row key.
PANEL_KEYS = ["period_id", "facility_id", "commodity_category"]


@dataclasses.dataclass(frozen=True)
class Metric:
    """One value the UI can show, described once (Notations.md §12).

    ``PANEL_VALUES``, the UI label dictionaries, the KPI row and the panel
    part of ``build_totals`` are all built from the ``METRICS`` list below.
    Adding a metric there is the only step: it cannot appear in a picker
    without a label, or miss the KPI row and the totals.
    """

    name: str
    label: str  # full label; the canonical column name is kept in braces
    short: str  # short label for the map hover box
    unit: str = "count"  # "count", "dollars" or "km" -- picks the KPI format
    panel_value: bool = False  # a value column of panel.parquet
    panel_total: bool = False  # summed over the panel into meta["totals"]
    kpi: bool = False  # shown as a tile in the KPI row
    more_is_worse: bool = False  # the KPI delta turns red when it grows


#: Every metric of a run, in display and storage order.
METRICS = [
    Metric(
        "quantity_sop",
        "Inventory at period start (quantity_sop)",
        "Inventory, start",
        panel_value=True,
    ),
    Metric(
        "quantity_eop",
        "Inventory at period end (quantity_eop)",
        "Inventory, end",
        panel_value=True,
    ),
    Metric("demand", "Demand (demand)", "Demand", panel_value=True, panel_total=True, kpi=True),
    Metric(
        "departed", "Departed (departed)", "Departed", panel_value=True, panel_total=True, kpi=True
    ),
    Metric("arrived", "Arrived (arrived)", "Arrived", panel_value=True, panel_total=True, kpi=True),
    Metric(
        "redirected",
        "Redirected (redirected)",
        "Redirected",
        panel_value=True,
        panel_total=True,
        kpi=True,
        more_is_worse=True,
    ),
    Metric(
        "lost_demand",
        "Lost demand (lost_demand)",
        "Lost (stockout)",
        panel_value=True,
        panel_total=True,
        kpi=True,
        more_is_worse=True,
    ),
    Metric(
        "lost_dock_full",
        "Lost at full docks (lost_dock_full)",
        "Lost (dock_full)",
        panel_value=True,
        panel_total=True,
        kpi=True,
        more_is_worse=True,
    ),
    Metric("cost", "Cost, $", "Cost, $", unit="dollars", kpi=True, more_is_worse=True),
    Metric("distance_km", "Distance, km", "Distance, km", unit="km", kpi=True),
]

#: The panel's value columns, in storage order (built from ``METRICS``).
PANEL_VALUES = [metric.name for metric in METRICS if metric.panel_value]

_DEFAULT_RUNS_ROOT = pathlib.Path(__file__).resolve().parents[1] / "data" / "runs"


def runs_root() -> pathlib.Path:
    """Folder that holds all run artifacts; override with ``GBP_RUNS_ROOT``."""
    return pathlib.Path(os.environ.get("GBP_RUNS_ROOT", _DEFAULT_RUNS_ROOT))


def run_dir(run_name: str, root: pathlib.Path | None = None) -> pathlib.Path:
    """Folder of one run artifact."""
    return (root or runs_root()) / run_name


def list_runs(root: pathlib.Path | None = None) -> list[str]:
    """Names of every saved run (folders with a ``meta.json``), sorted."""
    base = root or runs_root()
    if not base.exists():
        return []
    return sorted(p.name for p in base.iterdir() if (p / "meta.json").exists())


def next_free_run_name(base: str, root: pathlib.Path | None = None) -> str:
    """Return ``base`` if no saved run has that name, else ``base_version_{i}``.

    ``i`` counts up from 2, so re-running the same parameters gives
    ``name``, ``name_version_2``, ``name_version_3``, ... and a saved run is
    never overwritten.
    """
    taken = set(list_runs(root))
    if base not in taken:
        return base
    i = 2
    while f"{base}_version_{i}" in taken:
        i += 1
    return f"{base}_version_{i}"


# ---------------------------------------------------------------------------
# Builders: journal -> the tables the UI reads
# ---------------------------------------------------------------------------
def build_panel(flows: pd.DataFrame, initial_inventory: pd.DataFrame) -> pd.DataFrame:
    """Build the facility period panel from a finalized journal.

    One row per ``(period_id, facility_id, commodity_category)`` with the
    period's values side by side: start/end inventory (``quantity_sop`` /
    ``quantity_eop``), ``demand``, ``departed``, ``arrived``, ``redirected``
    (bounces at this facility as the full planned target), ``lost_demand``
    (stockout losses at the source) and ``lost_dock_full`` (losses at the full
    planned target). ``demand = departed + lost_demand``.

    Parameters
    ----------
    flows : pandas.DataFrame
        A finalized flow-event log (it must carry ``step_id``).
    initial_inventory : pandas.DataFrame
        Starting inventory: ``facility_id``, ``commodity_category``, ``quantity``.

    Returns
    -------
    pandas.DataFrame
        The panel, keyed by ``PANEL_KEYS`` with ``PANEL_VALUES`` columns.
    """
    panel = get_inventory_df(flows, initial_inventory)

    counts: dict[str, pd.DataFrame] = {
        "departed": flows_to_departures(flows),
        "arrived": flows_to_arrivals(flows),
        "redirected": flows_to_redirects(flows),
        "lost_demand": flows_to_losses(flows, "stockout"),
        "lost_dock_full": flows_to_losses(flows, "dock_full"),
    }
    for name, grouped in counts.items():
        grouped = grouped.rename(columns={"quantity": name})
        panel = panel.merge(grouped, on=PANEL_KEYS, how="left")
        panel[name] = panel[name].fillna(0).astype("int64")
        # The inventory grid must cover every event; a mismatch means an event
        # happened at a (facility, commodity) pair the grid does not know.
        if int(panel[name].sum()) != int(grouped[name].sum()):
            raise ValueError(f"panel dropped {name} events outside the inventory grid")

    panel["demand"] = panel["departed"] + panel["lost_demand"]
    return panel[PANEL_KEYS + PANEL_VALUES]


def build_arcs(flows: pd.DataFrame, routes: Routes, facilities_geo: pd.DataFrame) -> pd.DataFrame:
    """One row per arc: a ``(flow_id, move_id)`` physical edge of a trip.

    Pairs each arc's opening ``departed`` with the event that closed the arc
    (``arrived``, ``redirected`` or ``lost``). A stockout ``lost`` has no
    ``departed`` row, so it produces no arc. ``target_id`` is where the arc
    actually ended: the realized target when it docked, the planned target
    when it bounced or was lost there. ``distance_km`` is the length of the
    edge, measured by the run's routing mode (straight line or OSRM road
    network). The endpoint coordinates are saved on each row, so the trips
    map draws arcs without joining another table. ``flow_type`` tells a user
    ride (``user_trip``) from a bike carried by a truck (``rebalance``);
    ``resource_id`` is the truck on a rebalance arc, NA otherwise.

    Parameters
    ----------
    flows : pandas.DataFrame
        A finalized flow-event log.
    routes : gbp.routing.Routes
        The scenario's distance / travel-time answerer.
    facilities_geo : pandas.DataFrame
        Facility coordinates: ``facility_id``, ``lat``, ``lng``.

    Returns
    -------
    pandas.DataFrame
        Columns ``flow_id``, ``move_id``, ``flow_type``, ``resource_id``,
        ``commodity_category``, ``source_id``, ``target_id``,
        ``start_period``, ``end_period``, ``event_type`` (the closing
        outcome), ``reason``, ``quantity``, ``distance_km``, ``source_lat``,
        ``source_lng``, ``target_lat``, ``target_lng``.
    """
    opened = flows.loc[
        flows["event_type"] == "departed",
        [
            "flow_id",
            "move_id",
            "flow_type",
            "resource_id",
            "commodity_category",
            "source_id",
            "quantity",
            "period_id",
        ],
    ].rename(columns={"period_id": "start_period"})
    closed = flows.loc[
        flows["event_type"].isin(["arrived", "redirected", "lost"]),
        [
            "flow_id",
            "move_id",
            "event_type",
            "reason",
            "planned_target_id",
            "realized_target_id",
            "period_id",
        ],
    ].rename(columns={"period_id": "end_period"})

    arcs = opened.merge(closed, on=["flow_id", "move_id"], how="inner")
    arcs["target_id"] = arcs["realized_target_id"].fillna(arcs["planned_target_id"])

    arcs["distance_km"] = routes.distance_km(arcs["source_id"], arcs["target_id"])
    geo = facilities_geo.set_index("facility_id")
    for role in ("source", "target"):
        arcs[f"{role}_lat"] = arcs[f"{role}_id"].map(geo["lat"])
        arcs[f"{role}_lng"] = arcs[f"{role}_id"].map(geo["lng"])
    return arcs[
        [
            "flow_id",
            "move_id",
            "flow_type",
            "resource_id",
            "commodity_category",
            "source_id",
            "target_id",
            "start_period",
            "end_period",
            "event_type",
            "reason",
            "quantity",
            "distance_km",
            "source_lat",
            "source_lng",
            "target_lat",
            "target_lng",
        ]
    ]


def build_flow_totals(priced_flows: pd.DataFrame, arcs: pd.DataFrame) -> pd.DataFrame:
    """One row per flow with its whole-trip values.

    The row joins three sources: the flow's opening event (origin ``source_id``,
    original ``planned_target_id``, ``start_period``), its terminal event (the
    docking ``arrived`` or a dock-full ``lost``: outcome, ``reason``,
    ``end_period``, ``duration_periods``, ``cost``), and the sum of its arcs'
    ``distance_km``.

    Two kinds of rows are not here. A stockout loss has no flow at all
    (``flow_id`` is NA — the trip never departed); it lives in the panel as
    ``lost_demand`` only. A flow still riding when the run ends has no
    terminal event yet, so it has no whole-trip values to report.

    Parameters
    ----------
    priced_flows : pandas.DataFrame
        The journal widened by :func:`gbp.model.flows_with_measures`.
    arcs : pandas.DataFrame
        The arcs table from :func:`build_arcs`.

    Returns
    -------
    pandas.DataFrame
        Columns ``flow_id``, ``flow_type`` (``user_trip`` or ``rebalance``),
        ``commodity_category``, ``source_id``, ``planned_target_id``,
        ``realized_target_id``, ``start_period``, ``end_period``,
        ``event_type``, ``reason``, ``duration_periods``, ``distance_km``,
        ``cost``.
    """
    # A stockout loss has flow_id NA (no flow ever existed) -- drop those rows
    # here; the panel's lost_demand column is their home.
    flows = priced_flows[priced_flows["flow_id"].notna()]
    terminal = flows.loc[
        is_docking(flows) | (flows["event_type"] == "lost"),
        [
            "flow_id",
            "commodity_category",
            "realized_target_id",
            "event_type",
            "reason",
            "period_id",
            "elapsed_periods",
            "cost",
        ],
    ].rename(columns={"period_id": "end_period", "elapsed_periods": "duration_periods"})
    if terminal["flow_id"].duplicated().any():
        raise ValueError("a flow has more than one terminal event")

    opening = (
        flows.sort_values("event_id")
        .drop_duplicates("flow_id")
        .loc[:, ["flow_id", "flow_type", "source_id", "planned_target_id", "period_id"]]
        .rename(columns={"period_id": "start_period"})
    )
    totals = opening.merge(terminal, on="flow_id", how="inner")

    distance = arcs.groupby("flow_id", as_index=False)["distance_km"].sum()
    totals = totals.merge(distance, on="flow_id", how="left")
    return totals[
        [
            "flow_id",
            "flow_type",
            "commodity_category",
            "source_id",
            "planned_target_id",
            "realized_target_id",
            "start_period",
            "end_period",
            "event_type",
            "reason",
            "duration_periods",
            "distance_km",
            "cost",
        ]
    ]


def build_facilities(
    facilities: pd.DataFrame,
    facilities_geo: pd.DataFrame,
    facilities_capacities: pd.DataFrame,
) -> pd.DataFrame:
    """Facility attributes for the maps: category, coordinates, capacity."""
    return facilities.merge(facilities_geo, on="facility_id", how="left").merge(
        facilities_capacities[["facility_id", "capacity"]], on="facility_id", how="left"
    )


def build_totals(panel: pd.DataFrame, flow_totals: pd.DataFrame) -> dict[str, float]:
    """Whole-run sums for ``meta.json``: the numbers the KPI row shows."""
    totals: dict[str, float] = {
        metric.name: int(panel[metric.name].sum()) for metric in METRICS if metric.panel_total
    }
    totals["cost"] = round(float(flow_totals["cost"].sum()), 2)
    totals["distance_km"] = round(float(flow_totals["distance_km"].sum()), 2)
    duration = flow_totals["duration_periods"].dropna()
    totals["mean_duration_periods"] = round(float(duration.mean()), 3) if len(duration) else 0.0
    return totals


def build_meta(
    tables: dict[str, pd.DataFrame],
    *,
    run_name: str,
    demand_scale_factor: float,
    sizing_scale_factor: float,
    number_of_periods: int,
    period_len_hours: float,
    routing_mode: str,
    t0: Any,
    violations: list[str],
    rebalancing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build ``meta.json`` for one run: parameters, violations, and totals.

    This is the one place that defines the ``meta.json`` contract; the runner
    and the test fixtures both call it, so a saved artifact always carries the
    same fields (``t0`` and ``routing_mode`` included).

    Parameters
    ----------
    tables : dict of str to pandas.DataFrame
        The run tables from :func:`build_run_tables` (reads ``panel`` and
        ``flow_totals`` for the totals).
    run_name : str
        Folder name of the artifact; also written as ``scenario_id``.
    demand_scale_factor, sizing_scale_factor : float
        The run's demand multipliers.
    number_of_periods : int
        How many periods the run stepped.
    period_len_hours : float
        Wall-clock length of one period, in hours.
    routing_mode : str
        How distances were measured (``"haversine"`` or ``"osrm"``).
    t0 : timestamp-like
        Wall-clock start of period 0; the UI turns period ids into times with
        it. Anything ``pandas.Timestamp`` accepts.
    violations : list of str
        Run-invariant violations (empty = valid).
    rebalancing : dict, optional
        The run's rebalancing settings: ``enabled`` (bool) and, when on,
        ``truck_homes`` (home depot per truck) and ``truck_capacity_bikes``.
        Defaults to ``{"enabled": False}``.

    Returns
    -------
    dict
        The ``meta.json`` payload for :func:`save_run`.
    """
    return {
        "run_name": run_name,
        "scenario_id": run_name,
        "demand_scale_factor": demand_scale_factor,
        "sizing_scale_factor": sizing_scale_factor,
        "number_of_periods": number_of_periods,
        "period_len_hours": period_len_hours,
        "routing_mode": routing_mode,
        "t0": pd.Timestamp(t0).isoformat(),
        "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "violations": violations,
        "rebalancing": rebalancing if rebalancing is not None else {"enabled": False},
        "totals": build_totals(tables["panel"], tables["flow_totals"]),
    }


def build_run_tables(
    journal: pd.DataFrame,
    *,
    initial_inventory: pd.DataFrame,
    facilities: pd.DataFrame,
    facilities_geo: pd.DataFrame,
    facilities_capacities: pd.DataFrame,
    rates: pd.DataFrame,
    period_len: pd.Timedelta,
    routes: Routes,
) -> dict[str, pd.DataFrame]:
    """Build every run-artifact table from one finalized journal.

    Parameters
    ----------
    journal : pandas.DataFrame
        The finalized flow journal of the run.
    initial_inventory : pandas.DataFrame
        The initial inventory the run started from.
    facilities, facilities_geo, facilities_capacities : pandas.DataFrame
        The facility attribute tables of the scenario.
    rates : pandas.DataFrame
        Per-commodity price: ``commodity_category``, ``rate``.
    period_len : pandas.Timedelta
        Wall-clock length of one period (prices periods into dollars).
    routes : gbp.routing.Routes
        The scenario's distance / travel-time answerer (the arc distances).

    Returns
    -------
    dict of str to pandas.DataFrame
        The five tables of ``RUN_TABLES``, keyed by file stem.
    """
    priced = flows_with_measures(journal, routes=routes, rates=rates, period_len=period_len)
    arcs = build_arcs(journal, routes, facilities_geo)
    return {
        "flows": priced,
        "panel": build_panel(journal, initial_inventory),
        "arcs": arcs,
        "flow_totals": build_flow_totals(priced, arcs),
        "facilities": build_facilities(facilities, facilities_geo, facilities_capacities),
    }


# ---------------------------------------------------------------------------
# Save / load
# ---------------------------------------------------------------------------
def save_run(
    run_name: str,
    tables: dict[str, pd.DataFrame],
    meta: dict[str, Any],
    root: pathlib.Path | None = None,
) -> pathlib.Path:
    """Write one run artifact to ``<runs root>/<run_name>/``.

    ``meta.json`` is written last, so a folder with a ``meta.json`` is always
    a complete artifact (``list_runs`` keys on that file).

    Parameters
    ----------
    run_name : str
        Folder name of the artifact; also the name the UI shows.
    tables : dict of str to pandas.DataFrame
        The tables to save; keys must match ``RUN_TABLES``.
    meta : dict
        Run parameters, invariant violations, and totals.
    root : pathlib.Path, optional
        Runs root override (defaults to :func:`runs_root`).

    Returns
    -------
    pathlib.Path
        The artifact folder.
    """
    missing = set(RUN_TABLES) - set(tables)
    if missing:
        raise ValueError(f"missing run tables: {sorted(missing)}")
    folder = run_dir(run_name, root)
    folder.mkdir(parents=True, exist_ok=True)
    for name in RUN_TABLES:
        tables[name].to_parquet(folder / f"{name}.parquet", index=False)
    (folder / "meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    return folder


def load_run_table(run_name: str, table: str, root: pathlib.Path | None = None) -> pd.DataFrame:
    """Read one parquet table of a saved run (``table`` is a ``RUN_TABLES`` stem)."""
    if table not in RUN_TABLES:
        raise ValueError(f"unknown run table {table!r}; expected one of {RUN_TABLES}")
    return pd.read_parquet(run_dir(run_name, root) / f"{table}.parquet")


def load_run_meta(run_name: str, root: pathlib.Path | None = None) -> dict[str, Any]:
    """Read a saved run's ``meta.json``."""
    return json.loads((run_dir(run_name, root) / "meta.json").read_text())
