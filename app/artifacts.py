"""Build, save, and load run artifacts (Notations.md §12).

A run artifact is one folder under the runs root (``data/runs/<run_name>/``)
holding everything the UI needs to draw a finished run: the priced journal,
the facility period panel, the arcs, the flow totals, the facility attributes,
and ``meta.json``. The UI only reads these files; it never runs a simulation
and never recomputes what this module can precompute.
"""

from __future__ import annotations

import json
import os
import pathlib
from typing import Any

import pandas as pd

from gbp.model import (
    flows_with_costs,
    get_inventory_df,
    is_docking,
    is_user_departure,
)
from gbp.routing import Routes

#: The parquet tables a run artifact holds, by file stem.
RUN_TABLES = ("flows", "panel", "arcs", "flow_totals", "facilities")

#: The panel's row key.
PANEL_KEYS = ["period_id", "facility_id", "commodity_category"]

#: The panel's value columns, in storage order.
PANEL_VALUES = [
    "quantity_sop",
    "quantity_eop",
    "demand",
    "departed",
    "arrived",
    "redirected",
    "lost_demand",
    "lost_dock_full",
]

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

    lost = flows["event_type"] == "lost"
    counts: dict[str, tuple[pd.Series, str]] = {
        "departed": (is_user_departure(flows), "source_id"),
        "arrived": (is_docking(flows), "realized_target_id"),
        "redirected": (flows["event_type"] == "redirected", "planned_target_id"),
        "lost_demand": (lost & (flows["reason"] == "stockout"), "source_id"),
        "lost_dock_full": (lost & (flows["reason"] == "dock_full"), "planned_target_id"),
    }
    for name, (mask, facility_col) in counts.items():
        grouped = (
            flows.loc[mask]
            .groupby(["period_id", facility_col, "commodity_category"], as_index=False)["quantity"]
            .sum()
            .rename(columns={facility_col: "facility_id", "quantity": name})
        )
        panel = panel.merge(grouped, on=PANEL_KEYS, how="left")
        panel[name] = panel[name].fillna(0).astype("int64")
        # The inventory grid must cover every event; a mismatch means an event
        # happened at a (facility, commodity) pair the grid does not know.
        if int(panel[name].sum()) != int(flows.loc[mask, "quantity"].sum()):
            raise ValueError(f"panel dropped {name} events outside the inventory grid")

    panel["demand"] = panel["departed"] + panel["lost_demand"]
    return panel[PANEL_KEYS + PANEL_VALUES]


def build_arcs(flows: pd.DataFrame, routes: Routes) -> pd.DataFrame:
    """One row per arc: a ``(flow_id, move_id)`` physical edge of a trip.

    Pairs each arc's opening ``departed`` with the event that closed the arc
    (``arrived``, ``redirected`` or ``lost``). A stockout ``lost`` has no
    ``departed`` row, so it produces no arc. ``target_id`` is where the arc
    actually ended: the realized target when it docked, the planned target
    when it bounced or was lost there. ``distance_km`` is the length of the
    edge, measured by the run's routing mode (straight line or OSRM road
    network).

    Parameters
    ----------
    flows : pandas.DataFrame
        A finalized flow-event log.
    routes : gbp.routing.Routes
        The scenario's distance / travel-time answerer.

    Returns
    -------
    pandas.DataFrame
        Columns ``flow_id``, ``move_id``, ``commodity_category``,
        ``source_id``, ``target_id``, ``start_period``, ``end_period``,
        ``event_type`` (the closing outcome), ``reason``, ``quantity``,
        ``distance_km``.
    """
    opened = flows.loc[
        flows["event_type"] == "departed",
        ["flow_id", "move_id", "commodity_category", "source_id", "quantity", "period_id"],
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
    return arcs[
        [
            "flow_id",
            "move_id",
            "commodity_category",
            "source_id",
            "target_id",
            "start_period",
            "end_period",
            "event_type",
            "reason",
            "quantity",
            "distance_km",
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
        The journal widened by :func:`gbp.model.flows_with_costs`.
    arcs : pandas.DataFrame
        The arcs table from :func:`build_arcs`.

    Returns
    -------
    pandas.DataFrame
        Columns ``flow_id``, ``commodity_category``, ``source_id``,
        ``planned_target_id``, ``realized_target_id``, ``start_period``,
        ``end_period``, ``event_type``, ``reason``, ``duration_periods``,
        ``distance_km``, ``cost``.
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
        .loc[:, ["flow_id", "source_id", "planned_target_id", "period_id"]]
        .rename(columns={"period_id": "start_period"})
    )
    totals = opening.merge(terminal, on="flow_id", how="inner")

    distance = arcs.groupby("flow_id", as_index=False)["distance_km"].sum()
    totals = totals.merge(distance, on="flow_id", how="left")
    return totals[
        [
            "flow_id",
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
        name: int(panel[name].sum())
        for name in ["demand", "departed", "arrived", "redirected", "lost_demand", "lost_dock_full"]
    }
    totals["cost"] = round(float(flow_totals["cost"].sum()), 2)
    totals["distance_km"] = round(float(flow_totals["distance_km"].sum()), 2)
    duration = flow_totals["duration_periods"].dropna()
    totals["mean_duration_periods"] = round(float(duration.mean()), 3) if len(duration) else 0.0
    return totals


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
    priced = flows_with_costs(journal, rates, period_len)
    arcs = build_arcs(journal, routes)
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
