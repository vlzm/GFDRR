"""Build, save, and load run artifacts (Notations.md §12).

A run artifact is one folder under the runs root (``data/runs/<run_name>/``)
holding everything the UI needs to draw a finished run: the priced journal,
the facility period panel, the arcs, the flow totals, the facility attributes,
and ``meta.json``. The UI only reads these files; it never runs a simulation
and never recomputes what this module can precompute.

:func:`save_scenario_run` is the one operation that turns a finished run
(a ``ScenarioRun`` plus its scenario data) into a saved artifact. The terminal
runner, the two-level evaluation and the test fixtures all call it, so which
result field feeds which builder argument is written once, here.
"""

from __future__ import annotations

import dataclasses
import datetime
import os
import pathlib
import subprocess
from typing import TYPE_CHECKING, Any

import pandas as pd
import pandera.pandas as pa
import pydantic

from gbp.model import (
    PANEL_KEYS,
    flows_to_panel,
    flows_with_measures,
    is_docking,
)
from gbp.model import (
    PANEL_VALUES as _MODEL_PANEL_VALUES,
)
from gbp.model.journal_schema import FLOW_EVENT_SCHEMA, schema_violations
from gbp.routing import Routes

if TYPE_CHECKING:
    from gbp.consumers.simulator import ScenarioRun

#: The parquet tables a run artifact holds, by file stem.
RUN_TABLES = ("flows", "panel", "arcs", "flow_totals", "facilities")

# ---------------------------------------------------------------------------
# Schemas of the run-artifact tables (Notations.md §12)
# ---------------------------------------------------------------------------
# One pandera schema per table of ``RUN_TABLES``. ``save_run`` checks each
# table against its schema before writing, so a table with a wrong column set
# fails at save time -- not later, when a page tries to draw it. Loading is
# not checked: what was written was already valid, and pages load on every
# render. The id and category columns keep their dtype unchecked where the
# sources mix pandas ``string`` and plain ``object`` columns.

#: ``flows.parquet``: the journal schema plus the measure columns added by
#: ``flows_with_measures`` (§6.1), in that order. The duration and money
#: columns are NA on rows the measure does not apply to (e.g. a stockout
#: ``lost`` rode nothing), so they are nullable; ``rate`` is not -- a missing
#: rate means the rates table lacks a commodity.
FLOWS_TABLE_SCHEMA = FLOW_EVENT_SCHEMA.add_columns(
    {
        "planned_duration_periods": pa.Column("Int64", nullable=True),
        "realized_duration_periods": pa.Column("Int64", nullable=True),
        "planned_distance_km": pa.Column("float64", nullable=True),
        "realized_distance_km": pa.Column("float64", nullable=True),
        "rate": pa.Column("float64", nullable=False),
        "elapsed_periods": pa.Column("Int64", nullable=True),
        "cost": pa.Column("Float64", nullable=True),
    }
)
FLOWS_TABLE_SCHEMA.name = "flows"

#: ``panel.parquet``: one row per (period, facility, commodity) with the
#: period's values side by side. The inventory columns are not checked for
#: sign on purpose: a run that violates I5 is still saved (the violation list
#: lives in ``meta.json``), so the panel must be storable as-is.
PANEL_TABLE_SCHEMA = pa.DataFrameSchema(
    columns={
        "period_id": pa.Column("int64", nullable=False),
        "facility_id": pa.Column(nullable=False),
        "commodity_category": pa.Column(nullable=False),
        **{value: pa.Column("int64", nullable=False) for value in _MODEL_PANEL_VALUES},
    },
    unique=list(PANEL_KEYS),
    ordered=True,
    strict=True,
    name="panel",
)

#: ``arcs.parquet``: one row per ``(flow_id, move_id)`` physical edge, with
#: the endpoint coordinates saved on the row (the trips map joins nothing).
ARCS_TABLE_SCHEMA = pa.DataFrameSchema(
    columns={
        "flow_id": pa.Column(nullable=False),
        "move_id": pa.Column("Int64", nullable=False),
        "flow_type": pa.Column(nullable=False),
        "resource_id": pa.Column(nullable=True),
        "commodity_category": pa.Column(nullable=False),
        "source_id": pa.Column(nullable=False),
        "target_id": pa.Column(nullable=False),
        "start_period": pa.Column("Int64", nullable=False),
        "end_period": pa.Column("Int64", nullable=False),
        "event_type": pa.Column(nullable=False),
        "reason": pa.Column(nullable=True),
        "quantity": pa.Column("Int64", checks=pa.Check.ge(1), nullable=False),
        "distance_km": pa.Column("float64", checks=pa.Check.ge(0), nullable=False),
        "source_lat": pa.Column("float64", nullable=False),
        "source_lng": pa.Column("float64", nullable=False),
        "target_lat": pa.Column("float64", nullable=False),
        "target_lng": pa.Column("float64", nullable=False),
    },
    unique=["flow_id", "move_id"],
    ordered=True,
    strict=True,
    name="arcs",
)

#: ``flow_totals.parquet``: one row per flow with its whole-trip values.
FLOW_TOTALS_TABLE_SCHEMA = pa.DataFrameSchema(
    columns={
        "flow_id": pa.Column(nullable=False, unique=True),
        "flow_type": pa.Column(nullable=False),
        "commodity_category": pa.Column(nullable=False),
        "source_id": pa.Column(nullable=False),
        "planned_target_id": pa.Column(nullable=False),
        "realized_target_id": pa.Column(nullable=True),
        "start_period": pa.Column("Int64", nullable=False),
        "end_period": pa.Column("Int64", nullable=False),
        "event_type": pa.Column(nullable=False),
        "reason": pa.Column(nullable=True),
        "duration_periods": pa.Column("Int64", nullable=False),
        "distance_km": pa.Column("float64", checks=pa.Check.ge(0), nullable=False),
        "cost": pa.Column("Float64", nullable=False),
    },
    ordered=True,
    strict=True,
    name="flow_totals",
)

#: ``facilities.parquet``: facility attributes for the maps. The maps read
#: every column of every row, so nothing here is nullable.
FACILITIES_TABLE_SCHEMA = pa.DataFrameSchema(
    columns={
        "facility_id": pa.Column(nullable=False, unique=True),
        "facility_category": pa.Column(nullable=False),
        "lat": pa.Column("float64", nullable=False),
        "lng": pa.Column("float64", nullable=False),
        "capacity": pa.Column(checks=pa.Check.ge(0), nullable=False),
    },
    ordered=True,
    strict=True,
    name="facilities",
)

#: Schema per ``RUN_TABLES`` stem, in save order.
RUN_TABLE_SCHEMAS = {
    "flows": FLOWS_TABLE_SCHEMA,
    "panel": PANEL_TABLE_SCHEMA,
    "arcs": ARCS_TABLE_SCHEMA,
    "flow_totals": FLOW_TOTALS_TABLE_SCHEMA,
    "facilities": FACILITIES_TABLE_SCHEMA,
}


@dataclasses.dataclass(frozen=True)
class Metric:
    """One value the UI can show, described once (Notations.md §12).

    ``PANEL_VALUES``, the UI label dictionaries, the KPI row and the whole of
    ``build_totals`` are all built from the ``METRICS`` list below. Adding a
    metric there is the only step: it cannot appear in a picker without a
    label, or miss the KPI row and the totals.

    A metric's whole-run total comes from one of two sources: a panel metric
    sums its panel column (``panel_total=True``), a flow metric aggregates a
    ``flow_totals`` column (``flow_value`` names the column, ``flow_agg`` says
    how).
    """

    name: str
    label: str  # full label; the canonical column name is kept in braces
    short: str  # short label for the map hover box
    unit: str = "count"  # "count", "dollars", "km" or "periods" -- picks the KPI format
    panel_value: bool = False  # a value column of panel.parquet
    panel_total: bool = False  # summed over the panel into meta["totals"]
    kpi: bool = False  # shown as a tile in the KPI row
    more_is_worse: bool = False  # the KPI delta turns red when it grows
    flow_value: str | None = None  # flow_totals column its total aggregates
    flow_agg: str = "sum"  # how that column is aggregated ("sum" or "mean")


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
    Metric(
        "cost",
        "Cost, $",
        "Cost, $",
        unit="dollars",
        kpi=True,
        more_is_worse=True,
        flow_value="cost",
    ),
    Metric(
        "distance_km", "Distance, km", "Distance, km", unit="km", kpi=True, flow_value="distance_km"
    ),
    Metric(
        "mean_duration_periods",
        "Mean trip duration, periods (mean_duration_periods)",
        "Mean duration",
        unit="periods",
        flow_value="duration_periods",
        flow_agg="mean",
    ),
]

#: The panel's value columns, in storage order (built from ``METRICS``).
PANEL_VALUES = [metric.name for metric in METRICS if metric.panel_value]

_DEFAULT_DATA_DIR = pathlib.Path(__file__).resolve().parents[1] / "data"


def data_dir() -> pathlib.Path:
    """Root of the data folder (raw CSV, OSRM graph, run artifacts).

    Set the ``DATA_DIR`` environment variable to point somewhere else
    (in a container the data is mounted as ``/data``); without it, this is
    ``data/`` at the repository root.
    """
    return pathlib.Path(os.environ.get("DATA_DIR", _DEFAULT_DATA_DIR))


def runs_root() -> pathlib.Path:
    """Folder that holds all run artifacts: ``<data_dir>/runs``."""
    return data_dir() / "runs"


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
    """Whole-run values for ``meta.json``, one per ``METRICS`` entry that has a total.

    A panel metric (``panel_total=True``) sums its panel column; a flow metric
    (``flow_value`` set) aggregates its ``flow_totals`` column with
    ``flow_agg``. No metric total is computed anywhere else.
    """
    totals: dict[str, float] = {
        metric.name: int(panel[metric.name].sum()) for metric in METRICS if metric.panel_total
    }
    for metric in METRICS:
        if metric.flow_value is None:
            continue
        rows = flow_totals[metric.flow_value].dropna()
        value = float(rows.agg(metric.flow_agg)) if len(rows) else 0.0
        digits = 3 if metric.flow_agg == "mean" else 2
        totals[metric.name] = round(value, digits)
    return totals


class RebalancingMeta(pydantic.BaseModel):
    """The ``rebalancing`` block of ``meta.json`` (Notations.md §12, §14).

    ``truck_homes`` and ``truck_capacity_bikes`` are written only when
    rebalancing is on; a run without it carries ``enabled=False`` alone.
    """

    enabled: bool
    truck_homes: list[str] | None = None
    truck_capacity_bikes: int | None = None


class RunMeta(pydantic.BaseModel):
    """The ``meta.json`` contract of a run artifact (Notations.md §12).

    The runner writes it and the Streamlit app reads it later, possibly with a
    different code version -- so the fields are an explicit model, not a plain
    dict. :func:`build_meta` is the only builder; :func:`load_run_meta` is the
    only reader. An artifact missing a field fails at load with a pydantic
    error naming the field, instead of a ``KeyError`` in the middle of
    rendering a page.
    """

    run_name: str
    scenario_id: str
    demand_scale_factor: float
    sizing_scale_factor: float
    number_of_periods: int
    period_len_hours: float
    routing_mode: str
    t0: str
    created_at: str
    #: Where the run's demand table came from: ``"history"`` (the replay) or
    #: ``"forecast"`` (a forecast run, Notations.md §11). Defaults keep runs
    #: saved before the forecast phase loadable.
    demand_source: str = "history"
    #: Name of the forecast artifact a forecast run used; None on history runs.
    forecast_name: str | None = None
    #: File names of the raw source files the run was built from (for the
    #: canonical pipeline: the trip CSV). Empty for runs built from a
    #: synthetic journal, like the test fixtures.
    inputs: list[str]
    #: Git commit of the code that produced the run, ``-dirty`` appended when
    #: the working tree had uncommitted changes; ``unknown`` outside git.
    code_version: str
    violations: list[str]
    rebalancing: RebalancingMeta
    #: The sized state the run started from, precomputed at save time so no
    #: reader recovers it from the panel: all bikes at period 0, and the dock
    #: capacity summed over stations. None only on artifacts saved before
    #: these fields existed.
    initial_inventory_bikes: int | None = None
    station_capacity_docks: int | None = None
    totals: dict[str, float]


def code_version() -> str:
    """Short git commit of the codebase, for the ``code_version`` of ``meta.json``.

    ``-dirty`` is appended when the working tree has uncommitted changes, so
    a run saved mid-edit is never mistaken for the committed code. Returns
    ``"unknown"`` when git is unavailable or the code is not a git checkout.
    """
    repo = pathlib.Path(__file__).resolve().parent
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout.strip()
        changes = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return f"{commit}-dirty" if changes else commit


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
    inputs: list[str],
    violations: list[str],
    rebalancing: dict[str, Any] | None = None,
    demand_source: str = "history",
    forecast_name: str | None = None,
) -> RunMeta:
    """Build the ``meta.json`` model for one run: parameters, violations, totals.

    This is the one place that defines the ``meta.json`` contract;
    :func:`save_scenario_run` is its production caller, so a saved artifact
    always carries the same fields (``t0`` and ``routing_mode`` included).
    The sized state the run started from (``initial_inventory_bikes``,
    ``station_capacity_docks``) is computed here, so readers take it from
    ``meta.json`` instead of summing the panel.

    Parameters
    ----------
    tables : dict of str to pandas.DataFrame
        The run tables from :func:`build_run_tables` (reads ``panel``,
        ``flow_totals`` and ``facilities`` for the totals and the sized
        state).
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
    inputs : list of str
        File names of the raw source files the run was built from. Pass an
        empty list for runs built from a synthetic journal (the test
        fixtures). The code version is not a parameter: :func:`code_version`
        reads it from git here, so every artifact records it the same way.
    violations : list of str
        Run-invariant violations (empty = valid).
    rebalancing : dict, optional
        The run's rebalancing settings: ``enabled`` (bool) and, when on,
        ``truck_homes`` (home depot per truck) and ``truck_capacity_bikes``.
        Defaults to ``{"enabled": False}``.
    demand_source : str, optional
        Where the demand table came from: ``"history"`` (default) or
        ``"forecast"``.
    forecast_name : str, optional
        The forecast artifact a forecast run used; pass it whenever
        ``demand_source="forecast"``, so the run names its forecast.

    Returns
    -------
    RunMeta
        The validated ``meta.json`` payload for :func:`save_run`.
    """
    panel = tables["panel"]
    facilities = tables["facilities"]
    stations = facilities[facilities["facility_category"] == "station"]
    return RunMeta(
        run_name=run_name,
        scenario_id=run_name,
        demand_scale_factor=demand_scale_factor,
        sizing_scale_factor=sizing_scale_factor,
        number_of_periods=number_of_periods,
        period_len_hours=period_len_hours,
        routing_mode=routing_mode,
        t0=pd.Timestamp(t0).isoformat(),
        created_at=datetime.datetime.now().isoformat(timespec="seconds"),
        demand_source=demand_source,
        forecast_name=forecast_name,
        inputs=list(inputs),
        code_version=code_version(),
        violations=violations,
        rebalancing=RebalancingMeta.model_validate(
            rebalancing if rebalancing is not None else {"enabled": False}
        ),
        initial_inventory_bikes=int(panel.loc[panel["period_id"] == 0, "quantity_sop"].sum()),
        station_capacity_docks=int(stations["capacity"].fillna(0).sum()),
        totals=build_totals(panel, tables["flow_totals"]),
    )


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
    # The panel is the model's read-model; selecting PANEL_VALUES (built from
    # METRICS) fails loudly at build time if the two ever name different columns.
    panel = flows_to_panel(journal, initial_inventory)[PANEL_KEYS + PANEL_VALUES]
    return {
        "flows": priced,
        "panel": panel,
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
    meta: RunMeta,
    root: pathlib.Path | None = None,
) -> pathlib.Path:
    """Write one run artifact to ``<runs root>/<run_name>/``.

    Each table is checked against its schema (``RUN_TABLE_SCHEMAS``) before
    anything is written, so a wrong table fails here, not when a page draws
    it. ``meta.json`` is written last, so a folder with a ``meta.json`` is
    always a complete artifact (``list_runs`` keys on that file).

    Parameters
    ----------
    run_name : str
        Folder name of the artifact; also the name the UI shows.
    tables : dict of str to pandas.DataFrame
        The tables to save; keys must match ``RUN_TABLES``.
    meta : RunMeta
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
    violations: list[str] = []
    for name in RUN_TABLES:
        violations += schema_violations(RUN_TABLE_SCHEMAS[name], tables[name])
    if violations:
        raise ValueError("run tables break their schemas:\n" + "\n".join(violations))
    folder = run_dir(run_name, root)
    folder.mkdir(parents=True, exist_ok=True)
    for name in RUN_TABLES:
        tables[name].to_parquet(folder / f"{name}.parquet", index=False)
    (folder / "meta.json").write_text(meta.model_dump_json(indent=2))
    return folder


def save_scenario_run(
    result: ScenarioRun,
    data: Any,
    *,
    run_name: str,
    number_of_periods: int,
    demand_scale_factor: float = 1.0,
    sizing_scale_factor: float = 1.0,
    rebalancing: dict[str, Any] | None = None,
    demand_source: str = "history",
    forecast_name: str | None = None,
    root: pathlib.Path | None = None,
) -> pathlib.Path:
    """Save one finished sized run as a run artifact: build the tables, the meta, write.

    This is the one operation that turns a run result and its scenario data
    into a saved artifact. It reads the journal, the sized state tables and
    the violations off ``result``, and the facility tables, the rates, the
    period length, the routes, ``routing_mode``, ``t0`` and ``trips_path``
    off ``data`` — no caller wires those fields by hand.

    Parameters
    ----------
    result : ScenarioRun
        A finished run from ``run_sized_scenario``.
    data : scenario data
        The scenario the run actually used. For a forecast run pass the copy
        with the forecast demand applied (its period grid and ``t0`` are the
        forecast horizon's), not the original. ``ResolvedModelData`` carries
        every field read here; a synthetic supplier must carry
        ``facilities_df``, ``facilities_geo_df``,
        ``commodities_categories_rates_df``, ``period_len``, ``routes``,
        ``routing_mode``, ``t0`` and ``trips_path`` (None when the scenario
        was built from a synthetic journal, not a raw file).
    run_name : str
        Folder name of the artifact; also the name the UI shows.
    number_of_periods : int
        How many periods the run stepped.
    demand_scale_factor, sizing_scale_factor : float, optional
        The run's demand multipliers (see :func:`build_meta`).
    rebalancing : dict, optional
        The run's rebalancing settings (see :func:`build_meta`).
    demand_source : str, optional
        Where the demand table came from: ``"history"`` (default) or
        ``"forecast"``.
    forecast_name : str, optional
        The forecast artifact a forecast run used.
    root : pathlib.Path, optional
        Runs root override (defaults to :func:`runs_root`).

    Returns
    -------
    pathlib.Path
        The saved artifact folder.
    """
    tables = build_run_tables(
        result.simulated_flows_df,
        initial_inventory=result.initial_inventory_df,
        facilities=data.facilities_df,
        facilities_geo=data.facilities_geo_df,
        facilities_capacities=result.facilities_capacities_df,
        rates=data.commodities_categories_rates_df,
        period_len=data.period_len,
        routes=data.routes,
    )
    meta = build_meta(
        tables,
        run_name=run_name,
        demand_scale_factor=demand_scale_factor,
        sizing_scale_factor=sizing_scale_factor,
        number_of_periods=number_of_periods,
        period_len_hours=data.period_len / pd.Timedelta(hours=1),
        routing_mode=data.routing_mode,
        t0=data.t0,
        inputs=[pathlib.Path(data.trips_path).name] if data.trips_path else [],
        violations=result.violations,
        rebalancing=rebalancing,
        demand_source=demand_source,
        forecast_name=forecast_name,
    )
    return save_run(run_name, tables, meta, root)


def load_run_table(run_name: str, table: str, root: pathlib.Path | None = None) -> pd.DataFrame:
    """Read one parquet table of a saved run (``table`` is a ``RUN_TABLES`` stem)."""
    if table not in RUN_TABLES:
        raise ValueError(f"unknown run table {table!r}; expected one of {RUN_TABLES}")
    return pd.read_parquet(run_dir(run_name, root) / f"{table}.parquet")


def load_run_meta(run_name: str, root: pathlib.Path | None = None) -> RunMeta:
    """Read a saved run's ``meta.json``, validated against :class:`RunMeta`.

    An artifact missing a field fails here, at load, with a pydantic error
    naming the field -- not later, while a page renders.
    """
    return RunMeta.model_validate_json((run_dir(run_name, root) / "meta.json").read_text())
