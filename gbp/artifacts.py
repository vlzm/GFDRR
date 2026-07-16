"""Build, save, and load run artifacts."""

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

from gbp.loaders.dataloader_graph import PeriodGrid
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
    from gbp.consumers.run import RunRequest
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
    """One value the UI can show, described once."""

    name: str
    title: str  # display words alone, no column name (the KPI tile label)
    short: str  # short label for the map hover box
    unit: str = "count"  # "count", "dollars", "km" or "periods" -- picks the KPI format
    panel_value: bool = False  # a value column of panel.parquet
    inventory: bool = False  # an inventory level at a moment, not a per-period count
    panel_total: bool = False  # summed over the panel into meta["totals"]
    kpi: bool = False  # shown as a tile in the KPI row
    more_is_worse: bool = False  # the KPI delta turns red when it grows
    flow_value: str | None = None  # flow_totals column its total aggregates
    flow_agg: str = "sum"  # how that column is aggregated ("sum" or "mean")

    @property
    def label(self) -> str:
        """Full picker label: the title plus the canonical column name in parentheses."""
        return f"{self.title} ({self.name})"


#: Every metric of a run, in display and storage order.
METRICS = [
    Metric(
        "quantity_sop",
        "Inventory at period start",
        "Inventory, start",
        panel_value=True,
        inventory=True,
    ),
    Metric(
        "quantity_eop",
        "Inventory at period end",
        "Inventory, end",
        panel_value=True,
        inventory=True,
    ),
    Metric("demand", "Demand", "Demand", panel_value=True, panel_total=True, kpi=True),
    Metric("departed", "Departed", "Departed", panel_value=True, panel_total=True, kpi=True),
    Metric("arrived", "Arrived", "Arrived", panel_value=True, panel_total=True, kpi=True),
    Metric(
        "redirected",
        "Redirected",
        "Redirected",
        panel_value=True,
        panel_total=True,
        kpi=True,
        more_is_worse=True,
    ),
    Metric(
        "lost_demand",
        "Lost demand",
        "Lost (stockout)",
        panel_value=True,
        panel_total=True,
        kpi=True,
        more_is_worse=True,
    ),
    Metric(
        "lost_dock_full",
        "Lost at full docks",
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
        "Mean trip duration, periods",
        "Mean duration",
        unit="periods",
        flow_value="duration_periods",
        flow_agg="mean",
    ),
]

#: The panel's value columns, in storage order (built from ``METRICS``).
PANEL_VALUES = [metric.name for metric in METRICS if metric.panel_value]

#: The panel's value columns that count what happened during the period --
#: every panel value except the inventory levels (``inventory=True``).
PANEL_FLOW_VALUES = [m.name for m in METRICS if m.panel_value and not m.inventory]

_DEFAULT_DATA_DIR = pathlib.Path(__file__).resolve().parents[1] / "data"


def data_dir() -> pathlib.Path:
    """Root of the data folder (the ``DATA_DIR`` env var, else ``data/`` at the repo root)."""
    return pathlib.Path(os.environ.get("DATA_DIR", _DEFAULT_DATA_DIR))


def runs_root() -> pathlib.Path:
    """Folder that holds all run artifacts: ``<data_dir>/runs``."""
    return data_dir() / "runs"


def run_dir(run_name: str, root: pathlib.Path | None = None) -> pathlib.Path:
    """Folder of one run artifact."""
    return (root or runs_root()) / run_name


def table_path(run_name: str, table: str, root: pathlib.Path | None = None) -> pathlib.Path:
    """File of one saved table: ``<run folder>/<table>.parquet`` (``table`` a RUN_TABLES stem)."""
    if table not in RUN_TABLES:
        raise ValueError(f"unknown run table {table!r}; expected one of {RUN_TABLES}")
    return run_dir(run_name, root) / f"{table}.parquet"


def meta_path(run_name: str, root: pathlib.Path | None = None) -> pathlib.Path:
    """File of one saved run's ``meta.json`` (its existence marks a complete artifact)."""
    return run_dir(run_name, root) / "meta.json"


def list_runs(root: pathlib.Path | None = None) -> list[str]:
    """Names of every saved run (folders with a ``meta.json``), sorted."""
    base = root or runs_root()
    if not base.exists():
        return []
    return sorted(p.name for p in base.iterdir() if meta_path(p.name, base).exists())


def next_free_run_name(base: str, root: pathlib.Path | None = None) -> str:
    """Return ``base`` if free, else ``base_version_{i}`` counting from 2 (never overwrites)."""
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
    """One row per arc: a ``(flow_id, move_id)`` physical edge of a trip."""
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
    """One row per flow with its whole-trip values (two terminal events raise: a double close)."""
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
    """Whole-run values for ``meta.json``, one per ``METRICS`` entry that has a total."""
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
    """The ``rebalancing`` block of ``meta.json``."""

    enabled: bool
    truck_homes: list[str] | None = None
    truck_capacity_bikes: int | None = None


class RunMeta(pydantic.BaseModel):
    """The ``meta.json`` contract of a run artifact."""

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
    #: Share of the forecast demand total cut before the run because the
    #: scenario has no OD rows for it (stations or station-hours the trip CSV
    #: has never seen). 0.0 when nothing was cut; None on history runs and on
    #: artifacts saved before this field existed.
    forecast_dropped_share: float | None = None
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
    """Short git commit of the codebase (``-dirty`` if uncommitted, ``"unknown"`` outside git)."""
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
    forecast_dropped_share: float | None = None,
) -> RunMeta:
    """Build the ``meta.json`` model for one run: parameters, violations, totals."""
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
        forecast_dropped_share=forecast_dropped_share,
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
    """Build every run-artifact table from one finalized journal."""
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
    """Write one run artifact (each table schema-checked first, ``meta.json`` written last)."""
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
        tables[name].to_parquet(table_path(run_name, name, root), index=False)
    meta_path(run_name, root).write_text(meta.model_dump_json(indent=2))
    return folder


def save_scenario_run(
    result: ScenarioRun,
    data: Any,
    request: RunRequest,
    *,
    forecast_dropped_share: float | None = None,
    root: pathlib.Path | None = None,
) -> pathlib.Path:
    """Save one finished sized run as a run artifact: build the tables, the meta, write."""
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
    # The run's horizon is one period grid; meta.json stores its three facts
    # (t0, number_of_periods, period_len_hours) as separate fields, all read
    # off the same PeriodGrid so they cannot disagree.
    run_grid = PeriodGrid(data.t0, request.number_of_periods, data.period_len)
    meta = build_meta(
        tables,
        run_name=request.run_name,
        demand_scale_factor=request.demand_scale_factor,
        sizing_scale_factor=request.sizing_scale_factor,
        number_of_periods=run_grid.n_periods,
        period_len_hours=run_grid.period_len_hours,
        routing_mode=data.routing_mode,
        t0=run_grid.t0,
        inputs=[pathlib.Path(data.trips_path).name] if data.trips_path else [],
        violations=result.violations,
        rebalancing=request.rebalancing_meta(),
        demand_source=request.demand_source,
        forecast_name=request.forecast_name,
        forecast_dropped_share=forecast_dropped_share,
    )
    return save_run(request.run_name, tables, meta, root)


def load_run_table(run_name: str, table: str, root: pathlib.Path | None = None) -> pd.DataFrame:
    """Read one parquet table of a saved run (``table`` is a ``RUN_TABLES`` stem)."""
    return pd.read_parquet(table_path(run_name, table, root))


def load_run_meta(run_name: str, root: pathlib.Path | None = None) -> RunMeta:
    """Read a saved run's ``meta.json``, validated against ``RunMeta``."""
    return RunMeta.model_validate_json(meta_path(run_name, root).read_text())
