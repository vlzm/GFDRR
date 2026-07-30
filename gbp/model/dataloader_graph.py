"""Resolved model data: the tables one scenario runs on, and the marginals derived from them."""

import copy
import dataclasses
from typing import TYPE_CHECKING

import pandas as pd
import pandera.pandas as pa
import structlog

from gbp.model.flows import (
    flows_to_arrivals,
    flows_to_departures,
    flows_to_od_matrix,
    get_inventory_df,
    inventory_at_moments,
)
from gbp.model.journal_schema import schema_violations
from gbp.routing import DEFAULT_OSRM_URL, Routes, RoutingMode

if TYPE_CHECKING:
    from gbp.consumers.simulator.inputs import ScenarioInputs

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Schemas of the tables the engine and its phases read
# ---------------------------------------------------------------------------
# One pandera schema per table the engine reads from ``ResolvedModelData``.
# They are checked once, at the end of ``ResolvedModelData.__init__``; the two
# sized-state tables are checked again in ``run_sized_scenario`` right after
# sizing replaces them. The id and category columns keep their dtype
# unchecked on purpose (the sources mix pandas ``string`` and plain ``object``
# columns); the contract is the column set, non-null ids, non-negative
# quantities, and unique keys.

#: The period grid: one row per ``period_id`` with its wall-clock bounds.
PERIODS_SCHEMA = pa.DataFrameSchema(
    columns={
        "period_id": pa.Column(nullable=False, unique=True),
        "start_timestamp": pa.Column(nullable=False),
        "end_timestamp": pa.Column(nullable=False),
    },
    strict=False,
    name="periods",
)

#: Starting inventory: one row per ``(facility_id, commodity_category)``.
INITIAL_INVENTORY_SCHEMA = pa.DataFrameSchema(
    columns={
        "facility_id": pa.Column(nullable=False),
        "commodity_category": pa.Column(nullable=False),
        "quantity": pa.Column(checks=pa.Check.ge(0), nullable=False),
    },
    unique=["facility_id", "commodity_category"],
    strict=False,
    name="initial_inventory",
)

#: Historical demand marginal: quantity per (period, facility, commodity).
HISTORICAL_DEMAND_SCHEMA = pa.DataFrameSchema(
    columns={
        "period_id": pa.Column(nullable=False),
        "facility_id": pa.Column(nullable=False),
        "commodity_category": pa.Column(nullable=False),
        "quantity": pa.Column(checks=pa.Check.ge(0), nullable=False),
    },
    strict=False,
    name="historical_demand",
)

#: The OD matrix: target probability and mean duration per source pair.
HISTORICAL_OD_MATRIX_SCHEMA = pa.DataFrameSchema(
    columns={
        "source_id": pa.Column(nullable=False),
        "planned_target_id": pa.Column(nullable=False),
        "period_id": pa.Column(nullable=False),
        "commodity_category": pa.Column(nullable=False),
        "count": pa.Column(checks=pa.Check.ge(1), nullable=False),
        "duration": pa.Column(checks=pa.Check.ge(0), nullable=False),
        "probability": pa.Column(checks=[pa.Check.gt(0), pa.Check.le(1)], nullable=False),
    },
    strict=False,
    name="historical_od_matrix",
)

#: Dock capacities: one row per facility.
FACILITIES_CAPACITIES_SCHEMA = pa.DataFrameSchema(
    columns={
        "facility_id": pa.Column(nullable=False, unique=True),
        "capacity": pa.Column(checks=pa.Check.ge(0), nullable=False),
    },
    strict=False,
    name="facilities_capacities",
)

#: Facility geography: one coordinate pair per facility.
FACILITIES_GEO_SCHEMA = pa.DataFrameSchema(
    columns={
        "facility_id": pa.Column(nullable=False, unique=True),
        "lat": pa.Column(nullable=False),
        "lng": pa.Column(nullable=False),
    },
    strict=False,
    name="facilities_geo",
)

#: The engine-facing tables of ``ResolvedModelData``, with the schema of each
#: (the same list as the class docstring, ``routes`` excluded -- it is an
#: object, not a table).
ENGINE_TABLE_SCHEMAS = {
    "periods_df": PERIODS_SCHEMA,
    "initial_inventory_df": INITIAL_INVENTORY_SCHEMA,
    "historical_demand_df": HISTORICAL_DEMAND_SCHEMA,
    "historical_od_matrix_df": HISTORICAL_OD_MATRIX_SCHEMA,
    "facilities_capacities_df": FACILITIES_CAPACITIES_SCHEMA,
    "facilities_geo_df": FACILITIES_GEO_SCHEMA,
}

# ---------------------------------------------------------------------------
# Period grid (the simulation clock)
# ---------------------------------------------------------------------------
#: Default length of a single simulation period.
DEFAULT_PERIOD_LEN = pd.Timedelta(hours=1)


def to_period_id(ts: pd.Series, t0: pd.Timestamp, period_len: pd.Timedelta) -> pd.Series:
    """Map timestamps to zero-based period ids relative to ``t0``."""
    return ((ts - t0) // period_len).astype("int64")


def hour_of_week(ts: pd.Series) -> pd.Series:
    """Map timestamps to the hour of the week: ``weekday * 24 + hour``, 0..167 (0 is Mon 00:00)."""
    return (ts.dt.dayofweek * 24 + ts.dt.hour).astype("int64")


@dataclasses.dataclass(frozen=True)
class PeriodGrid:
    """A numbering of periods from ``t0`` with a fixed period length, starting at 0."""

    t0: pd.Timestamp
    n_periods: int
    period_len: pd.Timedelta = DEFAULT_PERIOD_LEN

    @property
    def period_len_hours(self) -> float:
        """The period length as a number of hours (the ``meta.json`` field)."""
        return float(self.period_len / pd.Timedelta(hours=1))

    def frame(self) -> pd.DataFrame:
        """One row per period: ``period_id``, ``start_timestamp``, ``end_timestamp``."""
        periods_df = pd.DataFrame({"period_id": range(self.n_periods)})
        periods_df["start_timestamp"] = self.t0 + periods_df["period_id"] * self.period_len
        periods_df["end_timestamp"] = periods_df["start_timestamp"] + self.period_len
        return periods_df

    def align_to(self, other: "PeriodGrid") -> pd.DataFrame:
        """Line this grid's periods up with ``other`` by wall-clock time (empty when none align)."""
        empty = pd.DataFrame({"period_id": [], "other_period_id": []}).astype("int64")
        if self.period_len != other.period_len:
            return empty
        grid = self.frame()
        span_end = other.t0 + other.n_periods * other.period_len
        inside = grid[(grid["start_timestamp"] >= other.t0) & (grid["start_timestamp"] < span_end)]
        if inside.empty:
            return empty
        offsets = inside["start_timestamp"] - other.t0
        if (offsets % other.period_len != pd.Timedelta(0)).any():
            return empty
        return pd.DataFrame(
            {
                "period_id": inside["period_id"].astype("int64"),
                "other_period_id": (offsets // other.period_len).astype("int64"),
            }
        ).reset_index(drop=True)


def get_forecast_periods_df(
    t0: pd.Timestamp, number_of_periods: int, period_len: pd.Timedelta
) -> pd.DataFrame:
    """Build the period grid of a forecast horizon: ``number_of_periods`` from ``t0``, ids at 0."""
    return PeriodGrid(t0, number_of_periods, period_len).frame()


# ---------------------------------------------------------------------------
# Forecast path: run the scenario on a forecast demand table (Notations.md §17)
# ---------------------------------------------------------------------------
def map_od_matrix_by_hour_of_week(
    od_matrix_df: pd.DataFrame,
    periods_df: pd.DataFrame,
    forecast_periods_df: pd.DataFrame,
) -> pd.DataFrame:
    """Carry the historical OD matrix onto forecast periods by hour of week."""
    period_hours = periods_df[["period_id"]].assign(
        hour_of_week=hour_of_week(periods_df["start_timestamp"])
    )
    od = od_matrix_df.merge(period_hours, on="period_id", how="left")
    if od["hour_of_week"].isna().any():
        missing = od.loc[od["hour_of_week"].isna(), "period_id"].unique()[:5].tolist()
        raise ValueError(f"OD matrix has periods outside the period grid: {missing}")

    od["duration_x_count"] = od["duration"] * od["count"]
    pooled = od.groupby(
        ["source_id", "planned_target_id", "hour_of_week", "commodity_category"], as_index=False
    ).agg(count=("count", "sum"), duration_x_count=("duration_x_count", "sum"))
    pooled["duration"] = (pooled["duration_x_count"] / pooled["count"]).round().astype("Int64")
    totals = pooled.groupby(["source_id", "hour_of_week", "commodity_category"])["count"].transform(
        "sum"
    )
    pooled["probability"] = pooled["count"] / totals

    forecast_hours = forecast_periods_df[["period_id"]].assign(
        hour_of_week=hour_of_week(forecast_periods_df["start_timestamp"])
    )
    out = pooled.merge(forecast_hours, on="hour_of_week", how="inner")
    return out[
        [
            "source_id",
            "planned_target_id",
            "period_id",
            "commodity_category",
            "count",
            "duration",
            "probability",
        ]
    ].reset_index(drop=True)


def restrict_demand_to_scenario(
    demand_df: pd.DataFrame,
    resolved: "ResolvedModelData",
    periods_df: pd.DataFrame,
) -> tuple[pd.DataFrame, float]:
    """Keep the demand rows the scenario can run; report the dropped share of the demand total."""
    od = resolved.historical_od_matrix_df.merge(
        resolved.periods_df[["period_id", "start_timestamp"]], on="period_id"
    )
    covered = (
        od.assign(hour_of_week=hour_of_week(od["start_timestamp"]))[
            ["source_id", "commodity_category", "hour_of_week"]
        ]
        .drop_duplicates()
        .rename(columns={"source_id": "facility_id"})
    )
    rows = demand_df.merge(periods_df[["period_id", "start_timestamp"]], on="period_id")
    rows["hour_of_week"] = hour_of_week(rows["start_timestamp"])
    kept = rows.merge(covered, on=["facility_id", "commodity_category", "hour_of_week"])
    total = demand_df["quantity"].sum()
    dropped_share = float(1.0 - kept["quantity"].sum() / total) if total else 0.0
    columns = list(HISTORICAL_DEMAND_SCHEMA.columns)
    kept = kept[columns].sort_values(columns[:3]).reset_index(drop=True)
    return kept, dropped_share


def apply_forecast_demand(
    resolved: "ResolvedModelData",
    forecast_demand_df: pd.DataFrame,
    forecast_periods_df: pd.DataFrame,
) -> "ResolvedModelData":
    """Return a shallow copy of ``resolved`` that runs on a forecast demand table."""
    violations = [
        *schema_violations(PERIODS_SCHEMA, forecast_periods_df),
        *schema_violations(HISTORICAL_DEMAND_SCHEMA, forecast_demand_df),
    ]
    if violations:
        raise ValueError("forecast tables break their schemas:\n" + "\n".join(violations))

    period_len = (
        forecast_periods_df["end_timestamp"].iloc[0]
        - (forecast_periods_df["start_timestamp"].iloc[0])
    )
    if period_len != resolved.period_len:
        raise ValueError(
            f"forecast period length {period_len} != scenario period length {resolved.period_len}"
        )

    known = set(resolved.facilities_df["facility_id"])
    unknown = sorted(set(forecast_demand_df["facility_id"]) - known)
    if unknown:
        raise ValueError(f"forecast demand names unknown facilities: {unknown[:5]}")

    od_matrix_df = map_od_matrix_by_hour_of_week(
        resolved.historical_od_matrix_df, resolved.periods_df, forecast_periods_df
    )

    demanded = forecast_demand_df[forecast_demand_df["quantity"] > 0]
    covered = od_matrix_df[["source_id", "period_id", "commodity_category"]].drop_duplicates()
    coverage = demanded.merge(
        covered.rename(columns={"source_id": "facility_id"}),
        on=["facility_id", "period_id", "commodity_category"],
        how="left",
        indicator=True,
    )
    uncovered = coverage[coverage["_merge"] == "left_only"]
    if not uncovered.empty:
        sample = uncovered[["period_id", "facility_id", "commodity_category"]].head(5)
        raise ValueError(
            f"{len(uncovered)} forecast demand rows have no OD rows for their "
            f"(facility, commodity, period); first rows:\n{sample.to_string(index=False)}"
        )

    out = copy.copy(resolved)
    out.periods_df = forecast_periods_df
    out.t0 = forecast_periods_df["start_timestamp"].iloc[0]
    out.historical_demand_df = forecast_demand_df
    out.historical_od_matrix_df = od_matrix_df
    return out


# ---------------------------------------------------------------------------
# Resource observations
# ---------------------------------------------------------------------------
RESOURCE_OBS_COLUMNS = ["period_id", "resource_id", "facility_id", "load"]


def empty_resources_obs_df() -> pd.DataFrame:
    """Empty resource-observation table (trucks are idle in the replay)."""
    return pd.DataFrame(
        {
            "period_id": pd.Series(dtype="Int64"),
            "resource_id": pd.Series(dtype="string"),
            "facility_id": pd.Series(dtype="string"),
            "load": pd.Series(dtype="Int64"),
        }
    )


# ---------------------------------------------------------------------------
# Self-consistent initial inventory for a clean replay (no stockout). The
# matching capacity and saturated-state functions live with the sizing run
# (gbp.consumers.simulator.sizing), their only caller.
# ---------------------------------------------------------------------------
def get_replay_initial_inventory_df(
    historical_flows_df: pd.DataFrame,
    facilities_df: pd.DataFrame,
    commodities_categories_df: pd.DataFrame,
) -> pd.DataFrame:
    """Smallest initial inventory that lets the replay run with no stockout (per inventory step)."""
    stations = facilities_df.loc[facilities_df["facility_category"] == "station", ["facility_id"]]
    grid = stations.merge(commodities_categories_df[["commodity_category"]], how="cross")

    moments = inventory_at_moments(historical_flows_df, grid.assign(quantity=0))
    low = moments.groupby(["facility_id", "commodity_category"], as_index=False)[
        "inventory_after"
    ].min()
    low["quantity"] = (-low["inventory_after"]).clip(lower=0).astype("int64")

    out = grid.merge(
        low[["facility_id", "commodity_category", "quantity"]],
        on=["facility_id", "commodity_category"],
        how="left",
    )
    out["quantity"] = out["quantity"].fillna(0).astype("int64")
    return out[["facility_id", "commodity_category", "quantity"]]


# ---------------------------------------------------------------------------
# Resolved model data container
# ---------------------------------------------------------------------------
class ResolvedModelData:
    """The tables one scenario runs on, checked once at build time.

    A domain loader supplies the base tables in the framework's column names:
    the entities and their attributes, the period grid, and the historical
    flow journal. Everything else is derived here and only here -- the
    historical marginals (demand, departures, arrivals, OD matrix), the start
    inventory that lets the replay run without a stockout, the inventory
    trajectory, and the ``Routes`` answerer for facility pairs. The
    ``simulated_*`` slots start empty; :func:`attach_simulation` fills them
    from a finished run.
    """

    def __init__(
        self,
        *,
        facilities_df: pd.DataFrame,
        resources_df: pd.DataFrame,
        commodities_categories_df: pd.DataFrame,
        facilities_geo_df: pd.DataFrame,
        facilities_capacities_df: pd.DataFrame,
        resources_capacities_df: pd.DataFrame,
        facilities_costs_df: pd.DataFrame,
        resources_rates_df: pd.DataFrame,
        commodities_categories_rates_df: pd.DataFrame,
        periods_df: pd.DataFrame,
        historical_flows_df: pd.DataFrame,
        t0: pd.Timestamp,
        period_len: pd.Timedelta,
        trip_speed_km_per_period: float,
        routing_mode: RoutingMode = "haversine",
        osrm_url: str = DEFAULT_OSRM_URL,
        trips_path: str | None = None,
    ) -> None:
        # Where the raw data came from, so a saved run can record it in the
        # ``inputs`` field of ``meta.json``.
        self.trips_path = trips_path

        # Entities
        self.facilities_df = facilities_df
        self.resources_df = resources_df
        self.commodities_categories_df = commodities_categories_df

        # Attributes
        self.facilities_geo_df = facilities_geo_df
        self.facilities_capacities_df = facilities_capacities_df
        self.resources_capacities_df = resources_capacities_df
        self.facilities_costs_df = facilities_costs_df
        self.resources_rates_df = resources_rates_df
        self.commodities_categories_rates_df = commodities_categories_rates_df

        # Time grid
        self.period_len = period_len
        self.t0 = t0
        self.periods_df = periods_df

        # Historical observations: the marginals of the flow log, derived with
        # the same read-model functions as the simulated set below.
        self.historical_flows_df = historical_flows_df
        self.historical_resources_df = empty_resources_obs_df()

        historical_departures_df = flows_to_departures(self.historical_flows_df)

        # Size the start inventory against the per-step low point of the inventory
        # trajectory, so the replay never hits a stockout (see the function's
        # docstring for why the per-period low point is not enough).
        self.initial_inventory_df = get_replay_initial_inventory_df(
            self.historical_flows_df, self.facilities_df, self.commodities_categories_df
        )
        self.historical_inventory_df = get_inventory_df(
            self.historical_flows_df, self.initial_inventory_df
        )

        self.historical_demand_df = historical_departures_df
        self.historical_departures_df = historical_departures_df
        self.historical_arrivals_df = flows_to_arrivals(self.historical_flows_df)
        self.historical_od_matrix_df = flows_to_od_matrix(self.historical_flows_df)

        # The one distance / travel-time answerer for facility pairs. In osrm
        # mode this fetches the full facility-to-facility table now, once.
        # Straight-line distances become travel times through the mean riding
        # speed the loader measured on the historical trips.
        self.trip_speed_km_per_period = trip_speed_km_per_period
        self.routing_mode = routing_mode
        self.routes = Routes(
            self.facilities_geo_df,
            routing_mode,
            trip_speed_km_per_period=trip_speed_km_per_period,
            period_len=period_len,
            osrm_url=osrm_url,
        )

        # Simulated observations -- filled by attach_simulation after a run
        self.simulated_flows_df: pd.DataFrame | None = None
        self.simulated_resources_df: pd.DataFrame | None = None
        self.simulated_inventory_df: pd.DataFrame | None = None
        self.simulated_demand_df: pd.DataFrame | None = None
        self.simulated_departures_df: pd.DataFrame | None = None
        self.simulated_arrivals_df: pd.DataFrame | None = None
        self.simulated_od_matrix_df: pd.DataFrame | None = None

        # Consistency check: the start-of-period inventory at period 0 is, by
        # construction, the initial inventory. Verify the two agree per commodity
        # category, so a mismatch in how either is built is caught early.
        init_by_cat = self.initial_inventory_df.groupby("commodity_category")["quantity"].sum()
        sop0_by_cat = (
            self.historical_inventory_df[self.historical_inventory_df["period_id"] == 0]
            .groupby("commodity_category")["quantity_sop"]
            .sum()
        )
        categories = init_by_cat.index.union(sop0_by_cat.index)
        init_by_cat = init_by_cat.reindex(categories, fill_value=0)
        sop0_by_cat = sop0_by_cat.reindex(categories, fill_value=0)
        assert init_by_cat.equals(sop0_by_cat), (
            "initial_inventory does not match historical_inventory quantity_sop at period 0 "
            f"per commodity category:\ninitial_inventory:\n{init_by_cat}\n"
            f"historical_inventory quantity_sop@period 0:\n{sop0_by_cat}"
        )

        # The load boundary: every table the engine reads is checked once
        # here, so a wrong shape fails now instead of as a mid-run pandas
        # error. (The assert above stays: it is a cross-table consistency
        # check, which a per-table schema cannot express.)
        violations = check_engine_tables(self)
        if violations:
            raise ValueError(
                "resolved model data breaks its table schemas:\n" + "\n".join(violations)
            )

        log.info(
            "graph_resolved",
            facilities=len(self.facilities_df),
            periods=len(self.periods_df),
            historical_events=len(self.historical_flows_df),
            period_len=str(period_len),
            routing_mode=routing_mode,
        )


def check_engine_tables(resolved: "ResolvedModelData") -> list[str]:
    """Check every engine-facing table of ``resolved`` against its schema (empty list = valid)."""
    violations: list[str] = []
    for attribute, schema in ENGINE_TABLE_SCHEMAS.items():
        violations += schema_violations(schema, getattr(resolved, attribute))
    return violations


def _supplies_scenario_inputs(resolved: ResolvedModelData) -> "ScenarioInputs":
    """Declare that the resolved data supplies the simulator's input contract (mypy-only)."""
    return resolved


# ---------------------------------------------------------------------------
# Wiring a finished run back into the resolved container
# ---------------------------------------------------------------------------
def attach_simulation(
    resolved: ResolvedModelData,
    simulated_flows_df: pd.DataFrame,
    simulated_resources_df: pd.DataFrame | None = None,
) -> None:
    """Populate the ``simulated_*`` observation slots from a finished run."""
    resolved.simulated_flows_df = simulated_flows_df
    resolved.simulated_resources_df = (
        simulated_resources_df if simulated_resources_df is not None else empty_resources_obs_df()
    )
    resolved.simulated_inventory_df = get_inventory_df(
        resolved.simulated_flows_df, resolved.initial_inventory_df
    )
    simulated_departures_df = flows_to_departures(resolved.simulated_flows_df)
    resolved.simulated_demand_df = simulated_departures_df
    resolved.simulated_departures_df = simulated_departures_df
    resolved.simulated_arrivals_df = flows_to_arrivals(resolved.simulated_flows_df)
    resolved.simulated_od_matrix_df = flows_to_od_matrix(resolved.simulated_flows_df)
