"""Simulator state primitives: flow-event schema, event builders, inventory
updates, the per-period state object, and the observation derivations.

This module is the low-level foundation of the simulator. It has no dependency
on the data loaders or the engine, so both can import from it freely.

The flow journal is the single source of truth for what happened in a run.
Inventory and the in-transit working set are *projections* of that journal:
they are maintained incrementally for speed (re-deriving them from the full
journal every period would be O(N) per period), but they carry no information
the journal does not. The marginal observations (departures, arrivals, demand,
supply, OD matrix) are pure functions of the journal and the current inventory,
so historical and simulated runs share one definition for each of them.
"""

import dataclasses
from typing import Any

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Flow-event schema
# ---------------------------------------------------------------------------
FLOW_EVENT_COLUMNS = [
    "event_id", "period_id", "flow_id", "flow_type", "event_type", "commodity_category",
    "source_id", "planned_target_id", "realized_target_id",
    "start_period", "planned_end_period", "realized_end_period",
    "resource_id", "quantity", "reason",
]

FLOW_EVENT_DTYPES = {
    "period_id": "Int64",
    "flow_id": "string",
    "flow_type": "string",
    "event_type": "string",
    "commodity_category": "string",
    "source_id": "string",
    "planned_target_id": "string",
    "realized_target_id": "string",
    "start_period": "Int64",
    "planned_end_period": "Int64",
    "realized_end_period": "Int64",
    "resource_id": "string",
    "quantity": "Int64",
    "reason": "string",
}


# ---------------------------------------------------------------------------
# Flow-event builders
# ---------------------------------------------------------------------------
def _typed_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """Cast event columns to the canonical dtypes so frames concat cleanly."""
    for col, dtype in FLOW_EVENT_DTYPES.items():
        events_df[col] = events_df[col].astype(dtype)
    events_df["event_order"] = events_df["event_order"].astype("int64")
    return events_df


def departed_events(trips: pd.DataFrame) -> pd.DataFrame:
    """One ``departed`` event row per trip leaving this period."""
    return _typed_events(pd.DataFrame({
        "period_id":           trips["start_period"],
        "flow_id":             trips["flow_id"],
        "flow_type":           "user_trip",
        "event_type":          "departed",
        "commodity_category":  trips["commodity_category"],
        "source_id":           trips["source_id"],
        "planned_target_id":   trips["planned_target_id"],
        "realized_target_id":  pd.NA,
        "start_period":        trips["start_period"],
        "planned_end_period":  trips["planned_end_period"],
        "realized_end_period": pd.NA,
        "resource_id":         pd.NA,
        "quantity":            1,
        "reason":              pd.NA,
        "event_order":         0,
    }))


def arrived_events(in_transit_due: pd.DataFrame, period_id: int | pd.Series) -> pd.DataFrame:
    """One ``arrived`` event row per in-transit flow docking at ``period_id``.

    ``period_id`` is the docking period: a single int when a whole batch docks
    in the same period (the simulator), or a per-row Series of end periods when
    each flow docks at its own time (the historical log).
    """
    return _typed_events(pd.DataFrame({
        "period_id":           period_id,
        "flow_id":             in_transit_due["flow_id"],
        "flow_type":           "user_trip",
        "event_type":          "arrived",
        "commodity_category":  in_transit_due["commodity_category"],
        "source_id":           in_transit_due["source_id"],
        "planned_target_id":   in_transit_due["planned_target_id"],
        "realized_target_id":  in_transit_due["planned_target_id"],
        "start_period":        in_transit_due["start_period"],
        "planned_end_period":  in_transit_due["planned_end_period"],
        "realized_end_period": period_id,
        "resource_id":         pd.NA,
        "quantity":            1,
        "reason":              pd.NA,
        "event_order":         1,
    }))


def empty_in_transit() -> pd.DataFrame:
    """Empty in-transit table (a ``departed``-event frame with no rows)."""
    return departed_events(pd.DataFrame({
        "flow_id":            pd.Series(dtype="string"),
        "source_id":          pd.Series(dtype="string"),
        "planned_target_id":  pd.Series(dtype="string"),
        "commodity_category": pd.Series(dtype="string"),
        "start_period":       pd.Series(dtype="Int64"),
        "planned_end_period": pd.Series(dtype="Int64"),
    }))


# ---------------------------------------------------------------------------
# Flow journal (the run's single source of truth)
# ---------------------------------------------------------------------------
def empty_flows_journal() -> pd.DataFrame:
    """Empty append-only flow journal.

    Holds the same columns the event builders emit (the canonical flow fields
    plus the transient ``event_order``); ``event_id`` is assigned only once at
    :func:`finalize_flows`, so it is absent while the journal is still growing.
    """
    journal = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in FLOW_EVENT_DTYPES.items()})
    journal["event_order"] = pd.Series(dtype="int64")
    return journal


def finalize_flows(journal: pd.DataFrame) -> pd.DataFrame:
    """Order the accumulated journal and assign a monotonic ``event_id``.

    Both the historical log (:func:`dataloader_graph.get_historical_flows_df`)
    and a replay run's journal are finalized through this one function, so they
    share their sort keys, ``event_id`` assignment and column projection by
    construction rather than by two definitions kept in sync by hand.

    Parameters
    ----------
    journal : pandas.DataFrame
        The append-only journal accumulated during a run.

    Returns
    -------
    pandas.DataFrame
        Event log with columns :data:`FLOW_EVENT_COLUMNS`, sorted by
        ``period_id`` then ``flow_id`` then ``event_order``, with a monotonic
        ``event_id``.
    """
    flows = journal.copy()
    for col, dtype in FLOW_EVENT_DTYPES.items():
        flows[col] = flows[col].astype(dtype)
    flows = flows.sort_values(["period_id", "flow_id", "event_order"]).reset_index(drop=True)
    flows["event_id"] = flows.index.astype("Int64")
    return flows[FLOW_EVENT_COLUMNS]


# ---------------------------------------------------------------------------
# Inventory updates
# ---------------------------------------------------------------------------
def adjust_inventory(inventory: pd.DataFrame, deltas: pd.DataFrame) -> pd.DataFrame:
    """Add signed ``delta`` per (facility_id, commodity_category)."""
    out = inventory.merge(deltas, on=["facility_id", "commodity_category"], how="outer")
    out["quantity"] = out["quantity"].fillna(0) + out["delta"].fillna(0)
    return out[["facility_id", "commodity_category", "quantity"]]


def departure_deltas(trips: pd.DataFrame) -> pd.DataFrame:
    """-1 per departing bike, grouped by (source, commodity)."""
    deltas = (
        trips.groupby(["source_id", "commodity_category"]).size()
        .reset_index(name="delta").rename(columns={"source_id": "facility_id"})
    )
    deltas["delta"] = -deltas["delta"]
    return deltas


def arrival_deltas(in_transit_due: pd.DataFrame) -> pd.DataFrame:
    """+1 per docking bike, grouped by (target, commodity)."""
    return (
        in_transit_due.groupby(["planned_target_id", "commodity_category"]).size()
        .reset_index(name="delta").rename(columns={"planned_target_id": "facility_id"})
    )


# ---------------------------------------------------------------------------
# Capacity-aware docking and overflow redirect
# ---------------------------------------------------------------------------
# The part that bites above the historical baseline: when a station's docks are
# full, arriving bikes overflow and are redirected to the nearest station with a
# free dock. Dormant in an exact replay, where capacity never binds.
def free_docks(inventory: pd.DataFrame, capacities: pd.DataFrame) -> pd.Series:
    """Free dock slots per facility: capacity minus bikes currently docked.

    Classic and electric bikes share the same physical docks, so occupancy is the
    total stock across commodities.

    Parameters
    ----------
    inventory : pandas.DataFrame
        Current stock: ``facility_id``, ``commodity_category``, ``quantity``.
    capacities : pandas.DataFrame
        Dock capacities: ``facility_id``, ``capacity``.

    Returns
    -------
    pandas.Series
        ``facility_id -> free slots`` (clipped at zero).
    """
    occupied = inventory.groupby("facility_id")["quantity"].sum()
    capacity = capacities.set_index("facility_id")["capacity"]
    idx = capacity.index.union(occupied.index)
    free = capacity.reindex(idx).fillna(0) - occupied.reindex(idx).fillna(0)
    return free.clip(lower=0).astype("int64")


def dock_up_to_capacity(
    due: pd.DataFrame, free: pd.Series
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split docking flows at their planned target into ``(fits, overflow)``.

    Within each target the first ``free`` flows (in row order) dock; the rest are
    overflow. Vectorized through a per-target cumulative count -- no Python loop.

    Parameters
    ----------
    due : pandas.DataFrame
        In-transit flows docking this period (``planned_target_id`` per row).
    free : pandas.Series
        Free dock slots per facility, from :func:`free_docks`.

    Returns
    -------
    tuple of (pandas.DataFrame, pandas.DataFrame)
        The flows that fit and the overflow flows, both subsets of ``due``.
    """
    if due.empty:
        return due, due
    rank = due.groupby("planned_target_id").cumcount()
    capacity_here = due["planned_target_id"].map(free).fillna(0)
    fits = rank < capacity_here
    return due[fits], due[~fits]


def redirected_events(
    flows: pd.DataFrame, realized_target_id: pd.Series, period_id: int
) -> pd.DataFrame:
    """One ``redirected`` docking event per overflow flow that docked elsewhere.

    ``realized_target_id`` is the station actually docked at (a per-row Series),
    which differs from ``planned_target_id``; ``reason`` records why.

    Parameters
    ----------
    flows : pandas.DataFrame
        The overflow flows that were redirected.
    realized_target_id : pandas.Series
        Per-row station actually docked at, aligned to ``flows``.
    period_id : int
        The docking period.

    Returns
    -------
    pandas.DataFrame
        One ``redirected`` flow-event row per flow.
    """
    return _typed_events(pd.DataFrame({
        "period_id":           period_id,
        "flow_id":             flows["flow_id"],
        "flow_type":           "user_trip",
        "event_type":          "redirected",
        "commodity_category":  flows["commodity_category"],
        "source_id":           flows["source_id"],
        "planned_target_id":   flows["planned_target_id"],
        "realized_target_id":  realized_target_id,
        "start_period":        flows["start_period"],
        "planned_end_period":  flows["planned_end_period"],
        "realized_end_period": period_id,
        "resource_id":         pd.NA,
        "quantity":            1,
        "reason":              "dock_full",
        "event_order":         1,
    }))


def _nearest_free_station(targets: pd.Series, free: pd.Series, geo: pd.DataFrame) -> pd.Series:
    """Nearest *other* station with a free dock for each station id in ``targets``.

    Distance is squared Euclidean on (lat, lng) -- enough to rank neighbours at
    this prototype stage. Returns a Series aligned to ``targets`` (NA if none).

    Parameters
    ----------
    targets : pandas.Series
        Planned target station ids needing a free neighbour.
    free : pandas.Series
        Free dock slots per facility, from :func:`free_docks`.
    geo : pandas.DataFrame
        Facility geography: ``facility_id``, ``lat``, ``lng``.

    Returns
    -------
    pandas.Series
        Aligned to ``targets``: the nearest other station with a free dock, or NA.
    """
    candidates = free[free > 0].index
    coords = geo.set_index("facility_id")[["lat", "lng"]]
    origins = (coords.loc[coords.index.intersection(targets.unique())]
               .reset_index().rename(columns={"facility_id": "origin"}))
    cand = (coords.loc[coords.index.intersection(candidates)]
            .reset_index().rename(columns={"facility_id": "candidate"}))
    pairs = origins.merge(cand, how="cross")
    pairs = pairs[pairs["origin"] != pairs["candidate"]]
    pairs["dist2"] = (pairs["lat_x"] - pairs["lat_y"]) ** 2 + (pairs["lng_x"] - pairs["lng_y"]) ** 2
    nearest = pairs.sort_values("dist2").drop_duplicates("origin").set_index("origin")["candidate"]
    return targets.map(nearest)


def redirect_overflow(
    inventory: pd.DataFrame,
    capacities: pd.DataFrame,
    geo: pd.DataFrame,
    overflow: pd.DataFrame,
    period_id: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Greedily dock overflow flows at the nearest station with a free dock.

    Rounds, not per-row loops: each round maps every still-unplaced flow to its
    nearest free station, docks up to capacity there, applies the arrivals to
    inventory, and repeats with the leftovers until none remain or no dock is free
    anywhere. The within-batch capacity coupling is therefore honoured exactly.

    Parameters
    ----------
    inventory : pandas.DataFrame
        Current stock: ``facility_id``, ``commodity_category``, ``quantity``.
    capacities : pandas.DataFrame
        Dock capacities: ``facility_id``, ``capacity``.
    geo : pandas.DataFrame
        Facility geography: ``facility_id``, ``lat``, ``lng``.
    overflow : pandas.DataFrame
        The flows that found no free dock at their planned target.
    period_id : int
        The docking period.

    Returns
    -------
    tuple of (pandas.DataFrame, pandas.DataFrame, pandas.DataFrame)
        The ``redirected`` events, the inventory with the redirected arrivals
        applied, and any flows that found no free dock anywhere (lost).
    """
    events = []
    remaining = overflow
    while not remaining.empty:
        free = free_docks(inventory, capacities)
        if not (free > 0).any():
            break
        target = _nearest_free_station(remaining["planned_target_id"], free, geo)
        placed = remaining.assign(realized_target_id=target)
        placed = placed[placed["realized_target_id"].notna()]
        if placed.empty:
            break
        rank = placed.groupby("realized_target_id").cumcount()
        fits = rank < placed["realized_target_id"].map(free)
        docked = placed[fits]
        events.append(redirected_events(docked, docked["realized_target_id"], period_id))
        deltas = (docked.groupby(["realized_target_id", "commodity_category"]).size()
                  .reset_index(name="delta").rename(columns={"realized_target_id": "facility_id"}))
        inventory = adjust_inventory(inventory, deltas)
        remaining = placed[~fits].drop(columns="realized_target_id")
    events_df = (pd.concat(events, ignore_index=True) if events
                 else redirected_events(overflow.iloc[:0], overflow["planned_target_id"].iloc[:0], period_id))
    return events_df, inventory, remaining


# ---------------------------------------------------------------------------
# Observation derivations (pure functions of the flow journal)
# ---------------------------------------------------------------------------
def flows_to_departures(flows: pd.DataFrame) -> pd.DataFrame:
    """Outflow per period and origin: count of ``departed`` events.

    Grouped by ``(period_id, facility_id, commodity_category)`` where
    ``facility_id`` is the trip's ``source_id``.
    """
    return (
        flows[flows["event_type"] == "departed"]
        .groupby(["period_id", "source_id", "commodity_category"], as_index=False)["quantity"].sum()
        .rename(columns={"source_id": "facility_id"})
    )


def flows_to_arrivals(flows: pd.DataFrame) -> pd.DataFrame:
    """Inflow per period and destination: count of ``arrived`` events.

    Grouped by ``(period_id, facility_id, commodity_category)`` where
    ``facility_id`` is the trip's ``realized_target_id``.
    """
    return (
        flows[flows["event_type"] == "arrived"]
        .groupby(["period_id", "realized_target_id", "commodity_category"], as_index=False)["quantity"].sum()
        .rename(columns={"realized_target_id": "facility_id"})
    )


def flows_to_od_matrix(flows: pd.DataFrame) -> pd.DataFrame:
    """Origin-destination demand model from ``departed`` events.

    Aggregates the journal's intended origin->destination structure of demand
    (not the realized docking, which may be redirected above baseline). For each
    ``(source_id, planned_target_id, commodity_category)``:

    - ``count`` -- number of trips on the pair,
    - ``probability`` -- ``P(target | source, commodity)``, normalized within
      each ``(source, commodity)``,
    - ``duration`` -- mean trip length in periods (``planned_end - start``),
      rounded to whole periods.

    The probability and duration columns turn the matrix into a generative demand
    model: given a count of departures from a source, they say where the bikes go
    and when they dock. In the trivial case ``simulated_od_matrix_df`` equals this
    historical matrix.
    """
    dep = flows[flows["event_type"] == "departed"].copy()
    dep["duration"] = dep["planned_end_period"] - dep["start_period"]
    od = (
        dep.groupby(["source_id", "planned_target_id", "commodity_category"], as_index=False)
        .agg(count=("quantity", "sum"), duration=("duration", "mean"))
    )
    totals = od.groupby(["source_id", "commodity_category"])["count"].transform("sum")
    od["probability"] = od["count"] / totals
    od["duration"] = od["duration"].round().astype("Int64")
    return od


def get_inventory_df(flows: pd.DataFrame, initial_inventory: pd.DataFrame) -> pd.DataFrame:
    """Per-period inventory as a pure function of the journal and initial stock.

    Inventory at the end of period ``t`` equals the initial stock plus the
    cumulative net flow (arrivals ``+1``, departures ``-1``) up to and including
    ``t``, per ``(facility_id, commodity_category)``. Because both historical and
    simulated inventory are defined this way, they need no per-period snapshot
    table — the journal is enough.

    Parameters
    ----------
    flows : pandas.DataFrame
        A flow-event log (historical or finalized simulated).
    initial_inventory : pandas.DataFrame
        Starting stock with ``facility_id``, ``commodity_category``, ``quantity``.

    Returns
    -------
    pandas.DataFrame
        Columns ``period_id``, ``facility_id``, ``commodity_category``,
        ``quantity`` for every period in ``[0, max(period_id)]``.
    """
    if flows.empty:
        return pd.DataFrame({
            "period_id":          pd.Series(dtype="int64"),
            "facility_id":        pd.Series(dtype="string"),
            "commodity_category": pd.Series(dtype="string"),
            "quantity":           pd.Series(dtype="int64"),
        })

    dep = (
        flows[flows["event_type"] == "departed"]
        .groupby(["period_id", "source_id", "commodity_category"], as_index=False)["quantity"].sum()
        .rename(columns={"source_id": "facility_id", "quantity": "delta"})
    )
    dep["delta"] = -dep["delta"]
    arr = (
        flows[flows["event_type"] == "arrived"]
        .groupby(["period_id", "realized_target_id", "commodity_category"], as_index=False)["quantity"].sum()
        .rename(columns={"realized_target_id": "facility_id", "quantity": "delta"})
    )
    deltas = pd.concat([dep, arr], ignore_index=True)
    deltas = deltas.groupby(
        ["period_id", "facility_id", "commodity_category"], as_index=False
    )["delta"].sum()

    n_periods = int(flows["period_id"].max()) + 1
    net = deltas.pivot_table(
        index=["facility_id", "commodity_category"],
        columns="period_id",
        values="delta",
        fill_value=0,
        aggfunc="sum",
    ).reindex(columns=range(n_periods), fill_value=0)
    net.columns.name = "period_id"

    initial = initial_inventory.set_index(["facility_id", "commodity_category"])["quantity"]
    full_index = net.index.union(initial.index)
    net = net.reindex(full_index, fill_value=0)
    cumulative = net.cumsum(axis=1).add(initial.reindex(full_index).fillna(0), axis=0)

    inventory = cumulative.stack().rename("quantity").reset_index()
    inventory["quantity"] = inventory["quantity"].astype("int64")
    return inventory[["period_id", "facility_id", "commodity_category", "quantity"]]


@dataclasses.dataclass(frozen=True)
class Observations:
    """The full set of marginals derived from a flow journal.

    Every field is a pure function of the journal (and, for ``inventory``, of the
    initial stock). Bundling them in one container means the historical and
    simulated observation sets are produced by the same code path and therefore
    coincide by construction: the base-replay invariant
    ``simulated_departures == historical_departures`` rests on a single
    definition rather than two hand-kept blocks.

    Attributes
    ----------
    inventory : pandas.DataFrame
        Per-period stock; see :func:`get_inventory_df`.
    departures : pandas.DataFrame
        Outflow per period and origin; see :func:`flows_to_departures`.
    arrivals : pandas.DataFrame
        Inflow per period and destination; see :func:`flows_to_arrivals`.
    demand : pandas.DataFrame
        Realized user demand; equals ``departures`` in an exact replay (see
        :func:`flows_to_departures` and the note on demand gating).
    od_matrix : pandas.DataFrame
        Origin-destination demand model; see :func:`flows_to_od_matrix`.
    """

    inventory: pd.DataFrame
    departures: pd.DataFrame
    arrivals: pd.DataFrame
    demand: pd.DataFrame
    od_matrix: pd.DataFrame


def observe(flows: pd.DataFrame, initial_inventory: pd.DataFrame) -> Observations:
    """Derive the full set of marginals from a flow journal.

    The single place that defines *what is in the observation set*. It is called
    once for the historical journal and once for each simulated one, so the two
    sets are identical by construction (in the base scenario their values are
    equal too).

    Parameters
    ----------
    flows : pandas.DataFrame
        A flow-event log (historical or finalized simulated).
    initial_inventory : pandas.DataFrame
        Starting stock with ``facility_id``, ``commodity_category``, ``quantity``.

    Returns
    -------
    Observations
        The inventory, departures, arrivals, demand and OD-matrix marginals.
    """
    departures = flows_to_departures(flows)
    return Observations(
        inventory=get_inventory_df(flows, initial_inventory),
        departures=departures,
        arrivals=flows_to_arrivals(flows),
        # In an exact replay every desired trip departs, so realized demand is
        # read off the journal as the departures (see ``state_demand_df``).
        demand=departures,
        od_matrix=flows_to_od_matrix(flows),
    )


# ---------------------------------------------------------------------------
# Demand realization and OD expansion (FormDepartures / FormPotentialTrips)
# ---------------------------------------------------------------------------
def realize_departures(demand_now: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    """Realized departures per (facility, commodity): ``min(demand, stock)``.

    Demand above the stock on hand is lost to a stockout; stock is per commodity,
    so classic and electric demand are gated independently. Dormant in an exact
    replay, where stock always covers the historical demand.

    Parameters
    ----------
    demand_now : pandas.DataFrame
        This period's demand: ``facility_id``, ``commodity_category``, ``quantity``.
    inventory : pandas.DataFrame
        Current stock: ``facility_id``, ``commodity_category``, ``quantity``.

    Returns
    -------
    pandas.DataFrame
        ``facility_id``, ``commodity_category``, ``realized``, ``lost``.
    """
    stock = inventory.rename(columns={"quantity": "available"})
    out = demand_now.merge(stock, on=["facility_id", "commodity_category"], how="left")
    out["available"] = out["available"].fillna(0)
    out["realized"] = out[["quantity", "available"]].min(axis=1).astype("int64")
    out["lost"] = (out["quantity"] - out["realized"]).astype("int64")
    return out[["facility_id", "commodity_category", "realized", "lost"]]


def departure_deltas_from_counts(realized: pd.DataFrame) -> pd.DataFrame:
    """``-realized`` per (facility, commodity) for the inventory decrement."""
    d = (realized[realized["realized"] > 0]
         .rename(columns={"realized": "delta"})
         [["facility_id", "commodity_category", "delta"]].copy())
    d["delta"] = -d["delta"]
    return d


def form_potential_trips(
    departures: pd.DataFrame, od_matrix: pd.DataFrame, period_id: int
) -> pd.DataFrame:
    """Split each source's departures across targets by the OD probabilities.

    Each ``(source, commodity)`` releases ``quantity`` bikes this period; the OD
    matrix ``P(target | source, commodity)`` decides their destinations. The
    expected count per target (``departures * probability``) is rounded to whole
    bikes by the largest-remainder method, so the per-source total is preserved
    exactly. Each OD pair's mean historical duration sets the arrival period.

    Parameters
    ----------
    departures : pandas.DataFrame
        Realized departures this period: ``source_id``, ``commodity_category``,
        ``quantity``.
    od_matrix : pandas.DataFrame
        OD demand model from :func:`flows_to_od_matrix` (``probability`` and
        ``duration`` per ``(source_id, planned_target_id, commodity_category)``).
    period_id : int
        The current (departure) period.

    Returns
    -------
    pandas.DataFrame
        ``period_id``, ``source_id``, ``target_id``, ``commodity_category``,
        ``quantity``, ``planned_end_period`` -- only rows with ``quantity > 0``.
    """
    cols = ["period_id", "source_id", "target_id", "commodity_category",
            "quantity", "planned_end_period"]
    dep = departures[departures["quantity"] > 0]
    if dep.empty:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

    m = dep.merge(od_matrix, on=["source_id", "commodity_category"], how="left")
    m = m[m["probability"].notna()].copy()
    m["expected"] = m["quantity"] * m["probability"]
    m["base"] = np.floor(m["expected"]).astype("int64")
    m["remainder"] = m["expected"] - m["base"]

    # Largest-remainder rounding: hand the per-source shortfall to the targets
    # with the largest fractional parts, so sum(quantity) == departures exactly.
    m = m.sort_values(["source_id", "commodity_category", "remainder"],
                      ascending=[True, True, False])
    grp = m.groupby(["source_id", "commodity_category"])
    m["rank"] = grp.cumcount()
    m["shortfall"] = m["quantity"] - grp["base"].transform("sum")
    m["qty"] = m["base"] + (m["rank"] < m["shortfall"]).astype("int64")
    m = m[m["qty"] > 0]

    return pd.DataFrame({
        "period_id":          period_id,
        "source_id":          m["source_id"].values,
        "target_id":          m["planned_target_id"].values,
        "commodity_category": m["commodity_category"].values,
        "quantity":           m["qty"].astype("Int64").values,
        "planned_end_period": (period_id + m["duration"]).astype("Int64").values,
    })


def expand_potential_trips(potential_trips: pd.DataFrame, period_id: int) -> pd.DataFrame:
    """Expand aggregate OD potential trips into one concrete departed row per bike.

    Each aggregate row carries ``quantity`` identical bikes; this repeats it into
    that many trip rows and assigns a simulator ``flow_id`` (``sim_`` prefix so it
    cannot collide with the historical ``hist_`` ids).
    """
    rep = (potential_trips.loc[potential_trips.index.repeat(potential_trips["quantity"])]
           .reset_index(drop=True))
    return pd.DataFrame({
        "flow_id":            "sim_" + str(period_id) + "_" + rep.index.astype("string"),
        "source_id":          rep["source_id"],
        "planned_target_id":  rep["target_id"],
        "commodity_category": rep["commodity_category"],
        "start_period":       period_id,
        "planned_end_period": rep["planned_end_period"],
    })


# ---------------------------------------------------------------------------
# Run config + scheduling
# ---------------------------------------------------------------------------
class SimulatorConfigError(Exception):
    """Raised when an environment is built with insufficient inputs."""


@dataclasses.dataclass(frozen=True)
class Schedule:
    """When a phase runs: every ``every_n`` periods (1 = every period)."""

    every_n: int = 1

    @classmethod
    def every(cls) -> "Schedule":
        return cls(1)

    @classmethod
    def every_n_periods(cls, n: int) -> "Schedule":
        return cls(n)

    def should_run(self, period: "PeriodRow") -> bool:
        return period.period_id % self.every_n == 0


@dataclasses.dataclass(frozen=True)
class PeriodRow:
    """One row of the period grid (the simulation clock)."""

    period_id: int
    start_timestamp: Any
    end_timestamp: Any


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
@dataclasses.dataclass(frozen=True)
class SimulationState:
    """Simulation facts at the current period (replaced, never mutated).

    ``state_flows_df`` is the append-only event journal and the single source of
    truth for what happened. ``state_inventory_df`` and ``in_transit`` are
    materialized projections of that journal, kept incrementally for speed.
    ``in_transit`` is deliberately *not* part of the named state contract: it is
    internal plumbing (the "departed but not yet docked" working set), the same
    kind of projection as inventory but not worth observing on its own.

    The "Additional" observations are exposed as read-only properties derived on
    demand: ``state_departures_df``, ``state_arrivals_df``, ``state_demand_df``,
    ``state_supply_df`` and ``state_od_matrix_df``.

    Attributes
    ----------
    state_period_id_obj : PeriodRow
        The current period (the clock position).
    state_inventory_df : pandas.DataFrame
        Current stock: ``facility_id``, ``commodity_category``, ``quantity``.
    state_flows_df : pandas.DataFrame
        Append-only flow-event journal accumulated so far this run.
    state_resources_df : pandas.DataFrame
        Resource (truck) observations; empty in the historical replay.
    in_transit : pandas.DataFrame
        Internal projection: ``departed`` flows not yet docked.
    intermediates : dict
        Transient per-period hand-offs between phases.
    """

    state_period_id_obj: PeriodRow
    state_inventory_df: pd.DataFrame
    state_flows_df: pd.DataFrame
    state_resources_df: pd.DataFrame
    in_transit: pd.DataFrame = dataclasses.field(default_factory=empty_in_transit)
    intermediates: dict[str, Any] = dataclasses.field(default_factory=dict)

    # -- clock ---------------------------------------------------------------
    @property
    def period_id(self) -> int:
        """Integer id of the current period."""
        return self.state_period_id_obj.period_id

    # -- derived "Additional" observations -----------------------------------
    @property
    def state_departures_df(self) -> pd.DataFrame:
        """Outflow per period and origin, derived from ``state_flows_df``."""
        return flows_to_departures(self.state_flows_df)

    @property
    def state_arrivals_df(self) -> pd.DataFrame:
        """Inflow per period and destination, derived from ``state_flows_df``."""
        return flows_to_arrivals(self.state_flows_df)

    @property
    def state_demand_df(self) -> pd.DataFrame:
        """Realized user demand (= departures in an exact replay).

        Demand is the number of trips users *wanted* to take. Above the
        historical baseline some of it is lost to stockouts and diverges from
        ``state_departures_df``; in the replay every desired trip departs, so the
        two coincide and demand is read off the journal as the departures.
        """
        return flows_to_departures(self.state_flows_df)

    @property
    def state_supply_df(self) -> pd.DataFrame:
        """Bikes currently available to depart: the current inventory."""
        return self.state_inventory_df

    @property
    def state_od_matrix_df(self) -> pd.DataFrame:
        """Origin-destination demand matrix, derived from ``state_flows_df``."""
        return flows_to_od_matrix(self.state_flows_df)

    # -- functional updates --------------------------------------------------
    def with_inventory(self, new_inventory: pd.DataFrame) -> "SimulationState":
        return dataclasses.replace(self, state_inventory_df=new_inventory)

    def with_in_transit(self, new_in_transit: pd.DataFrame) -> "SimulationState":
        return dataclasses.replace(self, in_transit=new_in_transit)

    def with_resources(self, new_resources: pd.DataFrame) -> "SimulationState":
        return dataclasses.replace(self, state_resources_df=new_resources)

    def append_flows(self, events: pd.DataFrame | None) -> "SimulationState":
        """Append a phase's emitted events to the journal (source of truth)."""
        if events is None or not len(events):
            return self
        journal = pd.concat([self.state_flows_df, events], ignore_index=True)
        return dataclasses.replace(self, state_flows_df=journal)

    def with_intermediates(self, **updates: Any) -> "SimulationState":
        return dataclasses.replace(self, intermediates={**self.intermediates, **updates})

    def advance_period(self, next_period_obj: PeriodRow) -> "SimulationState":
        # Intermediates are transient per-period hand-offs between phases.
        return dataclasses.replace(self, state_period_id_obj=next_period_obj, intermediates={})


@dataclasses.dataclass
class PhaseResult:
    """What a phase returns: the next state plus any flow events it emitted."""

    state: SimulationState
    events: Any = None   # DataFrame of new flow-event rows, or None

    @classmethod
    def empty(cls, state: SimulationState) -> "PhaseResult":
        return cls(state=state, events=None)
