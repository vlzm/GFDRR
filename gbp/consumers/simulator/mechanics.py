"""Mechanics: the rules a phase applies to the network state.

What actually happens to bikes and docks in a period -- capacity-aware docking
and overflow redirect (the dock side) and demand realization plus OD expansion
(the departure side). The phases decide *when* to apply these rules; this module
holds the rules themselves.

Mechanics never touch :class:`SimulationState` or the event journal: they take
plain frames and return *decisions* (what fits, what overflows, where each
overflow flow docks). Applying those decisions to the live state and writing the
events is the phase's job. So this module depends only on :mod:`state` for the
inventory arithmetic and on nothing above it: ``journal <- state <- mechanics <-
phases <- engine``.
"""

import numpy as np
import pandas as pd

from gbp.model import haversine_km, neighbor_distance_sq

from .state import adjust_inventory, dock_deltas


# ---------------------------------------------------------------------------
# Capacity-aware docking and overflow redirect
# ---------------------------------------------------------------------------
# The part that only matters above the historical baseline: when a station's docks are
# full, arriving bikes overflow and are redirected to the nearest station with a
# free dock. Never triggers in an exact replay, where capacity is never the limit.
def free_docks(inventory: pd.DataFrame, capacities: pd.DataFrame) -> pd.Series:
    """Free dock slots per facility: capacity minus bikes currently docked.

    Classic and electric bikes share the same physical docks, so occupancy is the
    total inventory across commodities.

    Parameters
    ----------
    inventory : pandas.DataFrame
        Current inventory: ``facility_id``, ``commodity_category``, ``quantity``.
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
    due: pd.DataFrame, free: pd.Series, target_col: str = "planned_target_id"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split docking flows at their target into ``(fits, overflow)``.

    The one docking rule: within each target the first ``free`` flows (in row
    order) dock; the rest are overflow. Vectorized through a per-target
    cumulative count -- no Python loop. ``target_col`` selects which station the
    flows dock at (the same pattern :func:`state.dock_deltas` uses):
    ``planned_target_id`` for flows docking at their planned station,
    ``realized_target_id`` for a redirect round docking at the station chosen
    for it.

    Parameters
    ----------
    due : pandas.DataFrame
        Flows docking this period (one station id per row in ``target_col``).
    free : pandas.Series
        Free dock slots per facility, from :func:`free_docks`.
    target_col : str, optional
        The column naming the station each flow docks at. Defaults to
        ``planned_target_id``.

    Returns
    -------
    tuple of (pandas.DataFrame, pandas.DataFrame)
        The flows that fit and the overflow flows, both subsets of ``due``.
    """
    if due.empty:
        return due, due
    rank = due.groupby(target_col).cumcount()
    capacity_here = due[target_col].map(free).fillna(0)
    fits = rank < capacity_here
    # Tier-1 contract: no target docks more flows than it has free slots.
    docked_n = due[fits].groupby(target_col).size()
    assert (docked_n <= free.reindex(docked_n.index).fillna(0)).all(), "docked over capacity"
    return due[fits], due[~fits]


def _nearest_free_station(targets: pd.Series, free: pd.Series, geo: pd.DataFrame) -> pd.Series:
    """Nearest *other* station with a free dock for each station id in ``targets``.

    Ranks candidates with :func:`gbp.model.neighbor_distance_sq` -- the one
    metric shared with the explainer ``redirect_neighbor_table``, so what the
    explainer shows is the order used here. Returns a Series aligned to
    ``targets`` (NA if none).

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
    target_coords = (
        coords.loc[coords.index.intersection(targets.unique())]
        .reset_index()
        .rename(columns={"facility_id": "target"})
    )
    cand = (
        coords.loc[coords.index.intersection(candidates)]
        .reset_index()
        .rename(columns={"facility_id": "candidate"})
    )
    pairs = target_coords.merge(cand, how="cross")
    pairs = pairs[pairs["target"] != pairs["candidate"]]
    pairs["dist2"] = neighbor_distance_sq(
        pairs["lat_x"], pairs["lng_x"], pairs["lat_y"], pairs["lng_y"]
    )
    # Stable sort so equally distant candidates tie-break deterministically.
    nearest = (
        pairs.sort_values("dist2", kind="stable")
        .drop_duplicates("target")
        .set_index("target")["candidate"]
    )
    return targets.map(nearest)


def _leg_durations(
    od_matrix: pd.DataFrame,
    geo: pd.DataFrame,
    trip_speed_km_per_period: float,
    source: pd.Series,
    target: pd.Series,
) -> pd.Series:
    """Travel time in periods for each (source, target) pair, aligned to ``source``.

    The pair's mean historical ``duration`` from the OD matrix, over all periods
    and commodities. For a pair no historical trip ever rode, the estimate is
    the great-circle distance between the two stations divided by
    ``trip_speed_km_per_period``. Both round to whole periods.
    """
    pair_duration = od_matrix.groupby(["source_id", "planned_target_id"])["duration"].mean()
    pairs = pd.MultiIndex.from_arrays([source, target])
    from_od = pd.Series(pair_duration.reindex(pairs).to_numpy(), index=source.index)
    coords = geo.set_index("facility_id")
    estimate = (
        haversine_km(
            source.map(coords["lat"]),
            source.map(coords["lng"]),
            target.map(coords["lat"]),
            target.map(coords["lng"]),
        )
        / trip_speed_km_per_period
    )
    return from_od.fillna(estimate).round().astype("int64")


def plan_overflow_redirect(
    inventory: pd.DataFrame,
    capacities: pd.DataFrame,
    geo: pd.DataFrame,
    od_matrix: pd.DataFrame,
    trip_speed_km_per_period: float,
    overflow: pd.DataFrame,
    period_id: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Plan a new leg for each overflow flow: to the nearest station with a free dock.

    A decision, not a state change: applying it (inventory, events) is the
    phase's job. Each planned leg gets three columns: ``realized_target_id``
    (the station it heads to), ``leg_end_period`` (``period_id`` plus the
    pair's travel time from the OD matrix, see :func:`_leg_durations`) and
    ``phase_round`` (the round that planned it, 1-based).

    Legs that dock in this same period (zero travel time) fill docks in rounds:
    each round docks up to the free capacity, and the next round sees those
    docks taken -- a running local copy of inventory tracks them. A leg that
    takes time holds no dock now: whether it fits is decided when it arrives,
    so it may bounce again there. A flow is lost only when no station in the
    network has a free dock.

    Parameters
    ----------
    inventory : pandas.DataFrame
        Current inventory: ``facility_id``, ``commodity_category``, ``quantity``.
    capacities : pandas.DataFrame
        Dock capacities: ``facility_id``, ``capacity``.
    geo : pandas.DataFrame
        Facility geography: ``facility_id``, ``lat``, ``lng``.
    od_matrix : pandas.DataFrame
        OD demand model; the source of the per-pair travel times.
    trip_speed_km_per_period : float
        Mean historical riding speed; the travel-time fallback for a pair with
        no OD entry (see :func:`_leg_durations`).
    overflow : pandas.DataFrame
        The flows that found no free dock at their arc's target.
    period_id : int
        The period the overflow happened in.

    Returns
    -------
    tuple of (pandas.DataFrame, pandas.DataFrame)
        ``redirects`` -- the overflow flows with the three planning columns --
        and the flows that found no free dock anywhere (lost).
    """
    planned = []
    remaining = overflow
    running = inventory
    round_no = 0
    while not remaining.empty:
        round_no += 1
        free = free_docks(running, capacities)
        nearest = _nearest_free_station(remaining["planned_target_id"], free, geo)
        found = remaining.assign(realized_target_id=nearest)
        found = found[found["realized_target_id"].notna()]
        if found.empty:
            break  # no station anywhere has a free dock: the rest is lost
        travel = _leg_durations(
            od_matrix,
            geo,
            trip_speed_km_per_period,
            found["planned_target_id"],
            found["realized_target_id"],
        )
        found = found.assign(leg_end_period=period_id + travel, phase_round=round_no)

        later = found[found["leg_end_period"] > period_id]
        now = found[found["leg_end_period"] == period_id]
        # The same docking rule as the planned dockings, at the redirect's target.
        docked_now, bounced = dock_up_to_capacity(now, free, "realized_target_id")
        planned += [later, docked_now]
        running = adjust_inventory(running, dock_deltas(docked_now, "realized_target_id"))
        remaining = bounced.drop(columns=["realized_target_id", "leg_end_period", "phase_round"])

    if planned:
        redirects = pd.concat(planned, ignore_index=True)
    else:
        redirects = overflow.iloc[:0].assign(
            realized_target_id=pd.Series(dtype="string"),
            leg_end_period=pd.Series(dtype="int64"),
            phase_round=pd.Series(dtype="int64"),
        )
    # Tier-1 contract: every overflow flow either gets a leg or is lost, never both.
    assert len(redirects) + len(remaining) == len(overflow), "overflow flows not conserved"
    return redirects, remaining


# ---------------------------------------------------------------------------
# Demand realization and OD expansion (FormDepartures / FormPotentialTrips)
# ---------------------------------------------------------------------------
def realize_departures(demand_now: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    """Departures per (facility, commodity): ``min(demand, inventory)``.

    Demand above the inventory is lost to a stockout; inventory is per commodity,
    so classic and electric demand are limited independently. Never triggers in an exact
    replay, where inventory always covers the historical demand.

    Parameters
    ----------
    demand_now : pandas.DataFrame
        This period's demand: ``facility_id``, ``commodity_category``, ``quantity``.
    inventory : pandas.DataFrame
        Current inventory: ``facility_id``, ``commodity_category``, ``quantity``.

    Returns
    -------
    pandas.DataFrame
        ``facility_id``, ``commodity_category``, ``departed``, ``lost``.
    """
    available = inventory.rename(columns={"quantity": "available"})
    out = demand_now.merge(available, on=["facility_id", "commodity_category"], how="left")
    out["available"] = out["available"].fillna(0)
    out["departed"] = out[["quantity", "available"]].min(axis=1).astype("int64")
    out["lost"] = (out["quantity"] - out["departed"]).astype("int64")
    # Tier-1 contracts: departures are bounded by inventory and loss is non-negative.
    assert (out["departed"] <= out["available"]).all(), "departed exceeds available inventory"
    assert (out["lost"] >= 0).all(), "stockout loss is negative"
    return out[["facility_id", "commodity_category", "departed", "lost"]]


def form_potential_trips(
    departures: pd.DataFrame, od_matrix: pd.DataFrame, period_id: int
) -> pd.DataFrame:
    """Split each source's departures across targets by the OD probabilities.

    Each ``(source, commodity)`` departs ``quantity`` bikes this period; the OD
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
        OD demand model from :func:`journal.flows_to_od_matrix` (``probability``
        and ``duration`` per ``(source_id, planned_target_id, commodity_category)``).
    period_id : int
        The current (departure) period.

    Returns
    -------
    pandas.DataFrame
        ``period_id``, ``source_id``, ``planned_target_id``, ``commodity_category``,
        ``quantity``, ``planned_end_period`` -- only rows with ``quantity > 0``.
    """
    cols = [
        "period_id",
        "source_id",
        "planned_target_id",
        "commodity_category",
        "quantity",
        "planned_end_period",
    ]
    dep = departures[departures["quantity"] > 0]
    if dep.empty:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

    m = dep.merge(
        od_matrix[od_matrix["period_id"] == period_id].drop(columns=["period_id"]),
        on=["source_id", "commodity_category"],
        how="left",
    )
    m = m[m["probability"].notna()].copy()
    m["expected"] = m["quantity"] * m["probability"]
    m["base"] = np.floor(m["expected"]).astype("int64")
    m["remainder"] = m["expected"] - m["base"]

    # Largest-remainder rounding: hand the per-source leftover to the targets
    # with the largest fractional parts, so sum(quantity) == departures exactly.
    m = m.sort_values(
        ["source_id", "commodity_category", "remainder"], ascending=[True, True, False]
    )
    grp = m.groupby(["source_id", "commodity_category"])
    m["rank"] = grp.cumcount()
    m["leftover"] = m["quantity"] - grp["base"].transform("sum")
    m["qty"] = m["base"] + (m["rank"] < m["leftover"]).astype("int64")
    m = m[m["qty"] > 0]

    return pd.DataFrame(
        {
            "period_id": period_id,
            "source_id": m["source_id"].values,
            "planned_target_id": m["planned_target_id"].values,
            "commodity_category": m["commodity_category"].values,
            "quantity": m["qty"].astype("Int64").values,
            "planned_end_period": (period_id + m["duration"]).astype("Int64").values,
        }
    )


def expand_potential_trips(potential_trips: pd.DataFrame, period_id: int) -> pd.DataFrame:
    """Expand aggregate OD potential trips into one concrete departed row per bike.

    Each aggregate row carries ``quantity`` identical bikes; this repeats it into
    that many trip rows and assigns a simulator ``flow_id`` (``sim_`` prefix so it
    cannot collide with the historical ``hist_`` ids).
    """
    rep = potential_trips.loc[
        potential_trips.index.repeat(potential_trips["quantity"])
    ].reset_index(drop=True)
    return pd.DataFrame(
        {
            "flow_id": "sim_" + str(period_id) + "_" + rep.index.astype("string"),
            "source_id": rep["source_id"],
            "planned_target_id": rep["planned_target_id"],
            "commodity_category": rep["commodity_category"],
            "start_period": period_id,
            "planned_end_period": rep["planned_end_period"],
        }
    )
