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
    # Tier-1 contract: no target docks more flows than it has free slots.
    docked_n = due[fits].groupby("planned_target_id").size()
    assert (docked_n <= free.reindex(docked_n.index).fillna(0)).all(), "docked over capacity"
    return due[fits], due[~fits]


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
    target_coords = (coords.loc[coords.index.intersection(targets.unique())]
                     .reset_index().rename(columns={"facility_id": "target"}))
    cand = (coords.loc[coords.index.intersection(candidates)]
            .reset_index().rename(columns={"facility_id": "candidate"}))
    pairs = target_coords.merge(cand, how="cross")
    pairs = pairs[pairs["target"] != pairs["candidate"]]
    pairs["dist2"] = (pairs["lat_x"] - pairs["lat_y"]) ** 2 + (pairs["lng_x"] - pairs["lng_y"]) ** 2
    nearest = pairs.sort_values("dist2").drop_duplicates("target").set_index("target")["candidate"]
    return targets.map(nearest)


def plan_overflow_redirect(
    inventory: pd.DataFrame,
    capacities: pd.DataFrame,
    geo: pd.DataFrame,
    overflow: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Plan where each overflow flow docks: the nearest station with a free dock.

    A decision, not a state change: this returns *where each flow goes* and leaves
    applying it (inventory, events) to the phase. Rounds, not per-row loops: each
    round maps every still-unplaced flow to its nearest free station, docks up to
    capacity there, and repeats with the leftovers until none remain or no dock is
    free anywhere. A running copy of inventory tracks the docks each round fills,
    so the within-batch capacity coupling is honoured exactly; that copy is local
    and never leaves the function.

    Parameters
    ----------
    inventory : pandas.DataFrame
        Current inventory: ``facility_id``, ``commodity_category``, ``quantity``.
    capacities : pandas.DataFrame
        Dock capacities: ``facility_id``, ``capacity``.
    geo : pandas.DataFrame
        Facility geography: ``facility_id``, ``lat``, ``lng``.
    overflow : pandas.DataFrame
        The flows that found no free dock at their planned target.

    Returns
    -------
    tuple of (pandas.DataFrame, pandas.DataFrame)
        ``redirected`` -- the overflow flows that docked, each with a
        ``realized_target_id`` column naming the station it docked at -- and the
        flows that found no free dock anywhere (lost).
    """
    redirected_batches = []
    remaining = overflow
    running = inventory
    while not remaining.empty:
        free = free_docks(running, capacities)
        if not (free > 0).any():
            break
        target = _nearest_free_station(remaining["planned_target_id"], free, geo)
        candidate = remaining.assign(realized_target_id=target)
        candidate = candidate[candidate["realized_target_id"].notna()]
        if candidate.empty:
            break
        rank = candidate.groupby("realized_target_id").cumcount()
        fits = rank < candidate["realized_target_id"].map(free)
        docked = candidate[fits]
        redirected_batches.append(docked)
        running = adjust_inventory(running, dock_deltas(docked, "realized_target_id"))
        remaining = candidate[~fits].drop(columns="realized_target_id")
    if redirected_batches:
        redirected = pd.concat(redirected_batches, ignore_index=True)
    else:
        redirected = overflow.iloc[:0].assign(
            realized_target_id=overflow["planned_target_id"].iloc[:0]
        )
    # Tier-1 contract: every overflow flow either docks or is lost, never both.
    assert len(redirected) + len(remaining) == len(overflow), "overflow flows not conserved"
    return redirected, remaining


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
    cols = ["period_id", "source_id", "planned_target_id", "commodity_category",
            "quantity", "planned_end_period"]
    dep = departures[departures["quantity"] > 0]
    if dep.empty:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

    m = dep.merge(od_matrix, on=["source_id", "commodity_category"], how="left")
    m = m[m["probability"].notna()].copy()
    m["expected"] = m["quantity"] * m["probability"]
    m["base"] = np.floor(m["expected"]).astype("int64")
    m["remainder"] = m["expected"] - m["base"]

    # Largest-remainder rounding: hand the per-source leftover to the targets
    # with the largest fractional parts, so sum(quantity) == departures exactly.
    m = m.sort_values(["source_id", "commodity_category", "remainder"],
                      ascending=[True, True, False])
    grp = m.groupby(["source_id", "commodity_category"])
    m["rank"] = grp.cumcount()
    m["leftover"] = m["quantity"] - grp["base"].transform("sum")
    m["qty"] = m["base"] + (m["rank"] < m["leftover"]).astype("int64")
    m = m[m["qty"] > 0]

    return pd.DataFrame({
        "period_id":          period_id,
        "source_id":          m["source_id"].values,
        "planned_target_id":  m["planned_target_id"].values,
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
        "planned_target_id":  rep["planned_target_id"],
        "commodity_category": rep["commodity_category"],
        "start_period":       period_id,
        "planned_end_period": rep["planned_end_period"],
    })
