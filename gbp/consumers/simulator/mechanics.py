"""Mechanics: the rules a phase applies to the network state.

What actually happens to bikes and docks in a period -- capacity-aware docking
and overflow redirect (the dock side) and demand realization plus OD expansion
(the departure side). The phases decide *when* to apply these rules; this module
holds the rules themselves.

It depends on :mod:`journal` for the event builders it emits and on :mod:`state`
for the inventory arithmetic, and on nothing above it: ``journal <- state <-
mechanics <- phases <- engine``.
"""

import numpy as np
import pandas as pd

from .journal import redirected_events
from .state import adjust_inventory


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
        OD demand model from :func:`journal.flows_to_od_matrix` (``probability``
        and ``duration`` per ``(source_id, planned_target_id, commodity_category)``).
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
