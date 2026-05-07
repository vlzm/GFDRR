"""Planner strategies for ``RebalancerTask``.

A *planner* turns a per-station imbalance — over-utilized sources and
under-utilized destinations — into a flat list of pickup-delivery pairs of
the form *(from station A, to station B, N units)*.  The pairs are then
fed to the OR-Tools PDP solver inside :class:`RebalancerTask`.

Splitting this stage into its own object lets a project mix and match
matching strategies without touching the routing/PDP machinery.  Different
strategies will trade off between:

- Speed (size of the pair list shown to OR-Tools).
- Geographic awareness (whether the matching considers distances at all).
- Optimality (the matching may force a sub-optimal route by hiding good
  pairs from the solver).

The default strategy, :class:`IntervalOverlapPlanner`, is geography-blind
and matches sources to destinations by interval-overlap on cumulative
excess/deficit.  It is fast and produces N+M-1 pairs in the worst case,
but can miss geographically attractive pairs whose excess/deficit
intervals do not overlap.
"""

from __future__ import annotations

from typing import Any, Protocol, TypedDict

import pandas as pd


class Pair(TypedDict):
    """One pickup-delivery instruction for the PDP solver.

    Attributes:
        pickup_node_id: Source station id.
        pickup_latitude: Source latitude (degrees).
        pickup_longitude: Source longitude (degrees).
        delivery_node_id: Destination station id.
        delivery_latitude: Destination latitude (degrees).
        delivery_longitude: Destination longitude (degrees).
        quantity: Units to move; always ``> 0`` and ``<= truck_capacity``.
    """

    pickup_node_id: str
    pickup_latitude: float
    pickup_longitude: float
    delivery_node_id: str
    delivery_latitude: float
    delivery_longitude: float
    quantity: int


class Planner(Protocol):
    """Strategy interface: turn imbalance frames into a list of PDP pairs."""

    def plan(
        self,
        sources: pd.DataFrame,
        destinations: pd.DataFrame,
        truck_capacity: float,
        distance_matrix: pd.DataFrame | None,
    ) -> list[Pair]:
        """Match sources to destinations and return pickup-delivery pairs.

        Args:
            sources: Over-utilized stations, with at least ``node_id``,
                ``latitude``, ``longitude``, ``excess`` columns.
            destinations: Under-utilized stations, with at least
                ``node_id``, ``latitude``, ``longitude``, ``deficit``
                columns.
            truck_capacity: Per-trip cap on a single pair's ``quantity``.
            distance_matrix: Optional distance matrix in km.  May be
                ignored by geography-blind strategies.

        Returns:
            A list of :class:`Pair` dicts.  Empty when nothing can be
            matched.
        """
        ...


class IntervalOverlapPlanner:
    """Geography-blind matching by interval overlap on cumulative sums.

    Sorts sources by ``excess`` descending and destinations by ``deficit``
    descending, lays both onto a shared 1D axis as cumulative intervals,
    and emits a pair ``(s, d, qty)`` for every (source, destination) whose
    intervals overlap.  Each pair's ``quantity`` is clipped to
    ``truck_capacity``.

    Salvaged from
    ``gbp/rebalancer/dataloader.py:DataLoaderRebalancer.create_pickup_delivery_pairs``.

    Note:
        ``distance_matrix`` is accepted for protocol compatibility but
        ignored — the matching never looks at geography.  This is the
        algorithm's main blind spot: a geographically attractive pair
        whose intervals do not overlap is simply hidden from the PDP
        solver.
    """

    def plan(
        self,
        sources: pd.DataFrame,
        destinations: pd.DataFrame,
        truck_capacity: float,
        distance_matrix: pd.DataFrame | None,
    ) -> list[Pair]:
        """Return interval-overlap pairs; see class docstring for semantics."""
        del distance_matrix  # geography-blind by design

        if sources.empty or destinations.empty:
            return []

        supply = sources.sort_values("excess", ascending=False).reset_index(drop=True)
        demand = destinations.sort_values("deficit", ascending=False).reset_index(drop=True)

        supply = supply.copy()
        demand = demand.copy()
        supply["end"] = supply["excess"].cumsum()
        supply["start"] = supply["end"] - supply["excess"]
        demand["end"] = demand["deficit"].cumsum()
        demand["start"] = demand["end"] - demand["deficit"]

        cross = supply.assign(_k=1).merge(
            demand.assign(_k=1), on="_k", suffixes=("_p", "_d"),
        )
        cross["quantity"] = (
            cross[["end_p", "end_d"]].min(axis=1)
            - cross[["start_p", "start_d"]].max(axis=1)
        ).clip(lower=0).astype(int)
        cross["quantity"] = cross["quantity"].clip(upper=int(truck_capacity))
        cross = cross[cross["quantity"] > 0]
        if cross.empty:
            return []

        pairs: list[Pair] = []
        for _, r in cross.iterrows():
            pairs.append(_row_to_pair(r))
        return pairs


def _row_to_pair(r: Any) -> Pair:
    """Map a merged cross-join row to a :class:`Pair`."""
    return Pair(
        pickup_node_id=str(r["node_id_p"]),
        pickup_latitude=float(r["latitude_p"]),
        pickup_longitude=float(r["longitude_p"]),
        delivery_node_id=str(r["node_id_d"]),
        delivery_latitude=float(r["latitude_d"]),
        delivery_longitude=float(r["longitude_d"]),
        quantity=int(r["quantity"]),
    )
