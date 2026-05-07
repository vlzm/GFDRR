"""Unit tests for ``rebalancer_planner.IntervalOverlapPlanner``.

The planner is a pure function over two frames; tests pin the
interval-overlap matching against a hand-traceable example and the
truck-capacity clip.
"""

from __future__ import annotations

import pandas as pd

from gbp.consumers.simulator.tasks.rebalancer_planner import (
    IntervalOverlapPlanner,
)


def _sources(rows: list[tuple[str, int]]) -> pd.DataFrame:
    """Build a sources frame from ``[(node_id, excess), ...]``.

    Latitude/longitude are filled with placeholder zeros — the planner is
    geography-blind, so the values do not affect the matching.
    """
    return pd.DataFrame(
        [
            {"node_id": nid, "latitude": 0.0, "longitude": 0.0, "excess": exc}
            for nid, exc in rows
        ]
    )


def _destinations(rows: list[tuple[str, int]]) -> pd.DataFrame:
    """Build a destinations frame from ``[(node_id, deficit), ...]``."""
    return pd.DataFrame(
        [
            {"node_id": nid, "latitude": 0.0, "longitude": 0.0, "deficit": dfc}
            for nid, dfc in rows
        ]
    )


def _pair_quantities(pairs: list) -> dict[tuple[str, str], int]:
    """Collapse a list of pair dicts into ``{(pickup, delivery): quantity}``."""
    return {(p["pickup_node_id"], p["delivery_node_id"]): p["quantity"] for p in pairs}


def test_empty_inputs_return_empty_pairs() -> None:
    planner = IntervalOverlapPlanner()
    assert planner.plan(_sources([]), _destinations([("d", 5)]), 100, None) == []
    assert planner.plan(_sources([("s", 5)]), _destinations([]), 100, None) == []


def test_interval_overlap_matches_hand_traced_example() -> None:
    """Known-good example, hand-traced.

    After sorting by excess/deficit descending:

    * Supply intervals: S2 [0,15], S0 [15,25], S1 [25,31]
    * Demand intervals: D1 [0,12], D0 [12,20], D2 [20,23]

    Non-zero overlaps: (S2,D1)=12, (S2,D0)=3, (S0,D0)=5, (S0,D2)=3.
    Total demand 23 is satisfied; 8 supply units (S0=2, S1=6) stay
    unmatched, mirroring supply > demand by 8.
    """
    sources = _sources([("S0", 10), ("S1", 6), ("S2", 15)])
    destinations = _destinations([("D0", 8), ("D1", 12), ("D2", 3)])

    pairs = IntervalOverlapPlanner().plan(
        sources, destinations, truck_capacity=100, distance_matrix=None,
    )

    assert _pair_quantities(pairs) == {
        ("S2", "D1"): 12,
        ("S2", "D0"): 3,
        ("S0", "D0"): 5,
        ("S0", "D2"): 3,
    }


def test_truck_capacity_clips_pair_quantity() -> None:
    """A single excess/deficit overlap larger than the truck cap is clipped."""
    sources = _sources([("S0", 50)])
    destinations = _destinations([("D0", 50)])

    pairs = IntervalOverlapPlanner().plan(
        sources, destinations, truck_capacity=20, distance_matrix=None,
    )

    assert len(pairs) == 1
    assert pairs[0]["quantity"] == 20


def test_distance_matrix_is_ignored() -> None:
    """Passing a distance matrix changes nothing for the geography-blind planner."""
    sources = _sources([("S0", 5)])
    destinations = _destinations([("D0", 5)])
    dm = pd.DataFrame(
        [{"source_id": "S0", "target_id": "D0", "distance": 999.0}]
    )

    without_dm = IntervalOverlapPlanner().plan(sources, destinations, 100, None)
    with_dm = IntervalOverlapPlanner().plan(sources, destinations, 100, dm)

    assert without_dm == with_dm
