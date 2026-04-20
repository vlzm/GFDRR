"""Tests for vectorized edge building."""

from __future__ import annotations

import pandas as pd

from gbp.build.edge_builder import build_edges


def test_rule_based_depot_to_station() -> None:
    """Edge rules create depot to station pairs."""
    facilities = pd.DataFrame(
        {
            "facility_id": ["d1", "s1", "s2"],
            "facility_type": ["depot", "station", "station"],
            "name": ["D", "S1", "S2"],
        }
    )
    rules = pd.DataFrame(
        {
            "source_type": ["depot"],
            "target_type": ["station"],
            "commodity_category": ["bike"],
            "modal_type": ["road"],
            "enabled": [True],
        }
    )
    out = build_edges(facilities, rules, manual_pairs=None, distance_matrix=None)
    assert len(out) == 2
    assert set(out["source_id"]) == {"d1"}
    assert set(out["target_id"]) == {"s1", "s2"}


def test_self_loop_excluded() -> None:
    """Facilities matching both source and target types exclude self edges."""
    facilities = pd.DataFrame(
        {
            "facility_id": ["s1", "s2"],
            "facility_type": ["station", "station"],
            "name": ["S1", "S2"],
        }
    )
    rules = pd.DataFrame(
        {
            "source_type": ["station"],
            "target_type": ["station"],
            "commodity_category": ["bike"],
            "modal_type": ["road"],
            "enabled": [True],
        }
    )
    out = build_edges(facilities, rules, None, None)
    assert not ((out["source_id"] == out["target_id"]).any())


def test_manual_pairs_union() -> None:
    """Manual pairs add edges not covered by rules."""
    facilities = pd.DataFrame(
        {
            "facility_id": ["a", "b"],
            "facility_type": ["depot", "station"],
            "name": ["A", "B"],
        }
    )
    rules = pd.DataFrame(
        {
            "source_type": ["depot"],
            "target_type": ["station"],
            "commodity_category": ["bike"],
            "modal_type": ["road"],
            "enabled": [True],
        }
    )
    manual = pd.DataFrame(
        {
            "source_id": ["b"],
            "target_id": ["a"],
            "modal_type": ["road"],
            "commodity_category": ["bike"],
        }
    )
    out = build_edges(facilities, rules, manual, None)
    assert len(out) == 2
