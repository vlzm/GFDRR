"""Vectorized edge construction from facility types and rules."""

from __future__ import annotations

import pandas as pd


def build_edges(
    facilities: pd.DataFrame,
    edge_rules: pd.DataFrame,
    manual_pairs: pd.DataFrame | None = None,
    distance_matrix: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build candidate edges from type rules plus optional manual pairs.

    Rule pass: cross join facilities matching (source_type, target_type) from
    ``edge_rules``, drop self-loops, keep modal_type and commodity_category
    from rules when present.

    Parameters
    ----------
    facilities
        Must contain ``facility_id``, ``facility_type``.
    edge_rules
        Must contain ``source_type``, ``target_type``; optional columns
        ``commodity_category``, ``modal_type``, ``enabled``.
    manual_pairs
        Optional scenario_manual_edges-like frame with ``source_id``,
        ``target_id``, ``modal_type``, ``commodity_category``. Default is
        ``None`` (no manual pairs).
    distance_matrix
        Optional frame with ``source_id``, ``target_id``, ``distance``, etc.
        Default is ``None`` (no distance merge).

    Returns
    -------
    pd.DataFrame
        At least ``source_id``, ``target_id``, ``modal_type``,
        ``commodity_category``; distance and other columns if merged.
    """
    rules = edge_rules.copy()
    if "enabled" in rules.columns:
        rules = rules[rules["enabled"]]

    sources = facilities.rename(
        columns={"facility_id": "source_id", "facility_type": "source_type"}
    )
    targets = facilities.rename(
        columns={"facility_id": "target_id", "facility_type": "target_type"}
    )

    candidates = sources.merge(rules, on="source_type", how="inner")
    edges = candidates.merge(targets, on="target_type", how="inner")
    edges = edges[edges["source_id"] != edges["target_id"]]

    rule_cols = ["source_id", "target_id", "modal_type", "commodity_category"]
    present = [c for c in rule_cols if c in edges.columns]
    built = edges[present].drop_duplicates()

    if manual_pairs is not None and not manual_pairs.empty:
        _manual_cols = ["source_id", "target_id", "modal_type", "commodity_category"]
        _present = [c for c in _manual_cols if c in manual_pairs.columns]
        mp = manual_pairs[_present].copy()
        built = pd.concat([built, mp], ignore_index=True).drop_duplicates()

    if distance_matrix is not None and not distance_matrix.empty:
        merge_keys = ["source_id", "target_id"]
        if all(k in distance_matrix.columns for k in merge_keys):
            extra = [c for c in distance_matrix.columns if c not in merge_keys]
            built = built.merge(
                distance_matrix[merge_keys + extra],
                on=merge_keys,
                how="left",
            )

    return built.reset_index(drop=True)
