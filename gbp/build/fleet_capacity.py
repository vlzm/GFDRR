"""Effective fleet capacity per home facility and resource category."""

from __future__ import annotations

import pandas as pd


def compute_fleet_capacity(
    resource_fleet: pd.DataFrame | None,
    resource_categories: pd.DataFrame,
    resources: pd.DataFrame | None = None,
) -> pd.DataFrame | None:
    """Compute ``effective_capacity`` per (facility, resource_category).

    If ``resources`` is provided, aggregate per-resource capacity at
    ``home_facility_id``. Otherwise use ``count * base_capacity`` from fleet.
    """
    if resource_fleet is None or resource_fleet.empty:
        return None

    rc = resource_categories
    base_cols = ["resource_category_id", "base_capacity"]
    if not all(c in rc.columns for c in base_cols):
        return None

    if resources is not None and not resources.empty:
        r = resources.merge(
            rc[["resource_category_id", "base_capacity"]],
            left_on="resource_category",
            right_on="resource_category_id",
            how="left",
        )
        r["effective_capacity"] = r["capacity_override"].fillna(r["base_capacity"])
        out = (
            r.groupby(["home_facility_id", "resource_category"], as_index=False)[
                "effective_capacity"
            ]
            .sum()
            .rename(columns={"home_facility_id": "facility_id"})
        )
        return out

    f = resource_fleet.merge(
        rc[["resource_category_id", "base_capacity"]],
        left_on="resource_category",
        right_on="resource_category_id",
        how="left",
    )
    f["effective_capacity"] = f["count"].astype(float) * f["base_capacity"].astype(float)
    out = f[["facility_id", "resource_category", "effective_capacity"]].copy()
    return out.reset_index(drop=True)
