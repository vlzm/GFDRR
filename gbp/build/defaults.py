"""Derivation helpers filling in optional ``RawModelData`` tables.

These helpers implement the "manual wins" rule: each function produces a
DataFrame that ``build_model`` will slot into the raw model *only* when
the corresponding user-supplied table is ``None``.  An explicitly empty
DataFrame is treated as a user choice of "no rows" and is not re-derived.

All operations are vectorized (groupby / merge) — no Python-level
iteration over data rows.
"""

from __future__ import annotations

import pandas as pd

from gbp.core.enums import FacilityRole
from gbp.core.roles import derive_roles


DEFAULT_COMMODITY_CATEGORY_ID = "DEFAULT_COMMODITY"
DEFAULT_RESOURCE_CATEGORY_ID = "DEFAULT_RESOURCE"


def derive_facility_roles(
    facilities: pd.DataFrame,
    facility_operations: pd.DataFrame,
) -> pd.DataFrame:
    """Derive ``facility_roles`` rows from facility types and enabled operations.

    Uses :func:`gbp.core.roles.derive_roles` per facility.  Facilities with
    no operation rows receive the default role set for their type (from
    ``DEFAULT_ROLES``) with any operation-dependent role trimmed out.

    Args:
        facilities: ``facility_id`` and ``facility_type`` columns.
        facility_operations: ``facility_id``, ``operation_type``, ``enabled``.

    Returns:
        DataFrame with columns ``facility_id``, ``role`` (one row per role).
    """
    enabled_ops = facility_operations[facility_operations["enabled"].astype(bool)]
    ops_per_facility: pd.Series = (
        enabled_ops.groupby("facility_id")["operation_type"]
        .apply(lambda s: set(s.astype(str)))
    )

    fac = facilities[["facility_id", "facility_type"]].copy()
    fac["facility_id"] = fac["facility_id"].astype(str)
    fac["facility_type"] = fac["facility_type"].astype(str)
    fac["operations"] = fac["facility_id"].map(ops_per_facility).apply(
        lambda v: v if isinstance(v, set) else set()
    )

    fac["roles"] = [
        derive_roles(ft, ops)
        for ft, ops in zip(fac["facility_type"], fac["operations"], strict=False)
    ]

    exploded = fac[["facility_id", "roles"]].explode("roles", ignore_index=True)
    exploded = exploded.dropna(subset=["roles"])
    exploded = exploded.rename(columns={"roles": "role"})
    exploded["role"] = exploded["role"].map(
        lambda r: r.value if isinstance(r, FacilityRole) else str(r)
    )
    return exploded.reset_index(drop=True)


def default_commodity_categories() -> pd.DataFrame:
    """Return a single-row commodity category DataFrame for minimal models."""
    return pd.DataFrame({
        "commodity_category_id": [DEFAULT_COMMODITY_CATEGORY_ID],
        "name": ["Default commodity"],
        "unit": ["unit"],
    })


def default_resource_categories() -> pd.DataFrame:
    """Return a single-row resource category DataFrame for minimal models."""
    return pd.DataFrame({
        "resource_category_id": [DEFAULT_RESOURCE_CATEGORY_ID],
        "name": ["Default resource"],
        "base_capacity": [0.0],
    })


def _organic_flow(observed_flow: pd.DataFrame) -> pd.DataFrame:
    """Restrict ``observed_flow`` to rows without an assigned resource.

    Rows whose ``resource_id`` is set represent operator-driven moves
    (e.g. rebalancing) and must not be double-counted as organic demand/supply.
    """
    if "resource_id" not in observed_flow.columns:
        return observed_flow
    return observed_flow.loc[observed_flow["resource_id"].isna()]


def derive_demand_from_flow(observed_flow: pd.DataFrame) -> pd.DataFrame:
    """Aggregate ``observed_flow`` into per-origin demand rows.

    Groups organic flow by ``source_id × date × commodity_category`` and
    renames ``source_id`` → ``facility_id`` so the result matches the
    ``Demand`` schema.  Flows attached to a resource (``resource_id``
    non-null) are excluded to avoid double-counting operator moves.
    """
    columns = ["facility_id", "commodity_category", "date", "quantity"]
    if observed_flow is None or observed_flow.empty:
        return pd.DataFrame(columns=columns)

    organic = _organic_flow(observed_flow)
    if organic.empty:
        return pd.DataFrame(columns=columns)

    grouped = (
        organic
        .groupby(["source_id", "commodity_category", "date"], as_index=False)
        ["quantity"].sum()
        .rename(columns={"source_id": "facility_id"})
    )
    return grouped[columns].reset_index(drop=True)


def derive_supply_from_flow(observed_flow: pd.DataFrame) -> pd.DataFrame:
    """Aggregate ``observed_flow`` into per-destination supply rows.

    Groups organic flow by ``target_id × date × commodity_category`` and
    renames ``target_id`` → ``facility_id`` so the result matches the
    ``Supply`` schema.  See :func:`derive_demand_from_flow` for the
    rationale on excluding resource-assigned flow.
    """
    columns = ["facility_id", "commodity_category", "date", "quantity"]
    if observed_flow is None or observed_flow.empty:
        return pd.DataFrame(columns=columns)

    organic = _organic_flow(observed_flow)
    if organic.empty:
        return pd.DataFrame(columns=columns)

    grouped = (
        organic
        .groupby(["target_id", "commodity_category", "date"], as_index=False)
        ["quantity"].sum()
        .rename(columns={"target_id": "facility_id"})
    )
    return grouped[columns].reset_index(drop=True)


def derive_inventory_initial(observed_inventory: pd.DataFrame) -> pd.DataFrame:
    """Take the earliest observed snapshot per facility × commodity_category.

    Returns rows matching the ``InventoryInitial`` schema.
    """
    columns = ["facility_id", "commodity_category", "quantity"]
    if observed_inventory is None or observed_inventory.empty:
        return pd.DataFrame(columns=columns)

    sorted_obs = observed_inventory.sort_values("date")
    first = sorted_obs.drop_duplicates(
        subset=["facility_id", "commodity_category"], keep="first",
    )
    return first[columns].reset_index(drop=True)


def derive_inventory_from_flow(observed_flow: pd.DataFrame) -> pd.DataFrame:
    """Seed initial inventory from the first observed day's outflow.

    For each ``(source_id, commodity_category)`` in the earliest date present
    in ``observed_flow``, sum the departures.  A facility that starts a day
    with N departures must have had at least N units in stock — this provides
    a reasonable non-zero seed when no ``observed_inventory`` telemetry is
    available (minimal-source case).

    Args:
        observed_flow: Columns ``source_id``, ``target_id``,
            ``commodity_category``, ``date``, ``quantity``. May include a
            ``resource_id`` column; rows with ``resource_id`` non-null are
            dropped to avoid double-counting operator moves.

    Returns:
        DataFrame with columns ``facility_id``, ``commodity_category``,
        ``quantity``.  Empty when *observed_flow* is empty or has only
        resource-assigned rows.
    """
    columns = ["facility_id", "commodity_category", "quantity"]
    if observed_flow is None or observed_flow.empty:
        return pd.DataFrame(columns=columns)

    organic = _organic_flow(observed_flow)
    if organic.empty:
        return pd.DataFrame(columns=columns)

    first_date = organic["date"].min()
    first_day = organic.loc[organic["date"] == first_date]

    grouped = (
        first_day
        .groupby(["source_id", "commodity_category"], as_index=False)
        ["quantity"].sum()
        .rename(columns={"source_id": "facility_id"})
    )
    return grouped[columns].reset_index(drop=True)
