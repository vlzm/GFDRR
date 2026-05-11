"""Quick-start factory for creating a valid ``RawModelData`` from minimal inputs.

Lowers the entry barrier for experimentation.  Instead of constructing 9+
required DataFrames manually, pass 3 entities and a date range — the factory
auto-generates temporal tables, facility roles, operations, and edge rules.

Example::

    from datetime import date
    from gbp.core.factory import make_raw_model
    import pandas as pd

    raw = make_raw_model(
        facilities=pd.DataFrame({
            "facility_id": ["d1", "s1", "s2"],
            "facility_type": ["depot", "station", "station"],
            "name": ["Depot", "St 1", "St 2"],
        }),
        commodity_categories=pd.DataFrame({
            "commodity_category_id": ["bike"],
            "name": ["Bike"], "unit": ["unit"],
        }),
        resource_categories=pd.DataFrame({
            "resource_category_id": ["truck"],
            "name": ["Truck"],
            "base_capacity": [20.0], "capacity_unit": ["unit"],
        }),
        planning_start=date(2025, 1, 1),
        planning_end=date(2025, 1, 8),
    )
    print(raw.table_summary())
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from gbp.core.attributes.registry import AttributeRegistry
from gbp.core.enums import FacilityRole, ModalType, OperationType, PeriodType
from gbp.core.model import RawModelData
from gbp.core.roles import DEFAULT_ROLES, derive_roles

_ALL_OPERATIONS = {OperationType.RECEIVING.value, OperationType.STORAGE.value, OperationType.DISPATCH.value}


def make_raw_model(
    facilities: pd.DataFrame,
    commodity_categories: pd.DataFrame,
    resource_categories: pd.DataFrame,
    *,
    planning_start: date,
    planning_end: date,
    period_type: str = "day",
    edge_rules: pd.DataFrame | None = None,
    demand: pd.DataFrame | None = None,
    supply: pd.DataFrame | None = None,
    attributes: AttributeRegistry | None = None,
    **extra_tables: pd.DataFrame,
) -> RawModelData:
    """Create a valid ``RawModelData`` from minimal inputs.

    Auto-generates:

    - ``planning_horizon`` + ``planning_horizon_segments`` + ``periods``
      from date range
    - ``facility_roles`` from ``facility_type`` using ``DEFAULT_ROLES``
    - ``facility_operations`` with all operations enabled per facility
    - ``edge_rules``: all-to-all ROAD if not provided

    Parameters
    ----------
    facilities
        Must have ``facility_id``, ``facility_type``, ``name``.
    commodity_categories
        Must have ``commodity_category_id``, ``name``, ``unit``.
    resource_categories
        Must have ``resource_category_id``, ``name``,
        ``base_capacity``.
    planning_start
        First day of the planning horizon (inclusive).
    planning_end
        Last day of the planning horizon (exclusive).
    period_type
        Period granularity (``"day"``, ``"week"``, ``"month"``).
        Default is ``"day"``.
    edge_rules
        Custom edge rules. If not provided, a single all-to-all
        ROAD rule is used.
    demand
        Optional demand table.
    supply
        Optional supply table.
    attributes
        Pre-populated ``AttributeRegistry`` for parametric data.
    **extra_tables
        Any additional tables to pass through to ``RawModelData``
        (e.g. ``inventory_initial``, ``edges``).

    Returns
    -------
    RawModelData
        A validated instance ready for ``build_model()``.

    Raises
    ------
    ValueError
        If validation of the assembled model fails.
    """
    temporal = _generate_temporal(planning_start, planning_end, period_type)
    facility_roles = _generate_facility_roles(facilities)
    facility_operations = _generate_facility_operations(facilities)

    if edge_rules is None:
        edge_rules = pd.DataFrame({
            "source_type": [None],
            "target_type": [None],
            "commodity_category": [None],
            "modal_type": [ModalType.ROAD.value],
            "enabled": [True],
        })

    kwargs: dict = {
        "facilities": facilities,
        "commodity_categories": commodity_categories,
        "resource_categories": resource_categories,
        **temporal,
        "facility_roles": facility_roles,
        "facility_operations": facility_operations,
        "edge_rules": edge_rules,
    }

    if demand is not None:
        kwargs["demand"] = demand
    if supply is not None:
        kwargs["supply"] = supply
    if attributes is not None:
        kwargs["attributes"] = attributes
    kwargs.update(extra_tables)

    raw = RawModelData(**kwargs)
    raw.validate()
    return raw


def _generate_temporal(
    start: date, end: date, period_type: str,
) -> dict[str, pd.DataFrame]:
    """Build planning_horizon, segments, and periods from a date range."""
    pt = PeriodType(period_type)

    if pt == PeriodType.DAY:
        freq = "D"
    elif pt == PeriodType.WEEK:
        freq = "W-MON"
    else:
        freq = "MS"

    date_range = pd.date_range(start=start, end=end, freq=freq, inclusive="left")
    if len(date_range) == 0:
        date_range = pd.DatetimeIndex([pd.Timestamp(start)])

    period_rows = []
    for i, ts in enumerate(date_range):
        p_start = ts.date()
        if i + 1 < len(date_range):
            p_end = date_range[i + 1].date()
        else:
            p_end = end
        period_rows.append({
            "period_id": f"p{i}",
            "planning_horizon_id": "h1",
            "segment_index": 0,
            "period_index": i,
            "period_type": pt.value,
            "start_date": p_start,
            "end_date": p_end,
        })

    return {
        "planning_horizon": pd.DataFrame({
            "planning_horizon_id": ["h1"],
            "name": ["auto_horizon"],
            "start_date": [start],
            "end_date": [end],
        }),
        "planning_horizon_segments": pd.DataFrame({
            "planning_horizon_id": ["h1"],
            "segment_index": [0],
            "start_date": [start],
            "end_date": [end],
            "period_type": [pt.value],
        }),
        "periods": pd.DataFrame(period_rows),
    }


def _generate_facility_roles(facilities: pd.DataFrame) -> pd.DataFrame:
    """Derive roles for each facility using ``DEFAULT_ROLES`` and ``derive_roles``."""
    # Pre-compute role set per unique facility_type (derive_roles is constant for fixed ops)
    type_to_roles: dict[str, list[str]] = {}
    for ftype in facilities["facility_type"].astype(str).unique():
        roles = derive_roles(ftype, _ALL_OPERATIONS)
        type_to_roles[ftype] = sorted(r.value for r in roles)

    # Explode via merge
    ftype_roles = pd.DataFrame([
        {"facility_type": ft, "role": r}
        for ft, roles in type_to_roles.items()
        for r in roles
    ])
    result = (
        facilities[["facility_id", "facility_type"]]
        .astype(str)
        .merge(ftype_roles, on="facility_type")
        [["facility_id", "role"]]
    )
    return result.reset_index(drop=True)


def _generate_facility_operations(facilities: pd.DataFrame) -> pd.DataFrame:
    """Enable all standard operations for each facility."""
    ops = sorted(_ALL_OPERATIONS)
    fids = facilities["facility_id"].astype(str)
    # Cross-join: each facility × each operation
    result = pd.DataFrame({
        "facility_id": fids.repeat(len(ops)).values,
        "operation_type": ops * len(fids),
        "enabled": True,
    })
    return result.reset_index(drop=True)
