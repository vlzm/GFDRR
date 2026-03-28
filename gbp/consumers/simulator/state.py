"""Simulation state: immutable snapshot of the world at a point in time.

Contains ``SimulationState`` (frozen dataclass), ``PeriodRow`` type,
and ``init_state`` factory that builds the initial state from resolved data.
"""

from __future__ import annotations

import dataclasses
from datetime import date
from typing import NamedTuple

import pandas as pd

from gbp.core.enums import ResourceStatus
from gbp.core.model import ResolvedModelData


class PeriodRow(NamedTuple):
    """Typed representation of one row from ``resolved.periods.itertuples()``.

    Field order must match the DataFrame column order produced by the build
    pipeline (see ``gbp/core/schemas/temporal.py``).  The ``Index`` field is
    injected by ``itertuples()`` and corresponds to the DataFrame integer index.
    """

    Index: int
    period_id: str
    planning_horizon_id: str
    segment_index: int
    period_index: int
    period_type: str
    start_date: date
    end_date: date


# -- Column constants ----------------------------------------------------------

INVENTORY_COLUMNS: list[str] = [
    "facility_id",
    "commodity_category",
    "quantity",
]

IN_TRANSIT_COLUMNS: list[str] = [
    "shipment_id",
    "source_id",
    "target_id",
    "commodity_category",
    "quantity",
    "resource_id",
    "departure_period",
    "arrival_period",
]

RESOURCE_COLUMNS: list[str] = [
    "resource_id",
    "resource_category",
    "home_facility_id",
    "current_facility_id",
    "status",
    "available_at_period",
]


# -- SimulationState -----------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class SimulationState:
    """Immutable snapshot of the world at a point in time.

    Phases must never mutate DataFrames in place — use the ``with_*`` helpers
    which return a **new** ``SimulationState`` via ``dataclasses.replace``.

    Attributes:
        period_index: Current period ordinal.
        period_id: Current period identifier (string).
        inventory: Commodity stock per facility
            (columns: facility_id, commodity_category, quantity).
        in_transit: Shipments currently en route
            (columns: shipment_id, source_id, target_id, commodity_category,
            quantity, resource_id, departure_period, arrival_period).
        resources: Instance-level resource positions and statuses
            (columns: resource_id, resource_category, home_facility_id,
            current_facility_id, status, available_at_period).
    """

    period_index: int
    period_id: str
    inventory: pd.DataFrame
    in_transit: pd.DataFrame
    resources: pd.DataFrame

    # -- Immutable update helpers ----------------------------------------------

    def with_inventory(self, new_inventory: pd.DataFrame) -> SimulationState:
        """Return a new state with replaced inventory."""
        return dataclasses.replace(self, inventory=new_inventory)

    def with_in_transit(self, new_in_transit: pd.DataFrame) -> SimulationState:
        """Return a new state with replaced in-transit shipments."""
        return dataclasses.replace(self, in_transit=new_in_transit)

    def with_resources(self, new_resources: pd.DataFrame) -> SimulationState:
        """Return a new state with replaced resource table."""
        return dataclasses.replace(self, resources=new_resources)

    def advance_period(
        self,
        next_period_index: int,
        next_period_id: str,
    ) -> SimulationState:
        """Return a new state positioned at the next period."""
        return dataclasses.replace(
            self,
            period_index=next_period_index,
            period_id=next_period_id,
        )


# -- Initialisation ------------------------------------------------------------


def init_state(resolved: ResolvedModelData) -> SimulationState:
    """Create the initial ``SimulationState`` from resolved model data.

    Args:
        resolved: Fully resolved model produced by ``build_model()``.

    Returns:
        A ``SimulationState`` positioned at the first period with inventory,
        in-transit, and resource DataFrames initialised from *resolved*.
    """
    # Inventory
    if resolved.inventory_initial is not None:
        inventory = resolved.inventory_initial[
            ["facility_id", "commodity_category", "quantity"]
        ].copy()
    else:
        inventory = pd.DataFrame(columns=INVENTORY_COLUMNS)

    # In-transit
    if resolved.inventory_in_transit is not None:
        in_transit = _init_in_transit(resolved.inventory_in_transit, resolved)
    else:
        in_transit = pd.DataFrame(columns=IN_TRANSIT_COLUMNS)

    # Resources
    resources = _init_resources(resolved)

    # First period
    first = resolved.periods.iloc[0]
    return SimulationState(
        period_index=int(first["period_index"]),
        period_id=str(first["period_id"]),
        inventory=inventory,
        in_transit=in_transit,
        resources=resources,
    )


def _init_in_transit(
    raw_in_transit: pd.DataFrame,
    resolved: ResolvedModelData,
) -> pd.DataFrame:
    """Convert raw in-transit inventory to simulation in-transit format.

    The raw table uses ``departure_date`` / ``expected_arrival_date`` while
    the simulation works with ``departure_period`` / ``arrival_period``
    (period_index integers).  This helper maps dates to period indices using
    the resolved periods table.
    """
    # TODO: Map departure_date and expected_arrival_date to period_index values
    #       using resolved.periods (date range lookup).
    #       For now return empty frame — in-transit at init is rare in MVP.
    return pd.DataFrame(columns=IN_TRANSIT_COLUMNS)


def _init_resources(resolved: ResolvedModelData) -> pd.DataFrame:
    """Build the initial resources DataFrame.

    If L3 ``resolved.resources`` are provided, use them directly.
    Otherwise generate individual resource instances from
    ``resolved.resource_fleet`` (facility_id x resource_category x count).

    Args:
        resolved: Fully resolved model.

    Returns:
        DataFrame with columns defined by ``RESOURCE_COLUMNS``.
    """
    if resolved.resources is not None:
        # L3 resources are explicitly provided
        resources = resolved.resources[
            ["resource_id", "resource_category", "home_facility_id"]
        ].copy()
        resources["current_facility_id"] = resources["home_facility_id"]
        resources["status"] = ResourceStatus.AVAILABLE.value
        resources["available_at_period"] = None
        return resources[RESOURCE_COLUMNS]

    if resolved.resource_fleet is not None:
        return _generate_resources_from_fleet(resolved.resource_fleet)

    # No resources at all — empty frame
    return pd.DataFrame(columns=RESOURCE_COLUMNS)


def _generate_resources_from_fleet(fleet: pd.DataFrame) -> pd.DataFrame:
    """Expand ``resource_fleet`` rows into individual resource instances.

    Uses vectorized repeat + cumcount to avoid iterrows.

    Args:
        fleet: DataFrame with columns (facility_id, resource_category, count).

    Returns:
        DataFrame with one row per resource instance, columns per
        ``RESOURCE_COLUMNS``.
    """
    # TODO: Vectorized expansion:
    #   1. expanded = fleet.loc[fleet.index.repeat(fleet["count"])].reset_index(drop=True)
    #   2. Assign resource_id = f"{resource_category}_{facility_id}_{cumcount}"
    #      using groupby cumcount for uniqueness within (facility_id, resource_category).
    #   3. Rename facility_id -> home_facility_id, add current_facility_id = home_facility_id.
    #   4. Set status = ResourceStatus.AVAILABLE.value, available_at_period = None.
    #   5. Return expanded[RESOURCE_COLUMNS].
    #
    # Skeleton implementation below handles the common case correctly
    # but the TODO above describes the fully vectorized approach.

    if fleet.empty:
        return pd.DataFrame(columns=RESOURCE_COLUMNS)

    expanded = fleet.loc[fleet.index.repeat(fleet["count"])].reset_index(drop=True)
    cumcount = expanded.groupby(["facility_id", "resource_category"]).cumcount()
    expanded["resource_id"] = (
        expanded["resource_category"]
        + "_"
        + expanded["facility_id"]
        + "_"
        + cumcount.astype(str)
    )
    expanded["home_facility_id"] = expanded["facility_id"]
    expanded["current_facility_id"] = expanded["facility_id"]
    expanded["status"] = ResourceStatus.AVAILABLE.value
    expanded["available_at_period"] = None

    return expanded[RESOURCE_COLUMNS].reset_index(drop=True)
