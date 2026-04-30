"""Simulation state: immutable snapshot of the world at a point in time.

Contains ``SimulationState`` (frozen dataclass), ``PeriodRow`` type,
and ``init_state`` factory that builds the initial state from resolved data.
"""

from __future__ import annotations

import dataclasses
from datetime import date
from typing import Any, NamedTuple

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
        intermediates: Transient per-period cache used to pass intermediate
            tables (e.g. latent demand marginals, OD probabilities) between
            phases within a single period.  Wiped automatically by
            ``advance_period`` so data cannot leak across periods.
    """

    period_index: int
    period_id: str
    inventory: pd.DataFrame
    in_transit: pd.DataFrame
    resources: pd.DataFrame
    intermediates: dict[str, Any] = dataclasses.field(default_factory=dict)

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

    def with_intermediates(self, **updates: Any) -> SimulationState:
        """Return a new state with *updates* merged into ``intermediates``.

        Existing keys are overwritten by *updates*; other keys are preserved.
        """
        merged = {**self.intermediates, **updates}
        return dataclasses.replace(self, intermediates=merged)

    def advance_period(
        self,
        next_period_index: int,
        next_period_id: str,
    ) -> SimulationState:
        """Return a new state positioned at the next period.

        ``intermediates`` is wiped — it is scoped to a single period.
        """
        return dataclasses.replace(
            self,
            period_index=next_period_index,
            period_id=next_period_id,
            intermediates={},
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
    inventory = resolved.inventory_initial[
        ["facility_id", "commodity_category", "quantity"]
    ].copy()

    # In-transit: declared pre-horizon shipments + organic returns from supply
    declared_transit = _init_in_transit(resolved.inventory_in_transit, resolved)
    organic_transit = _init_in_transit_from_supply(resolved)

    transit_frames = [df for df in (declared_transit, organic_transit) if not df.empty]
    if transit_frames:
        in_transit = pd.concat(transit_frames, ignore_index=True)[IN_TRANSIT_COLUMNS]
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


def _init_in_transit_from_supply(resolved: ResolvedModelData) -> pd.DataFrame:
    """Convert ``resolved.supply`` into in-transit shipments for ``ArrivalsPhase``.

    Each supply row represents organic arrivals (returns) at a facility during
    a given period.  Seeding them into ``state.in_transit`` at init time lets
    ``ArrivalsPhase`` pick them up automatically at the matching period and add
    their quantity to the facility's inventory — this is the mirror of
    ``DemandPhase`` processing ``resolved.demand``.

    Source shipments carry ``source_id="EXT"`` and ``resource_id=None`` so
    ``ArrivalsPhase`` treats them as organic (non-resource-backed) flow.

    Args:
        resolved: Fully resolved model.  ``resolved.supply`` may be empty,
            in which case an empty frame is returned.

    Returns:
        DataFrame with columns matching ``IN_TRANSIT_COLUMNS``.  Empty when
        no supply is present.
    """
    if resolved.supply.empty:
        return pd.DataFrame(columns=IN_TRANSIT_COLUMNS)

    period_map = dict(
        zip(
            resolved.periods["period_id"].astype(str),
            resolved.periods["period_index"].astype(int),
            strict=True,
        )
    )
    supply = resolved.supply.copy()
    supply["period_index"] = supply["period_id"].astype(str).map(period_map)
    supply = supply.dropna(subset=["period_index"])
    if supply.empty:
        return pd.DataFrame(columns=IN_TRANSIT_COLUMNS)

    period_index = supply["period_index"].astype(int).to_numpy()
    n = len(supply)
    shipments = pd.DataFrame({
        "shipment_id": [f"organic_supply_{i}" for i in range(n)],
        "source_id": ["EXT"] * n,
        "target_id": supply["facility_id"].to_numpy(),
        "commodity_category": supply["commodity_category"].to_numpy(),
        "quantity": supply["quantity"].to_numpy(),
        "resource_id": [None] * n,
        "departure_period": period_index,
        "arrival_period": period_index,
    })
    return shipments[IN_TRANSIT_COLUMNS]


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
    if not resolved.resources.empty:
        # L3 resources are explicitly provided
        resources = resolved.resources[
            ["resource_id", "resource_category", "home_facility_id"]
        ].copy()
        resources["current_facility_id"] = resources["home_facility_id"]
        resources["status"] = ResourceStatus.AVAILABLE.value
        resources["available_at_period"] = None
        return resources[RESOURCE_COLUMNS]

    if not resolved.resource_fleet.empty:
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
