"""Simulation log: accumulated history of a simulation run.

``SimulationLog`` collects per-period snapshots (inventory, resources) and
per-phase events (flows, unmet demand, rejected dispatches) into lists of
DataFrames.  ``to_dataframes()`` concatenates them into five final tables.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from gbp.consumers.simulator.phases import PhaseResult
    from gbp.consumers.simulator.state import PeriodRow, SimulationState


# -- Reject reason enum -------------------------------------------------------


class RejectReason(str, Enum):
    """Why a dispatch was rejected during Phase validation."""

    NO_AVAILABLE_RESOURCE = "no_available_resource"
    INSUFFICIENT_INVENTORY = "insufficient_inventory"
    OVER_CAPACITY = "over_capacity"
    INVALID_EDGE = "invalid_edge"
    INVALID_ARRIVAL = "invalid_arrival"


# -- Log column constants ------------------------------------------------------

INVENTORY_LOG_COLUMNS: list[str] = [
    "period_index",
    "period_id",
    "facility_id",
    "commodity_category",
    "quantity",
]

FLOW_LOG_COLUMNS: list[str] = [
    "period_index",
    "period_id",
    "phase_name",
    "source_id",
    "target_id",
    "commodity_category",
    "modal_type",
    "quantity",
    "resource_id",
]

RESOURCE_LOG_COLUMNS: list[str] = [
    "period_index",
    "period_id",
    "resource_id",
    "resource_category",
    "current_facility_id",
    "status",
    "available_at_period",
]

UNMET_DEMAND_LOG_COLUMNS: list[str] = [
    "period_index",
    "period_id",
    "phase_name",
    "facility_id",
    "commodity_category",
    "requested",
    "fulfilled",
    "deficit",
]

REJECTED_DISPATCHES_LOG_COLUMNS: list[str] = [
    "period_index",
    "period_id",
    "phase_name",
    "source_id",
    "target_id",
    "commodity_category",
    "quantity",
    "resource_id",
    "reason",
]


# -- SimulationLog -------------------------------------------------------------


class SimulationLog:
    """Accumulated simulation output.

    Internal storage uses lists of DataFrames (one per period/event batch)
    for efficiency.  Call ``to_dataframes()`` after the run to get the final
    concatenated tables.
    """

    def __init__(self) -> None:
        """Initialise empty log."""
        self._inventory: list[pd.DataFrame] = []
        self._flow: list[pd.DataFrame] = []
        self._resource: list[pd.DataFrame] = []
        self._unmet_demand: list[pd.DataFrame] = []
        self._rejected_dispatches: list[pd.DataFrame] = []

    # -- Recording helpers -----------------------------------------------------

    def record_period(self, state: SimulationState, period: PeriodRow) -> None:
        """Snapshot end-of-period inventory and resources."""
        # Inventory snapshot
        inv = state.inventory.copy()
        inv["period_index"] = period.period_index
        inv["period_id"] = period.period_id
        self._inventory.append(inv)

        # Resource snapshot
        res = state.resources.copy()
        res["period_index"] = period.period_index
        res["period_id"] = period.period_id
        self._resource.append(res)

    def record_events(
        self,
        result: PhaseResult,
        phase_name: str,
        period: PeriodRow,
    ) -> None:
        """Record phase events (flows, unmet demand, rejected dispatches)."""
        if not result.flow_events.empty:
            df = result.flow_events.copy()
            df["period_index"] = period.period_index
            df["period_id"] = period.period_id
            df["phase_name"] = phase_name
            self._flow.append(df)

        if not result.unmet_demand.empty:
            df = result.unmet_demand.copy()
            df["period_index"] = period.period_index
            df["period_id"] = period.period_id
            df["phase_name"] = phase_name
            self._unmet_demand.append(df)

        if not result.rejected_dispatches.empty:
            df = result.rejected_dispatches.copy()
            df["period_index"] = period.period_index
            df["period_id"] = period.period_id
            df["phase_name"] = phase_name
            self._rejected_dispatches.append(df)

    # -- Finalisation ----------------------------------------------------------

    def to_dataframes(self) -> dict[str, pd.DataFrame]:
        """Concatenate all per-period logs into final DataFrames.

        Returns:
            Dictionary with five keys:
            ``simulation_inventory_log``, ``simulation_flow_log``,
            ``simulation_resource_log``, ``simulation_unmet_demand_log``,
            ``simulation_rejected_dispatches_log``.
        """
        return {
            "simulation_inventory_log": _concat_or_empty(
                self._inventory, INVENTORY_LOG_COLUMNS
            ),
            "simulation_flow_log": _concat_or_empty(
                self._flow, FLOW_LOG_COLUMNS
            ),
            "simulation_resource_log": _concat_or_empty(
                self._resource, RESOURCE_LOG_COLUMNS
            ),
            "simulation_unmet_demand_log": _concat_or_empty(
                self._unmet_demand, UNMET_DEMAND_LOG_COLUMNS
            ),
            "simulation_rejected_dispatches_log": _concat_or_empty(
                self._rejected_dispatches, REJECTED_DISPATCHES_LOG_COLUMNS
            ),
        }


def _concat_or_empty(
    frames: list[pd.DataFrame],
    columns: list[str],
) -> pd.DataFrame:
    """Concatenate a list of DataFrames; return empty with correct columns if empty."""
    if not frames:
        return pd.DataFrame(columns=columns)
    result = pd.concat(frames, ignore_index=True)
    # Ensure column order matches the constant (extra columns are kept)
    present = [c for c in columns if c in result.columns]
    extra = [c for c in result.columns if c not in columns]
    return result[present + extra]
