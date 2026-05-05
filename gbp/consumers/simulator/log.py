"""Simulation log: accumulated history of a simulation run.

The log is built around a single registry of :class:`LogTableSchema` entries.
Each entry knows three things:

- ``short_name`` — the key under which a phase emits rows in
  ``PhaseResult.events`` (e.g. ``"flow_events"``);
- ``output_key`` — the key under which the concatenated table is exposed by
  :meth:`SimulationLog.to_dataframes` (e.g. ``"simulation_flow_log"``);
- ``columns``   — the canonical column order; the presence of ``"phase_name"``
  determines whether the recording loop annotates rows with the emitting
  phase.

Adding a new event type is therefore a one-place change: append a new
:class:`LogTableSchema` and emit rows under the corresponding ``short_name``
from any phase.
"""

from __future__ import annotations

from dataclasses import dataclass
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

LATENT_DEMAND_LOG_COLUMNS: list[str] = [
    "period_index",
    "period_id",
    "phase_name",
    "facility_id",
    "commodity_category",
    "latent_departures",
    "latent_arrivals",
]

LOST_DEMAND_LOG_COLUMNS: list[str] = [
    "period_index",
    "period_id",
    "phase_name",
    "facility_id",
    "commodity_category",
    "latent",
    "realized",
    "lost",
]

DOCK_BLOCKING_LOG_COLUMNS: list[str] = [
    "period_index",
    "period_id",
    "phase_name",
    "facility_id",
    "commodity_category",
    "incoming",
    "accepted",
    "blocked",
]

REDIRECTED_FLOW_LOG_COLUMNS: list[str] = [
    "period_index",
    "period_id",
    "phase_name",
    "source_id",
    "original_target_id",
    "redirected_target_id",
    "commodity_category",
    "quantity",
]

INVARIANT_VIOLATION_LOG_COLUMNS: list[str] = [
    "period_index",
    "period_id",
    "commodity_category",
    "baseline",
    "current",
    "delta",
]


# -- Log-table registry --------------------------------------------------------


@dataclass(frozen=True)
class LogTableSchema:
    """Metadata for one log table.

    Attributes:
        short_name: Key used by phases in ``PhaseResult.events`` and by
            :meth:`SimulationLog.record_period` for snapshot tables.
        output_key: Final key used by :meth:`SimulationLog.to_dataframes`.
        columns: Canonical column order for the output DataFrame.  When
            ``"phase_name"`` appears here, the record loop tags rows with
            the emitting phase's name; otherwise it does not.
    """

    short_name: str
    output_key: str
    columns: list[str]


LOG_TABLES: tuple[LogTableSchema, ...] = (
    LogTableSchema("inventory", "simulation_inventory_log", INVENTORY_LOG_COLUMNS),
    LogTableSchema("flow_events", "simulation_flow_log", FLOW_LOG_COLUMNS),
    LogTableSchema("resource", "simulation_resource_log", RESOURCE_LOG_COLUMNS),
    LogTableSchema(
        "unmet_demand",
        "simulation_unmet_demand_log",
        UNMET_DEMAND_LOG_COLUMNS,
    ),
    LogTableSchema(
        "rejected_dispatches",
        "simulation_rejected_dispatches_log",
        REJECTED_DISPATCHES_LOG_COLUMNS,
    ),
    LogTableSchema(
        "latent_demand",
        "simulation_latent_demand_log",
        LATENT_DEMAND_LOG_COLUMNS,
    ),
    LogTableSchema(
        "lost_demand",
        "simulation_lost_demand_log",
        LOST_DEMAND_LOG_COLUMNS,
    ),
    LogTableSchema(
        "dock_blocking",
        "simulation_dock_blocking_log",
        DOCK_BLOCKING_LOG_COLUMNS,
    ),
    LogTableSchema(
        "redirected_flow",
        "simulation_redirected_flow_log",
        REDIRECTED_FLOW_LOG_COLUMNS,
    ),
    LogTableSchema(
        "invariant_violation",
        "simulation_invariant_violation_log",
        INVARIANT_VIOLATION_LOG_COLUMNS,
    ),
)


_TABLE_BY_NAME: dict[str, LogTableSchema] = {t.short_name: t for t in LOG_TABLES}


# -- SimulationLog -------------------------------------------------------------


class SimulationLog:
    """Accumulated simulation output.

    Internal storage is a dict-of-lists keyed by ``LogTableSchema.short_name``.
    Call :meth:`to_dataframes` after the run to get the final concatenated
    tables, keyed by ``output_key``.
    """

    def __init__(self) -> None:
        """Initialise empty log buckets, one per registered table."""
        self._events: dict[str, list[pd.DataFrame]] = {
            schema.short_name: [] for schema in LOG_TABLES
        }

    # -- Recording helpers -----------------------------------------------------

    def record_period(self, state: SimulationState, period: PeriodRow) -> None:
        """Snapshot end-of-period inventory and resources."""
        self._record("inventory", state.inventory, period, phase_name=None)
        self._record("resource", state.resources, period, phase_name=None)

    def record_events(
        self,
        result: PhaseResult,
        phase_name: str,
        period: PeriodRow,
    ) -> None:
        """Route a phase's emitted events into the matching log buckets."""
        for short_name, df in result.events.items():
            self._record(short_name, df, period, phase_name=phase_name)

    def _record(
        self,
        short_name: str,
        df: pd.DataFrame,
        period: PeriodRow,
        phase_name: str | None,
    ) -> None:
        """Annotate *df* with period (and phase_name when applicable) and store it.

        Skips silently when *df* is empty.  Raises ``KeyError`` for unknown
        short names so typos in phase code surface immediately.
        """
        if df is None or df.empty:
            return
        schema = _TABLE_BY_NAME.get(short_name)
        if schema is None:
            msg = (
                f"Unknown log table {short_name!r}. "
                f"Known tables: {sorted(_TABLE_BY_NAME)}"
            )
            raise KeyError(msg)

        df = df.copy()
        df["period_index"] = period.period_index
        df["period_id"] = period.period_id
        if "phase_name" in schema.columns and phase_name is not None:
            df["phase_name"] = phase_name
        self._events[short_name].append(df)

    # -- Finalisation ----------------------------------------------------------

    def to_dataframes(self) -> dict[str, pd.DataFrame]:
        """Concatenate all per-period buckets into final DataFrames.

        Returns:
            Dictionary keyed by each table's ``output_key`` (e.g.
            ``simulation_flow_log``).  Empty buckets produce empty DataFrames
            with the canonical column order from the registry.
        """
        return {
            schema.output_key: _concat_or_empty(
                self._events[schema.short_name], schema.columns,
            )
            for schema in LOG_TABLES
        }


def _concat_or_empty(
    frames: list[pd.DataFrame],
    columns: list[str],
) -> pd.DataFrame:
    """Concatenate a list of DataFrames; return empty with correct columns if empty."""
    if not frames:
        return pd.DataFrame(columns=columns)
    result = pd.concat(frames, ignore_index=True)
    # Ensure column order matches the canonical schema; extras kept at the end.
    present = [c for c in columns if c in result.columns]
    extra = [c for c in result.columns if c not in columns]
    return result[present + extra]
