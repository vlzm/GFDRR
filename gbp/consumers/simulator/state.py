"""Simulator state: the network's facts at the current period.

This module owns the live state of one run -- the inventory, the in-transit
working set, the period clock and the run-config primitives (:class:`Schedule`,
:class:`PeriodRow`) -- plus the inventory arithmetic that maintains it
(:func:`adjust_inventory` and the per-event delta builders).

The flow journal that backs the state lives in :mod:`flows` (the single source
of truth); the rules that mutate the state in a period live in :mod:`mechanics`.
``SimulationState`` keeps the journal and two materialized projections of it
(inventory, ``in_transit``) and exposes the marginal observations as read-only
properties derived on demand through :mod:`flows`. Dependency direction:
``journal <- state <- mechanics <- phases <- engine``.
"""

import dataclasses
from typing import Any

import pandas as pd

from gbp.model import (
    empty_in_transit,
    flows_to_arrivals,
    flows_to_departures,
    flows_to_od_matrix,
)


# ---------------------------------------------------------------------------
# Inventory updates
# ---------------------------------------------------------------------------
def adjust_inventory(inventory: pd.DataFrame, deltas: pd.DataFrame) -> pd.DataFrame:
    """Add signed ``delta`` per (facility_id, commodity_category)."""
    out = inventory.merge(deltas, on=["facility_id", "commodity_category"], how="outer")
    out["quantity"] = out["quantity"].fillna(0) + out["delta"].fillna(0)
    return out[["facility_id", "commodity_category", "quantity"]]


def departure_deltas(trips: pd.DataFrame) -> pd.DataFrame:
    """-1 per departing bike, grouped by (source, commodity)."""
    deltas = (
        trips.groupby(["source_id", "commodity_category"])
        .size()
        .reset_index(name="delta")
        .rename(columns={"source_id": "facility_id"})
    )
    deltas["delta"] = -deltas["delta"]
    return deltas


def dock_deltas(docked: pd.DataFrame, target_col: str = "planned_target_id") -> pd.DataFrame:
    """+1 per docking bike, grouped by the station docked at and the commodity.

    ``target_col`` selects which station the bike docked at: ``planned_target_id``
    when it docked at its planned target, ``realized_target_id`` when an overflow
    flow was redirected elsewhere.
    """
    return (
        docked.groupby([target_col, "commodity_category"])
        .size()
        .reset_index(name="delta")
        .rename(columns={target_col: "facility_id"})
    )


def departure_deltas_from_counts(departures: pd.DataFrame) -> pd.DataFrame:
    """``-departed`` per (facility, commodity) for the inventory decrement."""
    d = (
        departures[departures["departed"] > 0]
        .rename(columns={"departed": "delta"})[["facility_id", "commodity_category", "delta"]]
        .copy()
    )
    d["delta"] = -d["delta"]
    return d


# ---------------------------------------------------------------------------
# Run config + scheduling
# ---------------------------------------------------------------------------
class SimulatorConfigError(Exception):
    """Raised when an environment is built with insufficient inputs."""


@dataclasses.dataclass(frozen=True)
class Schedule:
    """When a phase runs: every ``every_n`` periods (1 = every period)."""

    every_n: int = 1

    @classmethod
    def every(cls) -> "Schedule":
        """Build a schedule that runs every period."""
        return cls(1)

    @classmethod
    def every_n_periods(cls, n: int) -> "Schedule":
        """Build a schedule that runs once every ``n`` periods."""
        return cls(n)

    def should_run(self, period: "PeriodRow") -> bool:
        """Report whether ``period`` falls on this schedule."""
        return period.period_id % self.every_n == 0


@dataclasses.dataclass(frozen=True)
class PeriodRow:
    """One row of the period grid (the simulation clock)."""

    period_id: int
    start_timestamp: Any
    end_timestamp: Any


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
@dataclasses.dataclass(frozen=True)
class SimulationState:
    """Simulation facts at the current period (replaced, never mutated).

    ``state_flows_df`` is the append-only event journal and the single source of
    truth for what happened. ``state_inventory_df`` and ``in_transit`` are
    materialized projections of that journal, kept incrementally for speed.
    ``in_transit`` is deliberately *not* part of the named state contract: it is
    internal plumbing (the "departed but not yet docked" working set), the same
    kind of projection as inventory but not worth observing on its own.

    The "Additional" observations are exposed as read-only properties derived on
    demand: ``state_departures_df``, ``state_arrivals_df``, ``state_demand_df``,
    ``state_supply_df`` and ``state_od_matrix_df``.

    Attributes
    ----------
    state_period_id_obj : PeriodRow
        The current period (the clock position).
    state_inventory_df : pandas.DataFrame
        Current inventory: ``facility_id``, ``commodity_category``, ``quantity``.
    state_flows_df : pandas.DataFrame
        Append-only flow-event journal accumulated so far this run.
    state_resources_df : pandas.DataFrame
        Resource (truck) observations; empty in the historical replay.
    in_transit : pandas.DataFrame
        Internal projection: ``departed`` flows not yet docked.
    intermediates : dict
        Transient per-period hand-offs between phases.
    """

    state_period_id_obj: PeriodRow
    state_inventory_df: pd.DataFrame
    state_flows_df: pd.DataFrame
    state_resources_df: pd.DataFrame
    in_transit: pd.DataFrame = dataclasses.field(default_factory=empty_in_transit)
    intermediates: dict[str, Any] = dataclasses.field(default_factory=dict)

    # -- clock ---------------------------------------------------------------
    @property
    def period_id(self) -> int:
        """Integer id of the current period."""
        return self.state_period_id_obj.period_id

    # -- derived "Additional" observations -----------------------------------
    @property
    def state_departures_df(self) -> pd.DataFrame:
        """Outflow per period and source, derived from ``state_flows_df``."""
        return flows_to_departures(self.state_flows_df)

    @property
    def state_arrivals_df(self) -> pd.DataFrame:
        """Inflow per period and target, derived from ``state_flows_df``."""
        return flows_to_arrivals(self.state_flows_df)

    @property
    def state_demand_df(self) -> pd.DataFrame:
        """Realized user demand (= departures in an exact replay).

        Demand is the number of trips users *wanted* to take. Above the
        historical baseline some of it is lost to stockouts and diverges from
        ``state_departures_df``; in the replay every desired trip departs, so the
        two coincide and demand is read off the journal as the departures.
        """
        return flows_to_departures(self.state_flows_df)

    @property
    def state_supply_df(self) -> pd.DataFrame:
        """Bikes currently available to depart: the current inventory."""
        return self.state_inventory_df

    @property
    def state_od_matrix_df(self) -> pd.DataFrame:
        """Origin-destination demand matrix, derived from ``state_flows_df``."""
        return flows_to_od_matrix(self.state_flows_df)

    # -- functional updates --------------------------------------------------
    def with_inventory(self, new_inventory: pd.DataFrame) -> "SimulationState":
        """Return a copy with the inventory replaced."""
        return dataclasses.replace(self, state_inventory_df=new_inventory)

    def with_in_transit(self, new_in_transit: pd.DataFrame) -> "SimulationState":
        """Return a copy with the in-transit set replaced."""
        return dataclasses.replace(self, in_transit=new_in_transit)

    def with_resources(self, new_resources: pd.DataFrame) -> "SimulationState":
        """Return a copy with the resources replaced."""
        return dataclasses.replace(self, state_resources_df=new_resources)

    def append_flows(self, new_flows: pd.DataFrame | None) -> "SimulationState":
        """Append a phase's emitted flow events to ``flows`` (the journal, source of truth)."""
        if new_flows is None or not len(new_flows):
            return self
        flows = pd.concat([self.state_flows_df, new_flows], ignore_index=True)
        return dataclasses.replace(self, state_flows_df=flows)

    def with_intermediates(self, **updates: Any) -> "SimulationState":
        """Return a copy with the given per-period hand-offs merged in."""
        return dataclasses.replace(self, intermediates={**self.intermediates, **updates})

    def advance_period(self, next_period_obj: PeriodRow) -> "SimulationState":
        """Return a copy moved to ``next_period_obj``, clearing the intermediates."""
        # Intermediates are transient per-period hand-offs between phases.
        return dataclasses.replace(self, state_period_id_obj=next_period_obj, intermediates={})


@dataclasses.dataclass
class PhaseResult:
    """What a phase returns: the next state plus any flow events it emitted.

    ``new_flows`` is the batch of new flow-event rows the phase produced this
    period -- the same row schema as ``flows``, not yet appended. The engine
    appends it to the journal via :meth:`SimulationState.append_flows`.
    """

    state: SimulationState
    new_flows: Any = None  # DataFrame of new flow-event rows, or None

    @classmethod
    def empty(cls, state: SimulationState) -> "PhaseResult":
        """Build a result that changes nothing: the same state, no new flows."""
        return cls(state=state, new_flows=None)
