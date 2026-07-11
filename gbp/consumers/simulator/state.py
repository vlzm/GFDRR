"""Simulator state: the network's facts at the current period.

This module owns the live state of one run -- the inventory, the in-transit
working set and the period clock (:class:`PeriodRow`).

The flow journal that backs the state lives in :mod:`flows` (the single source
of truth); the rules that mutate the state in a period live in :mod:`mechanics`.
``SimulationState`` keeps the journal and two materialized projections of it
(inventory, ``in_transit``). Both projections are maintained by one write path,
:meth:`SimulationState.apply_step_events`: it derives the inventory change and
the in-transit change from the events themselves (the model-layer rules
:func:`gbp.model.inventory_deltas_from_events` and
:func:`gbp.model.in_transit_after_events`), so a phase cannot write events that
disagree with the projections. A decision in the middle of a phase reads the
inventory through :meth:`SimulationState.inventory_after_events` -- the events
built so far, applied by the same rule the write applies. Dependency direction:
``journal <- state <- mechanics <- phases <- engine``.
"""

import dataclasses
from typing import Any

import pandas as pd

from gbp.model import (
    empty_in_transit,
    in_transit_after_events,
    inventory_deltas_from_events,
)


# ---------------------------------------------------------------------------
# Inventory arithmetic (also used by mechanics for the redirect's round loop)
# ---------------------------------------------------------------------------
def adjust_inventory(inventory: pd.DataFrame, deltas: pd.DataFrame) -> pd.DataFrame:
    """Add signed ``delta`` per (facility_id, commodity_category)."""
    out = inventory.merge(deltas, on=["facility_id", "commodity_category"], how="outer")
    out["quantity"] = out["quantity"].fillna(0) + out["delta"].fillna(0)
    return out[["facility_id", "commodity_category", "quantity"]]


# ---------------------------------------------------------------------------
# Run config
# ---------------------------------------------------------------------------
class SimulatorConfigError(Exception):
    """Raised when an environment is built with insufficient inputs."""


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
    materialized projections of that journal, kept incrementally for speed and
    maintained only by :meth:`apply_step_events` -- a phase writes events and
    the projections follow. ``in_transit`` is deliberately *not* part of the
    named state contract: it is internal plumbing (the "departed but not yet
    docked" working set), the same kind of projection as inventory but not
    worth observing on its own.

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
    next_step_id : int
        The next inventory-step number to hand out (Notations.md §0.1). A phase
        writes its events through :meth:`apply_step_events`, which calls
        :meth:`open_step` per ordered batch; the counter only ever grows, so two
        ordered batches can never share a ``step_id``. Threaded through the
        immutable state, so the run stays deterministic.
    rebalance_plan : pandas.DataFrame
        The bike-level rebalance plan still to execute (Notations.md §14):
        one row per bike a truck will move, with its pickup and dropoff
        periods. Written by ``PlanRebalancingPhase`` once per window, consumed
        period by period by ``ApplyRebalancingPhase``. Empty outside a window.
    """

    state_period_id_obj: PeriodRow
    state_inventory_df: pd.DataFrame
    state_flows_df: pd.DataFrame
    state_resources_df: pd.DataFrame
    in_transit: pd.DataFrame = dataclasses.field(default_factory=empty_in_transit)
    next_step_id: int = 0
    rebalance_plan: pd.DataFrame = dataclasses.field(default_factory=pd.DataFrame)

    # -- clock ---------------------------------------------------------------
    @property
    def period_id(self) -> int:
        """Integer id of the current period."""
        return self.state_period_id_obj.period_id

    # -- functional updates --------------------------------------------------
    def with_rebalance_plan(self, new_plan: pd.DataFrame) -> "SimulationState":
        """Return a copy with the rebalance plan replaced (Notations.md §14)."""
        return dataclasses.replace(self, rebalance_plan=new_plan)

    def append_flows(self, new_flows: pd.DataFrame | None) -> "SimulationState":
        """Append flow events to ``flows`` (the journal, source of truth)."""
        if new_flows is None or not len(new_flows):
            return self
        flows = pd.concat([self.state_flows_df, new_flows], ignore_index=True)
        return dataclasses.replace(self, state_flows_df=flows)

    def open_step(self) -> tuple[int, "SimulationState"]:
        """Hand out the next inventory-step number and return the advanced state.

        Called per ordered inventory change (a dock batch, a period's
        departures, one redirect round) -- normally by :meth:`apply_step_events`,
        which stamps the returned number on the events of that step. Because the
        number comes from this counter and never from the event columns, two
        ordered batches always get different ``step_id`` values, even if they
        share a ``(period_id, phase_rank, phase_round)`` label (Notations.md
        §0.1).
        """
        return self.next_step_id, dataclasses.replace(self, next_step_id=self.next_step_id + 1)

    def inventory_after_events(self, new_flows: pd.DataFrame) -> pd.DataFrame:
        """Inventory as it will stand once these events are written.

        The read for a decision taken in the middle of a phase. A phase writes
        its events once, at the end (:meth:`apply_step_events`), but a decision
        inside the phase may depend on the events already built -- where a
        bounced bike can dock depends on the docks the same phase's planned
        dockings just took. This read applies the batch's ``+1`` / ``-1`` rule
        (:func:`gbp.model.inventory_deltas_from_events`) to the current
        inventory. :meth:`apply_step_events` moves the inventory through this
        same method, so what a decision sees and what the write produces cannot
        disagree. The state itself does not change.
        """
        deltas = inventory_deltas_from_events(new_flows)
        if deltas.empty:
            return self.state_inventory_df
        return adjust_inventory(self.state_inventory_df, deltas)

    def apply_step_events(self, new_flows: pd.DataFrame, phase_rank: int) -> "SimulationState":
        """Write one phase's events as numbered steps and apply what they imply.

        The single write path for a phase. The phase hands over the events it
        built this period (``new_flows``, the builder output) and its rank;
        behind this seam the state opens one inventory step per ordered batch,
        stamps the three ordering columns on every row, and appends the rows to
        ``state_flows_df``:

        - ``phase_rank`` is set to ``phase_rank`` on every row;
        - a per-row ``phase_round`` column marks the ordered batches of a phase
          that applies several in a row (a redirect's rounds). Rows without one
          (or with NA) are round 0;
        - one step is opened per distinct round, in round order, and its number
          is stamped as the rows' ``step_id``. Steps come from
          :meth:`open_step`, so two separately ordered batches never share a
          ``step_id`` (Notations.md §0.1).

        The same call maintains the two projections, so events and projections
        cannot disagree:

        - inventory moves by exactly the batch's ``+1`` / ``-1`` rule, through
          :meth:`inventory_after_events` -- the same read a mid-phase decision
          uses, so the two cannot disagree;
        - ``in_transit`` gains the batch's still-riding ``departed`` rows and
          loses the rows whose arc the batch closes
          (:func:`gbp.model.in_transit_after_events`).

        Rows that share a round share one step even when only some of them move
        inventory -- a step is the batch applied together, not only its ``+1`` /
        ``-1`` rows. Empty ``new_flows`` opens nothing (a step opened for
        nothing would leave a gap in the numbering) and changes nothing.
        """
        if new_flows.empty:
            return self
        flows = new_flows.copy()
        flows["phase_rank"] = phase_rank
        if "phase_round" not in flows.columns:
            flows["phase_round"] = 0
        rounds = pd.to_numeric(flows["phase_round"], errors="coerce")
        flows["phase_round"] = rounds.fillna(0).astype("int64")
        working = self
        step_by_round: dict[int, int] = {}
        for round_no in sorted(flows["phase_round"].unique()):
            step_by_round[round_no], working = working.open_step()
        flows["step_id"] = flows["phase_round"].map(step_by_round)

        return dataclasses.replace(
            working.append_flows(flows),
            state_inventory_df=self.inventory_after_events(flows),
            in_transit=in_transit_after_events(self.in_transit, flows),
        )

    def advance_period(self, next_period_obj: PeriodRow) -> "SimulationState":
        """Return a copy moved to ``next_period_obj``."""
        return dataclasses.replace(self, state_period_id_obj=next_period_obj)
