"""Simulator state: the network's facts at the current period (inventory, in-transit, clock)."""

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
    """Simulation facts at the current period (replaced, never mutated)."""

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
        """Return a copy with the rebalance plan replaced."""
        return dataclasses.replace(self, rebalance_plan=new_plan)

    def append_flows(self, new_flows: pd.DataFrame | None) -> "SimulationState":
        """Append flow events to ``flows`` (the journal, source of truth)."""
        if new_flows is None or not len(new_flows):
            return self
        flows = pd.concat([self.state_flows_df, new_flows], ignore_index=True)
        return dataclasses.replace(self, state_flows_df=flows)

    def open_step(self) -> tuple[int, "SimulationState"]:
        """Hand out the next inventory-step number and return the advanced state."""
        return self.next_step_id, dataclasses.replace(self, next_step_id=self.next_step_id + 1)

    def inventory_after_events(self, new_flows: pd.DataFrame) -> pd.DataFrame:
        """Inventory as it will stand once these events are written (state itself unchanged)."""
        deltas = inventory_deltas_from_events(new_flows)
        if deltas.empty:
            return self.state_inventory_df
        return adjust_inventory(self.state_inventory_df, deltas)

    def apply_step_events(self, new_flows: pd.DataFrame, phase_rank: int) -> "SimulationState":
        """Write one phase's events as numbered steps and apply what they imply."""
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
