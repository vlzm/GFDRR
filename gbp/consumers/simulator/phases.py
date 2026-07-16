"""The per-period simulation phases: dock earlier arrivals, form departures, dock same-period."""

from typing import Literal

import pandas as pd
import structlog

from gbp.model import (
    DOCK_PREVIOUS_RANK,
    DOCK_SAME_RANK,
    PERIOD_OWN_RANK,
    arrived_events,
    departed_events,
    empty_flows_journal,
    lost_events,
    redirect_leg_events,
    redirected_events,
)

from .inputs import ScenarioInputs
from .mechanics import (
    dock_up_to_capacity,
    expand_potential_trips,
    form_potential_trips,
    free_docks,
    plan_overflow_redirect,
    realize_departures,
)
from .state import PeriodRow, SimulationState

log = structlog.get_logger(__name__)


class Phase:
    """Base class for one phase: a single step of a period, run every period."""

    #: Where this phase's events sort inside a period (Notations.md §0.1).
    phase_rank: int

    def execute(
        self,
        state: SimulationState,
        resolved: ScenarioInputs,
        period: PeriodRow,
    ) -> SimulationState:
        """Run the phase: build this period's events and write them as one batch."""
        events = self.build_events(state, resolved, period)
        log.debug("phase_executed", phase=type(self).__name__, events=len(events))
        return state.apply_step_events(events, self.phase_rank)

    def build_events(
        self,
        state: SimulationState,
        resolved: ScenarioInputs,
        period: PeriodRow,
    ) -> pd.DataFrame:
        """Build this period's events; an empty frame means nothing happened."""
        raise NotImplementedError


class DockArrivals(Phase):
    """Dock the bikes that arrive this period; redirect what does not fit."""

    def __init__(self, when: Literal["previous", "same"]) -> None:
        if when not in ("previous", "same"):
            raise ValueError(f"when must be 'previous' or 'same', got {when!r}")
        self.when = when
        self.phase_rank = DOCK_PREVIOUS_RANK if when == "previous" else DOCK_SAME_RANK

    def _due_arrivals(self, in_transit: pd.DataFrame, t: int) -> pd.DataFrame:
        """Select the in-transit user trips this phase docks at period ``t`` (by ``when``)."""
        due_now = (in_transit["planned_end_period"] == t) & (in_transit["flow_type"] == "user_trip")
        if self.when == "previous":
            return in_transit[due_now & (in_transit["start_period"] < t)]
        return in_transit[due_now & (in_transit["start_period"] == t)]

    def build_events(
        self,
        state: SimulationState,
        resolved: ScenarioInputs,
        period: PeriodRow,
    ) -> pd.DataFrame:
        """Dock this period's due arrivals, redirect the overflow, lose what fits nowhere."""
        due = self._due_arrivals(state.in_transit, period.period_id)
        return dock_due_arrivals(due, state, resolved, period.period_id)


def dock_due_arrivals(
    due: pd.DataFrame,
    state: SimulationState,
    resolved: ScenarioInputs,
    t: int,
) -> pd.DataFrame:
    """Dock the due arrivals, redirect the overflow, lose what fits nowhere (empty if none due)."""
    if due.empty:
        return empty_flows_journal()

    # Dock at the planned station while free docks last.
    docked, overflow = dock_up_to_capacity(
        due, free_docks(state.state_inventory_df, resolved.facilities_capacities_df)
    )
    arrivals_docked = arrived_events(docked, t)

    # Resolve every bike that did not fit: docked at a redirect target, riding a
    # new leg, or lost. The redirect must see the docks the planned dockings
    # just took, so it reads the inventory as it will stand once those events
    # are written (a decision input; the real write happens in apply_step_events).
    after_docked = state.inventory_after_events(arrivals_docked)
    outcomes = plan_overflow_redirect(
        after_docked,
        resolved.facilities_capacities_df,
        resolved.facilities_geo_df,
        resolved.historical_od_matrix_df,
        resolved.routes,
        overflow,
        t,
    )
    # Check -- each due bike docked, left on a new leg, or was lost, exactly once.
    assert len(docked) + len(outcomes) == len(due), "due flows not conserved"

    # Turn the outcomes into events. A redirected bike always bounces and opens
    # a new leg; an outcome "docked" also arrives within this period.
    redirects = outcomes[outcomes["outcome"] != "lost"]
    lost = outcomes[outcomes["outcome"] == "lost"]
    bounces = redirected_events(redirects, t).assign(phase_round=redirects["phase_round"])
    legs = redirect_leg_events(redirects, t).assign(phase_round=redirects["phase_round"])
    legs_now = legs[redirects["outcome"] == "docked"]
    arrivals_now = arrived_events(legs_now, t).assign(phase_round=legs_now["phase_round"])

    lost_dock_full = lost_events(lost, t, "dock_full")
    # One inventory step per phase_round, in apply order: the planned dockings
    # (round 0), then each redirect round (Notations.md §0.1).
    return pd.concat(
        [arrivals_docked, lost_dock_full, bounces, legs, arrivals_now],
        ignore_index=True,
    )


class FormDeparturesPhase(Phase):
    """Form this period's departures: the demand split and the trips, in one phase."""

    phase_rank = PERIOD_OWN_RANK

    def build_events(
        self,
        state: SimulationState,
        resolved: ScenarioInputs,
        period: PeriodRow,
    ) -> pd.DataFrame:
        """Split this period's demand into departed flows and stockout losses."""
        t = period.period_id
        demand = resolved.historical_demand_df
        demand_now = demand[demand["period_id"] == t]
        if demand_now.empty:
            return empty_flows_journal()

        # Mechanics -- bound the demand by the inventory, then spread each
        # source's departures over the targets with the OD matrix and split
        # the totals into one departed flow per bike.
        departures = realize_departures(demand_now, state.state_inventory_df)
        lost_demand = departures[departures["lost"] > 0]
        departed_counts = departures.rename(
            columns={"facility_id": "source_id", "departed": "quantity"}
        )
        potential = form_potential_trips(departed_counts, resolved.historical_od_matrix_df, t)
        trips_now = expand_potential_trips(potential, t)

        # Events -- the departures and the stockout losses are one batch, so
        # apply_step_events gives them one shared step_id. It also takes the
        # departed bikes out of the inventory and puts them into in_transit.
        new_departed = departed_events(trips_now)
        batches = [] if new_departed.empty else [new_departed]
        if not lost_demand.empty:
            lost_demand = lost_demand.rename(
                columns={"facility_id": "source_id", "lost": "quantity"}
            )
            batches.append(lost_events(lost_demand, t, "stockout"))
        if not batches:
            return empty_flows_journal()
        return pd.concat(batches, ignore_index=True)
