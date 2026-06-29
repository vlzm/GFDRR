"""Simulation phases.

Each phase reads the state and returns a new state plus the events it created.
The four phases split one period into these steps: dock earlier arrivals -> form
departures -> (build the trips) -> dock arrivals from this same period.

:class:`DockArrivals` parks the bikes that arrive this period. It fills the free
dock slots first, and sends any extra bikes to the nearest station that still has
a free dock. Docking and redirect happen together, in one step. When we replay
the real history the docks are never full, so the capacity limit and the redirect
do nothing. They only start to matter when traffic goes above the historical
level.
"""

import pandas as pd

from gbp.loaders.dataloader_graph import ResolvedModelData
from gbp.model import (
    arrived_events,
    departed_events,
    lost_events,
    redirect_continuation_events,
    redirected_events,
)

from .config import EnvironmentConfig
from .mechanics import (
    dock_up_to_capacity,
    expand_potential_trips,
    form_potential_trips,
    free_docks,
    plan_overflow_redirect,
    realize_departures,
)
from .state import (
    PeriodRow,
    PhaseResult,
    Schedule,
    SimulationState,
    adjust_inventory,
    departure_deltas_from_counts,
    dock_deltas,
)


class Phase:
    """Base class for one phase: a single step of a period, run on a schedule."""

    name = "phase"

    def __init__(self, schedule: Schedule | None = None) -> None:
        self._schedule = schedule or Schedule.every()

    def should_run(self, period: PeriodRow) -> bool:
        """Report whether this phase runs in ``period`` (per its schedule)."""
        return self._schedule.should_run(period)

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> PhaseResult:
        """Apply the phase to the state; return the next state and emitted events."""
        raise NotImplementedError


class DockArrivals(Phase):
    """Dock the bikes that arrive this period, and redirect any that do not fit.

    This is one step: park the arriving bikes in the free dock slots, then send
    any bikes that did not fit to the nearest station with a free dock. The list
    of extra bikes stays inside this method -- it never leaves the phase.
    ``when`` chooses which arrivals this phase handles, because the two run at
    different moments of the period:

    - ``"previous"`` -- bikes that left in an earlier period and arrive now
      (docked before the departures are formed), and
    - ``"same"`` -- bikes that left and arrive inside this same period
      (docked after the departures are formed).

    When we replay the real history the docks are never full, so the capacity
    limit and the redirect do nothing. They only start to matter when traffic
    goes above the historical level.
    """

    def __init__(self, when: str, schedule: Schedule | None = None) -> None:
        super().__init__(schedule)
        if when not in ("previous", "same"):
            raise ValueError(f"when must be 'previous' or 'same', got {when!r}")
        self.when = when
        self.name = f"dock_arrivals_{when}"

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> PhaseResult:
        """Dock this period's arriving bikes, redirect the overflow, lose what fits nowhere."""
        # Reads -- the in-transit bikes that should dock this period, the dock
        # capacities, and the starting inventory the check below uses.
        t = period.period_id
        it = state.in_transit
        arrived_now = it["planned_end_period"] == t
        if self.when == "previous":
            due = it[arrived_now & (it["start_period"] < t)]
        else:
            due = it[arrived_now & (it["start_period"] == t)]
        if due.empty:
            return PhaseResult.empty(state)
        capacities = resolved.facilities_capacities_df
        inventory = state.state_inventory_df
        inventory_before = int(inventory["quantity"].sum())

        # Mechanics -- dock the bikes at their planned station while free slots
        # last, then send the rest to the nearest station with a free dock. Any
        # bike that finds no free dock anywhere is lost: the whole network is full.
        free = free_docks(inventory, capacities)
        docked, overflow = dock_up_to_capacity(due, free)
        new_flows = arrived_events(docked, t)
        inventory = adjust_inventory(inventory, dock_deltas(docked))
        n_redirected = n_lost = 0
        if not overflow.empty:
            redirected, lost = plan_overflow_redirect(
                inventory, capacities, resolved.facilities_geo_df, overflow
            )
            n_redirected, n_lost = len(redirected), len(lost)
            if not redirected.empty:
                # A redirect is two arcs: the bounce off the full station B (the
                # ``redirected`` event, no docking) and the continuation B->C (a
                # move-1 ``departed`` + ``arrived``). The continuation docks the
                # bike at C in this same phase, so its move-1 ``departed`` never
                # joins ``in_transit`` -- it is journalled here and closed at once.
                # Carry each flow's ``redirect_round`` (from plan_overflow_redirect)
                # onto its events so finalize_flows orders the rounds as steps.
                round_by_flow = redirected.set_index("flow_id")["redirect_round"]
                bounce = redirected_events(redirected, t)
                bounce["redirect_round"] = bounce["flow_id"].map(round_by_flow)
                continuation = redirect_continuation_events(redirected, t)
                continuation["redirect_round"] = continuation["flow_id"].map(round_by_flow)
                new_flows = pd.concat(
                    [new_flows, bounce, continuation],
                    ignore_index=True,
                )
                inventory = adjust_inventory(
                    inventory, dock_deltas(redirected, "realized_target_id")
                )
            if not lost.empty:
                # No free dock anywhere, so the bike leaves the system for good.
                # The event ends this bike's trip; the inventory does not change,
                # because the bike already left its start station at ``departed``
                # and now docks nowhere.
                new_flows = pd.concat(
                    [new_flows, lost_events(lost, t, "dock_full")],
                    ignore_index=True,
                )

        # Writes -- remove the docked bikes from the in-transit set and save the
        # inventory.
        in_transit = state.in_transit.drop(due.index)
        new_state = state.with_inventory(inventory).with_in_transit(in_transit)

        # Check -- each arriving bike docks, is redirected, or is lost exactly
        # once. A lost bike docks nowhere, so the inventory grows only by the bikes
        # that really docked (docked + redirected), never by the lost ones.
        assert len(docked) + n_redirected + n_lost == len(due), "due flows not conserved"
        assert int(inventory["quantity"].sum()) - inventory_before == len(docked) + n_redirected, (
            "lost or redirected count moved inventory incorrectly"
        )
        return PhaseResult(new_state, new_flows)


class FormDeparturesPhase(Phase):
    """Decide how many bikes leave each (source, commodity), limited by inventory.

    Counts only -- the real trips (their targets and durations) are built in
    :class:`FormPotentialTripsPhase`. The counts are passed on through the
    per-period ``intermediates``. The demand that did *not* fit the inventory
    becomes ``lost`` events (``reason="stockout"``), so the journal keeps the full
    split ``demand = departed + lost`` instead of quietly dropping the lost demand.
    """

    name = "form_departures"

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> PhaseResult:
        """Split this period's demand into departures and stockout losses, bounded by inventory."""
        t = period.period_id
        demand = resolved.historical_demand_df
        demand_now = demand[demand["period_id"] == t].copy()
        demand_scale_factor = getattr(config, "demand_scale_factor", 1.0)
        demand_now.loc[:, "quantity"] = (
            (demand_now["quantity"] * demand_scale_factor).round().astype("Int64")
        )
        if demand_now.empty:
            return PhaseResult.empty(state)
        inventory_before = int(state.state_inventory_df["quantity"].sum())

        departures = realize_departures(demand_now, state.state_inventory_df)
        inventory = adjust_inventory(
            state.state_inventory_df, departure_deltas_from_counts(departures)
        )
        lost_demand = departures[departures["lost"] > 0]

        new_state = state.with_inventory(inventory).with_intermediates(departures=departures)
        new_flows = None
        if not lost_demand.empty:
            lost_demand = lost_demand.rename(
                columns={"facility_id": "source_id", "lost": "quantity"}
            )
            new_flows = lost_events(lost_demand, t, "stockout")

        departed = inventory_before - int(inventory["quantity"].sum())
        assert departed == int(departures["departed"].sum()), "stockout moves no inventory"
        return PhaseResult(new_state, new_flows)


class FormPotentialTripsPhase(Phase):
    """Turn the departure counts into real trips, using the OD matrix.

    For each source, it spreads the departures over the destinations with the OD
    probabilities ``P(target | source, commodity)``, sets each trip's arrival
    period from the average historical duration of that source-target pair, then
    splits the totals into one ``departed`` flow per bike. In the simple case the
    OD matrix is the historical one, so a base run repeats the historical demand
    pattern.
    """

    name = "form_potential_trips"

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> PhaseResult:
        """Turn this period's departure counts into departed flows via the OD matrix."""
        # Reads -- the departure counts passed on by FormDeparturesPhase.
        t = period.period_id
        departures = state.intermediates.get("departures")
        if departures is None or departures.empty:
            return PhaseResult.empty(state)

        # Mechanics -- spread each source's departures over the destinations with
        # the OD matrix, then split the totals into one departed flow per bike.
        # (No check: this phase only reshapes counts, it moves no inventory.)
        departures = departures.rename(columns={"facility_id": "source_id", "departed": "quantity"})
        potential = form_potential_trips(departures, resolved.historical_od_matrix_df, t)
        trips_now = expand_potential_trips(potential, t)
        if trips_now.empty:
            return PhaseResult.empty(state)
        new_flows = departed_events(trips_now)

        # Writes -- add the new departed flows to the in-transit set.
        in_transit = pd.concat([state.in_transit, new_flows], ignore_index=True)
        new_state = state.with_in_transit(in_transit)
        return PhaseResult(new_state, new_flows)
