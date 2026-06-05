"""Simulation phases.

Each phase reads the state and returns a new state plus the events it emitted.
The six canonical phases split a period into: dock earlier arrivals ->
(redirect) -> form departures -> (sample extra trips) -> dock same-period
arrivals -> (redirect).

Fully implemented (the replay core): ArrivalsPreviousPhase, FormDeparturesPhase,
ArrivalsPhase. TODO skeletons (only bite above the historical baseline): the
two OverflowRedirect phases and FormPotentialTripsPhase.
"""

import pandas as pd

from gbp.loaders.dataloader_graph import ResolvedModelData

from .state import (
    PeriodRow,
    PhaseResult,
    Schedule,
    SimulationState,
    adjust_inventory,
    arrival_deltas,
    arrived_events,
    departed_events,
    departure_deltas_from_counts,
    expand_potential_trips,
    form_potential_trips,
    realize_departures,
)


class Phase:
    name = "phase"

    def __init__(self, schedule: Schedule | None = None) -> None:
        self._schedule = schedule or Schedule.every()

    def should_run(self, period: PeriodRow) -> bool:
        return self._schedule.should_run(period)

    def execute(self, state: SimulationState, resolved: ResolvedModelData,
                period: PeriodRow) -> PhaseResult:
        raise NotImplementedError


def dock_arrivals(state: SimulationState, due: pd.DataFrame, period_id: int):
    """Dock the in-transit flows ``due`` this period: +inventory, drop, emit arrived."""
    events = arrived_events(due, period_id)
    inventory = adjust_inventory(state.state_inventory_df, arrival_deltas(due))
    in_transit = state.in_transit.drop(due.index)
    return state.with_inventory(inventory).with_in_transit(in_transit), events


class ArrivalsPreviousPhase(Phase):
    """Dock bikes that departed in an earlier period and arrive now."""

    name = "arrivals_previous"

    def execute(self, state, resolved, period):
        t = period.period_id
        it = state.in_transit
        due = it[(it["planned_end_period"] == t) & (it["start_period"] < t)]
        if due.empty:
            return PhaseResult.empty(state)
        new_state, events = dock_arrivals(state, due, t)
        return PhaseResult(new_state, events)


class OverflowRedirectPreviousPhase(Phase):
    name = "overflow_redirect_previous"

    def execute(self, state, resolved, period):
        # TODO(core algorithm): if a station's docks are full when bikes arrive,
        # redirect the overflow to the nearest station with a free dock
        # (capacity = resolved.facilities_capacities_df, distance = resolved.facilities_geo_df),
        # emit `redirected` events and set realized_target_id / realized_end_period.
        # Dormant in the historical replay (dock capacity is not binding there).
        return PhaseResult.empty(state)


class FormDeparturesPhase(Phase):
    """Decide how many bikes depart per (source, commodity), gated by stock.

    Counts only -- the concrete trips (their targets and durations) are formed in
    :class:`FormPotentialTripsPhase`. The realized counts are handed over through
    the per-period ``intermediates``.
    """

    name = "form_departures"

    def execute(self, state, resolved, period):
        t = period.period_id
        demand = resolved.historical_demand_df
        demand_now = demand[demand["period_id"] == t]
        if demand_now.empty:
            return PhaseResult.empty(state)

        # Gate demand by the stock on hand; demand above stock is lost to a
        # stockout (dormant in an exact replay, where stock covers the baseline).
        realized = realize_departures(demand_now, state.state_inventory_df)
        inventory = adjust_inventory(state.state_inventory_df, departure_deltas_from_counts(realized))
        new_state = (state.with_inventory(inventory)
                     .with_intermediates(realized_departures=realized))
        return PhaseResult.empty(new_state)


class FormPotentialTripsPhase(Phase):
    """Turn the realized departure counts into concrete trips via the OD matrix.

    Splits each source's departures across destinations by the OD probabilities
    ``P(target | source, commodity)`` and sets each trip's arrival period from the
    OD pair's mean historical duration, then expands the aggregate into one
    ``departed`` flow per bike. In the trivial case the OD matrix is the
    historical one, so a base run reproduces the historical demand structure.
    """

    name = "form_potential_trips"

    def execute(self, state, resolved, period):
        t = period.period_id
        realized = state.intermediates.get("realized_departures")
        if realized is None or realized.empty:
            return PhaseResult.empty(state)

        departures = realized.rename(columns={"facility_id": "source_id", "realized": "quantity"})
        potential = form_potential_trips(departures, resolved.historical_od_matrix_df, t)
        trips_now = expand_potential_trips(potential, t)
        if trips_now.empty:
            return PhaseResult.empty(state)

        events = departed_events(trips_now)
        in_transit = pd.concat([state.in_transit, events], ignore_index=True)
        new_state = state.with_in_transit(in_transit)
        return PhaseResult(new_state, events)


class ArrivalsPhase(Phase):
    """Dock bikes that departed and arrive within this same period."""

    name = "arrivals"

    def execute(self, state, resolved, period):
        t = period.period_id
        it = state.in_transit
        due = it[(it["planned_end_period"] == t) & (it["start_period"] == t)]
        if due.empty:
            return PhaseResult.empty(state)
        new_state, events = dock_arrivals(state, due, t)
        return PhaseResult(new_state, events)


class OverflowRedirectPhase(Phase):
    name = "overflow_redirect"

    def execute(self, state, resolved, period):
        # TODO(core algorithm): same redirect rule as OverflowRedirectPreviousPhase,
        # for bikes that departed and arrived within the same period.
        return PhaseResult.empty(state)
