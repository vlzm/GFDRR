"""Simulation phases.

Each phase reads the state and returns a new state plus the events it emitted.
The four canonical phases split a period into: dock earlier arrivals -> form
departures -> (sample extra trips) -> dock same-period arrivals.

:class:`DockArrivals` docks the bikes due this period up to the free dock
capacity and redirects whatever overflows to the nearest station with a free
dock, all in one pass -- docking and redirect are one operation, not two phases
coupled through a shared intermediate. Capacity gating and overflow redirect stay
dormant in an exact replay (where capacity never binds) and only bite above the
historical baseline.
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
    dock_up_to_capacity,
    expand_potential_trips,
    form_potential_trips,
    free_docks,
    realize_departures,
    redirect_overflow,
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


def dock_arrivals(
    state: SimulationState, resolved: ResolvedModelData, due: pd.DataFrame, period_id: int
):
    """Dock the in-transit flows ``due`` this period, up to free dock capacity.

    Splits ``due`` into the flows that fit (docked: +inventory, ``arrived``
    events) and the overflow that found no free dock at its planned target. All
    of ``due`` leaves ``in_transit``; the overflow is returned for the caller
    (:class:`DockArrivals`) to redirect elsewhere.

    Returns
    -------
    tuple of (SimulationState, pandas.DataFrame, pandas.DataFrame)
        The updated state, the ``arrived`` events, and the overflow flows.
    """
    free = free_docks(state.state_inventory_df, resolved.facilities_capacities_df)
    docked, overflow = dock_up_to_capacity(due, free)
    events = arrived_events(docked, period_id)
    inventory = adjust_inventory(state.state_inventory_df, arrival_deltas(docked))
    in_transit = state.in_transit.drop(due.index)
    new_state = state.with_inventory(inventory).with_in_transit(in_transit)
    return new_state, events, overflow


class DockArrivals(Phase):
    """Dock the bikes arriving this period, redirecting any dock overflow.

    One docking operation: dock the in-transit flows due now up to free dock
    capacity, then redirect whatever overflowed to the nearest station with a free
    dock. The overflow is a local of this method -- it never leaves the phase.
    ``when`` selects which arrivals the phase handles, since the two run at
    different points of the period:

    - ``"previous"`` -- bikes that departed in an earlier period and arrive now
      (docked before departures form), and
    - ``"same"`` -- bikes that departed and arrive within this same period
      (docked after departures form).

    Capacity gating and overflow redirect stay dormant in an exact replay (dock
    capacity never binds there) and only bite above the historical baseline.
    """

    def __init__(self, when: str, schedule: Schedule | None = None) -> None:
        super().__init__(schedule)
        if when not in ("previous", "same"):
            raise ValueError(f"when must be 'previous' or 'same', got {when!r}")
        self.when = when
        self.name = f"dock_arrivals_{when}"

    def execute(self, state, resolved, period):
        t = period.period_id
        it = state.in_transit
        arrived_now = it["planned_end_period"] == t
        if self.when == "previous":
            due = it[arrived_now & (it["start_period"] < t)]
        else:
            due = it[arrived_now & (it["start_period"] == t)]
        if due.empty:
            return PhaseResult.empty(state)

        state, events, overflow = dock_arrivals(state, resolved, due, t)
        if not overflow.empty:
            redirected, inventory, _lost = redirect_overflow(
                state.state_inventory_df,
                resolved.facilities_capacities_df,
                resolved.facilities_geo_df,
                overflow,
                t,
            )
            state = state.with_inventory(inventory)
            events = pd.concat([events, redirected], ignore_index=True)
        return PhaseResult(state, events)


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


