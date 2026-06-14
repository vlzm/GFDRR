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
from gbp.model import arrived_events, departed_events, lost_events, redirected_events

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
    name = "phase"

    def __init__(self, schedule: Schedule | None = None) -> None:
        self._schedule = schedule or Schedule.every()

    def should_run(self, period: PeriodRow) -> bool:
        return self._schedule.should_run(period)

    def execute(self, state: SimulationState, resolved: ResolvedModelData,
                period: PeriodRow) -> PhaseResult:
        raise NotImplementedError


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

        capacities = resolved.facilities_capacities_df
        inventory = state.state_inventory_df
        stock_before = int(inventory["quantity"].sum())

        # Dock at the planned target, up to free dock capacity.
        free = free_docks(inventory, capacities)
        docked, overflow = dock_up_to_capacity(due, free)
        events = arrived_events(docked, t)
        inventory = adjust_inventory(inventory, dock_deltas(docked))

        # Redirect whatever overflowed to the nearest station with a free dock;
        # whatever still finds no dock anywhere is lost to a full network.
        n_placed = n_lost = 0
        if not overflow.empty:
            placed, lost = plan_overflow_redirect(
                inventory, capacities, resolved.facilities_geo_df, overflow
            )
            n_placed, n_lost = len(placed), len(lost)
            if not placed.empty:
                events = pd.concat(
                    [events, redirected_events(placed, placed["realized_target_id"], t)],
                    ignore_index=True,
                )
                inventory = adjust_inventory(inventory, dock_deltas(placed, "realized_target_id"))
            if not lost.empty:
                # No free dock anywhere: the bike leaves the system (a sink, per
                # the loss-logging design). The event closes the flow's spine;
                # inventory is untouched -- the bike already left its source at
                # ``departed`` and docks nowhere now.
                events = pd.concat(
                    [events, lost_events(lost, t, "dock_full")],
                    ignore_index=True,
                )

        # Tier-1 contracts: every due flow docks, redirects, or is lost exactly
        # once, and a lost flow docks nowhere -- so stock rises only by the bikes
        # that actually docked (docked + redirected), never by the lost ones.
        assert len(docked) + n_placed + n_lost == len(due), "due flows not conserved"
        assert int(inventory["quantity"].sum()) - stock_before == len(docked) + n_placed, (
            "lost or redirected count moved inventory incorrectly"
        )
        in_transit = state.in_transit.drop(due.index)
        new_state = state.with_inventory(inventory).with_in_transit(in_transit)
        return PhaseResult(new_state, events)


class FormDeparturesPhase(Phase):
    """Decide how many bikes depart per (source, commodity), gated by stock.

    Counts only -- the concrete trips (their targets and durations) are formed in
    :class:`FormPotentialTripsPhase`. The realized counts are handed over through
    the per-period ``intermediates``; the demand that did *not* fit the stock is
    emitted as ``lost`` (``reason="stockout"``) events so the journal carries the
    full demand split ``demand = departed + lost`` rather than silently dropping
    the shortfall.
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
        stock_before = int(state.state_inventory_df["quantity"].sum())
        inventory = adjust_inventory(
            state.state_inventory_df, departure_deltas_from_counts(realized)
        )
        # Tier-1 contract: stock falls by exactly the realized departures; the
        # stockout shortfall never left a dock, so it moves no inventory.
        dispatched = stock_before - int(inventory["quantity"].sum())
        assert dispatched == int(realized["realized"].sum()), "stockout moves no inventory"
        new_state = (state.with_inventory(inventory)
                     .with_intermediates(realized_departures=realized))

        # Journal the shortfall. A stockout loss is demand that never became a
        # flow: aggregated per origin, no flow_id, no target. It touches no
        # inventory (the bike never left), so it is emitted but not applied.
        shortfall = realized[realized["lost"] > 0]
        if shortfall.empty:
            return PhaseResult.empty(new_state)
        stockout = shortfall.rename(
            columns={"facility_id": "source_id", "lost": "quantity"}
        )
        return PhaseResult(new_state, lost_events(stockout, t, "stockout"))


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


