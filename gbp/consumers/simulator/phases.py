"""Simulation phases.

Each phase reads the state, applies its inventory changes, writes its events to
the journal through :meth:`SimulationState.apply_step_events`, and returns the
new state. The three phases split one period into these steps: dock earlier
arrivals -> form departures (and build the trips) -> dock arrivals from this
same period.

:class:`DockArrivals` parks the bikes that arrive this period. It fills the free
dock slots first, then sends any extra bikes on a new leg to the nearest station
that still has a free dock; a leg that takes time docks in a later period. When
we replay the real history the docks are never full, so the capacity limit and
the redirect do nothing. They only start to matter when traffic goes above the
historical level.
"""

import pandas as pd

from gbp.loaders.dataloader_graph import ResolvedModelData
from gbp.model import (
    DOCK_PREVIOUS_RANK,
    DOCK_SAME_RANK,
    PERIOD_OWN_RANK,
    arrived_events,
    departed_events,
    lost_events,
    redirect_leg_events,
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
    ) -> SimulationState:
        """Apply the phase to the state; return the next state (events appended)."""
        raise NotImplementedError


class DockArrivals(Phase):
    """Dock the bikes that arrive this period; redirect what does not fit.

    Arrivals dock at their planned station while it has free docks. Each bike
    that does not fit bounces and gets a new leg to the nearest station with a
    free dock. A zero-duration leg docks within this same phase; a leg that
    takes time (the pair's travel time from the OD matrix) re-enters
    ``in_transit`` and docks -- or bounces again -- when it arrives. A bike is
    lost only when no station in the network has a free dock. In an exact
    replay of history the docks are never full, so none of this fires.

    ``when`` picks which arrivals this phase handles, because the two run at
    different moments of the period: ``"previous"`` docks bikes that departed
    in an earlier period (before this period's departures are formed), and
    ``"same"`` docks bikes that departed within this period (after them).
    """

    def __init__(self, when: str, schedule: Schedule | None = None) -> None:
        super().__init__(schedule)
        if when not in ("previous", "same"):
            raise ValueError(f"when must be 'previous' or 'same', got {when!r}")
        self.when = when
        self.name = f"dock_arrivals_{when}"

    def _due_arrivals(self, in_transit: pd.DataFrame, t: int) -> pd.DataFrame:
        """Select the in-transit flows this phase docks at period ``t`` (picked by ``when``)."""
        due_now = in_transit["planned_end_period"] == t
        if self.when == "previous":
            return in_transit[due_now & (in_transit["start_period"] < t)]
        return in_transit[due_now & (in_transit["start_period"] == t)]

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> SimulationState:
        """Dock this period's due arrivals, redirect the overflow, lose what fits nowhere."""
        t = period.period_id
        due = self._due_arrivals(state.in_transit, t)
        if due.empty:
            return state
        inventory = state.state_inventory_df
        inventory_before = int(inventory["quantity"].sum())

        # Dock at the planned station while free docks last.
        docked, overflow = dock_up_to_capacity(
            due, free_docks(inventory, resolved.facilities_capacities_df)
        )
        inventory = adjust_inventory(inventory, dock_deltas(docked))

        # Plan a new leg for each bike that did not fit. Legs with zero travel
        # time dock right now; the others dock when they arrive.
        redirects, lost = plan_overflow_redirect(
            inventory,
            resolved.facilities_capacities_df,
            resolved.facilities_geo_df,
            resolved.historical_od_matrix_df,
            resolved.trip_speed_km_per_period,
            overflow,
            t,
        )
        bounces = redirected_events(redirects, t).assign(phase_round=redirects["phase_round"])
        legs = redirect_leg_events(redirects, t).assign(phase_round=redirects["phase_round"])
        legs_now = legs[legs["planned_end_period"] == t]
        legs_later = legs[legs["planned_end_period"] > t]
        arrivals_now = arrived_events(legs_now, t).assign(phase_round=legs_now["phase_round"])
        inventory = adjust_inventory(inventory, dock_deltas(legs_now))

        lost_dock_full = lost_events(lost, t, "dock_full")
        new_flows = pd.concat(
            [arrived_events(docked, t), lost_dock_full, bounces, legs, arrivals_now],
            ignore_index=True,
        )
        # One inventory step per phase_round, in apply order: the planned
        # dockings (round 0), then each redirect round (Notations.md §0.1).
        phase_rank = DOCK_PREVIOUS_RANK if self.when == "previous" else DOCK_SAME_RANK
        new_state = state.apply_step_events(new_flows, phase_rank)

        # The due bikes leave the in-transit set; legs that take time join it.
        in_transit = pd.concat(
            [state.in_transit.drop(due.index), legs_later.drop(columns="phase_round")],
            ignore_index=True,
        )
        new_state = new_state.with_inventory(inventory).with_in_transit(in_transit)

        # Check -- each due bike docked, left on a new leg, or was lost, exactly
        # once; only the bikes that docked now moved inventory.
        assert len(docked) + len(redirects) + len(lost) == len(due), "due flows not conserved"
        assert int(inventory["quantity"].sum()) - inventory_before == len(docked) + len(legs_now), (
            "docked count and inventory moved disagree"
        )
        return new_state


class FormDeparturesPhase(Phase):
    """Form this period's departures: the demand split and the trips, in one phase.

    The period's own activity (:data:`PERIOD_OWN_RANK`), start to finish:

    1. Decide how many bikes leave each ``(source, commodity)`` --
       ``min(demand, inventory)`` -- and take them out of the inventory.
    2. Book the demand that did *not* fit as ``lost`` events
       (``reason="stockout"``), so the journal keeps the full split
       ``demand = departed + lost`` instead of quietly dropping the lost demand.
    3. Spread the departures over the destinations with the OD probabilities
       ``P(target | source, commodity)``, set each trip's arrival period from
       the mean historical duration of its pair, and emit one ``departed`` flow
       per bike. In the simple case the OD matrix is the historical one, so a
       base run repeats the historical demand pattern.

    All of it is one inventory step: the ``departed`` flows and the stockout
    ``lost`` events share one ``step_id`` (Notations.md §0.1).
    """

    name = "form_departures"

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> SimulationState:
        """Split this period's demand into departed flows and stockout losses."""
        t = period.period_id
        demand = resolved.historical_demand_df
        demand_now = demand[demand["period_id"] == t].copy()
        demand_now.loc[:, "quantity"] = (
            (demand_now["quantity"] * config.demand_scale_factor).round().astype("Int64")
        )
        if demand_now.empty:
            return state
        inventory_before = int(state.state_inventory_df["quantity"].sum())

        # Mechanics -- bound the demand by the inventory, then spread each
        # source's departures over the destinations with the OD matrix and split
        # the totals into one departed flow per bike.
        departures = realize_departures(demand_now, state.state_inventory_df)
        inventory = adjust_inventory(
            state.state_inventory_df, departure_deltas_from_counts(departures)
        )
        lost_demand = departures[departures["lost"] > 0]
        departed_counts = departures.rename(
            columns={"facility_id": "source_id", "departed": "quantity"}
        )
        potential = form_potential_trips(departed_counts, resolved.historical_od_matrix_df, t)
        trips_now = expand_potential_trips(potential, t)

        # Events -- the departures and the stockout losses are one batch, so
        # apply_step_events gives them one shared step_id.
        new_departed = departed_events(trips_now)
        batches = [] if new_departed.empty else [new_departed]
        if not lost_demand.empty:
            lost_demand = lost_demand.rename(
                columns={"facility_id": "source_id", "lost": "quantity"}
            )
            batches.append(lost_events(lost_demand, t, "stockout"))
        if not batches:
            return state.with_inventory(inventory)
        new_flows = pd.concat(batches, ignore_index=True)
        new_state = state.apply_step_events(new_flows, PERIOD_OWN_RANK)

        # Writes -- the new departed flows enter the in-transit working set.
        if not new_departed.empty:
            in_transit = pd.concat([state.in_transit, new_departed], ignore_index=True)
            new_state = new_state.with_in_transit(in_transit)
        new_state = new_state.with_inventory(inventory)

        departed_total = inventory_before - int(inventory["quantity"].sum())
        assert departed_total == int(departures["departed"].sum()), "stockout moves no inventory"
        return new_state
