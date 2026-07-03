"""Simulation phases.

Each phase reads the state and returns a new state plus the events it created.
The four phases split one period into these steps: dock earlier arrivals -> form
departures -> (build the trips) -> dock arrivals from this same period.

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
    ) -> PhaseResult:
        """Dock this period's due arrivals, redirect the overflow, lose what fits nowhere."""
        t = period.period_id
        due = self._due_arrivals(state.in_transit, t)
        if due.empty:
            return PhaseResult.empty(state)
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
        new_flows["phase_rank"] = DOCK_PREVIOUS_RANK if self.when == "previous" else DOCK_SAME_RANK
        new_flows["phase_round"] = new_flows["phase_round"].fillna(0).astype("int64")

        # One inventory step per batch, in apply order: the planned dockings
        # (round 0), then each redirect round (Notations.md §0.1).
        working = state
        step_by_round: dict[int, int] = {}
        for round_no in sorted(new_flows["phase_round"].unique()):
            step_by_round[round_no], working = working.open_step()
        new_flows["step_id"] = new_flows["phase_round"].map(step_by_round)

        # The due bikes leave the in-transit set; legs that take time join it.
        in_transit = pd.concat(
            [state.in_transit.drop(due.index), legs_later.drop(columns="phase_round")],
            ignore_index=True,
        )
        new_state = working.with_inventory(inventory).with_in_transit(in_transit)

        # Check -- each due bike docked, left on a new leg, or was lost, exactly
        # once; only the bikes that docked now moved inventory.
        assert len(docked) + len(redirects) + len(lost) == len(due), "due flows not conserved"
        assert int(inventory["quantity"].sum()) - inventory_before == len(docked) + len(legs_now), (
            "docked count and inventory moved disagree"
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

        # Open the departures step. This one step spans two phases: the stockout
        # ``lost`` events here and the ``departed`` events that
        # :class:`FormPotentialTripsPhase` builds next both belong to it, so its
        # number is opened once here and passed forward through ``intermediates``.
        # Open only when the step will carry at least one event (a real departure
        # or a stockout); a step opened for nothing would leave a gap in the
        # numbering.
        will_depart = int(departures["departed"].sum()) > 0
        departures_step_id = None
        working = state
        if will_depart or not lost_demand.empty:
            departures_step_id, working = working.open_step()

        new_state = working.with_inventory(inventory).with_intermediates(
            departures=departures, departures_step_id=departures_step_id
        )
        new_flows = None
        if not lost_demand.empty:
            lost_demand = lost_demand.rename(
                columns={"facility_id": "source_id", "lost": "quantity"}
            )
            new_flows = lost_events(lost_demand, t, "stockout")
            # A stockout loss is this period's own activity (the middle phase).
            new_flows["phase_rank"] = PERIOD_OWN_RANK
            new_flows["step_id"] = departures_step_id

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
        # A real user departure is this period's own activity (the middle phase).
        new_flows["phase_rank"] = PERIOD_OWN_RANK
        # The departures step was opened by FormDeparturesPhase; reuse its number so
        # the departures and any stockout losses share one step (Notations.md §0.1).
        # A non-empty trips set means departures happened, so the step was opened.
        departures_step_id = state.intermediates.get("departures_step_id")
        assert departures_step_id is not None, "departures step was not opened"
        new_flows["step_id"] = departures_step_id

        # Writes -- add the new departed flows to the in-transit set.
        in_transit = pd.concat([state.in_transit, new_flows], ignore_index=True)
        new_state = state.with_in_transit(in_transit)
        return PhaseResult(new_state, new_flows)
