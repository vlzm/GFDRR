"""Simulation phases.

Each phase reads the state, builds this period's events, and writes them
through :meth:`SimulationState.apply_step_events` -- the one call that appends
the events to the journal and moves the projections (inventory, ``in_transit``)
by exactly what the events imply. The three phases split one period into these
steps: dock earlier arrivals -> form departures (and build the trips) -> dock
arrivals from this same period.

:class:`DockArrivals` parks the bikes that arrive this period. It fills the free
dock slots first, then sends any extra bikes on a new leg to the nearest station
that still has a free dock; a leg that takes time docks in a later period. When
we replay the real history the docks are never full, so the capacity limit and
the redirect do nothing. They only start to matter when traffic goes above the
historical level.
"""

from typing import Literal

import pandas as pd

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

from .config import EnvironmentConfig
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


class Phase:
    """Base class for one phase: a single step of a period, run every period.

    A phase declares where its events sort inside a period once
    (:attr:`phase_rank`) and builds this period's events
    (:meth:`build_events`). :meth:`execute` writes them through
    :meth:`SimulationState.apply_step_events`, which stamps the ordering
    columns and moves the projections -- so a normal phase implements only
    ``build_events``. The engine checks at construction time that the phase
    list is ordered by ``phase_rank``, so the list position and the stamped
    rank cannot disagree. A phase that also replaces a named state field (the
    rebalancing plan) overrides ``execute`` and writes its events through the
    same call.
    """

    #: Where this phase's events sort inside a period (Notations.md §0.1).
    phase_rank: int

    def execute(
        self,
        state: SimulationState,
        resolved: ScenarioInputs,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> SimulationState:
        """Run the phase: build this period's events and write them as one batch."""
        return state.apply_step_events(
            self.build_events(state, resolved, period, config), self.phase_rank
        )

    def build_events(
        self,
        state: SimulationState,
        resolved: ScenarioInputs,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> pd.DataFrame:
        """Build this period's events; an empty frame means nothing happened.

        Required for a phase that uses the default :meth:`execute`. A phase
        that overrides ``execute`` (the two rebalancing phases) never calls
        it, so this cannot be an ``abc`` abstract method: that would forbid
        instantiating those phases.
        """
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

    def __init__(self, when: Literal["previous", "same"]) -> None:
        if when not in ("previous", "same"):
            raise ValueError(f"when must be 'previous' or 'same', got {when!r}")
        self.when = when
        self.phase_rank = DOCK_PREVIOUS_RANK if when == "previous" else DOCK_SAME_RANK

    def _due_arrivals(self, in_transit: pd.DataFrame, t: int) -> pd.DataFrame:
        """Select the in-transit flows this phase docks at period ``t`` (picked by ``when``).

        User trips only: bikes riding on a truck (``flow_type == "rebalance"``)
        are also in transit, but their dropoff is applied by
        ``ApplyRebalancingPhase``, not here.
        """
        due_now = (in_transit["planned_end_period"] == t) & (in_transit["flow_type"] == "user_trip")
        if self.when == "previous":
            return in_transit[due_now & (in_transit["start_period"] < t)]
        return in_transit[due_now & (in_transit["start_period"] == t)]

    def build_events(
        self,
        state: SimulationState,
        resolved: ScenarioInputs,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> pd.DataFrame:
        """Dock this period's due arrivals, redirect the overflow, lose what fits nowhere."""
        t = period.period_id
        due = self._due_arrivals(state.in_transit, t)
        if due.empty:
            return empty_flows_journal()

        # Dock at the planned station while free docks last.
        docked, overflow = dock_up_to_capacity(
            due, free_docks(state.state_inventory_df, resolved.facilities_capacities_df)
        )
        arrivals_docked = arrived_events(docked, t)

        # Resolve every bike that did not fit: docked at a redirect target,
        # riding a new leg, or lost. The redirect must see the docks the
        # planned dockings just took, so it reads the inventory as it will
        # stand once those events are written (a decision input; the real
        # write happens in apply_step_events).
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

        # Turn the outcomes into events. A redirected bike always bounces and
        # opens a new leg; an outcome "docked" also arrives within this period.
        redirects = outcomes[outcomes["outcome"] != "lost"]
        lost = outcomes[outcomes["outcome"] == "lost"]
        bounces = redirected_events(redirects, t).assign(phase_round=redirects["phase_round"])
        legs = redirect_leg_events(redirects, t).assign(phase_round=redirects["phase_round"])
        legs_now = legs[redirects["outcome"] == "docked"]
        arrivals_now = arrived_events(legs_now, t).assign(phase_round=legs_now["phase_round"])

        lost_dock_full = lost_events(lost, t, "dock_full")
        # One inventory step per phase_round, in apply order: the planned
        # dockings (round 0), then each redirect round (Notations.md §0.1).
        return pd.concat(
            [arrivals_docked, lost_dock_full, bounces, legs, arrivals_now],
            ignore_index=True,
        )


class FormDeparturesPhase(Phase):
    """Form this period's departures: the demand split and the trips, in one phase.

    The period's own activity (:data:`PERIOD_OWN_RANK`), start to finish:

    1. Decide how many bikes leave each ``(source, commodity)`` --
       ``min(demand, inventory)`` -- and take them out of the inventory.
    2. Book the demand that did *not* fit as ``lost`` events
       (``reason="stockout"``), so the journal keeps the full split
       ``demand = departed + lost`` instead of quietly dropping the lost demand.
    3. Spread the departures over the targets with the OD probabilities
       ``P(target | source, commodity)``, set each trip's arrival period from
       the mean historical duration of its pair, and emit one ``departed`` flow
       per bike. In the simple case the OD matrix is the historical one, so a
       base run repeats the historical demand pattern.

    All of it is one inventory step: the ``departed`` flows and the stockout
    ``lost`` events share one ``step_id`` (Notations.md §0.1).
    """

    phase_rank = PERIOD_OWN_RANK

    def build_events(
        self,
        state: SimulationState,
        resolved: ScenarioInputs,
        period: PeriodRow,
        config: EnvironmentConfig,
    ) -> pd.DataFrame:
        """Split this period's demand into departed flows and stockout losses."""
        t = period.period_id
        demand = resolved.historical_demand_df
        demand_now = demand[demand["period_id"] == t].copy()
        demand_now.loc[:, "quantity"] = (
            (demand_now["quantity"] * config.demand_scale_factor).round().astype("Int64")
        )
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
