# ===========================================================================
# Phases. Each phase reads the state and returns a new state plus the events it
# emitted. The six canonical phases split a period into: dock earlier arrivals
# -> (redirect) -> form departures -> (sample extra trips) -> dock same-period
# arrivals -> (redirect).
#
# Fully implemented (the replay core): ArrivalsPreviousPhase, FormDeparturesPhase,
# ArrivalsPhase. TODO skeletons (only bite above the historical baseline): the
# two OverflowRedirect phases and FormPotentialTripsPhase.
# ===========================================================================
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
    inventory = adjust_inventory(state.inventory, arrival_deltas(due))
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
        # (capacity = resolved.facilities_capacities, distance = resolved.facilities_geo),
        # emit `redirected` events and set realized_target_id / realized_end_period.
        # Dormant in the historical replay (dock capacity is not binding there).
        return PhaseResult.empty(state)


class FormDeparturesPhase(Phase):
    """Release this period's desired trips and decrement source inventory."""

    name = "form_departures"

    def execute(self, state, resolved, period):
        t = period.period_id
        trips_now = resolved.potential_trips[resolved.potential_trips["start_period"] == t]
        if trips_now.empty:
            return PhaseResult.empty(state)

        # Historical replay: every desired trip departs.
        # TODO(core algorithm): gate by inventory -- a trip whose source is out of
        # stock for its commodity becomes `lost` (reason="stockout") instead of
        # `departed`. Only bites once demand exceeds the historical baseline.
        events = departed_events(trips_now)
        inventory = adjust_inventory(state.inventory, departure_deltas(trips_now))
        in_transit = pd.concat([state.in_transit, events], ignore_index=True)
        new_state = state.with_inventory(inventory).with_in_transit(in_transit)
        return PhaseResult(new_state, events)


class FormPotentialTripsPhase(Phase):
    name = "form_potential_trips"

    def execute(self, state, resolved, period):
        # TODO(core algorithm): for demand above the historical baseline, expand
        # the extra departures into concrete trips -- sample a target from the OD
        # matrix P(target | source, commodity) and a duration from the historical
        # duration distribution, then set planned_target_id / planned_end_period.
        # In the historical replay every trip already carries its real target and
        # duration, so there is nothing to sample.
        return PhaseResult.empty(state)


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


def build_potential_trips(historical_flows_df: pd.DataFrame) -> pd.DataFrame:
    """Replay demand: one concrete desired trip per historical departure.

    Carries the real target and duration of every trip, which is what makes the
    base run reproduce history exactly instead of resampling it.
    """
    cols = ["flow_id", "source_id", "planned_target_id",
            "commodity_category", "start_period", "planned_end_period"]
    return historical_flows_df.query("event_type == 'departed'")[cols].reset_index(drop=True)


def _period_flows(resolved: ResolvedModelData, period: PeriodRow):
    # TODO: return the observed flows for `period` once the extra-demand path
    # needs them (source_id, target_id, commodity_category, quantity). Returning
    # None keeps HistoricalODStructurePhase a safe no-op until then.
    return None


# template -- not wired into phases_canonical yet; ready for the extra-demand
# path (it builds the OD matrix that FormPotentialTripsPhase will sample from).
class HistoricalODStructurePhase(Phase):
    """Compute conditional destination probabilities ``P(j | i)`` from history.

    Reads ``resolved.observed_flow`` for the current period and builds, per
    ``(source_id, commodity_category)``, the conditional distribution over
    ``target_id``::

        P(target_id = j | source_id = i, commodity = k)
            = T_ijk / sum_j T_ijk

    where ``T_ijk`` is the observed quantity flowing from *i* to *j* in
    commodity *k* during the current period.

    The result is written into ``state.intermediates["od_probabilities"]``
    for downstream phases (e.g. trip sampling).  No log table is emitted:
    the OD matrix is potentially large (O(N^2) per period) and is meant
    as a transient hand-off between phases, not historical record.

    Inventory and in-transit are untouched.  By construction the probability
    column sums to 1.0 within every ``(source_id, commodity_category)`` group
    (modulo floating-point error), since the marginal ``O_i`` is recomputed
    from the same ``T_ij`` rather than read from elsewhere -- guaranteeing
    self-consistency and parity with the data the next phase will sample.

    Parameters
    ----------
    schedule
        Optional execution schedule.  Defaults to every period.
    """

    name: str = "HISTORICAL_OD_STRUCTURE"

    def __init__(self, schedule: Schedule | None = None) -> None:
        """Initialise with an optional schedule."""
        self._schedule = schedule or Schedule.every()

    def should_run(self, period: PeriodRow) -> bool:
        """Delegate to schedule."""
        return self._schedule.should_run(period)

    def execute(
        self,
        state: SimulationState,
        resolved: ResolvedModelData,
        period: PeriodRow,
    ) -> PhaseResult:
        """Build the conditional OD distribution for the current period."""
        flows = _period_flows(resolved, period)
        if flows is None:
            return PhaseResult.empty(state)

        joint = (
            flows.groupby(
                ["source_id", "target_id", "commodity_category"],
                as_index=False,
            )["quantity"]
            .sum()
            .rename(columns={"quantity": "joint"})
        )
        origin_total = (
            joint.groupby(["source_id", "commodity_category"], as_index=False)["joint"]
            .sum()
            .rename(columns={"joint": "origin_total"})
        )

        od = joint.merge(
            origin_total,
            on=["source_id", "commodity_category"],
            how="left",
        )
        od["probability"] = od["joint"] / od["origin_total"]
        od = od[["source_id", "target_id", "commodity_category", "probability"]]
        od = od.sort_values(
            ["source_id", "commodity_category", "target_id"],
            kind="stable",
        ).reset_index(drop=True)

        return PhaseResult(
            state=state.with_intermediates(od_probabilities=od),
        )