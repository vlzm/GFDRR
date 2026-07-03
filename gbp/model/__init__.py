"""Model layer: the shared domain vocabulary the whole system speaks.

The flow journal (:mod:`gbp.model.flows`) is the central abstraction of the
platform -- the format in which both the loaders (historical flows) and the
simulator (simulated flows) express what happened, and from which every marginal
observation is derived. It lives here, in its own layer, rather than inside any
one producer or consumer: the loaders and the simulator both depend on it, and
it depends on neither.

The public surface is the journal vocabulary: event constructors
(``departed_events``, ``arrived_events``, ``redirected_events``,
``redirect_leg_events``, ``lost_events``),
empty-state factories, ``finalize_flows``, the ``flows_to_*`` projections,
and the journal-level run invariants (``check_demand_split``, ``check_flow_closure``).
"""

from .flows import (
    DOCK_PREVIOUS_RANK,
    DOCK_SAME_RANK,
    PERIOD_OWN_RANK,
    arrived_events,
    check_demand_split,
    check_flow_closure,
    departed_events,
    empty_flows_journal,
    empty_in_transit,
    finalize_flows,
    flows_to_arrivals,
    flows_to_departures,
    flows_to_od_matrix,
    flows_with_inventory,
    get_inventory_df,
    inventory_at_moments,
    is_docking,
    is_user_departure,
    lost_events,
    neighbor_distance_sq,
    phase_rank_by_timing,
    redirect_leg_events,
    redirect_neighbor_table,
    redirected_events,
)

__all__ = [
    "DOCK_PREVIOUS_RANK",
    "DOCK_SAME_RANK",
    "PERIOD_OWN_RANK",
    "arrived_events",
    "check_demand_split",
    "check_flow_closure",
    "departed_events",
    "empty_flows_journal",
    "empty_in_transit",
    "finalize_flows",
    "flows_to_arrivals",
    "flows_to_departures",
    "flows_to_od_matrix",
    "flows_with_inventory",
    "get_inventory_df",
    "inventory_at_moments",
    "is_docking",
    "is_user_departure",
    "lost_events",
    "neighbor_distance_sq",
    "phase_rank_by_timing",
    "redirect_leg_events",
    "redirect_neighbor_table",
    "redirected_events",
]
