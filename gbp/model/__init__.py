"""Model layer: the shared domain vocabulary the whole system speaks.

The flow journal (:mod:`gbp.model.journal`) is the central abstraction of the
platform -- the format in which both the loaders (historical flows) and the
simulator (simulated flows) express what happened, and from which every marginal
observation is derived. It lives here, in its own layer, rather than inside any
one producer or consumer: the loaders and the simulator both depend on it, and
it depends on neither.

The public surface is the journal vocabulary: event constructors
(``departed_events``, ``arrived_events``, ``redirected_events``), empty-state
factories, ``finalize_flows``, the ``flows_to_*`` projections, and ``observe``
with its ``Observations`` result.
"""

from .journal import (
    Observations,
    arrived_events,
    departed_events,
    empty_flows_journal,
    empty_in_transit,
    finalize_flows,
    flows_to_arrivals,
    flows_to_departures,
    flows_to_od_matrix,
    observe,
    redirected_events,
)

__all__ = [
    "Observations",
    "arrived_events",
    "departed_events",
    "empty_flows_journal",
    "empty_in_transit",
    "finalize_flows",
    "flows_to_arrivals",
    "flows_to_departures",
    "flows_to_od_matrix",
    "observe",
    "redirected_events",
]
