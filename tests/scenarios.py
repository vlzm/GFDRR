"""Synthetic, offline simulation scenarios for the test suite.

The real pipeline reads a trip CSV and a live GBFS feed, neither of which a unit
test should depend on. These builders construct the small slice of
``ResolvedModelData`` that the engine and phases actually read -- the period
grid, initial inventory, dock capacities, geography, and the historical
marginals (demand and OD matrix) derived from a handful of hand-written trips --
so every scenario is deterministic and runs in milliseconds.

Each scenario is a known story:

- ``canonical``    -- saturated inventory and capacity; no constraint binds.
- ``overflow``     -- a full dock forces a redirect to a free station.
- ``stockout``     -- demand outruns the inventory at a source.
- ``network_full`` -- every dock is full, so a bike fits nowhere and is lost.
- ``single_trip``  -- the smallest non-empty run.

Tests run the real :class:`Environment` on them and assert the journal is
well-formed (:mod:`tests.invariants`) and the run invariants I1-I4 hold
(:func:`gbp.consumers.simulator.validation.validate_run`).
"""

import types

import pandas as pd

from gbp.consumers.simulator import (
    DockArrivals,
    FormDeparturesPhase,
    FormPotentialTripsPhase,
)
from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.engine import Environment
from gbp.loaders.dataloader_graph import build_potential_trips
from gbp.model import journal as J

CLASSIC = "classic_bike"


def _history(trips: list[tuple[str, str, int, int]]) -> pd.DataFrame:
    """Build a historical flow log from ``(source, target, start, end)`` trips."""
    df = pd.DataFrame({
        "flow_id":            [f"hist_{i}" for i in range(len(trips))],
        "source_id":          [t[0] for t in trips],
        "planned_target_id":  [t[1] for t in trips],
        "commodity_category": CLASSIC,
        "start_period":       [t[2] for t in trips],
        "planned_end_period": [t[3] for t in trips],
    })
    return J.finalize_flows(pd.concat(
        [J.departed_events(df), J.arrived_events(df, df["planned_end_period"])],
        ignore_index=True,
    ))


def build_resolved(
    trips: list[tuple[str, str, int, int]],
    *,
    capacities: dict[str, int] | None = None,
    initial_inventory: dict[str, int] | None = None,
    n_periods: int = 8,
) -> types.SimpleNamespace:
    """Assemble the resolved-data slice the engine reads, from synthetic trips.

    A real ``ResolvedModelData`` carries far more; the engine and the four phases
    only read the fields set below, so a light namespace is enough to drive a
    run. ``capacities`` / ``initial_inventory`` default per facility to "very
    large" / "empty"; pass a dict to make a constraint bind.
    """
    capacities = capacities or {}
    initial_inventory = initial_inventory or {}
    hist = _history(trips)
    facilities = sorted({t[0] for t in trips} | {t[1] for t in trips})

    geo = pd.DataFrame({
        "facility_id": facilities,
        "lat": [40.0 + i * 1e-3 for i in range(len(facilities))],
        "lng": [-74.0 + i * 1e-3 for i in range(len(facilities))],
    })
    periods = pd.DataFrame({"period_id": range(n_periods)})
    periods["start_timestamp"] = (
        pd.Timestamp("2026-01-01") + periods["period_id"] * pd.Timedelta(hours=1)
    )
    periods["end_timestamp"] = periods["start_timestamp"] + pd.Timedelta(hours=1)

    resolved = types.SimpleNamespace()
    resolved.periods_df = periods
    resolved.facilities_geo_df = geo
    resolved.facilities_capacities_df = pd.DataFrame(
        [{"facility_id": f, "capacity": capacities.get(f, 10_000)} for f in facilities]
    )
    resolved.initial_inventory_df = pd.DataFrame(
        [{"facility_id": f, "commodity_category": CLASSIC,
          "quantity": initial_inventory.get(f, 0)} for f in facilities]
    )
    resolved.historical_flows_df = hist
    resolved.historical_demand_df = J.flows_to_departures(hist)
    resolved.historical_od_matrix_df = J.flows_to_od_matrix(hist)
    resolved.potential_trips_df = build_potential_trips(hist)
    return resolved


def run(
    resolved: types.SimpleNamespace, *, demand_scale_factor: float = 1.0
) -> tuple[pd.DataFrame, object]:
    """Run the canonical four-phase loop on ``resolved``; return (journal, state).

    Invariant checking is left off here so the caller can assert on the
    violation list directly (a clearer failure than a raised error); the tests
    call :func:`validate_run` themselves.
    """
    phases = [
        DockArrivals("previous"),
        FormDeparturesPhase(),
        FormPotentialTripsPhase(),
        DockArrivals("same"),
    ]
    config = EnvironmentConfig(
        phases=phases,
        seed=42,
        scenario_id="test",
        validate=False,
        demand_scale_factor=demand_scale_factor,
        number_of_periods=len(resolved.periods_df),
    )
    env = Environment(resolved, config)
    state = env.run()
    return env.simulated_flows_df, state


# ---------------------------------------------------------------------------
# Named scenarios -- each returns the resolved data ready to run().
# ---------------------------------------------------------------------------
def canonical() -> types.SimpleNamespace:
    """Saturated replay: inventory and capacity never bind, so no constraint fires."""
    trips = [
        ("s1", "s2", 0, 1), ("s1", "s3", 0, 2),
        ("s2", "s1", 1, 2), ("s2", "s3", 1, 2),
        ("s3", "s1", 2, 3),
    ]
    return build_resolved(trips, initial_inventory={"s1": 500, "s2": 500, "s3": 500})


def overflow() -> types.SimpleNamespace:
    """Six bikes aim at s3, whose docks hold two -- four must redirect elsewhere."""
    trips = [("s1", "s3", 0, 1)] * 3 + [("s2", "s3", 0, 1)] * 3
    return build_resolved(
        trips, capacities={"s3": 2}, initial_inventory={"s1": 50, "s2": 50, "s3": 0}
    )


def stockout() -> types.SimpleNamespace:
    """Five want to leave s1, which holds two -- three are lost to a stockout."""
    trips = [("s1", "s2", 0, 1)] * 5
    return build_resolved(trips, initial_inventory={"s1": 2, "s2": 0})


def network_full() -> types.SimpleNamespace:
    """Both docks are full (capacity 0), so arriving bikes fit nowhere and are lost."""
    trips = [("s1", "s2", 0, 1)] * 3
    return build_resolved(
        trips, capacities={"s1": 0, "s2": 0}, initial_inventory={"s1": 50, "s2": 0}
    )


def single_trip() -> types.SimpleNamespace:
    """Smallest non-empty run: one bike, one normal trip."""
    return build_resolved([("s1", "s2", 0, 1)], initial_inventory={"s1": 5})


#: Every scenario, for the universal-invariant sweep.
ALL_SCENARIOS = {
    "canonical": canonical,
    "overflow": overflow,
    "stockout": stockout,
    "network_full": network_full,
    "single_trip": single_trip,
}
