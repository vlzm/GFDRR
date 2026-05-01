"""Unit tests for RebalancerTask.

Covers the public contract: schema, early-exit paths, pre-assignment,
arrival_period semantics. Multi-stop chain semantics are exercised by the
integration test in tests/integration/.
"""

from __future__ import annotations

import pandas as pd
import pytest

from gbp.build.pipeline import build_model
from gbp.consumers.simulator.state import init_state
from gbp.consumers.simulator.task import DISPATCH_COLUMNS
from gbp.consumers.simulator.tasks.rebalancer import RebalancerTask
from gbp.core.enums import ResourceStatus
from gbp.loaders import DataLoaderGraph, DataLoaderMock, GraphLoaderConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def resolved_with_trucks():
    """Resolved model with 3 trucks, 4 stations, 1 depot, hourly periods."""
    mock = DataLoaderMock(
        {"n_stations": 4, "n_depots": 1, "n_timestamps": 24},
        n_trucks=3,
        truck_capacity_bikes=20,
    )
    loader = DataLoaderGraph(mock, GraphLoaderConfig(distance_backend="haversine"))
    return build_model(loader.load())


_TEST_COMMODITY = "electric_bike"


@pytest.fixture()
def state_with_imbalance(resolved_with_trucks):
    """State with strong inventory imbalance forcing the PDP solver to find pairs.

    Half the stations get 100 bikes (over-utilized), the other half 0
    (under-utilized) for ``_TEST_COMMODITY``.
    """
    state = init_state(resolved_with_trucks)
    inv = state.inventory.copy()
    bike_mask = inv["commodity_category"] == _TEST_COMMODITY
    station_mask = inv["facility_id"].astype(str).str.startswith("station")
    rows = inv[bike_mask & station_mask].index.tolist()
    half = len(rows) // 2
    inv.loc[rows[:half], "quantity"] = 100.0
    inv.loc[rows[half:], "quantity"] = 0.0
    return state.with_inventory(inv)


@pytest.fixture()
def state_balanced(resolved_with_trucks):
    """State where all stations sit comfortably between min and max thresholds."""
    state = init_state(resolved_with_trucks)
    inv = state.inventory.copy()
    bike_mask = inv["commodity_category"] == _TEST_COMMODITY
    inv.loc[bike_mask, "quantity"] = 10.0  # mid-range utilization
    return state.with_inventory(inv)


def _task(**overrides) -> RebalancerTask:
    """Construct a RebalancerTask defaulted to ``_TEST_COMMODITY``."""
    kwargs = {"commodity_category": _TEST_COMMODITY}
    kwargs.update(overrides)
    return RebalancerTask(**kwargs)


def _period(resolved_with_trucks):
    """First period as a PeriodRow."""
    from gbp.consumers.simulator.state import PeriodRow

    row = next(iter(resolved_with_trucks.periods.itertuples()))
    return PeriodRow(**row._asdict())


# ---------------------------------------------------------------------------
# Contract tests
# ---------------------------------------------------------------------------


def test_returns_dispatch_columns_schema(resolved_with_trucks, state_with_imbalance):
    """RebalancerTask.run must return a DataFrame whose columns are exactly DISPATCH_COLUMNS."""
    out = _task().run(state_with_imbalance, resolved_with_trucks, _period(resolved_with_trucks))
    assert isinstance(out, pd.DataFrame)
    assert list(out.columns) == DISPATCH_COLUMNS


def test_empty_when_no_truck_available(resolved_with_trucks, state_with_imbalance):
    """No AVAILABLE truck → early-exit empty DataFrame (no solver work)."""
    state = state_with_imbalance
    resources = state.resources.copy()
    truck_mask = resources["resource_category"] == "rebalancing_truck"
    resources.loc[truck_mask, "status"] = ResourceStatus.IN_TRANSIT.value
    state = state.with_resources(resources)

    out = _task().run(state, resolved_with_trucks, _period(resolved_with_trucks))
    assert out.empty
    assert list(out.columns) == DISPATCH_COLUMNS


def test_empty_when_no_imbalance(resolved_with_trucks, state_balanced):
    """All stations within thresholds → no sources or destinations → empty."""
    # min=0.0, max=1.0 means NOTHING is over/under utilized.
    out = _task(min_threshold=0.0, max_threshold=1.0).run(
        state_balanced, resolved_with_trucks, _period(resolved_with_trucks),
    )
    assert out.empty


def test_pre_assigned_resource_id(resolved_with_trucks, state_with_imbalance):
    """Emitted rows pre-assign resource_id to an available truck (no nulls)."""
    out = _task().run(state_with_imbalance, resolved_with_trucks, _period(resolved_with_trucks))
    if out.empty:
        pytest.skip("solver did not produce a route on this fixture; tested elsewhere")
    assert out["resource_id"].notna().all()
    available = state_with_imbalance.resources[
        state_with_imbalance.resources["resource_category"] == "rebalancing_truck"
    ]
    assert set(out["resource_id"].astype(str)).issubset(
        set(available["resource_id"].astype(str))
    )


def test_arrival_period_strictly_after_current(resolved_with_trucks, state_with_imbalance):
    """All emitted arrival_period values are strictly greater than current period_index."""
    period = _period(resolved_with_trucks)
    out = _task().run(state_with_imbalance, resolved_with_trucks, period)
    if out.empty:
        pytest.skip("solver did not produce a route on this fixture; tested elsewhere")
    assert (out["arrival_period"] > period.period_index).all()


def test_resource_ids_drawn_from_available_pool(resolved_with_trucks, state_with_imbalance):
    """Every emitted ``resource_id`` is in the available truck pool (no phantoms)."""
    out = _task().run(state_with_imbalance, resolved_with_trucks, _period(resolved_with_trucks))
    if out.empty:
        pytest.skip("solver did not produce a route on this fixture; tested elsewhere")

    available_truck_ids = set(
        state_with_imbalance.resources.loc[
            state_with_imbalance.resources["resource_category"] == "rebalancing_truck",
            "resource_id",
        ].astype(str)
    )
    emitted = set(out["resource_id"].astype(str))
    assert emitted.issubset(available_truck_ids), (
        f"Emitted resource_ids {emitted - available_truck_ids} are not in the "
        f"available pool {available_truck_ids}"
    )


def test_random_seed_determinism(resolved_with_trucks, state_with_imbalance):
    """Same seed → identical output across runs."""
    period = _period(resolved_with_trucks)
    out1 = _task(pdp_random_seed=42).run(state_with_imbalance, resolved_with_trucks, period)
    out2 = _task(pdp_random_seed=42).run(state_with_imbalance, resolved_with_trucks, period)
    pd.testing.assert_frame_equal(
        out1.reset_index(drop=True),
        out2.reset_index(drop=True),
    )
