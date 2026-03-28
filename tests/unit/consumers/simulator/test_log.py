"""Tests for SimulationLog."""
# ruff: noqa: D102

from __future__ import annotations

from datetime import date

import pandas as pd

from gbp.consumers.simulator.log import (
    FLOW_LOG_COLUMNS,
    INVENTORY_LOG_COLUMNS,
    RESOURCE_LOG_COLUMNS,
    RejectReason,
    SimulationLog,
)
from gbp.consumers.simulator.phases import PhaseResult
from gbp.consumers.simulator.state import (
    PeriodRow,
    init_state,
)
from gbp.core.model import ResolvedModelData


def _make_period(period_index: int = 0, period_id: str = "p0") -> PeriodRow:
    return PeriodRow(
        Index=0,
        period_id=period_id,
        planning_horizon_id="h1",
        segment_index=0,
        period_index=period_index,
        period_type="day",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
    )


class TestEmptyLog:
    """to_dataframes on a fresh log returns empty DataFrames with correct columns."""

    def test_all_keys_present(self) -> None:
        log = SimulationLog()
        dfs = log.to_dataframes()

        assert set(dfs.keys()) == {
            "simulation_inventory_log",
            "simulation_flow_log",
            "simulation_resource_log",
            "simulation_unmet_demand_log",
            "simulation_rejected_dispatches_log",
        }

    def test_empty_columns(self) -> None:
        log = SimulationLog()
        dfs = log.to_dataframes()

        assert list(dfs["simulation_inventory_log"].columns) == INVENTORY_LOG_COLUMNS
        assert list(dfs["simulation_flow_log"].columns) == FLOW_LOG_COLUMNS
        assert list(dfs["simulation_resource_log"].columns) == RESOURCE_LOG_COLUMNS
        for df in dfs.values():
            assert len(df) == 0


class TestRecordPeriod:
    """record_period snapshots inventory and resources."""

    def test_snapshot_shapes(self, resolved_model: ResolvedModelData) -> None:
        state = init_state(resolved_model)
        log = SimulationLog()
        period = _make_period(0, "p0")

        log.record_period(state, period)

        dfs = log.to_dataframes()
        inv = dfs["simulation_inventory_log"]
        assert len(inv) == 3  # 3 facilities
        assert "period_index" in inv.columns
        assert (inv["period_index"] == 0).all()

        res = dfs["simulation_resource_log"]
        assert len(res) == 3  # 3 trucks
        assert (res["period_id"] == "p0").all()

    def test_multi_period(self, resolved_model: ResolvedModelData) -> None:
        state = init_state(resolved_model)
        log = SimulationLog()

        for i in range(3):
            p = _make_period(i, f"p{i}")
            log.record_period(state, p)

        dfs = log.to_dataframes()
        inv = dfs["simulation_inventory_log"]
        assert len(inv) == 9  # 3 facilities x 3 periods


class TestRecordEvents:
    """record_events appends non-empty event DataFrames."""

    def test_empty_result_no_events(self, resolved_model: ResolvedModelData) -> None:
        state = init_state(resolved_model)
        log = SimulationLog()
        result = PhaseResult.empty(state)
        period = _make_period()

        log.record_events(result, "DEMAND", period)

        dfs = log.to_dataframes()
        assert len(dfs["simulation_flow_log"]) == 0
        assert len(dfs["simulation_unmet_demand_log"]) == 0

    def test_flow_events_recorded(self, resolved_model: ResolvedModelData) -> None:
        state = init_state(resolved_model)
        log = SimulationLog()
        flow = pd.DataFrame(
            {
                "source_id": ["EXT"],
                "target_id": ["s1"],
                "commodity_category": ["working_bike"],
                "modal_type": [None],
                "quantity": [5.0],
                "resource_id": [None],
            }
        )
        result = PhaseResult(state=state, flow_events=flow)
        period = _make_period()

        log.record_events(result, "DEMAND", period)

        dfs = log.to_dataframes()
        fl = dfs["simulation_flow_log"]
        assert len(fl) == 1
        assert fl.iloc[0]["phase_name"] == "DEMAND"
        assert fl.iloc[0]["period_index"] == 0


class TestRejectReason:
    """RejectReason enum values."""

    def test_values(self) -> None:
        assert RejectReason.INSUFFICIENT_INVENTORY == "insufficient_inventory"
        assert RejectReason.NO_AVAILABLE_RESOURCE == "no_available_resource"
        assert RejectReason.OVER_CAPACITY == "over_capacity"
