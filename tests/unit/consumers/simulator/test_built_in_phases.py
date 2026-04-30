"""Tests for DemandPhase, ArrivalsPhase, and Organic*Phase."""
# ruff: noqa: D102

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from gbp.consumers.simulator.built_in_phases import (
    ArrivalsPhase,
    DemandPhase,
    DeparturePhysicsPhase,
    DockCapacityPhase,
    HistoricalLatentDemandPhase,
    HistoricalODStructurePhase,
    HistoricalTripSamplingPhase,
    OrganicArrivalPhase,
    OrganicDeparturePhase,
    OrganicFlowPhase,
)
from gbp.consumers.simulator.config import EnvironmentConfig
from gbp.consumers.simulator.engine import Environment
from gbp.consumers.simulator.phases import Phase, Schedule
from gbp.consumers.simulator.state import (
    IN_TRANSIT_COLUMNS,
    RESOURCE_COLUMNS,
    PeriodRow,
    SimulationState,
    init_state,
)
from gbp.core.model import ResolvedModelData


class _DummyResolved:
    """Resolved stub for tests that bypass real model construction."""


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


class TestDemandPhaseContract:
    """DemandPhase satisfies the Phase protocol and delegates scheduling."""

    def test_is_phase(self) -> None:
        assert isinstance(DemandPhase(), Phase)

    def test_name(self) -> None:
        assert DemandPhase().name == "DEMAND"

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = DemandPhase(schedule=Schedule.every_n(2))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))
        assert phase.should_run(_make_period(period_index=2))


class TestArrivalsPhaseContract:
    """ArrivalsPhase satisfies the Phase protocol and delegates scheduling."""

    def test_is_phase(self) -> None:
        assert isinstance(ArrivalsPhase(), Phase)

    def test_name(self) -> None:
        assert ArrivalsPhase().name == "ARRIVALS"


class TestDemandPhaseExecute:
    """DemandPhase.execute behaviour (enable after implementation)."""

    def test_demand_reduces_inventory(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        phase = DemandPhase()
        # resolved_model has demand at s1 for period p0: quantity=1.0
        result = phase.execute(state, resolved_model, _make_period(0, "p0"))

        new_inv = result.state.inventory
        s1_qty = new_inv.loc[
            new_inv["facility_id"] == "s1", "quantity"
        ].iloc[0]
        assert s1_qty == 12.0 - 1.0  # initial 12, demand 1

    def test_unmet_demand_logged(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        # Set s1 inventory to 0 so demand is unmet
        inv = state.inventory.copy()
        inv.loc[inv["facility_id"] == "s1", "quantity"] = 0.0
        state = state.with_inventory(inv)

        phase = DemandPhase()
        result = phase.execute(state, resolved_model, _make_period(0, "p0"))

        assert not result.unmet_demand.empty
        assert result.unmet_demand.iloc[0]["deficit"] > 0

    def test_no_demand_returns_empty(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        phase = DemandPhase()
        # period p99 has no demand
        result = phase.execute(state, resolved_model, _make_period(99, "p99"))
        assert result.state is state


class TestArrivalsPhaseExecute:
    """ArrivalsPhase.execute behaviour (enable after implementation)."""

    def test_arriving_shipment_transfers_to_inventory(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        # Add a shipment arriving at period 0
        transit = pd.DataFrame(
            {
                "shipment_id": ["shp_001"],
                "source_id": ["d1"],
                "target_id": ["s1"],
                "commodity_category": ["working_bike"],
                "quantity": [5.0],
                "resource_id": ["rebalancing_truck_d1_0"],
                "departure_period": [0],
                "arrival_period": [0],
            }
        )
        state = state.with_in_transit(transit)

        phase = ArrivalsPhase()
        result = phase.execute(state, resolved_model, _make_period(0, "p0"))

        # s1 inventory should increase by 5
        new_inv = result.state.inventory
        s1_qty = new_inv.loc[
            new_inv["facility_id"] == "s1", "quantity"
        ].iloc[0]
        assert s1_qty == 12.0 + 5.0

        # in_transit should be empty
        assert result.state.in_transit.empty

    def test_non_arriving_shipment_stays(
        self, resolved_model: ResolvedModelData
    ) -> None:
        state = init_state(resolved_model)
        transit = pd.DataFrame(
            {
                "shipment_id": ["shp_001"],
                "source_id": ["d1"],
                "target_id": ["s1"],
                "commodity_category": ["working_bike"],
                "quantity": [5.0],
                "resource_id": ["rebalancing_truck_d1_0"],
                "departure_period": [0],
                "arrival_period": [2],
            }
        )
        state = state.with_in_transit(transit)

        phase = ArrivalsPhase()
        result = phase.execute(state, resolved_model, _make_period(0, "p0"))

        assert len(result.state.in_transit) == 1


# ---------------------------------------------------------------------------
# OrganicDeparturePhase / OrganicArrivalPhase
# ---------------------------------------------------------------------------


class TestOrganicDeparturePhaseContract:
    """OrganicDeparturePhase satisfies Phase protocol."""

    def test_is_phase(self) -> None:
        assert isinstance(OrganicDeparturePhase(), Phase)

    def test_name(self) -> None:
        assert OrganicDeparturePhase().name == "ORGANIC_DEPARTURE"

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = OrganicDeparturePhase(schedule=Schedule.every_n(2))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))


class TestOrganicArrivalPhaseContract:
    """OrganicArrivalPhase satisfies Phase protocol."""

    def test_is_phase(self) -> None:
        assert isinstance(OrganicArrivalPhase(), Phase)

    def test_name(self) -> None:
        assert OrganicArrivalPhase().name == "ORGANIC_ARRIVAL"


class TestOrganicDeparturePhaseExecute:
    """OrganicDeparturePhase subtracts outflow from sources."""

    def test_departure_reduces_source_inventory(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model_with_obs)
        phase = OrganicDeparturePhase()
        # p0 observed_flow: s1→s2 qty=2, s2→s1 qty=1
        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )
        inv = result.state.inventory
        s1_qty = inv.loc[inv["facility_id"] == "s1", "quantity"].iloc[0]
        s2_qty = inv.loc[inv["facility_id"] == "s2", "quantity"].iloc[0]
        # s1: 8 - 2 = 6, s2: 12 - 1 = 11
        assert s1_qty == 6.0
        assert s2_qty == 11.0

    def test_departure_emits_flow_events(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model_with_obs)
        phase = OrganicDeparturePhase()
        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )
        assert not result.flow_events.empty
        assert "source_id" in result.flow_events.columns
        assert "target_id" in result.flow_events.columns

    def test_no_observed_flow_returns_empty(
        self, resolved_model: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model)
        phase = OrganicDeparturePhase()
        result = phase.execute(state, resolved_model, _make_period(0, "p0"))
        assert result.state is state


class TestOrganicArrivalPhaseExecute:
    """OrganicArrivalPhase adds inflow to targets."""

    def test_arrival_increases_target_inventory(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model_with_obs)
        phase = OrganicArrivalPhase()
        # p0 observed_flow: s1→s2 qty=2, s2→s1 qty=1
        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )
        inv = result.state.inventory
        s1_qty = inv.loc[inv["facility_id"] == "s1", "quantity"].iloc[0]
        s2_qty = inv.loc[inv["facility_id"] == "s2", "quantity"].iloc[0]
        # s1: 8 + 1 = 9, s2: 12 + 2 = 14
        assert s1_qty == 9.0
        assert s2_qty == 14.0

    def test_arrival_emits_no_flow_events(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        state = init_state(resolved_model_with_obs)
        phase = OrganicArrivalPhase()
        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )
        assert result.flow_events.empty


class TestOrganicPhaseParity:
    """Departure+Arrival produces identical results to OrganicFlowPhase."""

    def test_split_equals_combined_single_period(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        period = _make_period(0, "p0")
        state = init_state(resolved_model_with_obs)

        # Combined
        combined = OrganicFlowPhase()
        combined_result = combined.execute(
            state, resolved_model_with_obs, period,
        )

        # Split
        dep = OrganicDeparturePhase()
        arr = OrganicArrivalPhase()
        dep_result = dep.execute(state, resolved_model_with_obs, period)
        arr_result = arr.execute(
            dep_result.state, resolved_model_with_obs, period,
        )

        # Inventory must match exactly
        assert_frame_equal(
            combined_result.state.inventory.reset_index(drop=True),
            arr_result.state.inventory.reset_index(drop=True),
        )

        # Flow events must match exactly
        assert_frame_equal(
            combined_result.flow_events.reset_index(drop=True),
            dep_result.flow_events.reset_index(drop=True),
        )

    def test_full_run_parity(
        self, resolved_model_with_obs: ResolvedModelData,
    ) -> None:
        """Full simulation: split phases produce same logs as combined."""
        combined_log = Environment(
            resolved_model_with_obs,
            EnvironmentConfig(
                phases=[OrganicFlowPhase()], scenario_id="combined",
            ),
        ).run()

        split_log = Environment(
            resolved_model_with_obs,
            EnvironmentConfig(
                phases=[OrganicDeparturePhase(), OrganicArrivalPhase()],
                scenario_id="split",
            ),
        ).run()

        combined_tables = combined_log.to_dataframes()
        split_tables = split_log.to_dataframes()

        # Inventory logs must match
        assert_frame_equal(
            combined_tables["simulation_inventory_log"].reset_index(drop=True),
            split_tables["simulation_inventory_log"].reset_index(drop=True),
        )

        # Flow quantities must match (phase_name differs, so compare aggregates)
        flow_keys = ["period_id", "source_id", "target_id", "commodity_category"]
        combined_flow = (
            combined_tables["simulation_flow_log"]
            .groupby(flow_keys, as_index=False)["quantity"].sum()
            .sort_values(flow_keys)
            .reset_index(drop=True)
        )
        split_flow = (
            split_tables["simulation_flow_log"]
            .groupby(flow_keys, as_index=False)["quantity"].sum()
            .sort_values(flow_keys)
            .reset_index(drop=True)
        )
        assert_frame_equal(combined_flow, split_flow)


class TestHistoricalLatentDemandPhaseContract:
    """Phase satisfies the Phase protocol and delegates scheduling."""

    def test_is_phase(self) -> None:
        assert isinstance(HistoricalLatentDemandPhase(), Phase)

    def test_name(self) -> None:
        assert HistoricalLatentDemandPhase().name == "HISTORICAL_LATENT_DEMAND"

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = HistoricalLatentDemandPhase(schedule=Schedule.every_n(2))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))
        assert phase.should_run(_make_period(period_index=2))


class TestHistoricalLatentDemandPhaseExecute:
    """Phase publishes O_i and D_j marginals from observed_flow."""

    def test_marginals_match_observed_flow(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = HistoricalLatentDemandPhase()
        state = init_state(resolved_model_with_obs)
        period = _make_period(0, "p0")

        result = phase.execute(state, resolved_model_with_obs, period)
        latent = result.state.intermediates["latent_demand"]

        # Independent recomputation of expected marginals.
        flows_p0 = resolved_model_with_obs.observed_flow[
            resolved_model_with_obs.observed_flow["period_id"] == period.period_id
        ]
        expected_dep = (
            flows_p0.groupby(["source_id", "commodity_category"])["quantity"].sum()
        )
        expected_arr = (
            flows_p0.groupby(["target_id", "commodity_category"])["quantity"].sum()
        )

        for (fac, com), exp in expected_dep.items():
            row = latent[
                (latent["facility_id"] == fac)
                & (latent["commodity_category"] == com)
            ]
            assert not row.empty, f"missing departure row for {fac=} {com=}"
            assert row.iloc[0]["latent_departures"] == exp

        for (fac, com), exp in expected_arr.items():
            row = latent[
                (latent["facility_id"] == fac)
                & (latent["commodity_category"] == com)
            ]
            assert not row.empty, f"missing arrival row for {fac=} {com=}"
            assert row.iloc[0]["latent_arrivals"] == exp

    def test_facility_with_only_inflow_has_zero_outflow(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        """Outer merge fills missing departures with 0, not NaN."""
        phase = HistoricalLatentDemandPhase()
        state = init_state(resolved_model_with_obs)
        period = _make_period(0, "p0")

        result = phase.execute(state, resolved_model_with_obs, period)
        latent = result.state.intermediates["latent_demand"]

        assert latent["latent_departures"].notna().all()
        assert latent["latent_arrivals"].notna().all()

    def test_inventory_unchanged(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = HistoricalLatentDemandPhase()
        state = init_state(resolved_model_with_obs)
        period = _make_period(0, "p0")

        result = phase.execute(state, resolved_model_with_obs, period)

        # Phase publishes intermediates but never touches inventory or transit.
        assert_frame_equal(result.state.inventory, state.inventory)
        assert_frame_equal(result.state.in_transit, state.in_transit)

    def test_latent_demand_event_populated(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = HistoricalLatentDemandPhase()
        state = init_state(resolved_model_with_obs)
        period = _make_period(0, "p0")

        result = phase.execute(state, resolved_model_with_obs, period)

        assert not result.latent_demand.empty
        assert set(result.latent_demand.columns) >= {
            "facility_id",
            "commodity_category",
            "latent_departures",
            "latent_arrivals",
        }

    def test_no_observed_flow_returns_empty(
        self, resolved_model: ResolvedModelData
    ) -> None:
        """When observed_flow is absent, phase is a no-op."""
        phase = HistoricalLatentDemandPhase()
        state = init_state(resolved_model)
        period = _make_period(0, "p0")

        result = phase.execute(state, resolved_model, period)

        assert result.state.intermediates == {}
        assert result.latent_demand.empty
        assert_frame_equal(result.state.inventory, state.inventory)

    def test_output_is_stably_sorted(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        """Determinism precondition: output ordering is independent of input row order."""
        phase = HistoricalLatentDemandPhase()
        state = init_state(resolved_model_with_obs)
        period = _make_period(0, "p0")

        result = phase.execute(state, resolved_model_with_obs, period)
        latent = result.state.intermediates["latent_demand"]

        keys = list(zip(
            latent["facility_id"].tolist(),
            latent["commodity_category"].tolist(),
            strict=True,
        ))
        assert keys == sorted(keys)


class TestHistoricalODStructurePhaseContract:
    """Phase satisfies the Phase protocol and delegates scheduling."""

    def test_is_phase(self) -> None:
        assert isinstance(HistoricalODStructurePhase(), Phase)

    def test_name(self) -> None:
        assert HistoricalODStructurePhase().name == "HISTORICAL_OD_STRUCTURE"

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = HistoricalODStructurePhase(schedule=Schedule.every_n(2))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))


class TestHistoricalODStructurePhaseExecute:
    """Phase publishes P(j | i) probabilities derived from observed_flow."""

    def test_probabilities_sum_to_one_per_origin(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = HistoricalODStructurePhase()
        state = init_state(resolved_model_with_obs)
        period = _make_period(0, "p0")

        result = phase.execute(state, resolved_model_with_obs, period)
        od = result.state.intermediates["od_probabilities"]

        sums = od.groupby(["source_id", "commodity_category"])["probability"].sum()
        assert (sums - 1.0).abs().max() < 1e-9

    def test_probabilities_match_independent_recompute(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = HistoricalODStructurePhase()
        state = init_state(resolved_model_with_obs)
        period = _make_period(0, "p0")

        result = phase.execute(state, resolved_model_with_obs, period)
        od = result.state.intermediates["od_probabilities"]

        flows_p0 = resolved_model_with_obs.observed_flow[
            resolved_model_with_obs.observed_flow["period_id"] == period.period_id
        ]
        joint = flows_p0.groupby(
            ["source_id", "target_id", "commodity_category"]
        )["quantity"].sum()
        origin = joint.groupby(level=["source_id", "commodity_category"]).sum()

        for (src, tgt, com), tij in joint.items():
            expected = tij / origin.loc[(src, com)]
            row = od[
                (od["source_id"] == src)
                & (od["target_id"] == tgt)
                & (od["commodity_category"] == com)
            ]
            assert not row.empty
            assert abs(row.iloc[0]["probability"] - expected) < 1e-12

    def test_inventory_unchanged(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = HistoricalODStructurePhase()
        state = init_state(resolved_model_with_obs)

        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        assert_frame_equal(result.state.inventory, state.inventory)
        assert_frame_equal(result.state.in_transit, state.in_transit)

    def test_no_observed_flow_returns_empty(
        self, resolved_model: ResolvedModelData
    ) -> None:
        phase = HistoricalODStructurePhase()
        state = init_state(resolved_model)

        result = phase.execute(state, resolved_model, _make_period(0, "p0"))

        assert result.state.intermediates == {}

    def test_output_is_stably_sorted(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = HistoricalODStructurePhase()
        state = init_state(resolved_model_with_obs)

        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )
        od = result.state.intermediates["od_probabilities"]

        keys = list(zip(
            od["source_id"].tolist(),
            od["commodity_category"].tolist(),
            od["target_id"].tolist(),
            strict=True,
        ))
        assert keys == sorted(keys)

    def test_no_log_table_emitted(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        """OD matrix is too large to log; phase should not populate any log."""
        phase = HistoricalODStructurePhase()
        state = init_state(resolved_model_with_obs)

        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        assert result.flow_events.empty
        assert result.unmet_demand.empty
        assert result.rejected_dispatches.empty
        assert result.latent_demand.empty
        assert result.lost_demand.empty
        assert result.dock_blocking.empty


class TestDeparturePhysicsPhaseContract:
    """Contract: protocol, name, schedule, mode validation."""

    def test_is_phase(self) -> None:
        assert isinstance(DeparturePhysicsPhase(), Phase)

    def test_name(self) -> None:
        assert DeparturePhysicsPhase().name == "DEPARTURE_PHYSICS"

    def test_default_mode_is_permissive(self) -> None:
        phase = DeparturePhysicsPhase()
        assert phase._mode == "permissive"  # noqa: SLF001

    def test_invalid_mode_raises(self) -> None:
        with pytest.raises(ValueError, match="mode must be"):
            DeparturePhysicsPhase(mode="bogus")  # type: ignore[arg-type]

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = DeparturePhysicsPhase(schedule=Schedule.every_n(2))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))


def _state_with_latent(
    inventory_qty: float, latent_qty: float
) -> SimulationState:
    """Build a synthetic state with one facility, one commodity, and latent
    demand pre-populated in intermediates.

    Used by strict-mode tests that don't need a full ResolvedModelData.
    """
    inventory = pd.DataFrame({
        "facility_id": ["s1"],
        "commodity_category": ["working_bike"],
        "quantity": [inventory_qty],
    })
    latent = pd.DataFrame({
        "facility_id": ["s1"],
        "commodity_category": ["working_bike"],
        "latent_departures": [latent_qty],
        "latent_arrivals": [0.0],
    })
    return SimulationState(
        period_index=0,
        period_id="p0",
        inventory=inventory,
        in_transit=pd.DataFrame(columns=IN_TRANSIT_COLUMNS),
        resources=pd.DataFrame(columns=RESOURCE_COLUMNS),
    ).with_intermediates(latent_demand=latent)


class TestDeparturePhysicsPhaseExecute:
    """Permissive parity with OrganicDeparturePhase + strict clipping."""

    def test_no_intermediates_returns_empty(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = DeparturePhysicsPhase()
        state = init_state(resolved_model_with_obs)

        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        assert_frame_equal(result.state.inventory, state.inventory)
        assert result.lost_demand.empty

    def test_permissive_matches_organic_departure(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        """LatentDemand + DeparturePhysics(permissive) ≡ OrganicDeparturePhase."""
        period = _make_period(0, "p0")

        # Reference path: OrganicDeparturePhase.
        ref = OrganicDeparturePhase().execute(
            init_state(resolved_model_with_obs),
            resolved_model_with_obs,
            period,
        )

        # New path: HistoricalLatentDemand -> DeparturePhysics(permissive).
        state0 = init_state(resolved_model_with_obs)
        latent_result = HistoricalLatentDemandPhase().execute(
            state0, resolved_model_with_obs, period,
        )
        new_path = DeparturePhysicsPhase(mode="permissive").execute(
            latent_result.state, resolved_model_with_obs, period,
        )

        assert_frame_equal(
            new_path.state.inventory.sort_values(
                ["facility_id", "commodity_category"]
            ).reset_index(drop=True),
            ref.state.inventory.sort_values(
                ["facility_id", "commodity_category"]
            ).reset_index(drop=True),
        )

    def test_permissive_emits_no_lost_demand(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        period = _make_period(0, "p0")
        state0 = init_state(resolved_model_with_obs)
        latent_result = HistoricalLatentDemandPhase().execute(
            state0, resolved_model_with_obs, period,
        )

        result = DeparturePhysicsPhase(mode="permissive").execute(
            latent_result.state, resolved_model_with_obs, period,
        )

        assert result.lost_demand.empty

    def test_strict_clips_at_inventory_and_logs_loss(self) -> None:
        """inventory=3, latent=10 → realized=3, lost=7."""
        state = _state_with_latent(inventory_qty=3.0, latent_qty=10.0)

        result = DeparturePhysicsPhase(mode="strict").execute(
            state, _DummyResolved(), _make_period(0, "p0"),
        )

        assert result.state.inventory.iloc[0]["quantity"] == 0.0
        assert len(result.lost_demand) == 1
        row = result.lost_demand.iloc[0]
        assert row["latent"] == 10.0
        assert row["realized"] == 3.0
        assert row["lost"] == 7.0

    def test_strict_with_sufficient_inventory_emits_no_loss(self) -> None:
        state = _state_with_latent(inventory_qty=10.0, latent_qty=3.0)

        result = DeparturePhysicsPhase(mode="strict").execute(
            state, _DummyResolved(), _make_period(0, "p0"),
        )

        assert result.state.inventory.iloc[0]["quantity"] == 7.0
        assert result.lost_demand.empty

    def test_strict_treats_negative_inventory_as_zero(self) -> None:
        """Defensive: pre-existing negative quantity ⇒ realized=0, lost=latent."""
        state = _state_with_latent(inventory_qty=-2.0, latent_qty=5.0)

        result = DeparturePhysicsPhase(mode="strict").execute(
            state, _DummyResolved(), _make_period(0, "p0"),
        )

        # Inventory unchanged when realized=0 (quantity - 0 = -2).
        assert result.state.inventory.iloc[0]["quantity"] == -2.0
        row = result.lost_demand.iloc[0]
        assert row["realized"] == 0.0
        assert row["lost"] == 5.0

    def test_realized_departures_published(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        period = _make_period(0, "p0")
        state0 = init_state(resolved_model_with_obs)
        latent_result = HistoricalLatentDemandPhase().execute(
            state0, resolved_model_with_obs, period,
        )

        result = DeparturePhysicsPhase().execute(
            latent_result.state, resolved_model_with_obs, period,
        )

        assert "realized_departures" in result.state.intermediates
        realized = result.state.intermediates["realized_departures"]
        assert (realized["realized_departures"] > 0).all()


class TestHistoricalTripSamplingPhaseContract:
    """Contract: protocol, name, schedule."""

    def test_is_phase(self) -> None:
        assert isinstance(HistoricalTripSamplingPhase(), Phase)

    def test_name(self) -> None:
        assert (
            HistoricalTripSamplingPhase().name == "HISTORICAL_TRIP_SAMPLING"
        )

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = HistoricalTripSamplingPhase(schedule=Schedule.every_n(2))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))


class TestHistoricalTripSamplingPhaseExecute:
    """Phase replays observed_flow rows into state.in_transit."""

    def test_no_observed_flow_returns_empty(
        self, resolved_model: ResolvedModelData
    ) -> None:
        phase = HistoricalTripSamplingPhase()
        state = init_state(resolved_model)

        result = phase.execute(state, resolved_model, _make_period(0, "p0"))

        assert_frame_equal(result.state.in_transit, state.in_transit)

    def test_one_trip_per_observed_flow_row(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = HistoricalTripSamplingPhase()
        state = init_state(resolved_model_with_obs)
        period = _make_period(0, "p0")

        result = phase.execute(state, resolved_model_with_obs, period)

        flows_p0 = resolved_model_with_obs.observed_flow[
            resolved_model_with_obs.observed_flow["period_id"] == period.period_id
        ]
        added = len(result.state.in_transit) - len(state.in_transit)
        assert added == len(flows_p0)

    def test_inventory_unchanged(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = HistoricalTripSamplingPhase()
        state = init_state(resolved_model_with_obs)

        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        assert_frame_equal(result.state.inventory, state.inventory)

    def test_trips_arrive_in_same_period(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        """arrival_period == departure_period for parity with OrganicArrivalPhase."""
        phase = HistoricalTripSamplingPhase()
        state = init_state(resolved_model_with_obs)
        period = _make_period(0, "p0")

        result = phase.execute(state, resolved_model_with_obs, period)

        new_trips = result.state.in_transit[
            result.state.in_transit["shipment_id"].str.startswith("organic_trip_")
        ]
        assert (new_trips["departure_period"] == period.period_index).all()
        assert (new_trips["arrival_period"] == period.period_index).all()

    def test_shipment_ids_are_unique(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = HistoricalTripSamplingPhase()
        state = init_state(resolved_model_with_obs)
        period = _make_period(0, "p0")

        result = phase.execute(state, resolved_model_with_obs, period)

        assert (
            result.state.in_transit["shipment_id"].nunique()
            == len(result.state.in_transit)
        )

    def test_organic_trips_carry_no_resource(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        phase = HistoricalTripSamplingPhase()
        state = init_state(resolved_model_with_obs)

        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        new_trips = result.state.in_transit[
            result.state.in_transit["shipment_id"].str.startswith("organic_trip_")
        ]
        assert new_trips["resource_id"].isna().all()

    def test_existing_in_transit_preserved(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        """Phase appends new trips without dropping prior in_transit."""
        phase = HistoricalTripSamplingPhase()
        state = init_state(resolved_model_with_obs)
        prior = pd.DataFrame({
            "shipment_id": ["preexisting_001"],
            "source_id": ["d1"],
            "target_id": ["s1"],
            "commodity_category": ["working_bike"],
            "quantity": [3.0],
            "resource_id": ["truck_0"],
            "departure_period": [-1],
            "arrival_period": [5],
        })
        state = state.with_in_transit(
            pd.concat([state.in_transit, prior], ignore_index=True)
        )

        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        assert (
            result.state.in_transit["shipment_id"] == "preexisting_001"
        ).any()

    def test_no_log_emitted(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        """flow_events come from ArrivalsPhase on delivery, not from this phase."""
        phase = HistoricalTripSamplingPhase()
        state = init_state(resolved_model_with_obs)

        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        assert result.flow_events.empty
        assert result.unmet_demand.empty
        assert result.rejected_dispatches.empty
        assert result.latent_demand.empty
        assert result.lost_demand.empty
        assert result.dock_blocking.empty


class TestDockCapacityPhaseContract:
    """Contract."""

    def test_is_phase(self) -> None:
        assert isinstance(DockCapacityPhase(), Phase)

    def test_name(self) -> None:
        assert DockCapacityPhase().name == "DOCK_CAPACITY"

    def test_should_run_delegates_to_schedule(self) -> None:
        phase = DockCapacityPhase(schedule=Schedule.every_n(2))
        assert phase.should_run(_make_period(period_index=0))
        assert not phase.should_run(_make_period(period_index=1))


def _inject_storage_capacity(
    resolved: ResolvedModelData,
    capacities: dict[str, float],
    commodity: str = "working_bike",
) -> None:
    """Register an ``operation_capacity[storage]`` row per facility on *resolved*.

    Uses ``register_raw`` so existing attributes with the same name are
    overwritten without validation.  Mutates *resolved.attributes* in place.
    """
    from gbp.core.attributes.spec import AttributeSpec
    from gbp.core.enums import AttributeKind

    cap_data = pd.DataFrame({
        "facility_id": list(capacities.keys()),
        "operation_type": ["storage"] * len(capacities),
        "commodity_category": [commodity] * len(capacities),
        "capacity": list(capacities.values()),
    })
    spec = AttributeSpec(
        name="operation_capacity",
        kind=AttributeKind.CAPACITY,
        entity_type="facility",
        grain=("facility_id", "operation_type", "commodity_category"),
        resolved_grain=("facility_id", "operation_type", "commodity_category"),
        value_column="capacity",
        source_table="operation_capacity",
        unit=None,
        aggregation="min",
        nullable=True,
        eav_filter=None,
    )
    resolved.attributes.register_raw(spec, cap_data)


class TestDockCapacityPhaseExecute:
    """Phase clips inventory to operation_capacity[storage] and logs overflow."""

    def test_no_attribute_returns_empty(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        """Without operation_capacity registered, phase is a no-op."""
        phase = DockCapacityPhase()
        state = init_state(resolved_model_with_obs)

        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        assert_frame_equal(result.state.inventory, state.inventory)
        assert result.dock_blocking.empty

    def test_inventory_below_capacity_no_clip(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        _inject_storage_capacity(
            resolved_model_with_obs, {"s1": 100.0, "s2": 100.0},
        )
        phase = DockCapacityPhase()
        state = init_state(resolved_model_with_obs)
        # Initial s1=8, s2=12 are far below 100.

        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        assert_frame_equal(result.state.inventory, state.inventory)
        assert result.dock_blocking.empty

    def test_inventory_above_capacity_clipped_and_logged(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        _inject_storage_capacity(resolved_model_with_obs, {"s1": 20.0})
        phase = DockCapacityPhase()
        state0 = init_state(resolved_model_with_obs)
        bloated = state0.inventory.copy()
        bloated.loc[bloated["facility_id"] == "s1", "quantity"] = 100.0

        state = state0.with_inventory(bloated)
        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        s1_after = result.state.inventory.loc[
            result.state.inventory["facility_id"] == "s1", "quantity"
        ].iloc[0]
        assert s1_after == 20.0

        s1_log = result.dock_blocking[result.dock_blocking["facility_id"] == "s1"]
        assert len(s1_log) == 1
        assert s1_log.iloc[0]["incoming"] == 100.0
        assert s1_log.iloc[0]["accepted"] == 20.0
        assert s1_log.iloc[0]["blocked"] == 80.0

    def test_facility_without_capacity_row_is_unbounded(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        """Capacity registered only for s1; d1 has no row and is unbounded."""
        _inject_storage_capacity(resolved_model_with_obs, {"s1": 20.0})
        phase = DockCapacityPhase()
        state0 = init_state(resolved_model_with_obs)
        bloated = state0.inventory.copy()
        bloated.loc[bloated["facility_id"] == "d1", "quantity"] = 999_999.0

        state = state0.with_inventory(bloated)
        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        d1_after = result.state.inventory.loc[
            result.state.inventory["facility_id"] == "d1", "quantity"
        ].iloc[0]
        assert d1_after == 999_999.0
        assert not (result.dock_blocking["facility_id"] == "d1").any()

    def test_no_storage_rows_is_noop(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        """Capacity table exists but has only non-storage operation types."""
        from gbp.core.attributes.spec import AttributeSpec
        from gbp.core.enums import AttributeKind

        cap_data = pd.DataFrame({
            "facility_id": ["s1"],
            "operation_type": ["receiving"],  # not "storage"
            "commodity_category": ["working_bike"],
            "capacity": [10.0],
        })
        spec = AttributeSpec(
            name="operation_capacity",
            kind=AttributeKind.CAPACITY,
            entity_type="facility",
            grain=("facility_id", "operation_type", "commodity_category"),
            resolved_grain=("facility_id", "operation_type", "commodity_category"),
            value_column="capacity",
            source_table="operation_capacity",
            unit=None,
            aggregation="min",
            nullable=True,
            eav_filter=None,
        )
        resolved_model_with_obs.attributes.register_raw(spec, cap_data)
        phase = DockCapacityPhase()
        state0 = init_state(resolved_model_with_obs)
        bloated = state0.inventory.copy()
        bloated.loc[bloated["facility_id"] == "s1", "quantity"] = 1000.0
        state = state0.with_inventory(bloated)

        result = phase.execute(
            state, resolved_model_with_obs, _make_period(0, "p0"),
        )

        assert_frame_equal(result.state.inventory, state.inventory)
        assert result.dock_blocking.empty

    def test_attributes_none_returns_empty(
        self, resolved_model_with_obs: ResolvedModelData
    ) -> None:
        """Defensive: if resolved.attributes is None entirely, no-op."""
        phase = DockCapacityPhase()
        state = init_state(resolved_model_with_obs)

        class _ResolvedNoAttrs:
            attributes = None

        result = phase.execute(
            state, _ResolvedNoAttrs(), _make_period(0, "p0"),  # type: ignore[arg-type]
        )

        assert_frame_equal(result.state.inventory, state.inventory)
        assert result.dock_blocking.empty
