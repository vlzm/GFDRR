"""Integration tests for ``build_model``."""

from __future__ import annotations

import dataclasses

from gbp.build.pipeline import build_model
from gbp.build.validation import validate_raw_model
from tests.unit.build.fixtures import minimal_raw_model


def test_build_model_end_to_end() -> None:
    """Full pipeline produces resolved demand with period_id."""
    raw = minimal_raw_model()
    validate_raw_model(raw).raise_if_invalid()
    resolved = build_model(raw)
    assert resolved.demand is not None
    assert "period_id" in resolved.demand.columns
    assert "date" not in resolved.demand.columns
    assert resolved.edge_lead_time_resolved is not None
    assert not resolved.edge_lead_time_resolved.empty
    assert resolved.fleet_capacity is not None
    assert resolved.edges is not None


def test_build_model_without_prebuilt_edges() -> None:
    """Edges built from rules when raw.edges is None."""
    raw = minimal_raw_model()
    raw.edges = None
    raw.edge_commodities = None
    validate_raw_model(raw).raise_if_invalid()
    resolved = build_model(raw)
    assert resolved.edges is not None
    assert len(resolved.edges) == 2
    assert resolved.edge_commodities is not None


def test_fully_populated_raw_triggers_no_derivations() -> None:
    """When the user supplies every derivable table, BuildReport is empty."""
    raw = minimal_raw_model()
    resolved = build_model(raw)
    assert resolved.build_report is not None
    assert resolved.build_report.derivations == {}


def test_missing_periods_recorded_in_build_report() -> None:
    """Dropping ``periods`` triggers derivation from horizon segments."""
    raw = minimal_raw_model()
    raw = dataclasses.replace(raw, periods=None)
    resolved = build_model(raw)
    assert resolved.build_report is not None
    assert "periods" in resolved.build_report.derivations
    assert resolved.periods is not None and not resolved.periods.empty


def test_missing_facility_roles_derived_from_operations() -> None:
    """Dropping ``facility_roles`` triggers derivation from facility_operations."""
    raw = minimal_raw_model()
    raw = dataclasses.replace(raw, facility_roles=None)
    resolved = build_model(raw)
    assert resolved.build_report is not None
    assert "facility_roles" in resolved.build_report.derivations


def test_observed_flow_fills_demand_and_supply() -> None:
    """Observed flow back-fills both demand and supply when user skipped them."""
    raw = minimal_raw_model(with_demand=False, with_supply=False, with_observations=True)
    # Drop facility_roles so they are derived from operations (all stations get
    # source/sink/storage roles, consistent with the synthetic demand + supply).
    raw = dataclasses.replace(raw, demand=None, supply=None, facility_roles=None)
    resolved = build_model(raw)
    assert resolved.build_report is not None
    assert "demand" in resolved.build_report.derivations
    assert "supply" in resolved.build_report.derivations
    assert resolved.demand is not None and not resolved.demand.empty
    assert resolved.supply is not None and not resolved.supply.empty


def test_user_provided_demand_wins_over_observed_flow() -> None:
    """User-supplied ``demand`` must not be overwritten even when observed_flow exists."""
    import pandas as pd

    raw = minimal_raw_model(with_demand=True, with_observations=True)
    # Explicit empty supply = "user said no supply"; keeps derivation from firing.
    raw = dataclasses.replace(
        raw,
        supply=pd.DataFrame(
            columns=["facility_id", "commodity_category", "date", "quantity"],
        ),
    )
    resolved = build_model(raw)
    assert resolved.build_report is not None
    assert "demand" not in resolved.build_report.derivations
    assert "supply" not in resolved.build_report.derivations
