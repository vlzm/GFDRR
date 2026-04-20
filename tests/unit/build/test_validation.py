"""Tests for ``validate_raw_model``."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from gbp.build.validation import validate_raw_model
from gbp.core.enums import FacilityRole
from tests.unit.build.fixtures import minimal_raw_model


def test_validation_passes_on_minimal_fixture() -> None:
    """Bike-sharing minimal raw model has no blocking errors."""
    raw = minimal_raw_model()
    result = validate_raw_model(raw)
    assert result.is_valid


def test_demand_without_sink_role() -> None:
    """Demand at facility without SINK role fails referential check."""
    raw = minimal_raw_model()
    raw.facility_roles = pd.DataFrame(
        {
            "facility_id": ["d1", "d1", "s1", "s1", "s1", "s2", "s2"],
            "role": [
                FacilityRole.STORAGE.value,
                FacilityRole.TRANSSHIPMENT.value,
                FacilityRole.STORAGE.value,
                FacilityRole.SOURCE.value,
                FacilityRole.STORAGE.value,
                FacilityRole.SINK.value,
                FacilityRole.STORAGE.value,
            ],
        }
    )
    result = validate_raw_model(raw)
    assert not result.is_valid
    assert any("not SINK" in e.message for e in result.errors)


def test_unknown_facility_in_demand() -> None:
    """Unknown facility_id in demand is an error."""
    raw = minimal_raw_model()
    raw.demand = pd.DataFrame(
        {
            "facility_id": ["ghost"],
            "commodity_category": ["working_bike"],
            "date": [date(2025, 1, 1)],
            "quantity": [1.0],
        }
    )
    result = validate_raw_model(raw)
    assert not result.is_valid


def test_observations_pass_validation() -> None:
    """Observations with valid FKs produce no blocking errors."""
    raw = minimal_raw_model(with_observations=True)
    result = validate_raw_model(raw)
    assert result.is_valid


def test_observed_flow_unknown_facility() -> None:
    """Unknown facility in observed_flow produces warning (not blocking error)."""
    raw = minimal_raw_model()
    raw.observed_flow = pd.DataFrame(
        {
            "source_id": ["ghost"],
            "target_id": ["s1"],
            "commodity_category": ["working_bike"],
            "date": [date(2025, 1, 1)],
            "quantity": [1.0],
        }
    )
    result = validate_raw_model(raw)
    assert result.is_valid  # warnings don't block
    assert result.has_warnings
    assert any(
        e.entity == "observed_flow" and "ghost" in e.message for e in result.errors
    )


def test_raise_if_invalid() -> None:
    """raise_if_invalid propagates on errors."""
    raw = minimal_raw_model()
    raw.demand = pd.DataFrame(
        {
            "facility_id": ["ghost"],
            "commodity_category": ["working_bike"],
            "date": [date(2025, 1, 1)],
            "quantity": [1.0],
        }
    )
    result = validate_raw_model(raw)
    with pytest.raises(ValueError, match="validation failed"):
        result.raise_if_invalid()
