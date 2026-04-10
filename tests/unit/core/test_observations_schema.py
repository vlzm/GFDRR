"""Tests for ObservedFlow and ObservedInventory schemas."""

from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from gbp.core.schemas import ObservedFlow, ObservedInventory


class TestObservedFlow:
    def test_valid(self) -> None:
        row = ObservedFlow(
            source_id="s1",
            target_id="s2",
            commodity_category="working_bike",
            date=date(2025, 1, 1),
            quantity=5.0,
        )
        assert row.quantity == 5.0

    def test_optional_fields(self) -> None:
        row = ObservedFlow(
            source_id="s1",
            target_id="s2",
            commodity_category="working_bike",
            date=date(2025, 1, 1),
            quantity=1.0,
            modal_type="road",
            resource_id="truck_1",
        )
        assert row.modal_type == "road"
        assert row.resource_id == "truck_1"

    def test_defaults_none(self) -> None:
        row = ObservedFlow(
            source_id="s1",
            target_id="s2",
            commodity_category="working_bike",
            date=date(2025, 1, 1),
            quantity=1.0,
        )
        assert row.modal_type is None
        assert row.resource_id is None

    def test_negative_quantity_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ObservedFlow(
                source_id="s1",
                target_id="s2",
                commodity_category="working_bike",
                date=date(2025, 1, 1),
                quantity=-1.0,
                )

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ObservedFlow(
                source_id="s1",
                target_id="s2",
                commodity_category="working_bike",
                date=date(2025, 1, 1),
                quantity=1.0,
                    extra_field="bad",  # type: ignore[call-arg]
            )


class TestObservedInventory:
    def test_valid(self) -> None:
        row = ObservedInventory(
            facility_id="s1",
            commodity_category="working_bike",
            date=date(2025, 1, 1),
            quantity=10.0,
        )
        assert row.facility_id == "s1"

    def test_negative_quantity_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ObservedInventory(
                facility_id="s1",
                commodity_category="working_bike",
                date=date(2025, 1, 1),
                quantity=-1.0,
                )

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ObservedInventory(
                facility_id="s1",
                commodity_category="working_bike",
                date=date(2025, 1, 1),
                quantity=1.0,
                    extra_field="bad",  # type: ignore[call-arg]
            )
