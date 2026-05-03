"""Tests for core enumerations."""

from __future__ import annotations

import pytest

from gbp.core.enums import (
    AttributeKind,
    FacilityRole,
    FacilityType,
    ModalType,
    OperationType,
    PeriodType,
)


@pytest.mark.parametrize(
    "enum_cls, expected",
    [
        (
            FacilityRole,
            {"source", "sink", "storage", "transshipment"},
        ),
        (
            ModalType,
            {"road", "rail", "sea", "pipeline", "air", "digital"},
        ),
        (PeriodType, {"day", "week", "month"}),
        (
            AttributeKind,
            {"cost", "revenue", "rate", "capacity", "additional"},
        ),
        (FacilityType, {"station", "depot", "maintenance_hub"}),
        (
            OperationType,
            {
                "receiving",
                "storage",
                "dispatch",
                "handling",
                "repair",
                "consumption",
                "production",
            },
        ),
    ],
)
def test_enum_members(enum_cls: type, expected: set[str]) -> None:
    """Enum members match expected string values and inherit str."""
    assert {e.value for e in enum_cls} == expected
    assert issubclass(enum_cls, str)


def test_str_enum_behaves_as_string() -> None:
    """Bike-sharing FacilityType compares equal to its string value."""
    assert FacilityType.STATION == "station"
    assert FacilityType.STATION.value == "station"
