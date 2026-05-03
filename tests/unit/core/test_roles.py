"""Tests for default roles and ``derive_roles``."""

from __future__ import annotations

from gbp.core.enums import FacilityRole, FacilityType, OperationType
from gbp.core.roles import DEFAULT_ROLES, derive_roles


def test_default_roles_station_depot_hub() -> None:
    """Bike-sharing DEFAULT_ROLES match the data model doc."""
    assert DEFAULT_ROLES[FacilityType.STATION.value] == {
        FacilityRole.SOURCE,
        FacilityRole.SINK,
        FacilityRole.STORAGE,
    }
    assert DEFAULT_ROLES[FacilityType.DEPOT.value] == {
        FacilityRole.STORAGE,
        FacilityRole.TRANSSHIPMENT,
    }
    assert DEFAULT_ROLES[FacilityType.MAINTENANCE_HUB.value] == {
        FacilityRole.TRANSSHIPMENT,
        FacilityRole.STORAGE,
    }


def test_derive_roles_station_default() -> None:
    """Station with full ops keeps SOURCE, SINK, STORAGE."""
    ops = {
        OperationType.RECEIVING.value,
        OperationType.STORAGE.value,
        OperationType.DISPATCH.value,
    }
    roles = derive_roles(FacilityType.STATION.value, ops)
    assert FacilityRole.SOURCE in roles
    assert FacilityRole.SINK in roles
    assert FacilityRole.STORAGE in roles


def test_derive_roles_discard_storage_when_operation_missing() -> None:
    """STORAGE role removed when storage operation is disabled."""
    ops = {OperationType.RECEIVING.value, OperationType.DISPATCH.value}
    roles = derive_roles(FacilityType.STATION.value, ops)
    assert FacilityRole.STORAGE not in roles


def test_derive_roles_add_transshipment_when_receive_and_dispatch() -> None:
    """RECEIVING+DISPATCH adds TRANSSHIPMENT for unknown facility types."""
    ops = {OperationType.RECEIVING.value, OperationType.DISPATCH.value}
    roles = derive_roles("custom_type", ops)
    assert FacilityRole.TRANSSHIPMENT in roles


def test_derive_roles_override_wins() -> None:
    """Explicit role_overrides bypass derivation."""
    override = {FacilityRole.SINK}
    roles = derive_roles(
        FacilityType.STATION.value,
        {OperationType.STORAGE.value},
        role_overrides=override,
    )
    assert roles == override


def test_derive_roles_consumption_adds_sink() -> None:
    """CONSUMPTION on an unknown type adds SINK without adding SOURCE."""
    roles = derive_roles("customer", {OperationType.CONSUMPTION.value})
    assert FacilityRole.SINK in roles
    assert FacilityRole.SOURCE not in roles


def test_derive_roles_production_adds_source() -> None:
    """PRODUCTION on an unknown type adds SOURCE without adding SINK."""
    roles = derive_roles("producer", {OperationType.PRODUCTION.value})
    assert FacilityRole.SOURCE in roles
    assert FacilityRole.SINK not in roles


def test_derive_roles_consumption_and_production_unknown_type() -> None:
    """Both operations together add both SINK and SOURCE on an unknown type."""
    ops = {
        OperationType.CONSUMPTION.value,
        OperationType.PRODUCTION.value,
    }
    roles = derive_roles("hybrid", ops)
    assert FacilityRole.SINK in roles
    assert FacilityRole.SOURCE in roles


def test_derive_roles_overrides_still_win_over_consumption() -> None:
    """role_overrides bypass even when consumption/production are enabled."""
    override = {FacilityRole.STORAGE}
    roles = derive_roles(
        "customer",
        {
            OperationType.CONSUMPTION.value,
            OperationType.PRODUCTION.value,
        },
        role_overrides=override,
    )
    assert roles == override
