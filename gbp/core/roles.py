"""Default facility roles and role derivation (bike-sharing domain)."""

from __future__ import annotations

from gbp.core.enums import FacilityRole, FacilityType, OperationType

# Bike-sharing defaults: facility_type string -> semantic roles in the flow network.
DEFAULT_ROLES: dict[str, set[FacilityRole]] = {
    FacilityType.STATION.value: {
        FacilityRole.SOURCE,
        FacilityRole.SINK,
        FacilityRole.STORAGE,
    },
    FacilityType.DEPOT.value: {
        FacilityRole.STORAGE,
        FacilityRole.TRANSSHIPMENT,
    },
    FacilityType.MAINTENANCE_HUB.value: {
        FacilityRole.TRANSSHIPMENT,
        FacilityRole.STORAGE,
    },
}


def derive_roles(
    facility_type: str,
    operations: set[str],
    role_overrides: set[FacilityRole] | None = None,
) -> set[FacilityRole]:
    """Derive facility roles from type, enabled operations, and optional overrides.

    Args:
        facility_type: Facility type string (e.g. ``station``, ``depot``).
        operations: Set of enabled operation type values (e.g. ``{"receiving", "dispatch"}``).
        role_overrides: If set, returned as-is (manual override).

    Returns:
        Derived or overridden set of ``FacilityRole`` values.
    """
    if role_overrides is not None:
        return set(role_overrides)

    roles = set(DEFAULT_ROLES.get(facility_type, set()))

    if OperationType.STORAGE.value not in operations:
        roles.discard(FacilityRole.STORAGE)

    if (
        OperationType.RECEIVING.value in operations
        and OperationType.DISPATCH.value in operations
    ):
        roles.add(FacilityRole.TRANSSHIPMENT)

    return roles
