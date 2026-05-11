"""Default facility roles and role derivation.

Role derivation is symmetric across all four roles:

- ``STORAGE`` is dropped from the type-default set when the ``storage``
  operation is missing.
- ``TRANSSHIPMENT`` is added when both ``receiving`` and ``dispatch`` are
  enabled (pass-through node).
- ``SINK`` is added when ``consumption`` is enabled (the node destroys
  flow exiting the network).
- ``SOURCE`` is added when ``production`` is enabled (the node creates
  flow entering the network from outside).

The ``DEFAULT_ROLES`` mapping below carries L3 bike-sharing defaults so
that stations and depots get sensible roles without needing the new
``consumption`` / ``production`` operations.  Other domains (e.g. gas
delivery) can describe consumer / producer nodes purely through the
operation set.
"""

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

    Parameters
    ----------
    facility_type
        Facility type string (e.g. ``"station"``, ``"depot"``).
    operations
        Set of enabled operation type values
        (e.g. ``{"receiving", "dispatch"}``).
    role_overrides
        If provided, returned as-is (manual override).

    Returns
    -------
    set[FacilityRole]
        Derived or overridden set of facility roles.
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

    if OperationType.CONSUMPTION.value in operations:
        roles.add(FacilityRole.SINK)

    if OperationType.PRODUCTION.value in operations:
        roles.add(FacilityRole.SOURCE)

    return roles
