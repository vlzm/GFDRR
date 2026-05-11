"""Shared helpers for the build pipeline."""

from __future__ import annotations

# Approximate calendar hours per period type (used for lead-time scaling).
PERIOD_DURATION_HOURS: dict[str, float] = {
    "day": 24.0,
    "week": 168.0,
    "month": 720.0,  # ~30 days for strategic month buckets
}


def get_duration_hours(period_type: str) -> float:
    """Return nominal duration in hours for a ``PeriodType`` value string.

    Parameters
    ----------
    period_type
        Lower-case period type string (``"day"``, ``"week"``, ``"month"``).

    Returns
    -------
    float
        Approximate calendar hours for the given period type.

    Raises
    ------
    ValueError
        If *period_type* is not recognized.
    """
    key = str(period_type).lower()
    if key not in PERIOD_DURATION_HOURS:
        raise ValueError(f"Unknown period_type for duration: {period_type!r}")
    return PERIOD_DURATION_HOURS[key]
