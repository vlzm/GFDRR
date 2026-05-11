"""Period-duration helpers for the simulator runtime.

Single source of truth for "how many hours does one period span" when the
answer must come from the actual ``resolved.periods`` DataFrame (e.g. when
two adjacent ``start_date`` values are 24 h apart).

Conceptually distinct from :func:`gbp.build._helpers.get_duration_hours`,
which is a grain-based lookup over the :class:`PeriodType` enum
(``"day"`` -> 24.0, ``"week"`` -> 168.0, ``"month"`` -> 720.0).  The two
helpers serve different layers and must not be unified: build-time uses
the nominal grain to compute scalings; simulator-runtime uses observed
deltas to compute arrival arithmetic.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from gbp.core.model import ResolvedModelData


def period_duration_hours(resolved: ResolvedModelData) -> float:
    """Return the period duration in hours derived from ``resolved.periods``.

    Subtracts the first two rows' ``start_date`` values.  Falls back to
    ``1.0`` when the period table is too small (fewer than two rows) or
    when the dates cannot be subtracted.

    Parameters
    ----------
    resolved
        A built model carrying a ``periods`` DataFrame.

    Returns
    -------
    float
        Period duration in hours as a positive float.  ``1.0`` is returned
        as a safe default rather than zero so that downstream divisions
        do not raise.
    """
    periods = resolved.periods
    if periods is None or len(periods) < 2:
        return 1.0
    try:
        d0 = pd.Timestamp(periods.iloc[0]["start_date"])
        d1 = pd.Timestamp(periods.iloc[1]["start_date"])
        delta = (d1 - d0).total_seconds() / 3600.0
    except (KeyError, TypeError, ValueError):
        return 1.0
    if delta <= 0:
        return 1.0
    return float(delta)
