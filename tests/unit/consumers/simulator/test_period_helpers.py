"""Tests for the simulator-runtime period-duration helper."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pandas as pd

from gbp.consumers.simulator._period_helpers import period_duration_hours
from gbp.core.model import ResolvedModelData


def _stub(periods: pd.DataFrame | None) -> ResolvedModelData:
    """Build a duck-typed ResolvedModelData stub carrying only ``periods``."""
    return cast(ResolvedModelData, SimpleNamespace(periods=cast(Any, periods)))


def test_period_duration_hours_two_periods_24h() -> None:
    """Two adjacent day-grain periods → 24.0 hours."""
    periods = pd.DataFrame(
        {
            "period_id": ["p0", "p1"],
            "start_date": [
                pd.Timestamp("2025-01-01"),
                pd.Timestamp("2025-01-02"),
            ],
        }
    )
    assert period_duration_hours(_stub(periods)) == 24.0


def test_period_duration_hours_two_periods_week() -> None:
    """Two adjacent week-grain periods → 168.0 hours."""
    periods = pd.DataFrame(
        {
            "period_id": ["p0", "p1"],
            "start_date": [
                pd.Timestamp("2025-01-06"),
                pd.Timestamp("2025-01-13"),
            ],
        }
    )
    assert period_duration_hours(_stub(periods)) == 168.0


def test_period_duration_hours_fallback_when_too_few_periods() -> None:
    """A single-row period frame falls back to 1.0 hours."""
    periods = pd.DataFrame(
        {"period_id": ["p0"], "start_date": [pd.Timestamp("2025-01-01")]}
    )
    assert period_duration_hours(_stub(periods)) == 1.0


def test_period_duration_hours_fallback_when_periods_missing() -> None:
    """``None`` periods table falls back to 1.0 hours."""
    assert period_duration_hours(_stub(None)) == 1.0


def test_period_duration_hours_fallback_when_zero_delta() -> None:
    """Identical adjacent timestamps fall back to 1.0 hours."""
    periods = pd.DataFrame(
        {
            "period_id": ["p0", "p1"],
            "start_date": [
                pd.Timestamp("2025-01-01"),
                pd.Timestamp("2025-01-01"),
            ],
        }
    )
    assert period_duration_hours(_stub(periods)) == 1.0
