"""Map raw calendar dates to planning periods with aggregation."""

from __future__ import annotations

import pandas as pd

from gbp.core.attributes.registry import AttributeRegistry
from gbp.core.enums import PeriodType
from gbp.core.model import RawModelData


_PERIOD_FREQ: dict[str, str] = {
    PeriodType.DAY.value: "D",
    PeriodType.WEEK.value: "W-MON",
    PeriodType.MONTH.value: "MS",
}


def build_periods_from_segments(
    planning_horizon: pd.DataFrame,
    planning_horizon_segments: pd.DataFrame,
) -> pd.DataFrame:
    """Build a ``periods`` DataFrame from horizon segments.

    Each segment's ``[start_date, end_date)`` range is sliced into bins of
    the segment's ``period_type`` (day / week / month).  Period IDs are
    assigned globally as ``p0``, ``p1``, ... in segment-order to match the
    convention used by existing loaders and fixtures.

    Args:
        planning_horizon: Must contain ``planning_horizon_id`` (one row expected).
        planning_horizon_segments: Must contain ``planning_horizon_id``,
            ``segment_index``, ``start_date``, ``end_date``, ``period_type``.

    Returns:
        DataFrame with columns ``period_id``, ``planning_horizon_id``,
        ``segment_index``, ``period_index``, ``period_type``, ``start_date``,
        ``end_date``.
    """
    horizon_id = str(planning_horizon["planning_horizon_id"].iloc[0])

    segments = planning_horizon_segments.sort_values("segment_index").copy()
    segments["start_date"] = pd.to_datetime(segments["start_date"]).dt.normalize()
    segments["end_date"] = pd.to_datetime(segments["end_date"]).dt.normalize()

    per_segment_frames: list[pd.DataFrame] = []
    running_index = 0

    for segment in segments.itertuples(index=False):
        period_type = str(segment.period_type)
        freq = _PERIOD_FREQ.get(period_type)
        if freq is None:
            raise ValueError(
                f"Unsupported period_type '{period_type}' in segment "
                f"{segment.segment_index}; expected one of {list(_PERIOD_FREQ)}"
            )

        starts = pd.date_range(
            start=segment.start_date,
            end=segment.end_date - pd.Timedelta(days=1),
            freq=freq,
        )
        if len(starts) == 0:
            continue

        ends = _advance_period_ends(starts, period_type)
        ends_series = pd.Series(ends).where(
            pd.Series(ends) <= segment.end_date, other=segment.end_date,
        )
        ends = pd.DatetimeIndex(ends_series)

        count = len(starts)
        frame = pd.DataFrame({
            "period_id": [f"p{running_index + i}" for i in range(count)],
            "planning_horizon_id": horizon_id,
            "segment_index": int(segment.segment_index),
            "period_index": list(range(count)),
            "period_type": period_type,
            "start_date": starts.date,
            "end_date": ends.date,
        })
        per_segment_frames.append(frame)
        running_index += count

    if not per_segment_frames:
        return pd.DataFrame(
            columns=[
                "period_id", "planning_horizon_id", "segment_index",
                "period_index", "period_type", "start_date", "end_date",
            ]
        )

    return pd.concat(per_segment_frames, ignore_index=True)


def _advance_period_ends(starts: pd.DatetimeIndex, period_type: str) -> pd.DatetimeIndex:
    """Compute the exclusive end timestamp for each period start."""
    if period_type == PeriodType.DAY.value:
        return starts + pd.Timedelta(days=1)
    if period_type == PeriodType.WEEK.value:
        return starts + pd.Timedelta(days=7)
    # MONTH
    return starts + pd.offsets.MonthBegin(1)


def resolve_to_periods(
    param_df: pd.DataFrame,
    periods: pd.DataFrame,
    value_columns: list[str],
    group_grain: list[str],
    agg_func: str = "mean",
) -> pd.DataFrame:
    """Map ``date`` rows into ``period_id`` buckets and aggregate values.

    A row's ``date`` falls in period ``p`` when
    ``p.start_date <= date < p.end_date`` (end exclusive).

    Uses ``pd.merge_asof`` for O(N log N) assignment instead of iterating periods.

    Args:
        param_df: Must contain ``date`` plus ``group_grain`` columns.
        periods: Must contain ``period_id``, ``start_date``, ``end_date``.
        value_columns: Numeric columns to aggregate.
        group_grain: Non-time key columns (excluding ``date``).
        agg_func: Pandas aggregate name (``mean``, ``sum``, ``min``, ``max``).

    Returns:
        DataFrame with ``group_grain + [period_id] + value_columns``.
    """
    empty_cols = list(group_grain) + ["period_id"] + value_columns
    if param_df.empty:
        return pd.DataFrame(columns=empty_cols)

    df = param_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values("date")

    per = periods[["period_id", "start_date", "end_date"]].copy()
    per["start_date"] = pd.to_datetime(per["start_date"]).dt.normalize()
    per["end_date"] = pd.to_datetime(per["end_date"]).dt.normalize()
    per = per.sort_values("start_date")

    # For each date, find the latest period whose start_date <= date
    merged = pd.merge_asof(
        df, per, left_on="date", right_on="start_date", direction="backward",
    )
    # Keep only rows where date < end_date (within the period)
    merged = merged[merged["end_date"].notna() & (merged["date"] < merged["end_date"])]

    if merged.empty:
        return pd.DataFrame(columns=empty_cols)

    gb_cols = list(group_grain) + ["period_id"]
    out = merged.groupby(gb_cols, as_index=False)[value_columns].agg(agg_func)
    return out


def resolve_registry_attributes(
    raw_registry: AttributeRegistry,
    periods: pd.DataFrame,
) -> AttributeRegistry:
    """Resolve time-varying registry attributes into period grain.

    Non-time-varying attributes are copied through unchanged.

    Returns:
        New ``AttributeRegistry`` with resolved data (``period_id`` replaces ``date``).
    """
    resolved_registry = AttributeRegistry()

    for attr in raw_registry.specs:
        data = raw_registry.get(attr.name).data

        if not attr.time_varying:
            resolved_registry.register_raw(attr, data)
            continue

        if data.empty or "date" not in data.columns:
            resolved_registry.register_raw(attr, data)
            continue

        group_grain = [g for g in attr.grain if g != "date"]
        # Preserve eav_filter columns (e.g. attribute_name) through aggregation
        if attr.eav_filter:
            for fk in attr.eav_filter:
                if fk in data.columns and fk not in group_grain:
                    group_grain.append(fk)
        existing_grain = [c for c in group_grain if c in data.columns]
        value_cols = [attr.value_column] if attr.value_column in data.columns else []

        if not value_cols:
            resolved_registry.register_raw(attr, data)
            continue

        resolved_data = resolve_to_periods(
            data, periods, value_cols, existing_grain, agg_func=attr.aggregation,
        )

        resolved_registry.register_raw(attr, resolved_data)

    return resolved_registry


def resolve_all_time_varying(raw: RawModelData, periods: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Resolve structural time-varying tables from ``raw`` to period grain.

    Returns a dict of table name -> resolved DataFrame. Only includes tables
    that were present and non-empty in ``raw``.

    Parametric attributes are resolved via ``resolve_registry_attributes()``.
    """
    resolved: dict[str, pd.DataFrame] = {}

    specs: list[tuple[str, pd.DataFrame | None, list[str], list[str], str]] = [
        ("demand", raw.demand, ["facility_id", "commodity_category"], ["quantity"], "sum"),
        ("supply", raw.supply, ["facility_id", "commodity_category"], ["quantity"], "sum"),
        (
            "edge_capacities",
            raw.edge_capacities,
            ["source_id", "target_id", "modal_type"],
            ["capacity"],
            "min",
        ),
        (
            "edge_commodity_capacities",
            raw.edge_commodity_capacities,
            ["source_id", "target_id", "modal_type", "commodity_category"],
            ["max_shipment"],
            "min",
        ),
        (
            "facility_availability",
            raw.facility_availability,
            ["facility_id"],
            ["capacity_factor"],
            "mean",
        ),
        (
            "observed_flow",
            raw.observed_flow,
            ["source_id", "target_id", "commodity_category"],
            ["quantity"],
            "sum",
        ),
        (
            "observed_inventory",
            raw.observed_inventory,
            ["facility_id", "commodity_category"],
            ["quantity"],
            "last",
        ),
    ]

    for name, df, grain, value_cols, agg in specs:
        if df is None or df.empty:
            continue
        if "date" not in df.columns:
            continue
        vc = [c for c in value_cols if c in df.columns]
        if not vc:
            continue
        g = [c for c in grain if c in df.columns]
        resolved[name] = resolve_to_periods(df, periods, vc, g, agg_func=agg)

    return resolved
