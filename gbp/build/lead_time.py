"""Resolve edge lead times from hours to integer period offsets."""

from __future__ import annotations

import numpy as np
import pandas as pd

from gbp.build._helpers import get_duration_hours


def resolve_lead_times(edges: pd.DataFrame, periods: pd.DataFrame) -> pd.DataFrame:
    """Build ``edge_lead_time_resolved``: one row per edge x departure period.

    Output columns: ``source_id``, ``target_id``, ``modal_type``,
    ``period_id``, ``lead_time_periods``, ``arrival_period_id`` (nullable
    if arrival is past the horizon).

    Parameters
    ----------
    edges
        Must include ``source_id``, ``target_id``, ``modal_type``,
        ``lead_time_hours``.
    periods
        Must include ``period_id``, ``start_date``, ``end_date``,
        ``period_index``, ``period_type``.

    Returns
    -------
    pd.DataFrame
        One row per edge x departure period with lead time in periods and
        the resolved arrival period.
    """
    if edges.empty or periods.empty:
        return pd.DataFrame(
            columns=[
                "source_id",
                "target_id",
                "modal_type",
                "period_id",
                "lead_time_periods",
                "arrival_period_id",
            ]
        )

    edge_cols = ["source_id", "target_id", "modal_type", "lead_time_hours"]
    e = edges[edge_cols].drop_duplicates().copy()
    per = periods[
        ["period_id", "start_date", "end_date", "period_index", "period_type"]
    ].copy()
    per["start_date"] = pd.to_datetime(per["start_date"])
    per["end_date"] = pd.to_datetime(per["end_date"])

    if per["period_type"].nunique() == 1:
        ptype = str(per["period_type"].iloc[0])
        dur = get_duration_hours(ptype)
        e["lead_time_periods"] = np.ceil(e["lead_time_hours"] / dur).astype(int)
        cross = e.merge(per[["period_id", "period_index"]], how="cross")
        cross["arrival_period_index"] = cross["period_index"] + cross["lead_time_periods"]
        idx_to_pid = per.set_index("period_index")["period_id"]
        cross["arrival_period_id"] = cross["arrival_period_index"].map(idx_to_pid)
        _cols = [
            "source_id",
            "target_id",
            "modal_type",
            "period_id",
            "lead_time_periods",
            "arrival_period_id",
        ]
        out = cross[_cols]
        return out

    return _resolve_lead_times_multi_resolution(e, per)


def _resolve_lead_times_multi_resolution(
    edges_unique: pd.DataFrame,
    periods: pd.DataFrame,
) -> pd.DataFrame:
    """Compute per-departure-period lead time when segment lengths differ.

    Uses vectorized cross-join + ``np.searchsorted`` for O(E*P*log P).

    Parameters
    ----------
    edges_unique
        Deduplicated edges with ``lead_time_hours``.
    periods
        Period table with ``period_id``, ``period_index``, ``start_date``,
        ``end_date``.

    Returns
    -------
    pd.DataFrame
        Same schema as :func:`resolve_lead_times` output.
    """
    per_sorted = periods.sort_values("period_index").reset_index(drop=True)

    # Cross join: edges × periods
    cross = edges_unique.merge(
        per_sorted[["period_id", "period_index", "start_date"]], how="cross",
    )
    # Compute arrival timestamps vectorized
    arrival_ts = cross["start_date"] + pd.to_timedelta(cross["lead_time_hours"], unit="h")

    # Use searchsorted on period boundaries to find arrival period
    starts = per_sorted["start_date"].values  # sorted
    ends = per_sorted["end_date"].values
    period_indices = per_sorted["period_index"].values
    period_ids = per_sorted["period_id"].values

    # For each arrival, find the period where start <= arrival < end
    # searchsorted("right") on starts gives the index of the first start > arrival
    # so the candidate period is at pos - 1
    pos = np.searchsorted(starts, arrival_ts.values, side="right") - 1

    n = len(cross)
    arr_period_idx = np.full(n, -1, dtype=int)
    arr_period_id = np.empty(n, dtype=object)
    arr_period_id[:] = None

    valid = (pos >= 0) & (pos < len(per_sorted))
    valid_pos = pos[valid]
    within = arrival_ts.values[valid] < ends[valid_pos]

    # Rows where arrival falls within a valid period
    full_valid = np.zeros(n, dtype=bool)
    full_valid[valid] = within
    arr_period_idx[full_valid] = period_indices[pos[full_valid]]
    arr_period_id[full_valid] = period_ids[pos[full_valid]]

    dep_idx = cross["period_index"].values
    max_idx = int(period_indices.max())

    lt_periods = np.where(full_valid, arr_period_idx - dep_idx, np.maximum(0, max_idx - dep_idx))

    return pd.DataFrame({
        "source_id": cross["source_id"].values,
        "target_id": cross["target_id"].values,
        "modal_type": cross["modal_type"].values,
        "period_id": cross["period_id"].values,
        "lead_time_periods": lt_periods,
        "arrival_period_id": arr_period_id,
    })
