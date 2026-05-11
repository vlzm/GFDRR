"""Vectorised helpers for applying per-(facility, commodity) deltas to inventory.

Phases that move commodity in or out of the world repeatedly need the same
mechanical sequence: aggregate a stream of rows by (facility, commodity),
left-merge onto ``state.inventory``, fill missing rows with zero, apply
arithmetic, and reduce back to ``INVENTORY_COLUMNS``.  This module owns
that plumbing so phases focus on their domain logic.

Three functions cover the spectrum:

- :func:`to_inventory_delta` aggregates a row stream into the canonical
  ``(facility_id, commodity_category, quantity)`` delta shape.
- :func:`apply_delta` adds/subtracts a delta against current inventory,
  with optional clipping at zero.  Use this for the common cases.
- :func:`merge_with_inventory` performs only the merge + fillna and returns
  the merged frame.  Use this when the arithmetic is bespoke (e.g.
  :class:`~gbp.consumers.simulator.built_in_phases.DemandPhase` clips
  fulfilled at available stock and emits ``deficit``).
"""

from __future__ import annotations

from typing import Literal

import pandas as pd

from gbp.consumers.simulator.state import INVENTORY_COLUMNS

InventoryDeltaOp = Literal["subtract", "add", "add_clip_zero"]


def to_inventory_delta(
    rows: pd.DataFrame,
    *,
    facility_col: str,
    quantity_col: str = "quantity",
) -> pd.DataFrame:
    """Aggregate *rows* to a ``(facility_id, commodity_category, quantity)`` delta.

    Parameters
    ----------
    rows
        Input DataFrame (e.g. observed flows, dispatches).  Must have
        ``facility_col``, ``commodity_category``, and ``quantity_col``.
    facility_col
        Column to use as the facility key — ``"source_id"`` for
        outflow, ``"target_id"`` for inflow.  Renamed to ``"facility_id"``
        in the output.
    quantity_col
        Column whose values are summed.  Renamed to
        ``"quantity"`` in the output.  Defaults to ``"quantity"``.

    Returns
    -------
    pd.DataFrame
        DataFrame with exactly ``[facility_id, commodity_category, quantity]``.
    """
    return (
        rows.groupby([facility_col, "commodity_category"], as_index=False)[quantity_col]
        .sum()
        .rename(columns={facility_col: "facility_id", quantity_col: "quantity"})
    )


def apply_delta(
    inventory: pd.DataFrame,
    delta: pd.DataFrame,
    *,
    op: InventoryDeltaOp,
) -> pd.DataFrame:
    """Apply a per-(facility, commodity) delta to *inventory*.

    Parameters
    ----------
    inventory
        Current inventory DataFrame in ``INVENTORY_COLUMNS`` shape.
    delta
        DataFrame with ``[facility_id, commodity_category, quantity]``;
        the ``quantity`` column carries the delta magnitude.  Facilities
        absent from *delta* are treated as delta = 0.
    op
        Arithmetic mode.  ``"subtract"`` and ``"add"`` apply the delta
        directly.  ``"add_clip_zero"`` adds the delta and clips the
        result at zero — used for organic arrivals where the source
        facility's transient negative stock must not propagate.

    Returns
    -------
    pd.DataFrame
        New inventory DataFrame in ``INVENTORY_COLUMNS`` shape.

    Raises
    ------
    ValueError
        If *op* is not one of the supported modes.
    """
    merged = inventory.merge(
        delta.rename(columns={"quantity": "_delta"}),
        on=["facility_id", "commodity_category"],
        how="left",
    )
    merged["_delta"] = merged["_delta"].fillna(0.0)

    if op == "subtract":
        merged["quantity"] = merged["quantity"] - merged["_delta"]
    elif op == "add":
        merged["quantity"] = merged["quantity"] + merged["_delta"]
    elif op == "add_clip_zero":
        merged["quantity"] = (merged["quantity"] + merged["_delta"]).clip(lower=0.0)
    else:
        msg = f"Unknown inventory delta op: {op!r}"
        raise ValueError(msg)

    return merged[INVENTORY_COLUMNS].copy()


def merge_with_inventory(
    inventory: pd.DataFrame,
    delta: pd.DataFrame,
    *,
    value_col: str,
    default: float = 0.0,
) -> pd.DataFrame:
    """Left-merge *delta* onto *inventory* and zero-fill *value_col*.

    Use this when the post-merge arithmetic is too bespoke for
    :func:`apply_delta`.  Caller is responsible for the arithmetic and for
    reducing the result back to ``INVENTORY_COLUMNS`` shape.

    Parameters
    ----------
    inventory
        Current inventory DataFrame.
    delta
        DataFrame to merge.  Must contain ``[facility_id,
        commodity_category, value_col]``.
    value_col
        The column to ``fillna(default)`` after the merge.
    default
        Fill value for missing rows.  Defaults to ``0.0``.

    Returns
    -------
    pd.DataFrame
        Merged DataFrame including the inventory's ``quantity`` column and
        the filled *value_col*.
    """
    merged = inventory.merge(
        delta, on=["facility_id", "commodity_category"], how="left",
    )
    merged[value_col] = merged[value_col].fillna(default)
    return merged
