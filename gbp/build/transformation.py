"""Join transformation definitions with facilities (denormalized view)."""

from __future__ import annotations

import pandas as pd


def resolve_transformations(
    facilities: pd.DataFrame,
    transformations: pd.DataFrame | None,
    transformation_inputs: pd.DataFrame | None,
    transformation_outputs: pd.DataFrame | None,
) -> pd.DataFrame | None:
    """Return one row per facility x transformation x input x output pair.

    Parameters
    ----------
    facilities
        Must contain ``facility_id``.
    transformations
        Transformation definitions with ``transformation_id``,
        ``facility_id``. ``None`` yields an immediate ``None`` return.
    transformation_inputs
        Input commodity rows. ``None`` yields ``None``.
    transformation_outputs
        Output commodity rows. ``None`` yields ``None``.

    Returns
    -------
    pd.DataFrame or None
        Denormalized join, or ``None`` if any table is missing or
        *transformations* is empty.
    """
    if (
        transformations is None
        or transformation_inputs is None
        or transformation_outputs is None
    ):
        return None
    if transformations.empty:
        return None

    tin = transformation_inputs.merge(transformations, on="transformation_id", how="inner")
    tin = tin.rename(columns={"commodity_category": "commodity_category_in", "ratio": "ratio_in"})

    tout = transformation_outputs.copy()
    tout = tout.rename(
        columns={
            "commodity_category": "commodity_category_out",
            "ratio": "ratio_out",
        }
    )

    merged = tin.merge(tout, on="transformation_id", how="inner")
    out = facilities.merge(merged, on="facility_id", how="inner")
    return out.reset_index(drop=True)
