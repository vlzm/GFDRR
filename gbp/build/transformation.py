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

    Returns None if any transformation table is missing or transformations empty.
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
