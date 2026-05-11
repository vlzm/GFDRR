"""Column validation helpers for loaded DataFrames."""

from __future__ import annotations

import pandas as pd

from gbp.core.model import RawModelData, _required_column_names


def validate_csv_columns(
    table_name: str,
    df: pd.DataFrame,
    *,
    strict: bool = False,
) -> list[str]:
    """Check ``df`` columns against the Pydantic schema for *table_name*.

    Parameters
    ----------
    table_name
        Logical table name used to look up the expected schema.
    df
        DataFrame whose columns are validated.
    strict
        When ``True``, extra columns not in the schema are reported as errors.
        Default is ``False``.

    Returns
    -------
    list[str]
        Error/warning messages (empty list means validation passed).
    """
    schema_map = RawModelData._SCHEMAS
    if table_name not in schema_map:
        return []

    row_model = schema_map[table_name]
    req = _required_column_names(row_model)
    errors: list[str] = []

    missing = [c for c in req if c not in df.columns]
    if missing:
        errors.append(f"{table_name}: missing required columns {missing}")

    if strict:
        all_schema_cols = set(row_model.model_fields.keys())
        extra = [c for c in df.columns if c not in all_schema_cols]
        if extra:
            errors.append(f"{table_name}: unexpected columns {extra}")

    return errors
