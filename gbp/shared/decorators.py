"""Pandera-based validation decorator for function inputs/outputs."""

from __future__ import annotations

import inspect
import functools
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import pandera.pandas as pa


def validate(
    *,
    inputs: dict[str, type[pa.DataFrameModel]] | None = None,
    output: type[pa.DataFrameModel]
    | tuple[type[pa.DataFrameModel] | None, ...]
    | None = None,
):
    """Validate function DataFrames against Pandera schemas.

    Parameters
    ----------
    inputs : dict[str, DataFrameModel], optional
        Map parameter names to Pandera schema classes.
    output : DataFrameModel or tuple[DataFrameModel | None, ...], optional
        Schema(s) for the return value.  Use a tuple when the function
        returns a tuple of DataFrames — each element is validated
        against the corresponding schema (use ``None`` to skip an element).
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if inputs:
                sig = inspect.signature(func)
                bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()
                for param_name, schema in inputs.items():
                    if param_name in bound.arguments:
                        val = bound.arguments[param_name]
                        if isinstance(val, pd.DataFrame):
                            schema.validate(val)

            result = func(*args, **kwargs)

            if output is not None:
                if isinstance(output, tuple):
                    for schema, val in zip(output, result):
                        if schema is not None and isinstance(val, pd.DataFrame):
                            schema.validate(val)
                elif isinstance(result, pd.DataFrame):
                    output.validate(result)

            return result

        return wrapper

    return decorator
