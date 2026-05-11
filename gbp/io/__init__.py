"""I/O for RawModelData and ResolvedModelData (Parquet and dict/JSON).

Modules
-------
dict_io
    Dict/JSON round-trip for ``RawModelData`` and ``ResolvedModelData``.
parquet
    Parquet directory round-trip for ``RawModelData`` and ``ResolvedModelData``.
"""

from gbp.io.dict_io import (
    raw_from_dict,
    raw_to_dict,
    resolved_from_dict,
    resolved_to_dict,
)
from gbp.io.parquet import (
    load_raw_parquet,
    load_resolved_parquet,
    save_raw_parquet,
    save_resolved_parquet,
)

__all__ = [
    "load_raw_parquet",
    "load_resolved_parquet",
    "raw_from_dict",
    "raw_to_dict",
    "resolved_from_dict",
    "resolved_to_dict",
    "save_raw_parquet",
    "save_resolved_parquet",
]
