"""Load a folder of CSV files into ``RawModelData``."""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path

import pandas as pd

from gbp.core.model import RawModelData
from gbp.loaders.validators import validate_csv_columns

_DEFAULT_DATE_COLUMNS = frozenset({
    "date",
    "start_date",
    "end_date",
    "departure_date",
    "expected_arrival_date",
})


class CsvLoader:
    """Read ``{field_name}.csv`` files from a directory into ``RawModelData``.

    Parameters
    ----------
    directory
        Path to the directory containing CSV files.
    validate
        Run column and model validation after loading. Default is ``True``.
    date_columns
        Per-table override for date column names. Keys are table names, values
        are lists of column names to parse as dates. When ``None``, the module
        default set is used.
    """

    def __init__(
        self,
        directory: str | Path,
        *,
        validate: bool = True,
        date_columns: dict[str, list[str]] | None = None,
    ) -> None:
        self._directory = Path(directory)
        self._validate = validate
        self._date_columns = date_columns

    def load(self) -> RawModelData:
        """Scan directory, read CSVs, and assemble ``RawModelData``.

        Returns
        -------
        RawModelData
            Loaded (and optionally validated) raw model data.

        Raises
        ------
        FileNotFoundError
            If the directory does not exist or a required CSV is missing.
        ValueError
            If column validation finds errors.
        """
        if not self._directory.is_dir():
            raise FileNotFoundError(f"Directory not found: {self._directory}")

        csv_map = {p.stem: p for p in self._directory.glob("*.csv")}
        kwargs: dict[str, pd.DataFrame | None] = {}
        errors: list[str] = []

        for f in fields(RawModelData):
            if f.name.startswith("_"):
                continue
            if f.name in csv_map:
                df = pd.read_csv(csv_map[f.name])
                self._parse_dates(f.name, df)
                if self._validate:
                    errs = validate_csv_columns(f.name, df)
                    errors.extend(errs)
                kwargs[f.name] = df
            elif f.name in RawModelData._REQUIRED:
                raise FileNotFoundError(
                    f"Required CSV file {f.name}.csv not found in {self._directory}"
                )

        if errors:
            raise ValueError(
                "CSV column validation failed:\n" + "\n".join(errors)
            )

        raw = RawModelData(**kwargs)
        if self._validate:
            raw.validate()
        return raw

    def _parse_dates(self, table_name: str, df: pd.DataFrame) -> None:
        """Convert known date columns from strings to ``datetime.date``.

        Parameters
        ----------
        table_name
            Logical table name used to look up per-table date column overrides.
        df
            DataFrame whose date columns are converted in place.
        """
        date_cols: set[str]
        if self._date_columns and table_name in self._date_columns:
            date_cols = set(self._date_columns[table_name])
        else:
            date_cols = _DEFAULT_DATE_COLUMNS

        for col in date_cols:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col]).dt.date
                except (ValueError, TypeError):
                    pass


def load_csv_folder(directory: str | Path, **kwargs: object) -> RawModelData:
    """Load a CSV folder via ``CsvLoader`` (convenience wrapper).

    Parameters
    ----------
    directory
        Path to the directory containing CSV files.
    **kwargs
        Forwarded to ``CsvLoader.__init__``.

    Returns
    -------
    RawModelData
        Loaded raw model data.
    """
    return CsvLoader(directory, **kwargs).load()  # type: ignore[arg-type]
