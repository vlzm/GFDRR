"""Load a folder of CSV files into ``RawModelData``."""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path

import pandas as pd

from gbp.core.model import RawModelData
from gbp.loading.validators import validate_csv_columns

_DEFAULT_DATE_COLUMNS = frozenset({
    "date",
    "start_date",
    "end_date",
    "departure_date",
    "expected_arrival_date",
})


class CsvLoader:
    """Read ``{field_name}.csv`` files from a directory into ``RawModelData``."""

    def __init__(
        self,
        directory: str | Path,
        *,
        validate: bool = True,
        date_columns: dict[str, list[str]] | None = None,
    ) -> None:
        """Configure loader with a CSV directory and optional validation."""
        self._directory = Path(directory)
        self._validate = validate
        self._date_columns = date_columns

    def load(self) -> RawModelData:
        """Scan directory, read CSVs, and assemble ``RawModelData``."""
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
        """Convert known date columns from strings to ``datetime.date``."""
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
    """Convenience wrapper around ``CsvLoader``."""
    return CsvLoader(directory, **kwargs).load()  # type: ignore[arg-type]
