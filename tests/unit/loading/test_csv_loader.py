"""Tests for CSV folder loading."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pytest

from gbp.loading.csv_loader import CsvLoader, load_csv_folder

_FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "bike_minimal"


def test_load_minimal_bike() -> None:
    """Load the bike_minimal CSV fixture into ``RawModelData``."""
    raw = load_csv_folder(_FIXTURE_DIR)
    assert len(raw.facilities) == 3
    assert len(raw.periods) == 3
    assert raw.edges is not None
    assert len(raw.edges) == 2


def test_missing_required_file_raises(tmp_path: Path) -> None:
    """Missing required CSV raises FileNotFoundError."""
    shutil.copytree(_FIXTURE_DIR, tmp_path / "data")
    (tmp_path / "data" / "facilities.csv").unlink()
    with pytest.raises(FileNotFoundError, match="facilities"):
        load_csv_folder(tmp_path / "data")


def test_optional_files_are_none(tmp_path: Path) -> None:
    """Loading with only required CSVs leaves optional fields None."""
    shutil.copytree(_FIXTURE_DIR, tmp_path / "data")
    for p in (tmp_path / "data").glob("*.csv"):
        if p.stem not in (
            "facilities",
            "commodity_categories",
            "resource_categories",
            "planning_horizon",
            "planning_horizon_segments",
            "periods",
            "facility_roles",
            "facility_operations",
            "edge_rules",
        ):
            p.unlink()
    raw = load_csv_folder(tmp_path / "data")
    assert raw.edges is None
    assert raw.demand is None


def test_date_parsing() -> None:
    """Date columns in demand.csv are parsed as datetime.date."""
    raw = load_csv_folder(_FIXTURE_DIR)
    assert raw.demand is not None
    assert raw.demand["date"].iloc[0] == date(2025, 1, 1)


def test_validate_flag(tmp_path: Path) -> None:
    """``validate=False`` skips column checks."""
    shutil.copytree(_FIXTURE_DIR, tmp_path / "data")
    raw = CsvLoader(tmp_path / "data", validate=False).load()
    assert len(raw.facilities) == 3


def test_nonexistent_directory_raises() -> None:
    """Non-existent directory raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="not found"):
        load_csv_folder("/no/such/dir")
