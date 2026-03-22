"""End-to-end: CSV folder -> RawModelData -> build_model -> ResolvedModelData."""

from __future__ import annotations

from pathlib import Path

from gbp.build.pipeline import build_model
from gbp.loading.csv_loader import load_csv_folder

_FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "bike_minimal"


def test_csv_folder_to_resolved_model() -> None:
    """Full pipeline from CSV files to ResolvedModelData with spines."""
    raw = load_csv_folder(_FIXTURE_DIR)
    resolved = build_model(raw)

    assert len(resolved.facilities) == 3
    assert len(resolved.periods) == 3
    assert resolved.edges is not None
    assert resolved.resource_spines is not None
    assert resolved.facility_spines is not None
