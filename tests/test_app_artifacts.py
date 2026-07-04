"""Smoke tests for the UI layer: artifact building and every Streamlit page.

The artifact tables are built from the synthetic offline scenarios in
``tests/scenarios.py`` (no CSV, no network). The Streamlit pages then run
against two saved artifacts through ``streamlit.testing.v1.AppTest``: the test
asserts each page renders without an exception, for both the single-scenario
and the comparison view.
"""

import pathlib
import sys

import pandas as pd
import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "app"))

import artifacts  # noqa: E402  (needs the app folder on sys.path)

from tests import scenarios  # noqa: E402

RATES = pd.DataFrame({"commodity_category": [scenarios.CLASSIC], "rate": [3.0]})
PERIOD_LEN = pd.Timedelta(hours=1)


def _build_tables(resolved, journal):
    """Run the artifact builders on a synthetic scenario's journal."""
    return artifacts.build_run_tables(
        journal,
        initial_inventory=resolved.initial_inventory_df,
        facilities=resolved.facilities_df,
        facilities_geo=resolved.facilities_geo_df,
        facilities_capacities=resolved.facilities_capacities_df,
        rates=RATES,
        period_len=PERIOD_LEN,
    )


def _save_run(name, resolved, journal, root):
    """Build and save one run artifact under ``root``; return its meta."""
    tables = _build_tables(resolved, journal)
    meta = {
        "run_name": name,
        "scenario_id": name,
        "demand_scale_factor": 1.0,
        "sizing_scale_factor": 1.0,
        "number_of_periods": int(resolved.periods_df["period_id"].max()) + 1,
        "period_len_hours": 1.0,
        "created_at": "2026-01-01T00:00:00",
        "violations": [],
        "totals": artifacts.build_totals(tables["panel"], tables["flow_totals"]),
    }
    artifacts.save_run(name, tables, meta, root)
    return meta


# ---------------------------------------------------------------------------
# Artifact builders
# ---------------------------------------------------------------------------
def test_panel_counts_match_the_journal():
    resolved = scenarios.overflow()
    journal, _ = scenarios.run(resolved)
    panel = artifacts.build_panel(journal, resolved.initial_inventory_df)

    from gbp.model import is_docking, is_user_departure

    assert panel["departed"].sum() == journal[is_user_departure(journal)]["quantity"].sum()
    assert panel["arrived"].sum() == journal[is_docking(journal)]["quantity"].sum()
    assert (panel["demand"] == panel["departed"] + panel["lost_demand"]).all()
    # The overflow scenario bounces at least one bike off a full station.
    assert panel["redirected"].sum() > 0


def test_stockout_shows_up_as_lost_demand():
    resolved = scenarios.stockout()
    journal, _ = scenarios.run(resolved)
    panel = artifacts.build_panel(journal, resolved.initial_inventory_df)
    assert panel["lost_demand"].sum() > 0
    assert (panel["demand"] == panel["departed"] + panel["lost_demand"]).all()


def test_arcs_pair_every_departed_with_its_close():
    resolved = scenarios.redirect_chain()
    journal, _ = scenarios.run(resolved)
    arcs = artifacts.build_arcs(journal, resolved.facilities_geo_df)

    n_departed = int((journal["event_type"] == "departed").sum())
    assert len(arcs) == n_departed
    assert arcs["target_id"].notna().all()
    assert (arcs["distance_km"] >= 0).all()
    # The chain scenario has a flow with more than one arc.
    assert arcs.groupby("flow_id")["move_id"].count().max() >= 2


def test_flow_totals_one_row_per_flow():
    resolved = scenarios.overflow()
    journal, _ = scenarios.run(resolved)
    tables = _build_tables(resolved, journal)
    flow_totals = tables["flow_totals"]

    assert flow_totals["flow_id"].is_unique
    assert set(flow_totals["flow_id"]) == set(journal["flow_id"].dropna())
    arrived = flow_totals[flow_totals["event_type"] == "arrived"]
    assert (arrived["duration_periods"] == arrived["end_period"] - arrived["start_period"]).all()


def test_flow_totals_skip_stockout_losses():
    # A stockout loss has flow_id NA (the trip never departed): it must appear
    # in the panel as lost_demand but produce no flow_totals row.
    resolved = scenarios.stockout()
    journal, _ = scenarios.run(resolved)
    tables = _build_tables(resolved, journal)

    assert journal["flow_id"].isna().any()
    assert tables["flow_totals"]["flow_id"].notna().all()
    assert tables["flow_totals"]["flow_id"].is_unique
    lost_quantity = journal.loc[journal["flow_id"].isna(), "quantity"].sum()
    assert tables["panel"]["lost_demand"].sum() == lost_quantity


def test_save_and_load_round_trip(tmp_path):
    resolved = scenarios.overflow()
    journal, _ = scenarios.run(resolved)
    meta = _save_run("overflow", resolved, journal, tmp_path)

    assert artifacts.list_runs(tmp_path) == ["overflow"]
    panel = artifacts.load_run_table("overflow", "panel", tmp_path)
    assert panel["demand"].sum() == meta["totals"]["demand"]
    assert artifacts.load_run_meta("overflow", tmp_path)["run_name"] == "overflow"


# ---------------------------------------------------------------------------
# Streamlit pages
# ---------------------------------------------------------------------------
PAGES = [
    "home.py",
    "station_map.py",
    "trips_map.py",
    "costs.py",
    "distance_duration.py",
    "facility_detail.py",
]


@pytest.fixture(scope="module")
def runs_root(tmp_path_factory):
    """Two saved synthetic runs, so the comparison view has an A and a B."""
    root = tmp_path_factory.mktemp("runs")
    for name, builder in [("base", scenarios.overflow), ("scaled", scenarios.stockout)]:
        resolved = builder()
        journal, _ = scenarios.run(resolved)
        _save_run(name, resolved, journal, root)
    return root


def _run_page(page, runs_root, monkeypatch, compare):
    """Render one page under AppTest; return the finished test object."""
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("GBP_RUNS_ROOT", str(runs_root))
    at = AppTest.from_file(str(_REPO_ROOT / "app" / "views" / page), default_timeout=30)
    if compare:
        at.session_state["scenario_a_value"] = "base"
        at.session_state["scenario_b_value"] = "scaled"
    at.run()
    return at


@pytest.mark.parametrize("compare", [False, True], ids=["single", "compare"])
@pytest.mark.parametrize("page", PAGES)
def test_page_renders_without_exception(page, compare, runs_root, monkeypatch):
    at = _run_page(page, runs_root, monkeypatch, compare)
    assert not at.exception, at.exception[0].value if at.exception else None
