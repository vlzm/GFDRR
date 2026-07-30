"""Smoke tests for the UI layer: artifact building and every Streamlit page.

The artifact tables are built from the synthetic offline scenarios in
``tests/scenarios.py`` (no CSV, no network). The Streamlit pages then run
against two saved artifacts through ``streamlit.testing.v1.AppTest``: the test
asserts each page renders without an exception, for both the single-scenario
and the comparison view.
"""

import pathlib
import sys
import types

import pandas as pd
import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "app"))

import runner  # noqa: E402  (needs the app folder on sys.path)

from gbp import artifacts  # noqa: E402
from gbp.model import flows_to_panel  # noqa: E402
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
        routes=resolved.routes,
    )


def _save_run(name, resolved, journal, root):
    """Save one run artifact under ``root``; return its loaded meta.

    ``save_scenario_run`` is the same operation the runner and the evaluation
    use, so the saved folder matches the real ``meta.json`` contract (``t0``
    and ``routing_mode`` included) and the page tests exercise the same path
    as a real run. The synthetic scenario carries no rates, routing fields,
    or raw file, so they are supplied here (``trips_path=None`` means no raw
    file to record in ``inputs``).
    """
    result = types.SimpleNamespace(
        simulated_flows_df=journal,
        initial_inventory_df=resolved.initial_inventory_df,
        facilities_capacities_df=resolved.facilities_capacities_df,
        violations=[],
    )
    data = types.SimpleNamespace(
        facilities_df=resolved.facilities_df,
        facilities_geo_df=resolved.facilities_geo_df,
        commodities_categories_rates_df=RATES,
        period_len=PERIOD_LEN,
        routes=resolved.routes,
        routing_mode="haversine",
        t0=resolved.periods_df["start_timestamp"].iloc[0],
        trips_path=None,
    )
    artifacts.save_scenario_run(
        result,
        data,
        runner.RunRequest(
            run_name=name,
            number_of_periods=int(resolved.periods_df["period_id"].max()) + 1,
        ),
        root=root,
    )
    return artifacts.load_run_meta(name, root)


# ---------------------------------------------------------------------------
# Artifact builders
# ---------------------------------------------------------------------------
def test_panel_counts_match_the_journal():
    resolved = scenarios.overflow()
    journal, _ = scenarios.run(resolved)
    panel = flows_to_panel(journal, resolved.initial_inventory_df)

    from gbp.model import is_docking, is_user_departure

    assert panel["departed"].sum() == journal[is_user_departure(journal)]["quantity"].sum()
    assert panel["arrived"].sum() == journal[is_docking(journal)]["quantity"].sum()
    assert (panel["demand"] == panel["departed"] + panel["lost_demand"]).all()
    # The overflow scenario bounces at least one bike off a full station.
    assert panel["redirected"].sum() > 0


def test_stockout_shows_up_as_lost_demand():
    resolved = scenarios.stockout()
    journal, _ = scenarios.run(resolved)
    panel = flows_to_panel(journal, resolved.initial_inventory_df)
    assert panel["lost_demand"].sum() > 0
    assert (panel["demand"] == panel["departed"] + panel["lost_demand"]).all()


def test_arcs_pair_every_departed_with_its_close():
    resolved = scenarios.redirect_chain()
    journal, _ = scenarios.run(resolved)
    arcs = artifacts.build_arcs(journal, resolved.routes, resolved.facilities_geo_df)

    n_departed = int((journal["event_type"] == "departed").sum())
    assert len(arcs) == n_departed
    assert arcs["target_id"].notna().all()
    assert (arcs["distance_km"] >= 0).all()
    # The endpoint coordinates are saved on the arcs, so the trips map needs no join.
    for column in ["source_lat", "source_lng", "target_lat", "target_lng"]:
        assert arcs[column].notna().all()
    # The chain scenario has a flow with more than one arc.
    assert arcs.groupby("flow_id")["move_id"].count().max() >= 2
    # No trucks in this scenario: every arc is a user ride with no resource.
    assert (arcs["flow_type"] == "user_trip").all()
    assert arcs["resource_id"].isna().all()


def test_arcs_keep_the_truck_on_rebalance_moves():
    # A bike carried by a truck must stay tellable apart from user rides on
    # the arcs table: the truck trips page filters on flow_type and shows the
    # truck (resource_id) on each arc.
    from gbp.model import rebalance_arrived_events, rebalance_departed_events

    resolved = scenarios.canonical()
    journal, _ = scenarios.run(resolved)
    pickups = pd.DataFrame(
        {
            "flow_id": ["rb_1_0"],
            "source_id": ["s1"],
            "planned_target_id": ["s2"],
            "commodity_category": [scenarios.CLASSIC],
            "resource_id": ["truck_1"],
            "start_period": [1],
            "planned_end_period": [2],
        }
    )
    with_truck = pd.concat(
        [
            journal,
            rebalance_departed_events(pickups),
            rebalance_arrived_events(pickups.assign(realized_target_id="s2"), 2),
        ],
        ignore_index=True,
    )
    arcs = artifacts.build_arcs(with_truck, resolved.routes, resolved.facilities_geo_df)

    truck_arcs = arcs[arcs["flow_type"] == "rebalance"]
    assert len(truck_arcs) == 1
    assert truck_arcs["resource_id"].iloc[0] == "truck_1"
    assert truck_arcs["source_id"].iloc[0] == "s1"
    assert truck_arcs["target_id"].iloc[0] == "s2"
    assert arcs.loc[arcs["flow_type"] == "user_trip", "resource_id"].isna().all()


def test_flows_table_carries_the_measures():
    # flows.parquet is the journal widened by flows_with_measures: every event
    # row carries the duration, distance and money columns (Notations.md §6.1).
    resolved = scenarios.canonical()
    journal, _ = scenarios.run(resolved)
    flows = _build_tables(resolved, journal)["flows"]

    arrived = flows[flows["event_type"] == "arrived"]
    assert (
        arrived["realized_duration_periods"]
        == arrived["realized_end_period"] - arrived["start_period"]
    ).all()
    assert (
        arrived["planned_duration_periods"]
        == arrived["planned_end_period"] - arrived["start_period"]
    ).all()
    assert arrived["planned_distance_km"].notna().all()
    assert arrived["realized_distance_km"].notna().all()
    assert (arrived["cost"] == arrived["rate"] * arrived["elapsed_periods"]).all()


def test_flow_totals_one_row_per_flow():
    resolved = scenarios.overflow()
    journal, _ = scenarios.run(resolved)
    tables = _build_tables(resolved, journal)
    flow_totals = tables["flow_totals"]

    assert flow_totals["flow_id"].is_unique
    assert set(flow_totals["flow_id"]) == set(journal["flow_id"].dropna())
    assert (flow_totals["flow_type"] == "user_trip").all()
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
    assert panel["demand"].sum() == meta.totals["demand"]
    assert artifacts.load_run_meta("overflow", tmp_path).run_name == "overflow"
    # The file paths of an artifact come from artifacts.py alone; every table
    # the saver wrote sits where table_path points.
    for table in artifacts.RUN_TABLES:
        assert artifacts.table_path("overflow", table, tmp_path).exists()
    assert artifacts.meta_path("overflow", tmp_path).exists()
    with pytest.raises(ValueError):
        artifacts.table_path("overflow", "not_a_table", tmp_path)


def test_next_free_run_name_versions_taken_names(tmp_path):
    # The Run page names runs from the parameters; a taken name must get a
    # _version_{i} suffix instead of overwriting the saved artifact.
    resolved = scenarios.overflow()
    journal, _ = scenarios.run(resolved)
    _save_run("overflow", resolved, journal, tmp_path)

    assert artifacts.next_free_run_name("fresh", tmp_path) == "fresh"
    assert artifacts.next_free_run_name("overflow", tmp_path) == "overflow_version_2"
    _save_run("overflow_version_2", resolved, journal, tmp_path)
    assert artifacts.next_free_run_name("overflow", tmp_path) == "overflow_version_3"


def test_every_kpi_metric_has_a_total():
    # The KPI row reads meta["totals"][metric.name] for every kpi=True metric,
    # so each of them must get a value in build_totals.
    resolved = scenarios.overflow()
    journal, _ = scenarios.run(resolved)
    tables = _build_tables(resolved, journal)
    totals = artifacts.build_totals(tables["panel"], tables["flow_totals"])
    for metric in artifacts.METRICS:
        if metric.kpi:
            assert metric.name in totals, f"KPI metric {metric.name} missing from totals"
        if metric.panel_value:
            assert metric.name in tables["panel"].columns


def test_meta_carries_t0_and_the_run_parameters():
    # build_meta is the one place that defines the meta.json contract; this
    # pins the fields the UI reads, t0 and routing_mode included.
    resolved = scenarios.canonical()
    journal, _ = scenarios.run(resolved)
    tables = _build_tables(resolved, journal)
    meta = artifacts.build_meta(
        tables,
        run_name="canonical",
        demand_scale_factor=1.0,
        sizing_scale_factor=1.0,
        number_of_periods=len(resolved.periods_df),
        period_len_hours=1.0,
        routing_mode="haversine",
        t0=resolved.periods_df["start_timestamp"].iloc[0],
        inputs=["202601-citibike-tripdata_1.csv"],
        violations=[],
    )
    assert meta.t0 == "2026-01-01T00:00:00"
    assert meta.routing_mode == "haversine"
    assert meta.scenario_id == "canonical"
    assert meta.inputs == ["202601-citibike-tripdata_1.csv"]
    # build_meta reads the code version from git itself; in a git checkout it
    # is a commit hash, outside git it is "unknown" -- never empty.
    assert meta.code_version
    assert meta.totals == artifacts.build_totals(tables["panel"], tables["flow_totals"])
    # The sized state is precomputed into meta.json, so readers (the two-level
    # evaluation) never sum the panel or the facilities table for it. In a
    # station-only scenario it equals the sums of the input state tables.
    assert meta.initial_inventory_total == int(resolved.initial_inventory_df["quantity"].sum())
    assert meta.station_capacity_total == int(resolved.facilities_capacities_df["capacity"].sum())
    # t0 + period * period_len is what the pages show on the time axis.
    t0 = pd.Timestamp(meta.t0)
    assert t0 + 3 * pd.Timedelta(hours=meta.period_len_hours) == pd.Timestamp("2026-01-01T03:00:00")


# ---------------------------------------------------------------------------
# Streamlit pages
# ---------------------------------------------------------------------------
PAGES = [
    "home.py",
    "run_scenario.py",
    "station_map.py",
    "trips_map.py",
    "truck_trips.py",
    "costs.py",
    "distance_duration.py",
    "facility_detail.py",
    "downloads.py",
]


@pytest.fixture(scope="module")
def runs_root(tmp_path_factory):
    """Two saved synthetic runs, so the comparison view has an A and a B."""
    root = tmp_path_factory.mktemp("data") / "runs"
    for name, builder in [("base", scenarios.overflow), ("scaled", scenarios.stockout)]:
        resolved = builder()
        journal, _ = scenarios.run(resolved)
        _save_run(name, resolved, journal, root)
    return root


def _run_page(page, runs_root, monkeypatch, compare):
    """Render one page under AppTest; return the finished test object."""
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("DATA_DIR", str(runs_root.parent))
    # The pages must render on the disk backend, whatever the outer environment.
    monkeypatch.delenv("API_URL", raising=False)
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


# ---------------------------------------------------------------------------
# ui_shared helpers (pure functions, tested without a page render)
# ---------------------------------------------------------------------------
def test_delta_b_minus_a_owns_direction_and_sign():
    import ui_shared

    assert ui_shared.delta_b_minus_a(10, 12) == "+2"
    assert ui_shared.delta_b_minus_a(12, 10) == "-2"
    assert ui_shared.delta_b_minus_a(5, 5) == "+0"


def _meta_with_rebalancing(rebalancing: dict) -> artifacts.RunMeta:
    """Build a minimal valid RunMeta carrying the given rebalancing block."""
    return artifacts.RunMeta(
        run_name="r",
        scenario_id="r",
        demand_scale_factor=1.0,
        sizing_scale_factor=1.0,
        number_of_periods=1,
        period_len_hours=1.0,
        routing_mode="haversine",
        t0="2026-01-01T00:00:00",
        created_at="2026-01-01T00:00:00",
        inputs=[],
        code_version="unknown",
        violations=[],
        rebalancing=artifacts.RebalancingMeta.model_validate(rebalancing),
        totals={},
    )


def test_rebalancing_settings_reads_the_block_and_falls_back():
    import ui_shared

    off = ui_shared.rebalancing_settings(_meta_with_rebalancing({"enabled": False}))
    assert off.enabled is False and off.truck_homes == []
    meta = _meta_with_rebalancing({"enabled": True, "truck_homes": ["depot_1", "depot_1"]})
    settings = ui_shared.rebalancing_settings(meta)
    assert settings.enabled is True
    assert len(settings.truck_homes) == 2


def test_arc_map_rows_sums_quantity_and_keeps_endpoints():
    import ui_shared

    arcs = pd.DataFrame(
        {
            "source_id": ["s1", "s1", "s1"],
            "target_id": ["s2", "s2", "s3"],
            "event_type": ["arrived", "arrived", "arrived"],
            "quantity": [1, 1, 1],
            "distance_km": [2.0, 2.0, 4.0],
            "source_lat": [40.0, 40.0, 40.0],
            "source_lng": [-74.0, -74.0, -74.0],
            "target_lat": [40.1, 40.1, 40.2],
            "target_lng": [-74.1, -74.1, -74.2],
        }
    )
    rows = ui_shared.arc_map_rows(arcs, ["source_id", "target_id", "event_type"], "trips")
    assert rows["trips"].tolist() == [2, 1]
    assert rows["target_lat"].tolist() == [40.1, 40.2]
    assert rows["distance_km"].tolist() == [2.0, 4.0]


def test_metric_labels_and_flow_values_come_from_metrics():
    # The KPI tile shows the metric's title; the full picker label adds the
    # canonical column name in braces. No page parses one out of the other.
    demand = next(metric for metric in artifacts.METRICS if metric.name == "demand")
    assert demand.title == "Demand"
    assert demand.label == "Demand (demand)"
    # The inventory flag lives in METRICS, so a page picks the per-period flow
    # columns without naming quantity_sop / quantity_eop itself.
    assert set(artifacts.PANEL_VALUES) - set(artifacts.PANEL_FLOW_VALUES) == {
        "quantity_sop",
        "quantity_eop",
    }


def _tiny_panel(demand: dict[str, int]) -> pd.DataFrame:
    """One-period panel with the given demand per facility, zeros elsewhere."""
    frame = pd.DataFrame(
        {
            "period_id": 0,
            "facility_id": list(demand),
            "commodity_category": "classic_bike",
            **dict.fromkeys(artifacts.PANEL_VALUES, 0),
        }
    )
    frame["demand"] = list(demand.values())
    return frame


def test_panel_slice_pair_carries_a_b_and_the_difference():
    import ui_shared

    pair = ui_shared.panel_slice_pair(
        _tiny_panel({"s1": 2, "s2": 3}),
        _tiny_panel({"s1": 5, "s3": 7}),
        period=0,
        commodity=None,
    ).set_index("facility_id")

    assert pair.loc["s1", "demand"] == 2
    assert pair.loc["s1", ui_shared.value_b("demand")] == 5
    assert pair.loc["s1", ui_shared.value_diff("demand")] == 3
    # A facility only one run touched reads as zeros in the other run.
    assert pair.loc["s2", ui_shared.value_b("demand")] == 0
    assert pair.loc["s2", ui_shared.value_diff("demand")] == -3
    assert pair.loc["s3", "demand"] == 0
    assert pair.loc["s3", ui_shared.value_diff("demand")] == 7


def test_build_totals_covers_every_metric_with_a_total():
    # Every METRICS entry that declares a total (panel or flow) must land in
    # meta["totals"] -- no metric total is computed anywhere else.
    resolved = scenarios.overflow()
    journal, _ = scenarios.run(resolved)
    tables = _build_tables(resolved, journal)
    totals = artifacts.build_totals(tables["panel"], tables["flow_totals"])
    expected = {m.name for m in artifacts.METRICS if m.panel_total or m.flow_value}
    assert expected <= set(totals)
