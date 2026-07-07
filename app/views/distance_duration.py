"""Distance and duration charts: whole-run, per period, per commodity, per facility."""

import artifacts
import streamlit as st
import ui_shared

st.title("Distance & duration")

run_a, run_b = ui_shared.pick_scenario_pair()
if run_a is None:
    st.stop()


def _duration_tile(value: float, meta: artifacts.RunMeta) -> str:
    """Whole-run tile text: the mean duration, also as wall-clock minutes."""
    minutes = value * meta.period_len_hours * 60
    return f"{value:.2f} periods (~{minutes:.0f} min)"


MEASURES = {
    "Distance, km (distance_km)": ui_shared.FlowTotalsView(
        value="distance_km",
        agg="sum",
        tile_label="Distance, km (distance_km)",
        y_title="Distance per period, km",
        totals_key="distance_km",
        fmt=lambda v: f"{ui_shared.fmt_int(v)} km",
    ),
    "Duration, periods (duration_periods)": ui_shared.FlowTotalsView(
        value="duration_periods",
        agg="mean",
        tile_label="Duration, periods (duration_periods)",
        y_title="Mean trip duration, periods",
        totals_key="mean_duration_periods",
        fmt=lambda v: f"{v:.2f} periods",
        fmt_global=_duration_tile,
        global_note=(
            "This is the mean over trips of duration_periods, which counts period "
            "edges: a trip that departs and docks inside one period has duration 0; "
            "a trip that ends in the next period has 1. Most trips are shorter than "
            "one period, so the mean sits near 0.2 and barely moves between "
            "scenarios — the demand multiplier changes how many trips ride, not "
            "how long each one takes."
        ),
    ),
}

left, right = st.columns(2)
measure_label = left.radio("Measure", list(MEASURES), horizontal=True)
level = right.selectbox("Detail level", ui_shared.LEVELS)
ui_shared.flow_totals_page(run_a, run_b, MEASURES[measure_label], level)

modes = {name: ui_shared.load_meta(name).routing_mode for name in filter(None, [run_a, run_b])}
modes_text = "; ".join(f"{name}: {mode}" for name, mode in modes.items())
st.caption(
    "Distance is the sum of a trip's arc lengths, measured by the run's routing mode "
    f"(haversine = straight line, osrm = road network) — {modes_text}. "
    "Duration is duration_periods on the trip's terminal event. A trip is attributed to the "
    "period and facility it departed from (start_period, source_id)."
)
