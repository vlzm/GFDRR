"""Distance and duration charts: whole-run, per period, per commodity, per facility."""

import streamlit as st
import ui_shared

st.title("Distance & duration")

run_a, run_b = ui_shared.pick_scenario_pair()
if run_a is None:
    st.stop()

MEASURES = {
    "Distance, km (distance_km)": ("distance_km", "sum", "Distance per period, km"),
    "Duration, periods (duration_periods)": (
        "duration_periods",
        "mean",
        "Mean trip duration, periods",
    ),
}

left, right = st.columns(2)
measure_label = left.radio("Measure", list(MEASURES), horizontal=True)
value, agg, y_title = MEASURES[measure_label]
level = right.selectbox("Detail level", ui_shared.LEVELS)

frames = {run_a: ui_shared.load_table(run_a, "flow_totals")}
if run_b:
    frames[run_b] = ui_shared.load_table(run_b, "flow_totals")


def _global_value(run_name: str) -> float:
    """Whole-run value from meta totals: total distance or the mean trip duration."""
    totals = ui_shared.load_meta(run_name)["totals"]
    key = "distance_km" if value == "distance_km" else "mean_duration_periods"
    return float(totals[key])


def _fmt(number: float) -> str:
    """Format the measure with its unit."""
    if value == "distance_km":
        return f"{ui_shared.fmt_int(number)} km"
    return f"{number:.2f} periods"


if level == ui_shared.LEVEL_GLOBAL:
    # The whole-run values are precomputed once (build_totals) and read from meta.
    columns = st.columns(len(frames) + 1)
    values = {}
    for column, run_name in zip(columns, frames, strict=False):
        values[run_name] = _global_value(run_name)
        column.metric(f"{measure_label} — {run_name}", _fmt(values[run_name]))
    if run_b:
        diff = values[run_b] - values[run_a]
        columns[-1].metric("Difference (B − A)", f"{'+' if diff >= 0 else '-'}{_fmt(abs(diff))}")
else:
    facilities = None
    if level == ui_shared.LEVEL_FACILITY:
        options = sorted(frames[run_a]["source_id"].dropna().unique())
        facilities = st.multiselect(
            "Origin facilities (source_id)",
            options,
            default=ui_shared.top_facilities(frames[run_a], value),
        )
        if not facilities:
            st.info("Pick at least one facility.")
            st.stop()
    data = ui_shared.aggregate_flow_totals(frames, value, agg, level, facilities)
    fig = ui_shared.level_line_chart(
        data, value, y_title, ui_shared.scenario_color_map(run_a, run_b)
    )
    st.plotly_chart(fig, width="stretch")
    with st.expander("Data table"):
        st.dataframe(data, hide_index=True, width="stretch")

modes = {name: ui_shared.load_meta(name)["routing_mode"] for name in frames}
modes_text = "; ".join(f"{name}: {mode}" for name, mode in modes.items())
st.caption(
    "Distance is the sum of a trip's arc lengths, measured by the run's routing mode "
    f"(haversine = straight line, osrm = road network) — {modes_text}. "
    "Duration is duration_periods on the trip's terminal event. A trip is attributed to the "
    "period and facility it departed from (start_period, source_id)."
)
