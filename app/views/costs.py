"""Cost charts: whole-run total, per period, per commodity, per facility."""

import streamlit as st
import ui_shared

st.title("Costs")

run_a, run_b = ui_shared.pick_scenario_pair()
if run_a is None:
    st.stop()

level = st.selectbox("Detail level", ui_shared.LEVELS)

frames = {run_a: ui_shared.load_table(run_a, "flow_totals")}
if run_b:
    frames[run_b] = ui_shared.load_table(run_b, "flow_totals")

if level == ui_shared.LEVEL_GLOBAL:
    columns = st.columns(len(frames) + 1)
    values = {}
    for column, (run_name, flow_totals) in zip(columns, frames.items(), strict=False):
        values[run_name] = float(flow_totals["cost"].sum())
        column.metric(f"Cost, $ — {run_name}", f"${ui_shared.fmt_int(values[run_name])}")
    if run_b:
        diff = values[run_b] - values[run_a]
        columns[-1].metric(
            "Difference (B − A)",
            f"{'+' if diff >= 0 else '-'}${ui_shared.fmt_int(abs(diff))}",
        )
else:
    facilities = None
    if level == ui_shared.LEVEL_FACILITY:
        options = sorted(frames[run_a]["source_id"].dropna().unique())
        facilities = st.multiselect(
            "Origin facilities (source_id)",
            options,
            default=ui_shared.top_facilities(frames[run_a], "cost"),
        )
        if not facilities:
            st.info("Pick at least one facility.")
            st.stop()
    data = ui_shared.aggregate_flow_totals(frames, "cost", "sum", level, facilities)
    fig = ui_shared.level_line_chart(
        data, "cost", "Cost per period, $", ui_shared.scenario_color_map(run_a, run_b)
    )
    st.plotly_chart(fig, width="stretch")
    with st.expander("Data table"):
        st.dataframe(data, hide_index=True, width="stretch")

st.caption(
    "A trip's cost is rate × elapsed_periods × hours per period (flows_with_costs); the trip "
    "is attributed to the period and facility it departed from (start_period, source_id). "
    "Demand lost to a stockout costs nothing: the trip never departed."
)
