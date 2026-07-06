"""Cost charts: whole-run total, per period, per commodity, per facility."""

import streamlit as st
import ui_shared

st.title("Costs")

run_a, run_b = ui_shared.pick_scenario_pair()
if run_a is None:
    st.stop()

COST_VIEW = ui_shared.FlowTotalsView(
    value="cost",
    agg="sum",
    tile_label="Cost, $",
    y_title="Cost per period, $",
    totals_key="cost",
    fmt=lambda v: f"${ui_shared.fmt_int(v)}",
)

level = st.selectbox("Detail level", ui_shared.LEVELS)
ui_shared.flow_totals_page(run_a, run_b, COST_VIEW, level)

st.caption(
    "A trip's cost is rate × elapsed_periods × hours per period (flows_with_costs); the trip "
    "is attributed to the period and facility it departed from (start_period, source_id). "
    "Demand lost to a stockout costs nothing: the trip never departed."
)
