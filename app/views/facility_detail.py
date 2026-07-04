"""One facility: inventory over periods, any metric per period, the raw panel rows."""

import pandas as pd
import plotly.express as px
import streamlit as st
import ui_shared
from artifacts import PANEL_KEYS, PANEL_VALUES

st.title("Single facility")

run_a, run_b = ui_shared.pick_scenario_pair()
if run_a is None:
    st.stop()

facilities = ui_shared.load_table(run_a, "facilities")
panel_a = ui_shared.load_table(run_a, "panel")

left, right = st.columns([2, 1])
facility_id = left.selectbox("Facility (facility_id)", sorted(facilities["facility_id"]))
commodity_pick = right.selectbox("Bike type", ui_shared.commodity_options(panel_a))
commodity = None if commodity_pick == ui_shared.COMMODITY_ALL else commodity_pick

capacity = int(facilities.set_index("facility_id").loc[facility_id, "capacity"])
st.caption(f"Dock capacity: {capacity}")


def _facility_rows(run_name: str):
    """Collect the facility's panel rows over periods, summed over commodities if needed."""
    panel = ui_shared.load_table(run_name, "panel")
    rows = panel[panel["facility_id"] == facility_id]
    if commodity is not None:
        rows = rows[rows["commodity_category"] == commodity]
    rows = rows.groupby("period_id", as_index=False)[PANEL_VALUES].sum()
    rows["scenario"] = run_name
    return rows


frames = [_facility_rows(run_a)]
if run_b:
    frames.append(_facility_rows(run_b))
data = pd.concat(frames, ignore_index=True)

color_map = ui_shared.scenario_color_map(run_a, run_b)

st.subheader("Inventory at period end (quantity_eop)")
fig = px.line(
    data,
    x="period_id",
    y="quantity_eop",
    color="scenario",
    color_discrete_map=color_map,
)
fig.update_traces(line_width=2)
fig.add_hline(
    y=capacity,
    line_dash="dot",
    line_color=ui_shared.INK_SECONDARY,
    annotation_text="capacity",
)
fig.update_xaxes(title="Period (period_id)")
fig.update_yaxes(title="Bikes in docks")
ui_shared.style_fig(fig)
st.plotly_chart(fig, width="stretch")

st.subheader("Metric per period")
metric = st.selectbox(
    "Metric",
    [name for name in PANEL_VALUES if name not in ("quantity_sop", "quantity_eop")],
    format_func=ui_shared.METRIC_LABELS.get,
)
bars = px.bar(
    data,
    x="period_id",
    y=metric,
    color="scenario",
    color_discrete_map=color_map,
    barmode="group",
)
bars.update_xaxes(title="Period (period_id)")
bars.update_yaxes(title=ui_shared.METRIC_LABELS[metric])
ui_shared.style_fig(bars)
st.plotly_chart(bars, width="stretch")

with st.expander("Panel rows of this facility"):
    for run_name in filter(None, [run_a, run_b]):
        panel = ui_shared.load_table(run_name, "panel")
        rows = panel[panel["facility_id"] == facility_id]
        if commodity is not None:
            rows = rows[rows["commodity_category"] == commodity]
        st.markdown(f"**{run_name}**")
        st.dataframe(rows[PANEL_KEYS + PANEL_VALUES], hide_index=True, width="stretch")
