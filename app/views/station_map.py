"""Station map: one period, one metric; hover shows every metric for A and B."""

import pandas as pd
import pydeck as pdk
import streamlit as st
import ui_shared
from ui_shared import PANEL_VALUES

st.title("Station map")

run_a, run_b = ui_shared.pick_scenario_pair()
if run_a is None:
    st.stop()

meta_a = ui_shared.load_meta(run_a)
meta_b = ui_shared.load_meta(run_b) if run_b else None
panel_a = ui_shared.load_panel(run_a)
facilities = ui_shared.load_facilities(run_a)

VIEW_A = f"A: {run_a}"
VIEW_B = f"B: {run_b}" if run_b else None
VIEW_DIFF = "Difference (B − A)"

# --- Controls ---------------------------------------------------------------
left, middle, right = st.columns([2, 1, 2])
metric = left.selectbox(
    "Metric (dot color and size)",
    PANEL_VALUES,
    format_func=ui_shared.METRIC_LABELS.get,
)
commodity_pick = middle.selectbox("Bike type", ui_shared.commodity_options(panel_a))
commodity = None if commodity_pick == ui_shared.COMMODITY_ALL else commodity_pick

view = VIEW_A
if run_b:
    view = right.radio("View", [VIEW_A, VIEW_B, VIEW_DIFF], horizontal=True)

period = st.slider("Period (period_id)", 0, ui_shared.slider_max_period(meta_a, meta_b), 0)
ui_shared.slider_time_caption(period, meta_a, meta_b)
show_depots = st.toggle("Show depots", value=False)

# --- Data: one row per facility with A (and B and B − A) metric columns -----
if run_b:
    values = ui_shared.panel_slice_pair(panel_a, ui_shared.load_panel(run_b), period, commodity)
    value_columns = PANEL_VALUES + ui_shared.PANEL_VALUES_B + ui_shared.PANEL_VALUES_DIFF
else:
    values = ui_shared.panel_slice(panel_a, period, commodity)
    value_columns = PANEL_VALUES
data = facilities.merge(values, on="facility_id", how="left")
if not show_depots:
    data = data[data["facility_category"] == "station"]
data[value_columns] = data[value_columns].fillna(0)

if view == VIEW_A:
    data["value"] = data[metric]
elif view == VIEW_B:
    data["value"] = data[ui_shared.value_b(metric)]
else:
    data["value"] = data[ui_shared.value_diff(metric)]

# --- Color and radius ---------------------------------------------------------
diverging = view == VIEW_DIFF
if diverging:
    data["color"] = ui_shared.diverging_colors(data["value"])
else:
    data["color"] = ui_shared.sequential_colors(data["value"])

magnitude = data["value"].abs()
top = magnitude.max() or 1.0
data["radius"] = 30 + 170 * (magnitude / top) ** 0.5


def _tooltip_html(row: pd.Series) -> str:
    """Hover box: every panel metric of this facility, for A (and B and B − A)."""
    header = (
        f"<b>{row['facility_id']}</b> ({row['facility_category']}) · "
        f"capacity {int(row['capacity'])}"
    )
    if run_b:
        lines = ["<tr><th></th><th>A</th><th>B</th><th>B − A</th></tr>"]
    else:
        lines = []
    for name in PANEL_VALUES:
        label = ui_shared.METRIC_SHORT[name]
        value_a = int(row[name])
        if run_b:
            value_b = int(row[ui_shared.value_b(name)])
            diff = int(row[ui_shared.value_diff(name)])
            lines.append(
                f"<tr><td>{label}</td><td>{value_a}</td><td>{value_b}</td><td>{diff:+d}</td></tr>"
            )
        else:
            lines.append(f"<tr><td>{label}</td><td>{value_a}</td></tr>")
    return header + "<table>" + "".join(lines) + "</table>"


data["tooltip_html"] = data.apply(_tooltip_html, axis=1)

# --- Deck ---------------------------------------------------------------------
layer = pdk.Layer(
    "ScatterplotLayer",
    data=data,
    get_position="[lng, lat]",
    get_fill_color="color",
    get_radius="radius",
    radius_min_pixels=2,
    radius_max_pixels=35,
    pickable=True,
    stroked=False,
)
view_state = pdk.ViewState(
    latitude=float(data["lat"].mean()),
    longitude=float(data["lng"].mean()),
    zoom=11,
)
deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    map_style=None,  # Streamlit substitutes a style that matches its theme
    tooltip={
        "html": "{tooltip_html}",
        "style": {"backgroundColor": "#1a1a19", "color": "#ffffff", "fontSize": "12px"},
    },
)
st.pydeck_chart(deck, height=620)

metric_label = ui_shared.METRIC_LABELS[metric]
if diverging:
    st.caption(
        f"{metric_label}, period {period}: a blue dot means less in B than in A; red means "
        "more; gray means no change. Dot size is the size of the difference."
    )
else:
    st.caption(
        f"{metric_label}, period {period}: the darker the blue and the larger the dot, the "
        f"higher the value (map maximum: {ui_shared.fmt_int(data['value'].max())})."
    )

with st.expander("Data table"):
    shown = ["facility_id", "facility_category", "capacity"] + PANEL_VALUES
    if run_b:
        shown += ui_shared.PANEL_VALUES_B
    st.dataframe(data[shown + ["value"]], hide_index=True, width="stretch")
