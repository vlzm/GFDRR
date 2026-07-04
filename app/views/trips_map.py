"""Trips map: arcs of the flows riding in the chosen period, colored by outcome."""

import pandas as pd
import pydeck as pdk
import streamlit as st
import ui_shared

st.title("Trips map")

run_a, run_b = ui_shared.pick_scenario_pair()
if run_a is None:
    st.stop()

meta_a = ui_shared.load_meta(run_a)
meta_b = ui_shared.load_meta(run_b) if run_b else None
facilities = ui_shared.load_table(run_a, "facilities")

OUTCOME_LABELS = {
    "arrived": "Arrived (arrived)",
    "redirected": "Redirected (redirected)",
    "lost": "Lost (lost)",
}

# --- Controls -----------------------------------------------------------------
left, right = st.columns([1, 2])
commodity_pick = left.selectbox(
    "Bike type",
    ui_shared.commodity_options(ui_shared.load_table(run_a, "panel")),
)
commodity = None if commodity_pick == ui_shared.COMMODITY_ALL else commodity_pick
outcomes = right.multiselect(
    "How the arc ended",
    list(OUTCOME_LABELS),
    default=list(OUTCOME_LABELS),
    format_func=OUTCOME_LABELS.get,
)
period = st.slider("Period (period_id)", 0, ui_shared.slider_max_period(meta_a, meta_b), 0)
ui_shared.slider_time_caption(period, meta_a, meta_b)


def _arc_rows(run_name: str) -> pd.DataFrame:
    """Arcs riding in the chosen period, grouped by (source, target, outcome)."""
    arcs = ui_shared.load_table(run_name, "arcs")
    active = arcs[(arcs["start_period"] <= period) & (arcs["end_period"] >= period)]
    if commodity is not None:
        active = active[active["commodity_category"] == commodity]
    active = active[active["event_type"].isin(outcomes)]
    # The endpoint coordinates are saved on every arc row, so grouping keeps them.
    grouped = active.groupby(["source_id", "target_id", "event_type"], as_index=False).agg(
        trips=("quantity", "sum"),
        distance_km=("distance_km", "mean"),
        source_lat=("source_lat", "first"),
        source_lng=("source_lng", "first"),
        target_lat=("target_lat", "first"),
        target_lng=("target_lng", "first"),
    )
    grouped["color"] = grouped["event_type"].map(
        lambda outcome: [*ui_shared.hex_to_rgb(ui_shared.OUTCOME_COLORS[outcome]), 190]
    )
    grouped["outcome_label"] = grouped["event_type"].map(OUTCOME_LABELS)
    grouped["distance_km"] = grouped["distance_km"].round(2)
    return grouped


def _deck(rows: pd.DataFrame) -> pdk.Deck:
    """Build the arc map of one scenario's rows."""
    layer = pdk.Layer(
        "ArcLayer",
        data=rows,
        get_source_position="[source_lng, source_lat]",
        get_target_position="[target_lng, target_lat]",
        get_source_color="color",
        get_target_color="color",
        get_width="trips",
        width_scale=1,
        width_min_pixels=1.5,
        width_max_pixels=10,
        pickable=True,
    )
    view_state = pdk.ViewState(
        latitude=float(facilities["lat"].mean()),
        longitude=float(facilities["lng"].mean()),
        zoom=11,
    )
    return pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style=None,
        tooltip={
            "html": (
                "<b>{source_id} → {target_id}</b><br/>"
                "Trips: {trips}<br/>Outcome: {outcome_label}<br/>Distance: {distance_km} km"
            ),
            "style": {"backgroundColor": "#1a1a19", "color": "#ffffff", "fontSize": "12px"},
        },
    )


def _legend() -> None:
    """Colored-dot legend for the three outcomes (labels, not color alone)."""
    dots = " · ".join(
        f'<span style="color:{ui_shared.OUTCOME_COLORS[outcome]}">●</span> {label}'
        for outcome, label in OUTCOME_LABELS.items()
    )
    st.markdown(dots, unsafe_allow_html=True)


frames = {run_a: _arc_rows(run_a)}
if run_b:
    frames[run_b] = _arc_rows(run_b)

columns = st.columns(len(frames))
for column, (run_name, rows) in zip(columns, frames.items(), strict=True):
    with column:
        st.subheader(run_name)
        st.caption(f"Arcs riding in period {period}: {ui_shared.fmt_int(rows['trips'].sum())}")
        st.pydeck_chart(_deck(rows), height=560)

_legend()
st.caption(
    "An arc is one physical edge of a trip (flow_id, move_id): from the facility it left to "
    "the facility where the edge ended. Width is the number of trips on the edge this period."
)

with st.expander("Data table"):
    for run_name, rows in frames.items():
        st.markdown(f"**{run_name}**")
        st.dataframe(
            rows[["source_id", "target_id", "outcome_label", "trips", "distance_km"]],
            hide_index=True,
            width="stretch",
        )
