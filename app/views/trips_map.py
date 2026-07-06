"""Trips map: arcs of the flows riding in the chosen period, colored by outcome."""

import pandas as pd
import streamlit as st
import ui_shared

st.title("Trips map")

run_a, run_b = ui_shared.pick_scenario_pair()
if run_a is None:
    st.stop()

meta_a = ui_shared.load_meta(run_a)
meta_b = ui_shared.load_meta(run_b) if run_b else None
facilities = ui_shared.load_facilities(run_a)

OUTCOME_LABELS = {
    "arrived": "Arrived (arrived)",
    "redirected": "Redirected (redirected)",
    "lost": "Lost (lost)",
}

# --- Controls -----------------------------------------------------------------
left, right = st.columns([1, 2])
commodity_pick = left.selectbox(
    "Bike type",
    ui_shared.commodity_options(ui_shared.load_panel(run_a)),
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


TOOLTIP = (
    "<b>{source_id} → {target_id}</b><br/>"
    "Trips: {trips}<br/>Outcome: {outcome_label}<br/>Distance: {distance_km} km"
)


def _arc_rows(run_name: str) -> pd.DataFrame:
    """Bike-trip arcs riding in the chosen period, grouped by (source, target, outcome)."""
    # Truck moves (flow_type == "rebalance") have their own page.
    arcs = ui_shared.load_arcs(run_name, flow_type="user_trip")
    active = arcs[(arcs["start_period"] <= period) & (arcs["end_period"] >= period)]
    if commodity is not None:
        active = active[active["commodity_category"] == commodity]
    active = active[active["event_type"].isin(outcomes)]
    grouped = ui_shared.arc_map_rows(active, ["source_id", "target_id", "event_type"], "trips")
    grouped["color"] = grouped["event_type"].map(
        lambda outcome: [*ui_shared.hex_to_rgb(ui_shared.OUTCOME_COLORS[outcome]), 190]
    )
    grouped["outcome_label"] = grouped["event_type"].map(OUTCOME_LABELS)
    grouped["distance_km"] = grouped["distance_km"].round(2)
    return grouped


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
        st.pydeck_chart(
            ui_shared.arc_deck(rows, facilities, "trips", TOOLTIP),
            height=560,
        )

_legend()
st.caption(
    "An arc is one physical edge of a trip (flow_id, move_id): from the facility it left to "
    "the facility where the edge ended. Width is the number of trips on the edge this period. "
    "Only user rides are shown; bikes moved by truck are on the Truck trips page."
)

with st.expander("Data table"):
    for run_name, rows in frames.items():
        st.markdown(f"**{run_name}**")
        st.dataframe(
            rows[["source_id", "target_id", "outcome_label", "trips", "distance_km"]],
            hide_index=True,
            width="stretch",
        )
