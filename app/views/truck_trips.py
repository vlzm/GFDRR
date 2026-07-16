"""Truck trips: the bikes trucks moved during overnight rebalancing, on a map."""

import pandas as pd
import streamlit as st
import ui_shared

st.title("Truck trips")
st.caption(
    "One arc per truck move: from the station where the truck picked bikes up to "
    "the station where it dropped them (Notations.md §14). Color is the truck; "
    "width is the number of bikes on the move."
)

run_a, run_b = ui_shared.pick_scenario_pair()
if run_a is None:
    st.stop()

facilities = ui_shared.load_facilities(run_a)


def _truck_moves(run_name: str) -> pd.DataFrame | None:
    """Truck moves, one row per (truck, source, target, periods); None if arcs lack flow_type."""
    moves = ui_shared.load_arcs(run_name, flow_type="rebalance")
    if moves is None:
        return None
    return ui_shared.arc_map_rows(
        moves, ["resource_id", "source_id", "target_id", "start_period", "end_period"], "bikes"
    )


def _truck_color_map(frames: dict[str, pd.DataFrame]) -> dict[str, str]:
    """One fixed color per truck id, shared by both scenarios, in id order."""
    trucks = sorted(set().union(*(set(rows["resource_id"]) for rows in frames.values())))
    palette = ui_shared.TRUCK_COLORS
    return {
        truck: palette[i] if i < len(palette) else ui_shared.TRUCK_OTHER_COLOR
        for i, truck in enumerate(trucks)
    }


TOOLTIP = (
    "<b>{resource_id}</b>: {source_id} → {target_id}<br/>"
    "Bikes: {bikes}<br/>Picked up period {start_period}, "
    "dropped period {end_period}<br/>Distance: {distance_km} km"
)

frames: dict[str, pd.DataFrame | None] = {run_a: _truck_moves(run_a)}
if run_b:
    frames[run_b] = _truck_moves(run_b)

drawable = {name: rows for name, rows in frames.items() if rows is not None and not rows.empty}
colors = _truck_color_map(drawable) if drawable else {}

columns = st.columns(len(frames))
for column, (run_name, rows) in zip(columns, frames.items(), strict=True):
    with column:
        st.subheader(run_name)
        meta = ui_shared.load_meta(run_name)
        rebalancing_on = ui_shared.rebalancing_settings(meta).enabled
        if rows is None:
            st.info(
                "This run was saved before truck moves were recorded on the arcs "
                "table. Re-run the scenario to see them."
            )
            continue
        if rows.empty:
            if rebalancing_on:
                st.info("Rebalancing is on, but no truck moved a bike in this run.")
            else:
                st.info("Rebalancing is off in this run — no truck trips.")
            continue
        rows = rows.copy()
        rows["color"] = rows["resource_id"].map(
            lambda truck: [*ui_shared.hex_to_rgb(colors[truck]), 200]
        )
        rows["distance_km"] = rows["distance_km"].round(2)
        left, middle, right = st.columns(3)
        left.metric("Bikes moved", ui_shared.fmt_int(rows["bikes"].sum()))
        middle.metric("Trucks used", ui_shared.fmt_int(rows["resource_id"].nunique()))
        right.metric("Truck moves", ui_shared.fmt_int(len(rows)))
        st.pydeck_chart(
            ui_shared.arc_deck(
                rows, facilities, "bikes", TOOLTIP, width_min_pixels=2, width_max_pixels=12
            ),
            height=520,
        )

if colors:
    dots = " · ".join(
        f'<span style="color:{color}">●</span> {truck}' for truck, color in colors.items()
    )
    st.markdown(dots, unsafe_allow_html=True)

with st.expander("Data table"):
    for run_name, rows in drawable.items():
        st.markdown(f"**{run_name}**")
        st.dataframe(
            rows[
                [
                    "resource_id",
                    "source_id",
                    "target_id",
                    "bikes",
                    "start_period",
                    "end_period",
                    "distance_km",
                ]
            ].sort_values(["resource_id", "start_period"]),
            hide_index=True,
            width="stretch",
        )
