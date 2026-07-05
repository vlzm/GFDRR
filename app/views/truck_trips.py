"""Truck trips: the bikes trucks moved during overnight rebalancing, on a map."""

import pandas as pd
import pydeck as pdk
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

facilities = ui_shared.load_table(run_a, "facilities")


def _truck_moves(run_name: str) -> pd.DataFrame | None:
    """Truck moves of a run: one row per (truck, source, target, periods) group.

    Returns None when the artifact predates the ``flow_type`` column on arcs
    (saved by an older version of the app) — the moves cannot be told apart
    from user trips there.
    """
    arcs = ui_shared.load_table(run_name, "arcs")
    if "flow_type" not in arcs.columns:
        return None
    moves = arcs[arcs["flow_type"] == "rebalance"]
    return moves.groupby(
        ["resource_id", "source_id", "target_id", "start_period", "end_period"],
        as_index=False,
    ).agg(
        bikes=("quantity", "sum"),
        distance_km=("distance_km", "first"),
        source_lat=("source_lat", "first"),
        source_lng=("source_lng", "first"),
        target_lat=("target_lat", "first"),
        target_lng=("target_lng", "first"),
    )


def _truck_color_map(frames: dict[str, pd.DataFrame]) -> dict[str, str]:
    """One fixed color per truck id, shared by both scenarios, in id order."""
    trucks = sorted(set().union(*(set(rows["resource_id"]) for rows in frames.values())))
    palette = ui_shared.TRUCK_COLORS
    return {
        truck: palette[i] if i < len(palette) else ui_shared.TRUCK_OTHER_COLOR
        for i, truck in enumerate(trucks)
    }


def _deck(rows: pd.DataFrame) -> pdk.Deck:
    """Build the arc map of one scenario's truck moves."""
    layer = pdk.Layer(
        "ArcLayer",
        data=rows,
        get_source_position="[source_lng, source_lat]",
        get_target_position="[target_lng, target_lat]",
        get_source_color="color",
        get_target_color="color",
        get_width="bikes",
        width_scale=1,
        width_min_pixels=2,
        width_max_pixels=12,
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
                "<b>{resource_id}</b>: {source_id} → {target_id}<br/>"
                "Bikes: {bikes}<br/>Picked up period {start_period}, "
                "dropped period {end_period}<br/>Distance: {distance_km} km"
            ),
            "style": {"backgroundColor": "#1a1a19", "color": "#ffffff", "fontSize": "12px"},
        },
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
        rebalancing_on = meta.get("rebalancing", {}).get("enabled", False)
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
        st.pydeck_chart(_deck(rows), height=520)

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
