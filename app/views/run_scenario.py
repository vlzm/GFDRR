"""Run page: set the demand multiplier and periods, run, save a new artifact."""

import artifacts
import runner
import streamlit as st
import ui_shared

st.title("Run scenario")
st.caption(
    "The historical simulation is a demand scale of 1.0. To get a scenario with "
    "increased demand, set the demand scale above the sizing scale: the initial "
    "inventory and dock capacities are sized for the smaller demand, so the "
    "limits (stockout, dock_full) start to take effect."
)


@st.cache_resource(show_spinner=False)
def _graph_data_cached(trips_path: str, gbfs_base: str):
    """Load the raw data once per source pair; later runs reuse the tables."""
    return runner.build_graph_data(trips_path, gbfs_base)


with st.form("run_form"):
    run_name = st.text_input("Run name (artifact folder name)", value="demand_x1")
    left, middle, right = st.columns(3)
    demand_scale = left.number_input(
        "Demand scale (demand_scale_factor)",
        min_value=0.1,
        max_value=10.0,
        value=1.0,
        step=0.1,
    )
    sizing_scale = middle.number_input(
        "Sizing scale (sizing_scale_factor)",
        min_value=0.1,
        max_value=10.0,
        value=1.0,
        step=0.1,
        help=(
            "The demand the system is guaranteed to survive with no loss: the "
            "initial inventory and dock capacities are sized for it."
        ),
    )
    periods = right.number_input(
        "Number of periods", min_value=1, max_value=2000, value=runner.DEFAULT_NUMBER_OF_PERIODS
    )
    with st.expander("Data sources"):
        trips_path = st.text_input("Trips CSV", value=runner.DEFAULT_TRIPS_PATH)
        gbfs_base = st.text_input("GBFS (station feed base URL)", value=runner.DEFAULT_GBFS_BASE)
    submitted = st.form_submit_button("Run", type="primary")

if submitted:
    name = run_name.strip()
    if not name:
        st.error("Enter a run name.")
        st.stop()
    if name in artifacts.list_runs():
        st.warning(f"Run “{name}” already exists — it will be overwritten.")
    with st.status("Running…", expanded=True) as status:
        st.write("Loading data (a few minutes the first time; cached afterwards)…")
        graph_data = _graph_data_cached(trips_path, gbfs_base)
        folder = runner.run_scenario(
            graph_data,
            run_name=name,
            demand_scale_factor=float(demand_scale),
            sizing_scale_factor=float(sizing_scale),
            number_of_periods=int(periods),
            on_progress=st.write,
        )
        status.update(label=f"Done: {folder}", state="complete", expanded=False)
    meta = ui_shared.load_meta(name)
    ui_shared.validation_badge(meta, name)
    st.subheader("Run totals")
    ui_shared.kpi_row(meta)
