"""Run page: set the scenario parameters, run, save a new artifact.

The page has two backends (docs/api.md). Without ``API_URL`` it calls
``runner.run_scenario`` in this process, as before. With ``API_URL`` set it
sends ``POST /runs`` to the API and polls ``GET /runs/{run_name}/status``,
showing the same progress lines the local path prints through
``on_progress``.
"""

import time

import api_client
import artifacts
import pandas as pd
import runner
import streamlit as st
import ui_shared

st.title("Run scenario")
st.caption(
    "A run replays the historical trips scaled by two multipliers. The first sets "
    "how much demand shows up; the second sets how much demand the starting bikes "
    "and dock capacities are prepared for. Equal values give a clean run with no "
    "losses; demand above the second value makes stations run out of bikes "
    "(stockout) or dock space (dock_full)."
)


@st.cache_resource(show_spinner=False)
def _graph_data_cached(trips_path: str):
    """Load the raw data once per source; later runs reuse the tables."""
    return runner.build_graph_data(trips_path)


def _name_from_parameters(
    demand_scale: float,
    sizing_scale: float,
    periods: int,
    rebalancing: bool,
    n_trucks: int,
) -> str:
    """Default run name, built from the parameters: ``demand_x2_sizing_x1_50p``."""
    base = f"demand_x{demand_scale:g}_sizing_x{sizing_scale:g}_{periods}p"
    if rebalancing:
        base += f"_{n_trucks}trucks"
    return base


def _run_on_server(request: dict) -> str:
    """Start a run over the API and poll it to the end; return the final run name.

    ``POST /runs`` answers with the final (never-overwriting) run name; the
    status endpoint carries the same progress lines ``on_progress`` prints
    locally. A failed run stops the page with the server's error text.
    """
    answer = api_client.start_run(request)
    final_name = answer["run_name"]
    with st.status("Running on the server…", expanded=True) as status:
        st.write(f"Queued on the server as {final_name}.")
        shown = 0
        while True:
            state = api_client.run_status(final_name)
            for line in state["progress"][shown:]:
                st.write(line)
            shown = len(state["progress"])
            if state["status"] == "done":
                status.update(label=f"Done: {final_name}", state="complete", expanded=False)
                return final_name
            if state["status"] == "failed":
                status.update(label=f"Run failed: {final_name}", state="error", expanded=True)
                st.error(state["error"])
                st.stop()
            time.sleep(2)


api_url = api_client.api_url()

left, middle, right = st.columns(3)
demand_scale = left.number_input(
    "Demand the run faces (demand_scale_factor)",
    min_value=0.1,
    max_value=10.0,
    value=1.0,
    step=0.1,
    help=(
        "Multiplier on the historical trips: 1.0 replays the month as it was, "
        "2.0 sends twice as many riders to every station."
    ),
)
sizing_scale = middle.number_input(
    "Demand the system is built for (sizing_scale_factor)",
    min_value=0.1,
    max_value=10.0,
    value=1.0,
    step=0.1,
    help=(
        "The starting bikes and dock capacities are sized to survive this much "
        "demand with no loss. Set it below the demand the run faces to see "
        "stockouts and full docks."
    ),
)
periods = right.number_input(
    "Number of periods", min_value=1, max_value=2000, value=runner.DEFAULT_NUMBER_OF_PERIODS
)
rebalancing = st.checkbox(
    "Overnight rebalancing",
    value=False,
    help=(
        "Trucks move bikes between stations at night (window opens at 01:00, "
        "two hours long) so the morning demand finds them. See Notations.md §14."
    ),
)
truck_homes: list[str] = []
truck_capacity = runner.DEFAULT_TRUCK_CAPACITY_BIKES
if rebalancing:
    with st.expander("Truck fleet", expanded=True):
        st.caption(
            "One row per truck; pick the truck's home depot — the truck starts and "
            "ends its night route there. Add or delete rows to change the fleet size."
        )
        fleet = st.data_editor(
            pd.DataFrame({"home_depot": runner.DEFAULT_TRUCK_HOMES}),
            column_config={
                "home_depot": st.column_config.SelectboxColumn(
                    "Home depot", options=runner.DEPOT_IDS, required=True
                )
            },
            num_rows="dynamic",
            hide_index=True,
            key="truck_fleet",
        )
        truck_homes = fleet["home_depot"].dropna().tolist()
        if truck_homes:
            named = ", ".join(f"truck_{i + 1} at {home}" for i, home in enumerate(truck_homes))
            st.caption(f"Trucks are numbered in row order: {named}.")
        truck_capacity = st.number_input(
            "Truck capacity (bikes per truck)", min_value=1, max_value=200, value=20
        )
trips_path = runner.DEFAULT_TRIPS_PATH
if api_url is None:
    with st.expander("Data sources"):
        trips_path = st.text_input("Trips CSV", value=runner.DEFAULT_TRIPS_PATH)

suggested = _name_from_parameters(
    float(demand_scale), float(sizing_scale), int(periods), rebalancing, len(truck_homes)
)
custom_name = st.text_input("Run name (leave blank to name the run from the parameters)", value="")
requested_name = custom_name.strip() or suggested
if api_url is None:
    run_name = artifacts.next_free_run_name(requested_name)
    location = f"data/runs/{run_name}"
else:
    # The server resolves the final name itself (docs/api.md): the runs live
    # on its disk, not here. POST /runs answers with the name to poll.
    run_name = requested_name
    location = f"{run_name} on the server"
st.caption(
    f"The run will be saved as {location}. A taken name gets a "
    "_version_2, _version_3, ... suffix instead of overwriting the saved run."
)

if st.button("Run", type="primary"):
    if rebalancing and not truck_homes:
        st.error("Rebalancing is on but the truck fleet is empty. Add at least one truck.")
        st.stop()
    if api_url is None:
        with st.status("Running…", expanded=True) as status:
            st.write("Loading data (a few minutes the first time; cached afterwards)…")
            graph_data = _graph_data_cached(trips_path)
            folder = runner.run_scenario(
                graph_data,
                run_name=run_name,
                demand_scale_factor=float(demand_scale),
                sizing_scale_factor=float(sizing_scale),
                number_of_periods=int(periods),
                rebalancing=bool(rebalancing),
                truck_homes=truck_homes,
                truck_capacity_bikes=int(truck_capacity),
                on_progress=st.write,
            )
            status.update(label=f"Done: {folder}", state="complete", expanded=False)
    else:
        run_name = _run_on_server(
            {
                "run_name": run_name,
                "demand_scale_factor": float(demand_scale),
                "sizing_scale_factor": float(sizing_scale),
                "number_of_periods": int(periods),
                "rebalancing": bool(rebalancing),
                "truck_homes": truck_homes or None,
                "truck_capacity_bikes": int(truck_capacity),
            }
        )
    meta = ui_shared.load_meta(run_name)
    ui_shared.validation_badge(meta, run_name)
    st.subheader("Run totals")
    ui_shared.kpi_row(meta)
