"""Run page: set the scenario parameters, run, save a new artifact.

The page never picks between the two backends itself (docs/reference/api.md):
``backend.current()`` returns the chosen one, and the page asks it for the
saved forecasts, the save location, and the run. Locally the run executes in
this process; with ``API_URL`` set it is queued on the server and polled, and
the status box shows the same progress lines either way.
"""

import backend
import pandas as pd
import runner
import streamlit as st
import ui_shared

st.title("Run scenario")
st.caption(
    "A run replays the historical trips — or a saved forecast — scaled by two multipliers. "
    "The first sets "
    "how much demand shows up; the second sets how much demand the starting bikes "
    "and dock capacities are prepared for. Equal values give a clean run with no "
    "losses; demand above the second value makes stations run out of bikes "
    "(stockout) or dock space (dock_full)."
)


def _name_from_parameters(
    demand_scale: float,
    sizing_scale: float,
    periods: int,
    rebalancing: bool,
    n_trucks: int,
    forecast_name: str | None,
) -> str:
    """Default run name, built from the parameters: ``demand_x2_sizing_x1_50p``."""
    base = f"demand_x{demand_scale:g}_sizing_x{sizing_scale:g}_{periods}p"
    if forecast_name:
        base = f"forecast_{forecast_name}_{base}"
    if rebalancing:
        base += f"_{n_trucks}trucks"
    return base


bk = backend.current()

demand_source = st.radio(
    "Demand source",
    options=list(runner.DEMAND_SOURCES),
    horizontal=True,
    help=(
        "history replays the historical trips; forecast runs on a saved forecast "
        "demand table from data/ml/forecasts/ (see Notations.md §17). The demand "
        "multiplier below applies to either source."
    ),
)
forecast_name: str | None = None
if demand_source == "forecast":
    saved = bk.list_forecasts()
    if saved is None:
        # The forecasts live on the server's disk, so the page cannot list
        # them; the server rejects an unknown name when the run starts.
        forecast_name = st.text_input("Forecast name (on the server)", value="")
        if not forecast_name:
            st.info("Enter the name of a forecast saved on the server.")
            st.stop()
    elif not saved:
        st.error(
            "No saved forecasts. Build one first: python -m gbp.ml.forecast "
            "--trips-path <csv> --forecast-name <name>"
        )
        st.stop()
    else:
        forecast_name = st.selectbox("Forecast", options=saved)

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
trips_path = bk.default_trips_path()
if trips_path is not None:
    with st.expander("Data sources"):
        trips_path = st.text_input("Trips CSV", value=trips_path)

suggested = _name_from_parameters(
    float(demand_scale),
    float(sizing_scale),
    int(periods),
    rebalancing,
    len(truck_homes),
    forecast_name,
)
custom_name = st.text_input("Run name (leave blank to name the run from the parameters)", value="")
requested_name = custom_name.strip() or suggested
st.caption(
    f"The run will be saved as {bk.save_location(requested_name)}. A taken name gets a "
    "_version_2, _version_3, ... suffix instead of overwriting the saved run."
)

if st.button("Run", type="primary"):
    if rebalancing and not truck_homes:
        st.error("Rebalancing is on but the truck fleet is empty. Add at least one truck.")
        st.stop()
    request = {
        "run_name": requested_name,
        "demand_scale_factor": float(demand_scale),
        "sizing_scale_factor": float(sizing_scale),
        "number_of_periods": int(periods),
        "demand_source": demand_source,
        "forecast_name": forecast_name,
        "rebalancing": bool(rebalancing),
        "truck_homes": truck_homes or None,
        "truck_capacity_bikes": int(truck_capacity),
    }
    with st.status("Running…", expanded=True) as status:
        try:
            run_name = bk.run_and_wait(request, trips_path, on_progress=st.write)
        except backend.RunFailed as failed:
            status.update(label=f"Run failed: {failed.run_name}", state="error", expanded=True)
            st.error(str(failed))
            st.stop()
        status.update(label=f"Done: {run_name}", state="complete", expanded=False)
    meta = ui_shared.load_meta(run_name)
    ui_shared.validation_badge(meta, run_name)
    st.subheader("Run totals")
    ui_shared.kpi_row(meta)
