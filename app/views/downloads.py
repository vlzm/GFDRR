"""Download page: the raw output tables of a saved run as CSV files."""

import artifacts
import streamlit as st
import ui_shared

st.title("Download data")
st.caption(
    "The raw output tables of a saved run (Notations.md §12), converted to CSV. "
    "The buttons download the same data the charts read."
)

run_a, run_b = ui_shared.pick_scenario_pair()
if run_a is None:
    st.stop()

#: The downloadable tables and what one row of each means.
TABLES = {
    "flow_totals": (
        "One row per trip (flow_id) with its whole-trip values: origin, target, "
        "start and end period, outcome, duration_periods, distance_km, cost."
    ),
    "panel": (
        "One row per (period_id, facility_id, commodity_category): inventory at "
        "the period start and end, demand, departed, arrived, redirected, "
        "lost_demand, lost_dock_full."
    ),
}


@st.cache_data(show_spinner=False)
def _csv_bytes(run_name: str, table: str, mtime: float) -> bytes:
    """CSV bytes of one run table; ``mtime`` invalidates the cache on rewrite."""
    return artifacts.load_run_table(run_name, table).to_csv(index=False).encode("utf-8")


for run_name in [name for name in (run_a, run_b) if name]:
    st.subheader(run_name)
    for table, description in TABLES.items():
        frame = ui_shared.load_table(run_name, table)
        path = artifacts.run_dir(run_name) / f"{table}.parquet"
        st.download_button(
            f"Download {table}.csv ({ui_shared.fmt_int(len(frame))} rows)",
            data=_csv_bytes(run_name, table, path.stat().st_mtime),
            file_name=f"{run_name}_{table}.csv",
            mime="text/csv",
            key=f"download_{run_name}_{table}",
        )
        st.caption(description)
        with st.expander(f"Preview {table} (first 20 rows)"):
            st.dataframe(frame.head(20), hide_index=True, width="stretch")
