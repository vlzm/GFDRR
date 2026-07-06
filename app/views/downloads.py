"""Download page: the raw output tables of a saved run as CSV files."""

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

#: The downloadable tables: the loader accessor and what one row means.
TABLES = {
    "flow_totals": (
        ui_shared.load_flow_totals,
        "One row per trip (flow_id) with its whole-trip values: origin, target, "
        "start and end period, outcome, duration_periods, distance_km, cost.",
    ),
    "panel": (
        ui_shared.load_panel,
        "One row per (period_id, facility_id, commodity_category): inventory at "
        "the period start and end, demand, departed, arrived, redirected, "
        "lost_demand, lost_dock_full.",
    ),
}


@st.cache_data(show_spinner=False)
def _csv_bytes(run_name: str, table: str, mtime: float) -> bytes:
    """CSV bytes of one run table; ``mtime`` invalidates the cache on rewrite."""
    loader, _ = TABLES[table]
    return loader(run_name).to_csv(index=False).encode("utf-8")


for run_name in [name for name in (run_a, run_b) if name]:
    st.subheader(run_name)
    for table, (loader, description) in TABLES.items():
        frame = loader(run_name)
        mtime = ui_shared.table_path(run_name, table).stat().st_mtime
        st.download_button(
            f"Download {table}.csv ({ui_shared.fmt_int(len(frame))} rows)",
            data=_csv_bytes(run_name, table, mtime),
            file_name=f"{run_name}_{table}.csv",
            mime="text/csv",
            key=f"download_{run_name}_{table}",
        )
        st.caption(description)
        with st.expander(f"Preview {table} (first 20 rows)"):
            st.dataframe(frame.head(20), hide_index=True, width="stretch")
