"""Overview page: pick scenarios A/B, whole-run totals, list of saved runs."""

import pandas as pd
import streamlit as st
import ui_shared

st.title("Citi Bike — run overview")

run_a, run_b = ui_shared.pick_scenario_pair()
if run_a is None:
    st.stop()

meta_a = ui_shared.load_meta(run_a)
meta_b = ui_shared.load_meta(run_b) if run_b else None

ui_shared.validation_badge(meta_a, f"Scenario A — {run_a}")
if run_b:
    ui_shared.validation_badge(meta_b, f"Scenario B — {run_b}")

st.subheader("Whole-run totals")
if run_b:
    st.caption(
        f"The value is scenario A ({run_a}); the line under it is the difference "
        f"vs scenario B ({run_b})."
    )
ui_shared.kpi_row(meta_a, meta_b)

st.subheader("All saved runs")
rows = []
for name in ui_shared.list_runs():
    meta = ui_shared.load_meta(name)
    totals = meta.totals
    rows.append(
        {
            "Run": name,
            "Demand scale": meta.demand_scale_factor,
            "Sizing scale": meta.sizing_scale_factor,
            "Periods": meta.number_of_periods,
            "Created": meta.created_at,
            "Invariant violations": len(meta.violations),
            "Demand": totals["demand"],
            "Lost demand": totals["lost_demand"],
            "Lost at full docks": totals["lost_dock_full"],
            "Redirected": totals["redirected"],
            "Cost, $": totals["cost"],
        }
    )
st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
