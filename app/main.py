"""Streamlit entry point: page registry and navigation.

Start with ``streamlit run app/main.py``. Every page is a reader of saved run
artifacts (Notations.md §12); the only page that computes anything heavy is
"Run scenario", which runs the simulator and saves a new artifact.
"""

import pathlib
import sys

import streamlit as st

# Pages import sibling modules (artifacts, ui_shared, runner) by name, so the
# app folder must be importable no matter where streamlit was started from.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

st.set_page_config(page_title="Citi Bike Simulation", page_icon="🚲", layout="wide")

pages = [
    st.Page("views/home.py", title="Overview & compare", icon="📊", default=True),
    st.Page("views/run_scenario.py", title="Run scenario", icon="▶️"),
    st.Page("views/station_map.py", title="Station map", icon="📍"),
    st.Page("views/trips_map.py", title="Trips map", icon="🚲"),
    st.Page("views/costs.py", title="Costs", icon="💰"),
    st.Page("views/distance_duration.py", title="Distance & duration", icon="📏"),
    st.Page("views/facility_detail.py", title="Single facility", icon="🔍"),
]
st.navigation(pages).run()
