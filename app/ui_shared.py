"""Shared helpers for the Streamlit pages: cached loaders, pickers, colors, charts.

Colors are the validated defaults of the data-viz method: scenario A is blue,
scenario B is orange (a colorblind-safe pair), sequential magnitude is the blue
ramp, and the difference view uses the blue-gray-red diverging pair.
"""

from __future__ import annotations

import artifacts
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from artifacts import METRICS, PANEL_VALUES

# --- Palette (validated with the data-viz checks) --------------------------
SCENARIO_A_COLOR = "#2a78d6"
SCENARIO_B_COLOR = "#eb6834"
OUTCOME_COLORS = {"arrived": "#2a78d6", "redirected": "#eda100", "lost": "#e34948"}
SEQUENTIAL_RAMP = ["#cde2fb", "#86b6ef", "#3987e5", "#256abf", "#1c5cab", "#0d366b"]
DIVERGING_NEG = "#2a78d6"  # value went down
DIVERGING_MID = "#e1e0d9"
DIVERGING_POS = "#d03b3b"  # value went up
GRID_COLOR = "#e1e0d9"
INK_SECONDARY = "#52514e"

#: Display label for every panel value column (from the one METRICS table).
METRIC_LABELS = {metric.name: metric.label for metric in METRICS if metric.panel_value}
#: Short labels for the map hover box (from the one METRICS table).
METRIC_SHORT = {metric.name: metric.short for metric in METRICS if metric.panel_value}
COMMODITY_ALL = "All types"

# --- Aggregation levels for the cost / distance / duration charts ----------
LEVEL_GLOBAL = "Whole-run total"
LEVEL_PERIOD = "Per period"
LEVEL_COMMODITY = "Per period and bike type"
LEVEL_FACILITY = "Per period, bike type and facility"
LEVELS = [LEVEL_GLOBAL, LEVEL_PERIOD, LEVEL_COMMODITY, LEVEL_FACILITY]


# --- Cached loaders ---------------------------------------------------------
@st.cache_data(show_spinner=False)
def _table_cached(run_name: str, table: str, mtime: float) -> pd.DataFrame:
    """Cache one parquet table; ``mtime`` invalidates the entry on rewrite."""
    return artifacts.load_run_table(run_name, table)


def load_table(run_name: str, table: str) -> pd.DataFrame:
    """Read one table of a saved run through the Streamlit cache."""
    path = artifacts.run_dir(run_name) / f"{table}.parquet"
    return _table_cached(run_name, table, path.stat().st_mtime)


@st.cache_data(show_spinner=False)
def _meta_cached(run_name: str, mtime: float) -> dict:
    """Cache one meta.json; ``mtime`` invalidates the entry on rewrite."""
    return artifacts.load_run_meta(run_name)


def load_meta(run_name: str) -> dict:
    """Read a saved run's meta.json through the Streamlit cache."""
    path = artifacts.run_dir(run_name) / "meta.json"
    return _meta_cached(run_name, path.stat().st_mtime)


# --- Scenario picking -------------------------------------------------------
def pick_scenario_pair() -> tuple[str | None, str | None]:
    """Sidebar pickers for scenario A and the optional comparison scenario B.

    The chosen names are kept in ``st.session_state`` so every page shows the
    same pair. Returns ``(None, None)`` when no run is saved yet.
    """
    runs = artifacts.list_runs()
    if not runs:
        st.info("No saved runs yet. Open the “Run scenario” page and start the first one.")
        return None, None

    with st.sidebar:
        st.subheader("Scenarios")
        stored_a = st.session_state.get("scenario_a_value")
        index_a = runs.index(stored_a) if stored_a in runs else 0
        run_a = st.selectbox("Scenario A", runs, index=index_a)
        st.session_state["scenario_a_value"] = run_a

        none_label = "— no comparison —"
        options_b = [none_label] + [r for r in runs if r != run_a]
        stored_b = st.session_state.get("scenario_b_value")
        index_b = options_b.index(stored_b) if stored_b in options_b else 0
        picked_b = st.selectbox("Scenario B (comparison)", options_b, index=index_b)
        st.session_state["scenario_b_value"] = picked_b
        run_b = None if picked_b == none_label else picked_b

        for label, name in [("A", run_a), ("B", run_b)]:
            if name is not None:
                meta = load_meta(name)
                # Runs saved before the rebalancing feature have no such key.
                rebalancing = meta.get("rebalancing", {}).get("enabled", False)
                trucks = meta.get("rebalancing", {}).get("truck_homes", [])
                suffix = f", rebalancing on ({len(trucks)} trucks)" if rebalancing else ""
                st.caption(
                    f"{label}: demand scale {meta['demand_scale_factor']}, "
                    f"{meta['number_of_periods']} periods{suffix}"
                )
    return run_a, run_b


def slider_max_period(meta_a: dict, meta_b: dict | None) -> int:
    """Last period the period slider can show (both scenarios must have it)."""
    last = meta_a["number_of_periods"] - 1
    if meta_b is not None:
        last = min(last, meta_b["number_of_periods"] - 1)
    return max(last, 0)


# --- Period ids to wall-clock time -------------------------------------------
def period_start_time(meta: dict, periods):
    """Wall-clock start of a period (or a Series of periods): ``t0 + period * period_len``.

    Every ``meta.json`` carries ``t0`` (see ``artifacts.build_meta``).
    """
    return pd.Timestamp(meta["t0"]) + periods * pd.Timedelta(hours=meta["period_len_hours"])


def slider_time_caption(period: int, meta_a: dict, meta_b: dict | None = None) -> None:
    """Show under the period slider when the chosen period starts on the clock."""
    starts = [
        (tag, period_start_time(meta, period))
        for tag, meta in (("A", meta_a), ("B", meta_b))
        if meta is not None
    ]
    if len(starts) == 2 and starts[0][1] == starts[1][1]:
        starts = starts[:1]
    if len(starts) == 1:
        st.caption(f"Period {period} starts {starts[0][1]:%Y-%m-%d %H:%M}.")
    else:
        shown = "; ".join(f"{tag}: {start:%Y-%m-%d %H:%M}" for tag, start in starts)
        st.caption(f"Period {period} starts — {shown}.")


# --- KPI row and validation badge -------------------------------------------
def fmt_int(value: float) -> str:
    """Format a count with thin-space thousands separators."""
    return f"{value:,.0f}".replace(",", " ")


#: KPI value formatter per metric unit.
_KPI_FORMATS = {
    "count": fmt_int,
    "dollars": lambda v: f"${fmt_int(v)}",
    "km": lambda v: f"{fmt_int(v)} km",
}


def kpi_row(meta_a: dict, meta_b: dict | None = None) -> None:
    """Whole-run totals as metric tiles; with B chosen, the delta is B − A.

    The tiles come from the one METRICS table (``kpi=True`` entries); the tile
    label is the metric's full label without the braces part.
    """
    totals_a = meta_a["totals"]
    totals_b = meta_b["totals"] if meta_b else None
    items = [metric for metric in METRICS if metric.kpi]
    columns = []
    for _ in range((len(items) + 3) // 4):
        columns.extend(st.columns(4))
    for column, metric in zip(columns, items, strict=False):
        fmt = _KPI_FORMATS[metric.unit]
        delta = None
        color = "off"
        if totals_b is not None:
            diff = totals_b[metric.name] - totals_a[metric.name]
            # ASCII sign: st.metric reads the arrow direction from a leading "-".
            delta = f"{'+' if diff >= 0 else '-'}{fmt(abs(diff))} (B − A)"
            # ``inverse`` marks "more is worse" numbers (losses, redirects, cost).
            color = ("inverse" if metric.more_is_worse else "off") if diff != 0 else "off"
        label = metric.label.split(" (")[0]
        column.metric(label, fmt(totals_a[metric.name]), delta=delta, delta_color=color)


def validation_badge(meta: dict, label: str) -> None:
    """Green badge when invariants I1-I5 held; red badge with the list otherwise."""
    violations = meta.get("violations", [])
    if not violations:
        st.success(f"{label}: run invariants I1–I5 hold", icon="✅")
    else:
        st.error(f"{label}: run invariants violated ({len(violations)})", icon="🚫")
        with st.expander("Violation list"):
            for violation in violations:
                st.text(violation)


# --- Panel helpers -----------------------------------------------------------
def panel_commodity_slice(
    rows: pd.DataFrame, commodity: str | None, keys: list[str]
) -> pd.DataFrame:
    """Panel rows for one bike type, or the "All types" sum, per ``keys``.

    This is the one place that defines the "All types" pick: ``commodity=None``
    sums the panel value columns over the bike types within each ``keys``
    group; a chosen commodity keeps only its rows. With ``commodity_category``
    in ``keys`` the rows stay as they are (the raw per-type view).
    """
    if commodity is not None:
        rows = rows[rows["commodity_category"] == commodity]
    return rows.groupby(keys, as_index=False)[PANEL_VALUES].sum()


def panel_slice(panel: pd.DataFrame, period: int, commodity: str | None) -> pd.DataFrame:
    """One row per facility at one period; sums over commodities unless one is chosen."""
    return panel_commodity_slice(panel[panel["period_id"] == period], commodity, ["facility_id"])


def commodity_options(panel: pd.DataFrame) -> list[str]:
    """Commodity filter options: the shared "all" label plus each category."""
    return [COMMODITY_ALL] + sorted(panel["commodity_category"].unique())


# --- Map colors --------------------------------------------------------------
def hex_to_rgb(color: str) -> tuple[int, int, int]:
    """``#rrggbb`` to an ``(r, g, b)`` tuple."""
    return tuple(int(color[i : i + 2], 16) for i in (1, 3, 5))


def _interpolate(stops: list[str], t: np.ndarray) -> np.ndarray:
    """Colors along a ramp: ``t`` in [0, 1] mapped over the ``stops`` list."""
    t = np.clip(t, 0.0, 1.0) * (len(stops) - 1)
    channels = np.array([hex_to_rgb(s) for s in stops], dtype=float)
    out = np.empty((len(t), 3))
    for i in range(3):
        out[:, i] = np.interp(t, np.arange(len(stops)), channels[:, i])
    return out.round().astype(int)


def sequential_colors(values: pd.Series, alpha: int = 200) -> list[list[int]]:
    """RGBA per row: the blue ramp scaled to the values' maximum."""
    v = values.to_numpy(dtype=float)
    vmax = np.nanmax(v) if len(v) and np.nanmax(v) > 0 else 1.0
    rgb = _interpolate(SEQUENTIAL_RAMP, v / vmax)
    return [[int(r), int(g), int(b), alpha] for r, g, b in rgb]


def diverging_colors(values: pd.Series, alpha: int = 220) -> list[list[int]]:
    """RGBA per row: blue for negative, gray at zero, red for positive."""
    v = values.to_numpy(dtype=float)
    vmax = np.nanmax(np.abs(v)) if len(v) and np.nanmax(np.abs(v)) > 0 else 1.0
    t = np.clip(v / vmax, -1.0, 1.0)
    rgb = _interpolate([DIVERGING_NEG, DIVERGING_MID, DIVERGING_POS], (t + 1) / 2)
    return [[int(r), int(g), int(b), alpha] for r, g, b in rgb]


# --- Charts ------------------------------------------------------------------
def scenario_color_map(run_a: str, run_b: str | None) -> dict[str, str]:
    """Map each scenario to its fixed color: A is always blue, B always orange."""
    colors = {run_a: SCENARIO_A_COLOR}
    if run_b is not None:
        colors[run_b] = SCENARIO_B_COLOR
    return colors


def style_fig(fig) -> None:
    """House style: white template, recessive grid, horizontal legend on top."""
    fig.update_layout(
        template="plotly_white",
        font={"color": INK_SECONDARY},
        margin={"l": 8, "r": 8, "t": 48, "b": 8},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0, "title": None},
        hovermode="x unified",
    )
    fig.update_xaxes(gridcolor=GRID_COLOR)
    fig.update_yaxes(gridcolor=GRID_COLOR)


def aggregate_flow_totals(
    frames: dict[str, pd.DataFrame],
    value: str,
    agg: str,
    level: str,
    facilities: list[str] | None = None,
) -> pd.DataFrame:
    """Aggregate ``flow_totals`` per scenario at one detail level.

    A flow's value belongs to its origin facility (``source_id``) and its
    ``start_period`` — the place and period the demand occurred.

    Parameters
    ----------
    frames : dict of str to pandas.DataFrame
        ``run_name -> flow_totals`` for one or two scenarios.
    value : str
        Column to aggregate: ``cost``, ``distance_km`` or ``duration_periods``.
    agg : str
        ``"sum"`` or ``"mean"``.
    level : str
        One of ``LEVELS``.
    facilities : list of str, optional
        Keep only these origin facilities (required at ``LEVEL_FACILITY``).

    Returns
    -------
    pandas.DataFrame
        Tidy rows: ``scenario``, the level's keys, and ``value``. Per-period
        rows also get ``start_time`` — the wall-clock start of
        ``start_period``.
    """
    keys: list[str] = []
    if level != LEVEL_GLOBAL:
        keys.append("start_period")
    if level in (LEVEL_COMMODITY, LEVEL_FACILITY):
        keys.append("commodity_category")
    if level == LEVEL_FACILITY:
        keys.append("source_id")

    metas = {run_name: load_meta(run_name) for run_name in frames}
    with_time = "start_period" in keys

    parts = []
    for run_name, flow_totals in frames.items():
        rows = flow_totals.dropna(subset=[value])
        if level == LEVEL_FACILITY and facilities is not None:
            rows = rows[rows["source_id"].isin(facilities)]
        if keys:
            grouped = rows.groupby(keys, as_index=False)[value].agg(agg)
        else:
            grouped = pd.DataFrame({value: [rows[value].agg(agg)]})
        if with_time:
            grouped["start_time"] = period_start_time(metas[run_name], grouped["start_period"])
        grouped["scenario"] = run_name
        parts.append(grouped)
    return pd.concat(parts, ignore_index=True)


def level_line_chart(
    data: pd.DataFrame,
    value: str,
    y_title: str,
    color_map: dict[str, str],
):
    """Line chart over periods for an :func:`aggregate_flow_totals` frame.

    Scenario carries the color; commodity (when present) carries the line
    dash; facilities (when present) become small multiples. The x axis is the
    period's wall-clock start (``start_time``).
    """
    x = "start_time"
    x_title = "Period start time"
    kwargs: dict = {"hover_data": ["start_period"]}
    if "commodity_category" in data.columns:
        kwargs["line_dash"] = "commodity_category"
    if "source_id" in data.columns:
        kwargs["facet_col"] = "source_id"
        kwargs["facet_col_wrap"] = 3
    fig = px.line(
        data.sort_values(x),
        x=x,
        y=value,
        color="scenario",
        color_discrete_map=color_map,
        **kwargs,
    )
    fig.update_traces(line_width=2)
    fig.update_xaxes(title=x_title)
    fig.update_yaxes(title=y_title)
    if "source_id" in data.columns:
        n_rows = -(-data["source_id"].nunique() // 3)
        fig.update_layout(height=max(320, 260 * n_rows))
        # Facet captions: keep only the facility id, not "source_id=...".
        fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    style_fig(fig)
    return fig


def top_facilities(flow_totals: pd.DataFrame, value: str, n: int = 6) -> list[str]:
    """Origin facilities with the largest total ``value`` (default chart set)."""
    return (
        flow_totals.dropna(subset=[value])
        .groupby("source_id")[value]
        .sum()
        .sort_values(ascending=False)
        .head(n)
        .index.tolist()
    )
