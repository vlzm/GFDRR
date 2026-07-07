"""Shared helpers for the Streamlit pages: cached loaders, pickers, colors, charts.

Colors are the validated defaults of the data-viz method: scenario A is blue,
scenario B is orange (a colorblind-safe pair), sequential magnitude is the blue
ramp, and the difference view uses the blue-gray-red diverging pair.
"""

from __future__ import annotations

import dataclasses
import pathlib
from collections.abc import Callable
from typing import NamedTuple

import api_client
import artifacts
import numpy as np
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st
from artifacts import METRICS, PANEL_KEYS, PANEL_VALUES  # noqa: F401  (re-exported to pages)

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
#: Fixed per-truck colors (the categorical slots of the palette, in order).
#: Assigned to trucks by id order and never re-ordered by the data.
TRUCK_COLORS = [
    "#2a78d6",  # blue
    "#1baf7a",  # aqua
    "#eda100",  # yellow
    "#008300",  # green
    "#4a3aa7",  # violet
    "#e34948",  # red
    "#e87ba4",  # magenta
    "#eb6834",  # orange
]
#: Trucks past the eighth share this gray; the tooltip still names the truck.
TRUCK_OTHER_COLOR = "#898781"

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


# --- The run-artifact loader --------------------------------------------------
# The one front door to a saved run (Notations.md §12). A page asks for a table
# through the typed accessors below; the file layout, the caching, the two
# backends (local files, or HTTP calls to the API when API_URL is set --
# docs/explanation/api.md), and the old-artifact fallbacks all live here, so a page never
# names a parquet file or re-checks which columns an older artifact carries.
def list_runs() -> list[str]:
    """Names of every saved run: from the API when ``API_URL`` is set, else the disk."""
    if api_client.api_url():
        return [meta.run_name for meta in api_client.list_runs()]
    return artifacts.list_runs()


@st.cache_data(show_spinner=False)
def _table_cached(run_name: str, table: str, cache_key: float) -> pd.DataFrame:
    """Cache one parquet table; ``cache_key`` comes from :func:`table_cache_key`."""
    if api_client.api_url():
        return api_client.load_table(run_name, table)
    return artifacts.load_run_table(run_name, table)


def table_path(run_name: str, table: str) -> pathlib.Path:
    """Path of one saved table on the local disk (the local cache key reads its mtime)."""
    return artifacts.run_dir(run_name) / f"{table}.parquet"


def table_cache_key(run_name: str, table: str) -> float:
    """Cache key of one saved table: the file mtime locally, a constant over HTTP.

    A local file can be rewritten, so the mtime must be part of the key. A
    served artifact cannot change (the API is the only writer on the server
    and never overwrites a saved run -- docs/explanation/api.md), so ``(run_name, table)``
    alone identifies the content and the key is a constant.
    """
    if api_client.api_url():
        return 0.0
    return table_path(run_name, table).stat().st_mtime


def _load_table(run_name: str, table: str) -> pd.DataFrame:
    """Read one table of a saved run through the Streamlit cache."""
    return _table_cached(run_name, table, table_cache_key(run_name, table))


def load_panel(run_name: str) -> pd.DataFrame:
    """Load the facility period panel: one row per (period, facility, commodity)."""
    return _load_table(run_name, "panel")


def load_flow_totals(run_name: str) -> pd.DataFrame:
    """One row per flow with its whole-trip values (origin, outcome, cost, ...)."""
    return _load_table(run_name, "flow_totals")


def load_facilities(run_name: str) -> pd.DataFrame:
    """Load the facility attributes: id, category, coordinates, capacity."""
    return _load_table(run_name, "facilities")


def load_arcs(run_name: str, flow_type: str | None = None) -> pd.DataFrame | None:
    """Load the arcs of a run, optionally only one ``flow_type``.

    ``flow_type`` is ``"user_trip"`` (bike rides) or ``"rebalance"`` (truck
    moves); ``None`` returns the whole table. The old-artifact fallback lives
    here: arcs saved before the ``flow_type`` column existed hold user trips
    only, so asking such a run for user trips returns the whole table, and
    asking it for truck moves returns ``None`` (they cannot be told apart).
    """
    arcs = _load_table(run_name, "arcs")
    if flow_type is None:
        return arcs
    if "flow_type" not in arcs.columns:
        return arcs if flow_type == "user_trip" else None
    return arcs[arcs["flow_type"] == flow_type]


@st.cache_data(show_spinner=False)
def _meta_cached(run_name: str, cache_key: float) -> artifacts.RunMeta:
    """Cache one meta.json; ``cache_key`` is the mtime locally, a constant over HTTP."""
    if api_client.api_url():
        return api_client.load_meta(run_name)
    return artifacts.load_run_meta(run_name)


def load_meta(run_name: str) -> artifacts.RunMeta:
    """Read a saved run's meta.json through the Streamlit cache."""
    if api_client.api_url():
        return _meta_cached(run_name, 0.0)
    path = artifacts.run_dir(run_name) / "meta.json"
    return _meta_cached(run_name, path.stat().st_mtime)


class RebalancingSettings(NamedTuple):
    """The rebalancing block of one run's meta.json, with ``truck_homes`` never None."""

    enabled: bool
    truck_homes: list[str]


def rebalancing_settings(meta: artifacts.RunMeta) -> RebalancingSettings:
    """Read the rebalancing block of ``meta.json`` (one place for the empty-list rule).

    ``truck_homes`` is written only when rebalancing is on; when it is off the
    block reads as no trucks (an empty list), so pages can take ``len(...)``.
    """
    block = meta.rebalancing
    return RebalancingSettings(block.enabled, block.truck_homes or [])


# --- Scenario picking -------------------------------------------------------
def pick_scenario_pair() -> tuple[str | None, str | None]:
    """Sidebar pickers for scenario A and the optional comparison scenario B.

    The chosen names are kept in ``st.session_state`` so every page shows the
    same pair. Returns ``(None, None)`` when no run is saved yet.
    """
    runs = list_runs()
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
                settings = rebalancing_settings(meta)
                trucks = len(settings.truck_homes)
                suffix = f", rebalancing on ({trucks} trucks)" if settings.enabled else ""
                st.caption(
                    f"{label}: demand scale {meta.demand_scale_factor}, "
                    f"{meta.number_of_periods} periods{suffix}"
                )
    return run_a, run_b


def slider_max_period(meta_a: artifacts.RunMeta, meta_b: artifacts.RunMeta | None) -> int:
    """Last period the period slider can show (both scenarios must have it)."""
    last = meta_a.number_of_periods - 1
    if meta_b is not None:
        last = min(last, meta_b.number_of_periods - 1)
    return max(last, 0)


# --- Period ids to wall-clock time -------------------------------------------
def period_start_time(meta: artifacts.RunMeta, periods):
    """Wall-clock start of a period (or a Series of periods): ``t0 + period * period_len``.

    Every ``meta.json`` carries ``t0`` (see ``artifacts.build_meta``).
    """
    return pd.Timestamp(meta.t0) + periods * pd.Timedelta(hours=meta.period_len_hours)


def slider_time_caption(
    period: int, meta_a: artifacts.RunMeta, meta_b: artifacts.RunMeta | None = None
) -> None:
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
    "periods": lambda v: f"{v:.2f} periods",
}


def delta_b_minus_a(value_a: float, value_b: float, fmt: Callable[[float], str] = fmt_int) -> str:
    """Format the one comparison convention: the difference is always B − A.

    Returns the signed text for ``st.metric``: an ASCII leading ``-`` is what
    ``st.metric`` reads as "went down", so the sign is put on by hand and the
    unit format is applied to the absolute value.
    """
    diff = value_b - value_a
    return f"{'+' if diff >= 0 else '-'}{fmt(abs(diff))}"


def kpi_row(meta_a: artifacts.RunMeta, meta_b: artifacts.RunMeta | None = None) -> None:
    """Whole-run totals as metric tiles; with B chosen, the delta is B − A.

    The tiles come from the one METRICS table (``kpi=True`` entries); the tile
    label is the metric's full label without the braces part.
    """
    totals_a = meta_a.totals
    totals_b = meta_b.totals if meta_b else None
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
            delta = f"{delta_b_minus_a(totals_a[metric.name], totals_b[metric.name], fmt)} (B − A)"
            # ``inverse`` marks "more is worse" numbers (losses, redirects, cost).
            color = ("inverse" if metric.more_is_worse else "off") if diff != 0 else "off"
        label = metric.label.split(" (")[0]
        column.metric(label, fmt(totals_a[metric.name]), delta=delta, delta_color=color)


def validation_badge(meta: artifacts.RunMeta, label: str) -> None:
    """Green badge when invariants I1-I5 held; red badge with the list otherwise."""
    violations = meta.violations
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


# --- Arc maps -------------------------------------------------------------------
def arc_map_rows(arcs: pd.DataFrame, group_keys: list[str], count_name: str) -> pd.DataFrame:
    """Group arc rows into one map row per ``group_keys``: the count and the endpoints.

    Owns the arc-row schema knowledge: every arc row carries its endpoint
    coordinates (``source_lat`` .. ``target_lng``), so grouping keeps them with
    ``"first"`` and the map needs no join. ``count_name`` names the summed
    ``quantity`` column ("trips" on the trips map, "bikes" on the truck map);
    ``distance_km`` comes back as the group mean (constant within a real group).
    """
    return arcs.groupby(group_keys, as_index=False).agg(
        **{count_name: ("quantity", "sum")},
        distance_km=("distance_km", "mean"),
        source_lat=("source_lat", "first"),
        source_lng=("source_lng", "first"),
        target_lat=("target_lat", "first"),
        target_lng=("target_lng", "first"),
    )


def arc_deck(
    rows: pd.DataFrame,
    facilities: pd.DataFrame,
    width_col: str,
    tooltip_html: str,
    width_min_pixels: float = 1.5,
    width_max_pixels: float = 10,
) -> pdk.Deck:
    """Build an arc map over the city: one pydeck ``ArcLayer`` with the house tooltip.

    Owns pydeck's ``[lng, lat]`` coordinate order and the tooltip style. The
    rows come from :func:`arc_map_rows` plus a per-row ``color`` (an RGBA
    list) the page chose; ``width_col`` scales the arc width.
    """
    layer = pdk.Layer(
        "ArcLayer",
        data=rows,
        get_source_position="[source_lng, source_lat]",
        get_target_position="[target_lng, target_lat]",
        get_source_color="color",
        get_target_color="color",
        get_width=width_col,
        width_scale=1,
        width_min_pixels=width_min_pixels,
        width_max_pixels=width_max_pixels,
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
            "html": tooltip_html,
            "style": {"backgroundColor": "#1a1a19", "color": "#ffffff", "fontSize": "12px"},
        },
    )


# --- The flow_totals metric page ----------------------------------------------
@dataclasses.dataclass(frozen=True)
class FlowTotalsView:
    """One ``flow_totals`` chart page, described once.

    The Costs and Distance & duration pages are the same four-block page over
    a different measure; this config names the differences and
    :func:`flow_totals_page` renders the shared body. A new metric page is a
    new ``FlowTotalsView``, not a copied page.
    """

    value: str  # flow_totals column to aggregate
    agg: str  # "sum" or "mean"
    tile_label: str  # label of the whole-run metric tile
    y_title: str  # y-axis title of the per-period chart
    totals_key: str  # METRICS name of the precomputed total in meta.totals
    fmt: Callable[[float], str]  # one value with its unit
    # Tile text for the whole-run view; gets the run's meta (for period length).
    fmt_global: Callable[[float, artifacts.RunMeta], str] | None = None
    global_note: str | None = None  # caption under the whole-run tiles


def flow_totals_page(run_a: str, run_b: str | None, view: FlowTotalsView, level: str) -> None:
    """Render the shared body of a ``flow_totals`` metric page at one detail level.

    The whole-run level shows one tile per scenario (the precomputed total
    from ``meta.totals``) and the B − A difference; every other level
    aggregates with :func:`aggregate_flow_totals` and draws
    :func:`level_line_chart` plus the data table.
    """
    frames = {run_a: load_flow_totals(run_a)}
    if run_b:
        frames[run_b] = load_flow_totals(run_b)

    if level == LEVEL_GLOBAL:
        columns = st.columns(len(frames) + 1)
        values: dict[str, float] = {}
        for column, run_name in zip(columns, frames, strict=False):
            meta = load_meta(run_name)
            values[run_name] = float(meta.totals[view.totals_key])
            text = (
                view.fmt_global(values[run_name], meta)
                if view.fmt_global
                else view.fmt(values[run_name])
            )
            column.metric(f"{view.tile_label} — {run_name}", text)
        if run_b:
            columns[-1].metric(
                "Difference (B − A)", delta_b_minus_a(values[run_a], values[run_b], view.fmt)
            )
        if view.global_note:
            st.caption(view.global_note)
        return

    facilities = None
    if level == LEVEL_FACILITY:
        options = sorted(frames[run_a]["source_id"].dropna().unique())
        facilities = st.multiselect(
            "Origin facilities (source_id)",
            options,
            default=top_facilities(frames[run_a], view.value),
        )
        if not facilities:
            st.info("Pick at least one facility.")
            st.stop()
    data = aggregate_flow_totals(frames, view.value, view.agg, level, facilities)
    fig = level_line_chart(data, view.value, view.y_title, scenario_color_map(run_a, run_b))
    st.plotly_chart(fig, width="stretch")
    with st.expander("Data table"):
        st.dataframe(data, hide_index=True, width="stretch")
