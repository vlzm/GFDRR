"""Model monitoring: metric history per model version, degraded months, drift reports."""

import pandas as pd
import plotly.express as px
import streamlit as st
import ui_shared

from domains.citybike.ml.ops.monitoring import (
    ROLLING_MONTHS,
    drift_report_paths,
    list_drift_summaries,
    load_metrics,
    metric_history,
)

st.title("Model monitoring")

metrics = load_metrics()
if metrics.empty:
    st.info(
        "No monitoring metrics yet. After a new actual month arrives, score it with "
        "`python -m domains.citybike.ml.ops.monitoring --month <YYYYMM>`."
    )
    st.stop()

history = metric_history(metrics)
history["model"] = history["model_name"] + " v" + history["model_version"]

st.header("Metric history")
st.caption(
    "One point per model version and scored month, averaged over that version's forecasts. "
    f"A month is degraded when the version's rolling MAE (last {ROLLING_MONTHS} scored months) "
    "is worse than the seasonal naive baseline for that month."
)

METRIC_LABELS = {
    "mae": "MAE, bikes per station-hour",
    "poisson_deviance": "Poisson deviance",
    "bias": "Bias (predicted - actual), bikes per station-hour",
}
metric = st.selectbox("Metric", list(METRIC_LABELS), format_func=METRIC_LABELS.get)

fig = px.line(history, x="month", y=metric, color="model", markers=True)
fig.update_yaxes(title=METRIC_LABELS[metric])
if metric == "mae":
    baseline = history.groupby("month", as_index=False)["naive_mae"].mean()
    fig.add_scatter(
        x=baseline["month"],
        y=baseline["naive_mae"],
        mode="lines",
        line={"dash": "dash", "color": "gray"},
        name="seasonal naive baseline",
    )
degraded = history[history["degraded"]]
if not degraded.empty:
    fig.add_scatter(
        x=degraded["month"],
        y=degraded[metric],
        mode="markers",
        marker={"symbol": "x", "size": 12, "color": "red"},
        name="degraded",
    )
ui_shared.style_fig(fig)
st.plotly_chart(fig, width="stretch")


def _mark_degraded(row: pd.Series) -> list[str]:
    style = "background-color: rgba(255, 75, 75, 0.25)" if row["degraded"] else ""
    return [style] * len(row)


shown = history.drop(columns="model")
st.dataframe(
    shown.style.apply(_mark_degraded, axis=1).format(
        dict.fromkeys(["mae", "poisson_deviance", "bias", "naive_mae", "rolling_mae"], "{:.4f}")
    ),
    width="stretch",
    hide_index=True,
)

st.header("Drift reports")
st.caption(
    "Each report compares one month's feature distributions against the training data of the "
    "champion at the time. A column drifted when its distance is at or above the threshold."
)
summaries = list_drift_summaries()
if not summaries:
    st.caption(
        "No drift reports yet — "
        "`python -m domains.citybike.ml.ops.monitoring --month <YYYYMM>` builds one."
    )
for summary in summaries:
    drifted = f"{summary['drifted_count']} of {len(summary['columns'])} columns drifted"
    with st.expander(f"{summary['month']} — {drifted}"):
        st.caption(
            f"Champion: {summary['champion_family']} v{summary['champion_version']} · "
            f"reference months {summary['reference_months'][0]}..{summary['reference_months'][-1]} "
            f"· {summary['current_rows']:,} current rows vs "
            f"{summary['reference_rows']:,} reference rows · built {summary['created_at']}"
        )
        st.dataframe(
            pd.DataFrame(summary["columns"]),
            width="stretch",
            hide_index=True,
        )
        html_path, _ = drift_report_paths(summary["month"])
        if html_path.exists():
            st.download_button(
                "Download the full Evidently report (HTML)",
                data=html_path.read_bytes(),
                file_name=html_path.name,
                mime="text/html",
                key=f"drift-download-{summary['month']}",
            )
