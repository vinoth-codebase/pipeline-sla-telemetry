"""PipelineSLA-Telemetry :: Streamlit visual management cockpit.

Designed to be launched straight into the **VS Code Simple Browser**:

    Cmd/Ctrl+Shift+P  ->  "Simple Browser: Show"  ->  http://localhost:8501

or from a terminal:

    streamlit run src/pipeline_sla/dashboard/app.py --server.port 8501

Three panes:
* **Morning Dispatch Radar** -- live countdown to the 07:00 SLA cutoff.
* **Downstream Lineage Impact Explorer** -- NetworkX blast radius map.
* **Volume Anomaly Inspector** -- actual vs expected ingestion + CUSUM alarms.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Allow `streamlit run src/pipeline_sla/dashboard/app.py` without an install.
_SRC = Path(__file__).resolve().parents[2]
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import pandas as pd
import streamlit as st

from pipeline_sla.analytics.weibull_forecaster import WeibullForecaster
from pipeline_sla.config import load_settings
from pipeline_sla.dashboard import views
from pipeline_sla.lineage.dag_graph import LineageGraph
from pipeline_sla.storage.warehouse import TelemetryWarehouse

st.set_page_config(
    page_title="PipelineSLA Telemetry Cockpit",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------------------------------------------------------------------- #
# Data loading (cached)
# --------------------------------------------------------------------------- #
@st.cache_resource(show_spinner="Connecting to the telemetry warehouse...")
def get_settings():
    return load_settings()


@st.cache_resource(show_spinner="Opening DuckDB warehouse...")
def get_warehouse(db_path: str, in_memory: bool):
    return TelemetryWarehouse(None if in_memory else db_path, in_memory=in_memory)


@st.cache_resource(show_spinner="Building lineage graph...")
def get_lineage():
    return LineageGraph.from_settings(get_settings())


def _resolve_db(settings) -> tuple[str, bool]:
    """Prefer an explicit env override, else the configured path, else memory."""
    override = os.environ.get("PIPELINE_SLA_DB")
    if override:
        return override, override == ":memory:"
    root = Path(__file__).resolve().parents[3]
    path = Path(settings.storage.database_path)
    if not path.is_absolute():
        path = root / path
    if not path.exists():
        return ":memory:", True
    return str(path), False


settings = get_settings()
db_path, in_memory = _resolve_db(settings)
warehouse = get_warehouse(db_path, in_memory)
lineage = get_lineage()

# --------------------------------------------------------------------------- #
# Header
# --------------------------------------------------------------------------- #
st.title("📡 PipelineSLA Telemetry Cockpit")
st.caption(
    "Upstream ETL health & downstream business impact — "
    f"warehouse: `{db_path}` | SLA cutoff: {settings.sla.morning_cutoff} {settings.sla.timezone}"
)

kpis = views.estate_kpis(warehouse)
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Estate SLA Attainment", f"{kpis['sla_attainment']:.1f}%")
c2.metric("Total Runs", f"{kpis['total_runs']:,}")
c3.metric("Failed Runs", f"{kpis['failed_runs']:,}")
c4.metric("First-Run Success", f"{kpis['first_run_success']:.1f}%")
c5.metric("Pipelines Monitored", f"{kpis['pipelines']}")

if in_memory and kpis["total_runs"] == 0:
    st.warning(
        "No telemetry found. Seed the warehouse first:\n\n"
        "```bash\npython scripts/generate_pipeline_runs.py --days 180\n```"
    )

st.divider()

tab_radar, tab_lineage, tab_volume, tab_forecast = st.tabs(
    ["🌅 Morning Dispatch Radar", "🕸️ Lineage Impact Explorer",
     "📉 Volume Anomaly Inspector", "🔮 SLA Breach Forecaster"]
)

# --------------------------------------------------------------------------- #
# Pane 1 - Morning Dispatch Radar
# --------------------------------------------------------------------------- #
with tab_radar:
    dispatch = views.morning_dispatch(warehouse, settings)
    if dispatch.empty:
        st.info("No runs available to build the dispatch radar.")
    else:
        mins = dispatch.attrs.get("minutes_to_cutoff", 0.0)
        cutoff = dispatch.attrs.get("cutoff", settings.sla.morning_cutoff)
        if mins >= 0:
            st.success(f"⏳ **{mins/60:.1f} hours** until the {cutoff} morning business SLA cutoff.")
        else:
            st.error(f"⚠️ Morning cutoff ({cutoff}) has passed by {abs(mins)/60:.1f} hours.")

        for _, row in dispatch.iterrows():
            col_a, col_b = st.columns([3, 1])
            with col_a:
                st.markdown(f"**{row['dag_id']}** — last run {row['last_run']} ({row['state']})")
                st.progress(min(1.0, row["utilisation_pct"] / 100.0))
            with col_b:
                colour = row["colour"]
                st.markdown(
                    f"<span style='color:{colour};font-weight:700'>{row['status']}</span><br>"
                    f"<small>{row['runtime_min']:.0f}m / {row['sla_min']}m SLA</small>",
                    unsafe_allow_html=True,
                )
        st.dataframe(dispatch.drop(columns=["colour"]), use_container_width=True, hide_index=True)

# --------------------------------------------------------------------------- #
# Pane 2 - Lineage Impact Explorer
# --------------------------------------------------------------------------- #
with tab_lineage:
    tasks = lineage.tasks
    default = "reconcile_subledgers" if "reconcile_subledgers" in tasks else (tasks[0] if tasks else "")
    selected = st.selectbox("Simulate a stall on task:", tasks, index=tasks.index(default) if default in tasks else 0)
    fig = views.lineage_figure(lineage, highlight_node=selected)
    st.plotly_chart(fig, use_container_width=True)

    calc_br = views.blast_radius_table(lineage, settings)
    st.subheader("Downstream Blast Radius — Top Tasks")
    st.dataframe(calc_br, use_container_width=True, hide_index=True)

# --------------------------------------------------------------------------- #
# Pane 3 - Volume Anomaly Inspector
# --------------------------------------------------------------------------- #
with tab_volume:
    tasks_df = warehouse.get_task_instances()
    if tasks_df.empty:
        st.info("No task telemetry available.")
    else:
        col_a, col_b = st.columns(2)
        dags = sorted(tasks_df["dag_id"].unique())
        dag_choice = col_a.selectbox("Pipeline", dags, key="vol_dag")
        task_options = sorted(tasks_df[tasks_df["dag_id"] == dag_choice]["task_id"].unique())
        task_choice = col_b.selectbox("Task", task_options, key="vol_task")

        series = warehouse.get_volume_series(dag_choice, task_choice)
        st.plotly_chart(views.volume_anomaly_figure(series), use_container_width=True)
        st.plotly_chart(views.cusum_figure(series), use_container_width=True)

        detector = views.VolumeAnomalyDetector()
        detected = detector.detect(series)
        flagged = detected[detected["is_anomaly"]] if not detected.empty else detected
        st.subheader(f"Flagged anomalies ({0 if flagged is None else len(flagged)})")
        if flagged is not None and not flagged.empty:
            st.dataframe(
                flagged[["execution_date", "rows_loaded", "rolling_median", "zscore",
                         "cusum_direction", "severity", "reason"]],
                use_container_width=True, hide_index=True,
            )
        else:
            st.success("No anomalies detected for this task.")

# --------------------------------------------------------------------------- #
# Pane 4 - SLA Breach Forecaster
# --------------------------------------------------------------------------- #
with tab_forecast:
    tasks_df = warehouse.get_task_instances()
    if tasks_df.empty:
        st.info("No task telemetry available.")
    else:
        col_a, col_b, col_c = st.columns(3)
        dags = sorted(tasks_df["dag_id"].unique())
        dag_choice = col_a.selectbox("Pipeline", dags, key="fc_dag")
        task_options = sorted(tasks_df[tasks_df["dag_id"] == dag_choice]["task_id"].unique())
        task_choice = col_b.selectbox("Task", task_options, key="fc_task")
        cutoff_minutes = col_c.slider("SLA cutoff (minutes)", 10, 240, 60)

        durations = warehouse.get_task_durations(dag_choice, task_choice)
        forecaster = WeibullForecaster()
        forecaster.fit(durations)

        if forecaster.best_fit is None:
            st.warning("Not enough successful history to fit a distribution.")
        else:
            elapsed = st.slider("Elapsed runtime so far (seconds)", 0, int(cutoff_minutes * 60), int(cutoff_minutes * 30))
            result = forecaster.forecast(dag_choice, task_choice, elapsed, cutoff_minutes * 60)
            badge = {"LOW": "🟢", "ELEVATED": "🟡", "CRITICAL": "🔴"}[result.risk_level]
            st.metric(
                f"{badge} Breach probability (best fit: {result.best_distribution})",
                f"{result.breach_probability:.1%}",
                delta=f"{result.risk_level}",
            )
            st.plotly_chart(
                views.weibull_probability_figure(
                    durations, elapsed_seconds=elapsed,
                    cutoff_seconds=cutoff_minutes * 60,
                    title=f"Weibull forecast — {task_choice}",
                ),
                use_container_width=True,
            )
            col1, col2 = st.columns(2)
            col1.metric("Expected remaining", f"{result.expected_remaining_seconds/60:.1f} min")
            col2.metric("Remaining budget", f"{result.remaining_budget_seconds/60:.1f} min")
            st.dataframe(
                pd.DataFrame([f.to_dict() for f in forecaster.fits.values()]),
                use_container_width=True, hide_index=True,
            )

st.divider()
st.caption("PipelineSLA-Telemetry v1.0.0 — built for VS Code Simple Browser · Streamlit")
