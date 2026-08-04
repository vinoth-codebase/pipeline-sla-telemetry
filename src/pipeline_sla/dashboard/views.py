"""Dashboard view layer.

Pure, Streamlit-free functions that turn warehouse telemetry into Plotly figures
and pandas frames. Keeping them free of any ``streamlit`` import means they can
be unit-tested headlessly and reused by the notebook walkthrough and the VS Code
interactive script.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import networkx as nx
import numpy as np
import pandas as pd
import plotly.graph_objects as go

from pipeline_sla.analytics.cusum_detector import VolumeAnomalyDetector
from pipeline_sla.analytics.metrics import operational_summary
from pipeline_sla.analytics.weibull_forecaster import WeibullForecaster
from pipeline_sla.config import Settings, load_settings
from pipeline_sla.lineage.blast_radius import BlastRadiusCalculator
from pipeline_sla.lineage.dag_graph import LineageGraph, NodeKind

# ---- shared theme ----------------------------------------------------------
NAVY = "#1F3864"
ACCENT = "#2E75B6"
GREEN = "#378D3E"
AMBER = "#BF8F00"
RED = "#C0392B"
GREY = "#8C8C8C"
PLOT_TEMPLATE = "plotly_white"


# =============================================================================
# Data helpers
# =============================================================================
def kpi_overview(warehouse: Any) -> pd.DataFrame:
    """Per-pipeline reliability scorecard."""
    return operational_summary(warehouse)


def estate_kpis(warehouse: Any) -> dict[str, float]:
    """Headline estate-wide numbers for the KPI tiles."""
    overview = operational_summary(warehouse)
    if overview.empty:
        return {"sla_attainment": 0.0, "total_runs": 0, "failed_runs": 0,
                "first_run_success": 0.0, "pipelines": 0}
    return {
        "sla_attainment": round(float(overview["sla_attainment"].mean()), 2),
        "total_runs": int(overview["total_runs"].sum()),
        "failed_runs": int(overview["failed_runs"].sum()),
        "first_run_success": round(float(overview["first_run_success_rate"].mean()), 2),
        "pipelines": int(len(overview)),
    }


def parse_cutoff(cutoff: str, tz: str) -> datetime:
    """Build today's SLA cutoff datetime in the configured timezone."""
    hour, minute = (int(x) for x in cutoff.split(":"))
    now = datetime.now(ZoneInfo(tz))
    return now.replace(hour=hour, minute=minute, second=0, microsecond=0)


def morning_dispatch(warehouse: Any, settings: Settings | None = None) -> pd.DataFrame:
    """Morning Dispatch Radar: per-pipeline countdown to the SLA cutoff.

    For each pipeline we take its most recent run, compute how long it has been
    running and how much budget remains before the morning cutoff, and derive a
    colour-coded status.
    """
    settings = settings or load_settings()
    runs = warehouse.get_dag_runs()
    if runs.empty:
        return pd.DataFrame()

    cutoff = parse_cutoff(settings.sla.morning_cutoff, settings.sla.timezone)
    now = datetime.now(ZoneInfo(settings.sla.timezone))
    minutes_to_cutoff = (cutoff - now).total_seconds() / 60.0

    rows = []
    for dag_id, group in runs.groupby("dag_id"):
        latest = group.sort_values("execution_date").iloc[-1]
        duration = float(latest.get("duration_seconds") or 0.0)
        sla_minutes = float(latest.get("sla_minutes") or 240)
        utilisation = min(100.0, duration / (sla_minutes * 60.0) * 100.0)
        remaining = sla_minutes - duration / 60.0
        if str(latest["state"]) == "failed":
            status, colour = "FAILED", RED
        elif utilisation >= 100:
            status, colour = "BREACHED", RED
        elif utilisation >= 80:
            status, colour = "AT RISK", AMBER
        else:
            status, colour = "ON TRACK", GREEN
        rows.append(
            {
                "dag_id": dag_id,
                "last_run": pd.to_datetime(latest["execution_date"]).strftime("%Y-%m-%d"),
                "state": str(latest["state"]),
                "runtime_min": round(duration / 60.0, 1),
                "sla_min": int(sla_minutes),
                "budget_remaining_min": round(remaining, 1),
                "utilisation_pct": round(utilisation, 1),
                "status": status,
                "colour": colour,
            }
        )
    df = pd.DataFrame(rows).sort_values("utilisation_pct", ascending=False)
    df.attrs["minutes_to_cutoff"] = round(minutes_to_cutoff, 1)
    df.attrs["cutoff"] = cutoff.strftime("%H:%M")
    return df


# =============================================================================
# Figures
# =============================================================================
def weibull_probability_figure(
    durations: list[float],
    *,
    elapsed_seconds: float,
    cutoff_seconds: float,
    title: str = "Weibull SLA Breach Probability",
    fit_name: str | None = None,
) -> go.Figure:
    """Survival + density curve with the live breach probability shaded."""
    forecaster = WeibullForecaster()
    forecaster.fit(durations)
    fit = forecaster.get_fit(fit_name) or forecaster.best_fit
    fig = go.Figure()
    if fit is None:
        fig.add_annotation(text="Insufficient history to fit a distribution", showarrow=False)
        fig.update_layout(template=PLOT_TEMPLATE, title=title)
        return fig

    curve = forecaster.survival_curve(fit_name)
    fig.add_trace(
        go.Scatter(x=curve["t"], y=curve["survival"], name="Survival S(t)",
                   line=dict(color=NAVY, width=3), fill="tozeroy",
                   fillcolor="rgba(31,56,100,0.08)")
    )
    breach_p = forecaster.breach_probability(elapsed_seconds, cutoff_seconds, distribution=fit_name)

    fig.add_vline(x=elapsed_seconds, line=dict(color=ACCENT, width=2, dash="dot"),
                  annotation_text=f"elapsed {elapsed_seconds/60:.0f}m", annotation_position="top")
    fig.add_vline(x=cutoff_seconds, line=dict(color=RED, width=2),
                  annotation_text=f"SLA cutoff {cutoff_seconds/60:.0f}m", annotation_position="top left")

    fig.add_trace(
        go.Scatter(
            x=[elapsed_seconds, cutoff_seconds, cutoff_seconds, elapsed_seconds],
            y=[0, 0, float(fit.sf(cutoff_seconds)), float(fit.sf(elapsed_seconds))],
            fill="toself", fillcolor="rgba(192,57,43,0.18)",
            line=dict(color="rgba(0,0,0,0)"), name="Breach region", showlegend=True,
        )
    )
    for band, colour in zip(forecaster.bands, [GREEN, AMBER, RED]):
        val = fit.ppf(band / 100.0)
        fig.add_vline(x=val, line=dict(color=colour, width=1, dash="dash"),
                      annotation_text=f"P{band}", annotation_position="bottom")
    fig.update_layout(
        template=PLOT_TEMPLATE, title=f"{title}  |  P(breach) = {breach_p:.1%}",
        xaxis_title="Runtime (seconds)", yaxis_title="Probability of exceeding",
        legend=dict(orientation="h", y=1.08, x=0), margin=dict(t=80),
    )
    return fig


def volume_anomaly_figure(
    frame: pd.DataFrame,
    *,
    value_column: str = "rows_loaded",
    title: str = "Ingestion Volume vs Expected Baseline",
) -> go.Figure:
    """Actual vs expected volume with CUSUM / z-score alarm markers."""
    detector = VolumeAnomalyDetector()
    df = detector.detect(frame, value_column=value_column)
    fig = go.Figure()
    if df.empty:
        fig.add_annotation(text="No volume data", showarrow=False)
        fig.update_layout(template=PLOT_TEMPLATE, title=title)
        return fig

    dates = pd.to_datetime(df["execution_date"])
    fig.add_trace(go.Scatter(x=dates, y=df[value_column], name="Actual rows",
                             line=dict(color=NAVY, width=2), mode="lines+markers"))
    fig.add_trace(go.Scatter(x=dates, y=df["rolling_median"], name="Expected (rolling median)",
                             line=dict(color=ACCENT, width=2, dash="dash")))

    alarms = df[df["is_anomaly"]]
    if not alarms.empty:
        fig.add_trace(go.Scatter(
            x=pd.to_datetime(alarms["execution_date"]), y=alarms[value_column],
            mode="markers", name="Anomaly",
            marker=dict(color=RED, size=13, symbol="x", line=dict(width=1, color="white")),
            text=alarms["reason"], hovertemplate="%{x|%Y-%m-%d}<br>rows=%{y:,}<br>%{text}<extra></extra>",
        ))
    fig.update_layout(
        template=PLOT_TEMPLATE, title=title,
        xaxis_title="Execution date", yaxis_title="Rows loaded",
        legend=dict(orientation="h", y=1.1, x=0), margin=dict(t=80),
    )
    return fig


def cusum_figure(frame: pd.DataFrame, *, value_column: str = "rows_loaded",
                 title: str = "Two-Sided CUSUM Drift Monitor") -> go.Figure:
    """CUSUM statistic with the decision threshold marked."""
    detector = VolumeAnomalyDetector()
    df = detector.detect(frame, value_column=value_column)
    fig = go.Figure()
    if df.empty:
        fig.add_annotation(text="No volume data", showarrow=False)
        fig.update_layout(template=PLOT_TEMPLATE, title=title)
        return fig
    dates = pd.to_datetime(df["execution_date"])
    fig.add_trace(go.Scatter(x=dates, y=df["cusum_pos"], name="S+ (upward shift)",
                             line=dict(color=ACCENT, width=2)))
    fig.add_trace(go.Scatter(x=dates, y=df["cusum_neg"], name="S- (downward / silent drop)",
                             line=dict(color=RED, width=2)))
    fig.add_hline(y=detector.threshold_h, line=dict(color=AMBER, width=2, dash="dash"),
                  annotation_text="decision threshold h")
    fig.update_layout(template=PLOT_TEMPLATE, title=title,
                      xaxis_title="Execution date", yaxis_title="CUSUM statistic",
                      legend=dict(orientation="h", y=1.1, x=0), margin=dict(t=80))
    return fig


def lineage_figure(
    graph: LineageGraph,
    *,
    highlight_node: str | None = None,
    title: str = "Downstream Lineage Impact Explorer",
) -> go.Figure:
    """Interactive lineage map; blocked dashboards glow when a node stalls."""
    g = graph.graph
    if g.number_of_nodes() == 0:
        fig = go.Figure()
        fig.add_annotation(text="Empty lineage graph", showarrow=False)
        fig.update_layout(template=PLOT_TEMPLATE, title=title)
        return fig

    pos = _hierarchical_layout(g)
    impacted = set(graph.impacted_dashboards(highlight_node)) if highlight_node else set()

    edge_x, edge_y = [], []
    for u, v in g.edges():
        edge_x += [pos[u][0], pos[v][0], None]
        edge_y += [pos[u][1], pos[v][1], None]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=edge_x, y=edge_y, mode="lines",
                             line=dict(color="#D0D0D0", width=1), hoverinfo="none"))

    for kind, colour, size in [
        (NodeKind.TASK.value, ACCENT, 14),
        (NodeKind.STAGING.value, "#7F7F7F", 11),
        (NodeKind.DASHBOARD.value, NAVY, 18),
    ]:
        nodes = graph.nodes_of_kind(kind)
        if not nodes:
            continue
        node_colours = [
            RED if (n in impacted or n == highlight_node) else colour for n in nodes
        ]
        sizes = [size + 6 if (n in impacted or n == highlight_node) else size for n in nodes]
        fig.add_trace(go.Scatter(
            x=[pos[n][0] for n in nodes], y=[pos[n][1] for n in nodes],
            mode="markers+text", name=kind.replace("_", " ").title(),
            text=[g.nodes[n].get("label", n)[:22] for n in nodes],
            textposition="top center", textfont=dict(size=9),
            marker=dict(size=sizes, color=node_colours, line=dict(width=1, color="white")),
            hovertext=[f"{n}<br>{g.nodes[n].get('label','')}" for n in nodes],
            hoverinfo="text",
        ))
    fig.update_layout(
        template=PLOT_TEMPLATE, title=title, showlegend=True,
        xaxis=dict(showgrid=False, zeroline=False, visible=False),
        yaxis=dict(showgrid=False, zeroline=False, visible=False),
        legend=dict(orientation="h", y=1.08, x=0), margin=dict(t=80, l=20, r=20, b=20),
        height=620,
    )
    return fig


def _hierarchical_layout(g: nx.DiGraph) -> dict[str, tuple[float, float]]:
    """Left-to-right layered layout by longest-path depth."""
    try:
        layers = list(nx.topological_generations(g))
    except nx.NetworkXUnfeasible:  # pragma: no cover - defensive
        layers = [list(g.nodes())]
    pos: dict[str, tuple[float, float]] = {}
    n_layers = max(1, len(layers) - 1)
    for li, layer in enumerate(layers):
        x = li / n_layers if n_layers else 0.5
        count = max(1, len(layer))
        for ni, node in enumerate(sorted(layer)):
            y = 1.0 - (ni + 0.5) / count if count > 1 else 0.5
            pos[node] = (x, y)
    return pos


def blast_radius_table(graph: LineageGraph, settings: Settings | None = None, top_n: int = 10) -> pd.DataFrame:
    """Ranked blast radius per task."""
    calc = BlastRadiusCalculator(graph, settings)
    results = calc.rank_nodes(kind=NodeKind.TASK)
    rows = [
        {
            "task_id": r.node_id,
            "blast_radius_score": r.score,
            "category": r.category,
            "dashboards_at_risk": r.n_dashboards,
            "total_audience": r.total_audience,
        }
        for r in results[:top_n]
    ]
    return pd.DataFrame(rows)
