#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""PipelineSLA-Telemetry :: VS Code Python Interactive analysis walkthrough.

Open this file in VS Code and click **"Run Cell"** above any ``# %%`` marker.
The output renders in the native **Python Interactive** pane: interactive Plotly
distributions, filterable DataFrames and Weibull probability curves — all
without leaving the editor.

Run headlessly too::

    python scripts/interactive_analysis.py

which prints the same tables and writes the Plotly figures to
``reports/interactive/`` as standalone HTML.
"""

# %%
# =============================================================================
# Cell 1 — Bootstrap: connect to the telemetry warehouse
# =============================================================================
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from IPython.display import display

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

from pipeline_sla.analytics.metrics import operational_summary
from pipeline_sla.analytics.weibull_forecaster import WeibullForecaster
from pipeline_sla.analytics.cusum_detector import VolumeAnomalyDetector
from pipeline_sla.config import load_settings
from pipeline_sla.dashboard import views
from pipeline_sla.lineage.dag_graph import LineageGraph
from pipeline_sla.lineage.blast_radius import BlastRadiusCalculator
from pipeline_sla.storage.warehouse import TelemetryWarehouse

pd.set_option("display.max_columns", 40)
pd.set_option("display.width", 160)

settings = load_settings()
db_path = _ROOT / settings.storage.database_path
if not db_path.exists():
    print("Warehouse not found — seeding a fresh 180-day estate...")
    from pipeline_sla.client.synthetic_generator import SyntheticPipelineGenerator

    _wh = TelemetryWarehouse(db_path)
    _gen = SyntheticPipelineGenerator(settings=settings, seed=42, days=180)
    _res = _gen.generate(180)
    _wh.insert_dag_runs(_res.dag_runs)
    _wh.insert_task_instances(_res.task_instances)
    _wh.insert_dashboard_lineage(_res.dashboard_lineage)
    _wh.close()

warehouse = TelemetryWarehouse(db_path, read_only=True)
print(f"Connected to {db_path}")
print(warehouse.counts())

# %%
# =============================================================================
# Cell 2 — Estate reliability scorecard (filterable DataFrame)
# =============================================================================
overview = operational_summary(warehouse)
display(overview.style.format({
    "mttr_seconds": "{:,.0f}",
    "mtbf_hours": "{:,.1f}",
    "first_run_success_rate": "{:.1f}%",
    "sla_attainment": "{:.1f}%",
    "avg_duration_seconds": "{:,.0f}",
    "p95_duration_seconds": "{:,.0f}",
    "total_rows_loaded": "{:,.0f}",
}).background_gradient(subset=["sla_attainment"], cmap="RdYlGn", vmin=80, vmax=100))

# %%
# =============================================================================
# Cell 3 — Weibull fit for the finance reconciliation task
# =============================================================================
DAG = "gl_financial_reconciliation"
TASK = "reconcile_subledgers"

durations = warehouse.get_task_durations(DAG, TASK)
print(f"{len(durations)} successful runs for {DAG}.{TASK}")

forecaster = WeibullForecaster().fit(durations)

fit_table = pd.DataFrame([f.to_dict() for f in forecaster.fits.values()])
display(fit_table)

percentiles = forecaster.percentiles()
print("\nRolling completion boundaries (minutes):")
display(pd.DataFrame([{f"P{k}": round(v / 60, 1) for k, v in percentiles.items()}]))

# %%
# =============================================================================
# Cell 4 — Interactive Weibull survival curve + live breach probability
# =============================================================================
elapsed_seconds = 1_200       # the overnight run has been going 20 minutes
cutoff_seconds = 3_600        # the 07:00 SLA allows 60 minutes

result = forecaster.forecast(DAG, TASK, elapsed_seconds, cutoff_seconds)
print(f"Best fit            : {result.best_distribution}")
print(f"P(SLA breach)       : {result.breach_probability:.2%}")
print(f"Risk level          : {result.risk_level}")
print(f"Expected remaining  : {result.expected_remaining_seconds/60:.1f} min")

fig_weibull = views.weibull_probability_figure(
    durations, elapsed_seconds=elapsed_seconds, cutoff_seconds=cutoff_seconds,
    title=f"Weibull SLA Breach Forecast — {TASK}",
)
fig_weibull.show()

# %%
# =============================================================================
# Cell 5 — Two-Sided CUSUM: hunting the silent zero-row ingestion failure
# =============================================================================
detector = VolumeAnomalyDetector()
series = warehouse.get_volume_series("sales_ingestion_etl", "load_sales_fact")
detected = detector.detect(series)

silent = detected[detected["is_silent_empty"]]
print(f"Silent empty runs detected: {len(silent)}")
display(silent[["execution_date", "rows_loaded", "rolling_median", "severity", "reason"]])

fig_cusum = views.cusum_figure(series, title="Two-Sided CUSUM — load_sales_fact")
fig_cusum.show()

# %%
# =============================================================================
# Cell 6 — Volume anomaly overlay (actual vs expected + alarm markers)
# =============================================================================
fig_volume = views.volume_anomaly_figure(
    series, title="Ingestion Volume vs Expected — sales_ingestion_etl.load_sales_fact"
)
fig_volume.show()

# %%
# =============================================================================
# Cell 7 — Downstream blast radius: whose dashboards go dark?
# =============================================================================
graph = LineageGraph.from_settings(settings)
calculator = BlastRadiusCalculator(graph)

stalled = "reconcile_subledgers"
impact = calculator.compute(stalled)
print(f"Task '{stalled}' → blast radius {impact.score:,.0f} ({impact.category})")
display(pd.DataFrame([d.to_dict() for d in impact.impacted]))

fig_lineage = views.lineage_figure(graph, highlight_node=stalled)
fig_lineage.show()

# %%
# =============================================================================
# Cell 8 — Rank the whole estate by blast radius
# =============================================================================
ranking = views.blast_radius_table(graph, settings, top_n=10)
display(ranking.style.background_gradient(subset=["blast_radius_score"], cmap="Reds"))

# %%
# =============================================================================
# Cell 9 — Headless export of every figure (runs only as a script)
# =============================================================================
def export_figures(out_dir: Path | None = None) -> list[Path]:
    out = out_dir or (_ROOT / "reports" / "interactive")
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for name, figure in {
        "weibull_breach_curve": fig_weibull,
        "cusum_drift_monitor": fig_cusum,
        "volume_anomalies": fig_volume,
        "lineage_blast_radius": fig_lineage,
    }.items():
        path = out / f"{name}.html"
        figure.write_html(path, include_plotlyjs="cdn")
        written.append(path)
    return written


if __name__ == "__main__":  # pragma: no cover
    for p in export_figures():
        print(f"wrote {p}")
    warehouse.close()
