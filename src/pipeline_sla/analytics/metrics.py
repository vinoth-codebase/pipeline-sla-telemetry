"""Operational reliability metrics for the pipeline estate.

Computes the classic SRE / data-reliability KPIs that executives actually ask
for on a Monday morning:

* **MTTR** -- Mean Time To Resolution, the average time to recover from a
  failed run.
* **MTBF** -- Mean Time Between Failures, the average gap between failures.
* **First-Run Success Rate** -- share of runs that succeed without retry.
* **SLA Attainment** -- share of runs completing before their SLA envelope.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np
import pandas as pd

FAILED_STATES = {"failed", "upstream_failed"}


@dataclass
class OperationalMetrics:
    """Container for the computed reliability KPIs of one pipeline."""

    dag_id: str
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    mttr_seconds: float = 0.0
    mtbf_hours: float = 0.0
    first_run_success_rate: float = 0.0
    sla_attainment: float = 0.0
    avg_duration_seconds: float = 0.0
    p95_duration_seconds: float = 0.0
    total_rows_loaded: int = 0
    extras: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def compute_mttr(runs: pd.DataFrame, *, duration_column: str = "duration_seconds") -> float:
    """Mean Time To Resolution: mean recovery duration of failed runs (seconds).

    Recovery time is approximated by the failed run's own wall-clock duration,
    which is the operationally observable proxy when run-level repair events are
    not separately logged.
    """
    if runs is None or runs.empty or "state" not in runs.columns:
        return 0.0
    failed = runs[runs["state"].isin(FAILED_STATES)]
    if failed.empty or duration_column not in failed.columns:
        return 0.0
    durations = pd.to_numeric(failed[duration_column], errors="coerce").dropna()
    if durations.empty:
        return 0.0
    return float(durations.mean())


def compute_mtbf(runs: pd.DataFrame) -> float:
    """Mean Time Between Failures in hours, derived from execution timestamps."""
    if runs is None or runs.empty or "state" not in runs.columns:
        return 0.0
    ordered = runs.sort_values("execution_date")
    states = ordered["state"].tolist()
    failure_positions = [i for i, s in enumerate(states) if s in FAILED_STATES]
    if len(failure_positions) < 2:
        return 0.0
    times = pd.to_datetime(ordered["execution_date"])
    gaps_hours = [
        (times.iloc[b] - times.iloc[a]).total_seconds() / 3600.0
        for a, b in zip(failure_positions, failure_positions[1:])
    ]
    gaps_hours = [g for g in gaps_hours if g > 0]
    return float(np.mean(gaps_hours)) if gaps_hours else 0.0


def compute_first_run_success_rate(tasks: pd.DataFrame) -> float:
    """Percentage of successful task instances that needed zero retries."""
    if tasks is None or tasks.empty or "state" not in tasks.columns:
        return 0.0
    success = tasks[tasks["state"] == "success"]
    if success.empty:
        return 0.0
    retries = pd.to_numeric(success.get("retry_count", 0), errors="coerce").fillna(0)
    return float((retries == 0).sum() / len(success) * 100.0)


def compute_sla_attainment(runs: pd.DataFrame, *, sla_column: str = "sla_minutes") -> float:
    """Percentage of runs finishing within their SLA envelope."""
    if runs is None or runs.empty or sla_column not in runs.columns:
        return 0.0
    completed = runs[
        runs["state"].isin({"success", "failed"}) & runs["duration_seconds"].notna()
    ]
    if completed.empty:
        return 0.0
    limits = pd.to_numeric(completed[sla_column], errors="coerce") * 60.0
    durations = pd.to_numeric(completed["duration_seconds"], errors="coerce")
    within = (durations <= limits) & limits.notna()
    return float(within.sum() / len(completed) * 100.0)


def compute_pipeline_metrics(
    dag_id: str,
    runs: pd.DataFrame,
    tasks: pd.DataFrame | None = None,
) -> OperationalMetrics:
    """Compute the full KPI set for one pipeline."""
    if runs is None or runs.empty:
        return OperationalMetrics(dag_id=dag_id)

    successful = int((runs["state"] == "success").sum())
    failed = int(runs["state"].isin(FAILED_STATES).sum())
    durations = pd.to_numeric(runs["duration_seconds"], errors="coerce").dropna()

    rows_loaded = 0
    if tasks is not None and not tasks.empty and "rows_loaded" in tasks.columns:
        rows_loaded = int(pd.to_numeric(tasks["rows_loaded"], errors="coerce").fillna(0).sum())

    return OperationalMetrics(
        dag_id=dag_id,
        total_runs=int(len(runs)),
        successful_runs=successful,
        failed_runs=failed,
        mttr_seconds=round(compute_mttr(runs), 2),
        mtbf_hours=round(compute_mtbf(runs), 2),
        first_run_success_rate=round(compute_first_run_success_rate(tasks), 2)
        if tasks is not None
        else 0.0,
        sla_attainment=round(compute_sla_attainment(runs), 2),
        avg_duration_seconds=round(float(durations.mean()), 2) if not durations.empty else 0.0,
        p95_duration_seconds=round(float(np.percentile(durations, 95)), 2)
        if not durations.empty
        else 0.0,
        total_rows_loaded=rows_loaded,
    )


def operational_summary(
    warehouse: Any,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Build a per-pipeline KPI table straight from the warehouse."""
    runs = warehouse.get_dag_runs()
    tasks = warehouse.get_task_instances()
    if runs.empty:
        return pd.DataFrame()

    if start_date:
        runs = runs[pd.to_datetime(runs["execution_date"]) >= pd.Timestamp(start_date)]
    if end_date:
        runs = runs[pd.to_datetime(runs["execution_date"]) <= pd.Timestamp(end_date)]
    if start_date and not tasks.empty:
        tasks = tasks[pd.to_datetime(tasks["execution_date"]) >= pd.Timestamp(start_date)]

    records = []
    for dag_id in sorted(runs["dag_id"].unique()):
        dag_runs = runs[runs["dag_id"] == dag_id]
        dag_tasks = tasks[tasks["dag_id"] == dag_id] if not tasks.empty else None
        records.append(compute_pipeline_metrics(dag_id, dag_runs, dag_tasks).to_dict())
    return pd.DataFrame(records)
