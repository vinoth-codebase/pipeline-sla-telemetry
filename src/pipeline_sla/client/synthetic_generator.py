"""High-fidelity synthetic Airflow telemetry generator.

Produces a multi-year (default 180-day) corpus of DAG runs and task instances
that behaves like a real enterprise ETL estate. Rather than emitting clean
uniform data, it deliberately injects the operational pathologies that
observability tooling must survive:

* **Weekend volume drops** -- batch volumes fall sharply Sat/Sun.
* **Month-end financial surges** -- GL reconciliation volume balloons 3-4x.
* **Database-lock stalls** -- a task on a specific date takes many multiples
  of its normal duration while the downstream run slips toward the SLA.
* **Silent empty runs** -- the job exits ``state='success'`` but
  ``rows_loaded == 0``: the classic "green but empty" ingestion failure that
  CUSUM is designed to catch.
* **Intermittent upstream network timeouts** -- tasks fail, retry and often
  recover, inflating the run's tail latency.

The generator is fully deterministic for a given ``seed`` so tests and
screenshots are reproducible.
"""

from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from pipeline_sla.config import Settings, load_settings
from pipeline_sla.storage.schema import DashboardLineage, DagRun, TaskInstance

logger = logging.getLogger(__name__)

_AVG_BYTES_PER_ROW = 512
_TASK_QUEUE_SECONDS = 12.0


@dataclass
class GenerationResult:
    """Container for one generation pass."""

    dag_runs: list[DagRun] = field(default_factory=list)
    task_instances: list[TaskInstance] = field(default_factory=list)
    dashboard_lineage: list[DashboardLineage] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.dag_runs)


def _is_month_end_surge(day: datetime, window: int = 3) -> bool:
    last_day = calendar.monthrange(day.year, day.month)[1]
    return day.day > (last_day - window)


def _lognormal(base: float, rng: np.random.Generator, sigma: float = 0.34) -> float:
    """Right-skewed draw with mean approximately equal to ``base``."""
    if base <= 0:
        return 0.0
    mu = np.log(base) - (sigma**2) / 2.0
    return float(rng.lognormal(mean=mu, sigma=sigma))


class SyntheticPipelineGenerator:
    """Simulates an enterprise Airflow estate over a configurable horizon."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        seed: int = 42,
        days: int = 180,
        end_date: datetime | None = None,
    ) -> None:
        self.settings = settings or load_settings()
        self.seed = seed
        self.days = days
        self.end_date = end_date or datetime(2026, 8, 19)
        self.rng = np.random.default_rng(seed)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def generate(
        self,
        days: int | None = None,
        *,
        end_date: datetime | None = None,
    ) -> GenerationResult:
        """Generate the full telemetry corpus.

        Returns DAG runs, task instances and the dashboard lineage map.
        """
        days = days or self.days
        end = end_date or self.end_date
        start = end - timedelta(days=days - 1)

        result = GenerationResult()
        for pipeline in self.settings.pipelines:
            self._generate_pipeline(pipeline, start, days, result)
        result.dashboard_lineage = self._build_lineage()
        logger.info(
            "Synthesised %d dag_runs, %d task_instances across %d pipelines over %d days",
            len(result.dag_runs), len(result.task_instances),
            len(self.settings.pipelines), days,
        )
        return result

    # ------------------------------------------------------------------ #
    # Per-pipeline simulation
    # ------------------------------------------------------------------ #
    def _generate_pipeline(
        self,
        pipeline: dict[str, Any],
        start: datetime,
        days: int,
        result: GenerationResult,
    ) -> None:
        dag_id = pipeline["dag_id"]
        tasks = sorted(pipeline["tasks"], key=lambda t: t["order"])
        failure_modes = pipeline.get("failure_modes", {})
        empty_dates = set(failure_modes.get("silent_empty_run_dates", []))
        lock_dates = set(failure_modes.get("db_lock_dates", []))
        weekend_drop = float(failure_modes.get("weekend_volume_drop", 0.4))
        month_end_surge = float(failure_modes.get("month_end_surge", 1.5))
        base_rows = float(pipeline.get("baseline_rows", 50_000))
        base_duration = float(pipeline.get("baseline_duration_seconds", 1200))
        sla_minutes = int(pipeline.get("sla_minutes", 240))

        for day_index in range(1, days + 1):
            execution_date = start + timedelta(days=day_index - 1)
            run_id = f"scheduled__{execution_date.strftime('%Y-%m-%dT%H:%M:%S')}"
            weekend = execution_date.weekday() >= 5
            month_end = _is_month_end_surge(execution_date)

            # ---- volume envelope -------------------------------------
            volume_factor = 1.0
            if weekend:
                volume_factor *= 1.0 - weekend_drop
            if month_end:
                volume_factor *= month_end_surge
            volume_factor *= float(self.rng.normal(1.0, 0.05))
            volume = max(0.0, base_rows * volume_factor)

            # ---- duration envelope -----------------------------------
            duration_factor = 1.0
            if month_end:
                # Sub-linear scaling: 3x volume does not mean 3x runtime.
                duration_factor *= month_end_surge**0.35
            if weekend:
                duration_factor *= 0.88

            silent_empty = day_index in empty_dates
            db_lock = day_index in lock_dates
            network_timeout = bool(self.rng.random() < 0.07)
            hard_failure = network_timeout and bool(self.rng.random() < 0.25)

            task_records: list[TaskInstance] = []
            run_start = self._run_start(execution_date, pipeline.get("schedule", "0 1 * * *"))
            cursor = run_start
            any_failed = False

            for task in tasks:
                share = float(task["duration_share"])
                base_task = base_duration * share * duration_factor
                duration = _lognormal(base_task, self.rng)
                retry_count = 0
                state = "success"
                failure_reason = None

                # Database-lock stall: heavy tail on one task of the DAG.
                if db_lock and task["order"] == 2:
                    duration *= float(self.rng.uniform(3.0, 5.5))
                    failure_reason = "database_lock_timeout"

                # Intermittent network timeout -> retry storm.
                if network_timeout and task["order"] == 1:
                    retry_count = int(self.rng.integers(1, 4))
                    duration += retry_count * float(self.rng.uniform(30.0, 120.0))
                    if hard_failure:
                        state = "failed"
                        failure_reason = "upstream_connection_timeout"
                        any_failed = True

                task_rows = 0 if silent_empty else int(volume * float(self.rng.uniform(0.97, 1.02)))
                task_start = cursor
                task_end = cursor + timedelta(seconds=duration)
                cursor = task_end

                task_records.append(
                    TaskInstance(
                        dag_id=dag_id,
                        task_id=task["task_id"],
                        run_id=run_id,
                        state=state,
                        execution_date=execution_date,
                        start_date=task_start,
                        end_date=task_end,
                        duration_seconds=round(duration, 2),
                        retry_count=retry_count,
                        rows_loaded=task_rows,
                        bytes_processed=task_rows * _AVG_BYTES_PER_ROW,
                        queued_seconds=round(float(self.rng.uniform(2.0, _TASK_QUEUE_SECONDS)), 2),
                        task_order=int(task["order"]),
                        failure_reason=failure_reason,
                    )
                )

            run_end = cursor
            run_duration = (run_end - run_start).total_seconds()
            if any_failed:
                run_state = "failed"
            elif network_timeout:
                run_state = "success"  # recovered after retries
            else:
                run_state = "success"

            result.dag_runs.append(
                DagRun(
                    dag_id=dag_id,
                    run_id=run_id,
                    state=run_state,
                    execution_date=execution_date,
                    start_date=run_start,
                    end_date=run_end,
                    duration_seconds=round(run_duration, 2),
                    sla_minutes=sla_minutes,
                    business_tower=pipeline.get("business_tower"),
                    criticality_tier=pipeline.get("criticality_tier"),
                    run_type="scheduled",
                )
            )
            result.task_instances.extend(task_records)

    @staticmethod
    def _run_start(execution_date: datetime, schedule: str) -> datetime:
        """Derive the run's wall-clock start from its cron schedule hour."""
        hour = 1
        minute = 0
        try:
            parts = schedule.split()
            if len(parts) >= 2:
                minute = int(parts[0])
                hour = int(parts[1])
        except (ValueError, IndexError):  # pragma: no cover - defensive
            pass
        return execution_date.replace(hour=hour, minute=minute, second=0, microsecond=0)

    # ------------------------------------------------------------------ #
    # Lineage
    # ------------------------------------------------------------------ #
    def _build_lineage(self) -> list[DashboardLineage]:
        lineage: list[DashboardLineage] = []
        for dash in self.settings.dashboards:
            lineage.append(
                DashboardLineage(
                    dashboard_id=dash["dashboard_id"],
                    dashboard_name=dash["dashboard_name"],
                    business_tower=dash.get("business_tower", ""),
                    criticality_tier=dash.get("criticality_tier", "TIER_3"),
                    parent_task_id=dash["parent_task_id"],
                    parent_dag_id=dash.get("parent_dag_id", ""),
                    audience=dash.get("audience", "ANALYST"),
                    audience_size=int(dash.get("audience_size", 0)),
                    target_sla_time=dash.get("target_sla_time", "07:00"),
                    platform=dash.get("platform", "Power BI"),
                )
            )
        return lineage


def generate_and_persist(
    warehouse: Any,
    *,
    days: int = 180,
    seed: int = 42,
    settings: Settings | None = None,
) -> GenerationResult:
    """Generate telemetry and load it straight into a warehouse."""
    generator = SyntheticPipelineGenerator(settings=settings, seed=seed, days=days)
    result = generator.generate(days)
    warehouse.insert_dag_runs(result.dag_runs)
    warehouse.insert_task_instances(result.task_instances)
    warehouse.insert_dashboard_lineage(result.dashboard_lineage)
    return result
