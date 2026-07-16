"""Warehouse schema: typed records + DDL for the DuckDB telemetry store.

Three tables form the analytical backbone:

``dag_runs``
    One row per DAG execution (state, timings, duration).
``task_instances``
    One row per task within a run (duration, retries, rows loaded).
``dashboard_lineage``
    The downstream business map: which task output feeds which BI dashboard,
    how critical it is and how large its audience is.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

DAG_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS dag_runs (
    dag_id              VARCHAR NOT NULL,
    run_id              VARCHAR NOT NULL,
    state               VARCHAR NOT NULL,
    execution_date      TIMESTAMP NOT NULL,
    start_date          TIMESTAMP,
    end_date            TIMESTAMP,
    duration_seconds    DOUBLE,
    sla_minutes         INTEGER,
    business_tower      VARCHAR,
    criticality_tier    VARCHAR,
    run_type            VARCHAR DEFAULT 'scheduled',
    PRIMARY KEY (dag_id, run_id)
);
"""

TASK_INSTANCES_DDL = """
CREATE TABLE IF NOT EXISTS task_instances (
    dag_id              VARCHAR NOT NULL,
    task_id             VARCHAR NOT NULL,
    run_id              VARCHAR NOT NULL,
    state               VARCHAR NOT NULL,
    execution_date      TIMESTAMP NOT NULL,
    start_date          TIMESTAMP,
    end_date            TIMESTAMP,
    duration_seconds    DOUBLE,
    retry_count         INTEGER DEFAULT 0,
    rows_loaded         BIGINT DEFAULT 0,
    bytes_processed     BIGINT DEFAULT 0,
    queued_seconds      DOUBLE DEFAULT 0,
    task_order          INTEGER DEFAULT 0,
    failure_reason      VARCHAR
);
"""

DASHBOARD_LINEAGE_DDL = """
CREATE TABLE IF NOT EXISTS dashboard_lineage (
    dashboard_id        VARCHAR NOT NULL,
    dashboard_name      VARCHAR NOT NULL,
    business_tower      VARCHAR,
    criticality_tier    VARCHAR,
    parent_task_id      VARCHAR NOT NULL,
    parent_dag_id       VARCHAR,
    audience            VARCHAR,
    audience_size       BIGINT DEFAULT 0,
    target_sla_time     VARCHAR,
    platform            VARCHAR,
    PRIMARY KEY (dashboard_id)
);
"""

TASK_INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_task_instances_task_id ON task_instances (task_id);",
    "CREATE INDEX IF NOT EXISTS idx_task_instances_dag_id ON task_instances (dag_id);",
    "CREATE INDEX IF NOT EXISTS idx_dag_runs_execution_date ON dag_runs (execution_date);",
]

ALL_DDL = [DAG_RUNS_DDL, TASK_INSTANCES_DDL, DASHBOARD_LINEAGE_DDL, *TASK_INDEX_DDL]


@dataclass
class DagRun:
    """A single DAG execution record."""

    dag_id: str
    run_id: str
    state: str
    execution_date: datetime
    start_date: datetime | None = None
    end_date: datetime | None = None
    duration_seconds: float | None = None
    sla_minutes: int | None = None
    business_tower: str | None = None
    criticality_tier: str | None = None
    run_type: str = "scheduled"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "DagRun":
        return cls(**{k: row[k] for k in cls.__dataclass_fields__ if k in row})


@dataclass
class TaskInstance:
    """A single task execution within a DAG run."""

    dag_id: str
    task_id: str
    run_id: str
    state: str
    execution_date: datetime
    start_date: datetime | None = None
    end_date: datetime | None = None
    duration_seconds: float | None = None
    retry_count: int = 0
    rows_loaded: int = 0
    bytes_processed: int = 0
    queued_seconds: float = 0.0
    task_order: int = 0
    failure_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "TaskInstance":
        return cls(**{k: row[k] for k in cls.__dataclass_fields__ if k in row})


@dataclass
class DashboardLineage:
    """Downstream BI dashboard and the upstream task that feeds it."""

    dashboard_id: str
    dashboard_name: str
    business_tower: str
    criticality_tier: str
    parent_task_id: str
    parent_dag_id: str
    audience: str
    audience_size: int
    target_sla_time: str
    platform: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "DashboardLineage":
        return cls(**{k: row[k] for k in cls.__dataclass_fields__ if k in row})
