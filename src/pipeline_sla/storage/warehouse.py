"""In-process analytical telemetry warehouse built on DuckDB + Apache Arrow.

The warehouse is deliberately dependency-light: DuckDB runs embedded, needs no
server, and speaks Arrow natively, so the same class serves two very different
workloads:

* **production** -- a persistent ``data/telemetry.duckdb`` file shared by the
  CLI, the Streamlit cockpit and the reporting engines;
* **benchmarking / unit tests** -- ``in_memory=True`` gives a zero-disk instance
  that is created and torn down per test, with sub-millisecond query latency.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import duckdb
import pandas as pd

from pipeline_sla.storage.schema import (
    ALL_DDL,
    DashboardLineage,
    DagRun,
    TaskInstance,
)

logger = logging.getLogger(__name__)

_DAG_RUN_COLUMNS = list(DagRun.__dataclass_fields__)
_TASK_COLUMNS = list(TaskInstance.__dataclass_fields__)
_DASHBOARD_COLUMNS = list(DashboardLineage.__dataclass_fields__)


class TelemetryWarehouse:
    """Embedded DuckDB warehouse for pipeline telemetry.

    Parameters
    ----------
    database_path:
        Path to the persistent DuckDB file. Ignored when ``in_memory`` is True.
    in_memory:
        When True, open a private ``:memory:`` database (ideal for tests).
    read_only:
        Open the file read-only (useful for the dashboard against a locked DB).
    """

    def __init__(
        self,
        database_path: str | Path | None = None,
        *,
        in_memory: bool = False,
        read_only: bool = False,
    ) -> None:
        self.in_memory = in_memory
        self.read_only = read_only
        if in_memory or database_path in (None, ":memory:"):
            self.database_path = ":memory:"
            self.in_memory = True
        else:
            self.database_path = str(database_path)
            if not read_only:
                Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)

        self._conn = duckdb.connect(self.database_path, read_only=read_only)
        if not read_only:
            self.initialize_schema()

    # ------------------------------------------------------------------ #
    # Connection lifecycle
    # ------------------------------------------------------------------ #
    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def initialize_schema(self) -> None:
        """Create the three core tables plus secondary indexes (idempotent)."""
        for ddl in ALL_DDL:
            try:
                self._conn.execute(ddl)
            except duckdb.Error as exc:  # pragma: no cover - defensive
                # Index creation on a freshly-created table is safe to retry.
                if "already exists" not in str(exc).lower():
                    raise
        logger.debug("Warehouse schema initialised at %s", self.database_path)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:  # pragma: no cover - defensive
            pass

    def __enter__(self) -> "TelemetryWarehouse":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Run a block inside an explicit transaction, rolling back on error."""
        self._conn.execute("BEGIN TRANSACTION")
        try:
            yield self._conn
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------ #
    # Ingestion
    # ------------------------------------------------------------------ #
    def insert_dag_runs(self, runs: Iterable[DagRun] | pd.DataFrame) -> int:
        df = self._coerce(runs, _DAG_RUN_COLUMNS, DagRun)
        if df.empty:
            return 0
        self._conn.register("_dag_runs_stage", df)
        self._conn.execute(
            "INSERT OR REPLACE INTO dag_runs SELECT * FROM _dag_runs_stage"
        )
        self._conn.unregister("_dag_runs_stage")
        return len(df)

    def insert_task_instances(self, tasks: Iterable[TaskInstance] | pd.DataFrame) -> int:
        df = self._coerce(tasks, _TASK_COLUMNS, TaskInstance)
        if df.empty:
            return 0
        self._conn.register("_tasks_stage", df)
        self._conn.execute("INSERT INTO task_instances SELECT * FROM _tasks_stage")
        self._conn.unregister("_tasks_stage")
        return len(df)

    def insert_dashboard_lineage(self, rows: Iterable[DashboardLineage] | pd.DataFrame) -> int:
        df = self._coerce(rows, _DASHBOARD_COLUMNS, DashboardLineage)
        if df.empty:
            return 0
        self._conn.register("_lineage_stage", df)
        self._conn.execute(
            "INSERT OR REPLACE INTO dashboard_lineage SELECT * FROM _lineage_stage"
        )
        self._conn.unregister("_lineage_stage")
        return len(df)

    @staticmethod
    def _coerce(
        records: Iterable[Any] | pd.DataFrame,
        columns: Sequence[str],
        dataclass_type: type,
    ) -> pd.DataFrame:
        if isinstance(records, pd.DataFrame):
            df = records.copy()
        else:
            rows = [r.to_dict() if hasattr(r, "to_dict") else dict(r) for r in records]
            df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=list(columns))
        # Guarantee a stable column order matching the DDL.
        for col in columns:
            if col not in df.columns:
                df[col] = None
        return df[list(columns)]

    # ------------------------------------------------------------------ #
    # Extraction -> pandas / Arrow
    # ------------------------------------------------------------------ #
    def query_df(self, sql: str, params: Sequence[Any] | None = None) -> pd.DataFrame:
        """Execute SQL and return a pandas DataFrame."""
        return self._conn.execute(sql, params or []).fetchdf()

    def query_arrow(self, sql: str, params: Sequence[Any] | None = None):
        """Execute SQL and return an Arrow Table (zero-copy handoff)."""
        return self._conn.execute(sql, params or []).fetch_arrow_table()

    def get_dag_runs(self, dag_id: str | None = None) -> pd.DataFrame:
        if dag_id:
            return self.query_df(
                "SELECT * FROM dag_runs WHERE dag_id = ? ORDER BY execution_date", [dag_id]
            )
        return self.query_df("SELECT * FROM dag_runs ORDER BY execution_date")

    def get_task_instances(
        self,
        dag_id: str | None = None,
        task_id: str | None = None,
    ) -> pd.DataFrame:
        clauses, params = [], []
        if dag_id:
            clauses.append("dag_id = ?")
            params.append(dag_id)
        if task_id:
            clauses.append("task_id = ?")
            params.append(task_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self.query_df(
            f"SELECT * FROM task_instances {where} ORDER BY execution_date", params
        )

    def get_dashboard_lineage(self) -> pd.DataFrame:
        return self.query_df("SELECT * FROM dashboard_lineage ORDER BY criticality_tier")

    def get_task_durations(self, dag_id: str, task_id: str) -> list[float]:
        """Successful-task duration history, used to fit the SLA forecaster."""
        df = self.query_df(
            """
            SELECT duration_seconds FROM task_instances
            WHERE dag_id = ? AND task_id = ? AND state = 'success'
              AND duration_seconds IS NOT NULL
            ORDER BY execution_date
            """,
            [dag_id, task_id],
        )
        return [float(v) for v in df["duration_seconds"].tolist()]

    def get_volume_series(self, dag_id: str, task_id: str) -> pd.DataFrame:
        """Execution-date-indexed rows_loaded series for drift detection."""
        return self.query_df(
            """
            SELECT execution_date, rows_loaded, state
            FROM task_instances
            WHERE dag_id = ? AND task_id = ?
            ORDER BY execution_date
            """,
            [dag_id, task_id],
        )

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for table in ("dag_runs", "task_instances", "dashboard_lineage"):
            out[table] = int(self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        return out

    def table_names(self) -> list[str]:
        rows = self._conn.execute("SHOW TABLES").fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------ #
    # Parquet interchange + partition pruning
    # ------------------------------------------------------------------ #
    def export_table_to_parquet(self, table: str, directory: str | Path) -> Path:
        """Write a table out as a hive-partitioned Parquet dataset."""
        if table not in {"dag_runs", "task_instances", "dashboard_lineage"}:
            raise ValueError(f"Unknown table for export: {table}")
        out = Path(directory)
        out.mkdir(parents=True, exist_ok=True)
        target = out / f"{table}.parquet"
        self._conn.execute(
            f"COPY (SELECT * FROM {table}) TO '{target.as_posix()}' (FORMAT PARQUET)"
        )
        return target

    def ingest_parquet(self, table: str, path: str | Path) -> int:
        """Bulk-load a Parquet file into a table (fast columnar path)."""
        if table not in {"dag_runs", "task_instances", "dashboard_lineage"}:
            raise ValueError(f"Unknown table for ingest: {table}")
        before = int(self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        self._conn.execute(
            f"INSERT INTO {table} SELECT * FROM read_parquet('{Path(path).as_posix()}')"
        )
        after = int(self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        return after - before

    def parquet_partition_scan(
        self,
        table: str,
        date_column: str,
        start: str,
        end: str,
        directory: str | Path,
    ) -> pd.DataFrame:
        """Read a Parquet dataset with partition pruning on a date range."""
        pattern = Path(directory) / f"{table}.parquet"
        return self.query_df(
            f"""
            SELECT * FROM read_parquet('{pattern.as_posix()}')
            WHERE {date_column} BETWEEN ? AND ?
            ORDER BY {date_column}
            """,
            [start, end],
        )

    # ------------------------------------------------------------------ #
    # Convenience
    # ------------------------------------------------------------------ #
    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        self._conn.execute(sql, params or [])

    def latest_execution_date(self) -> pd.Timestamp | None:
        row = self._conn.execute("SELECT MAX(execution_date) FROM dag_runs").fetchone()
        if not row or row[0] is None:
            return None
        return pd.Timestamp(row[0])
