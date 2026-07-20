"""Shared pytest fixtures for the PipelineSLA-Telemetry test suite."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest

from pipeline_sla.client.synthetic_generator import SyntheticPipelineGenerator
from pipeline_sla.config import load_settings
from pipeline_sla.lineage.dag_graph import LineageGraph
from pipeline_sla.storage.warehouse import TelemetryWarehouse

# A horizon long enough to include the day-42 silent-empty drop and give the
# Weibull fitter well over its 20-sample minimum.
GENERATION_DAYS = 150
SEED = 42


@pytest.fixture(scope="session")
def settings():
    return load_settings()


@pytest.fixture(scope="session")
def generated(settings):
    """A deterministic 150-day synthetic estate, shared across the session."""
    generator = SyntheticPipelineGenerator(settings=settings, seed=SEED, days=GENERATION_DAYS)
    return generator.generate(GENERATION_DAYS)


@pytest.fixture()
def warehouse(generated):
    """In-memory warehouse pre-loaded with the synthetic estate."""
    wh = TelemetryWarehouse(in_memory=True)
    wh.insert_dag_runs(generated.dag_runs)
    wh.insert_task_instances(generated.task_instances)
    wh.insert_dashboard_lineage(generated.dashboard_lineage)
    yield wh
    wh.close()


@pytest.fixture()
def empty_warehouse():
    wh = TelemetryWarehouse(in_memory=True)
    yield wh
    wh.close()


@pytest.fixture(scope="session")
def lineage_graph(settings):
    return LineageGraph.from_settings(settings)


@pytest.fixture()
def tmp_db(tmp_path) -> Path:
    return tmp_path / "test_telemetry.duckdb"


# --------------------------------------------------------------------------- #
# Mock Airflow server
# --------------------------------------------------------------------------- #
class MockAirflowHandler:
    """A tiny in-memory Airflow REST API used to exercise the client offline."""

    def __init__(self, *, fail_first: int = 0, page_size: int = 2) -> None:
        self.fail_first = fail_first
        self.page_size = page_size
        self.calls: list[str] = []
        self._attempts = 0

    def _dag_runs(self, dag_id: str) -> list[dict]:
        base = datetime(2026, 6, 1, 1, 0, 0)
        runs = []
        for i in range(5):
            start = base + timedelta(days=i)
            end = start + timedelta(minutes=30 + i)
            runs.append({
                "dag_run_id": f"scheduled__{start.isoformat()}",
                "state": "success",
                "execution_date": start.isoformat(),
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "run_type": "scheduled",
            })
        return runs

    def _task_instances(self, dag_id: str, run_id: str) -> list[dict]:
        return [
            {
                "task_id": "extract_source",
                "state": "success",
                "execution_date": "2026-06-01T01:00:00",
                "start_date": "2026-06-01T01:00:00",
                "end_date": "2026-06-01T01:05:00",
                "duration": 300.0,
                "try_number": 1,
                "task_order": 1,
                "queued_when": "2026-06-01T00:59:00",
            },
            {
                "task_id": "load_target",
                "state": "success",
                "execution_date": "2026-06-01T01:00:00",
                "start_date": "2026-06-01T01:05:00",
                "end_date": "2026-06-01T01:20:00",
                "duration": 900.0,
                "try_number": 2,
                "task_order": 2,
            },
        ]

    def _xcom(self, dag_id: str, run_id: str, task_id: str) -> list[dict]:
        return [
            {"key": "row_count_loaded", "value": 12345},
            {"key": "bytes_processed", "value": 6312960},
        ]

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self._attempts += 1
        self.calls.append(str(request.url))
        if self._attempts <= self.fail_first:
            return httpx.Response(503, json={"detail": "temporarily unavailable"})

        path = request.url.path
        params = dict(request.url.params)
        offset = int(params.get("offset", 0))
        limit = int(params.get("limit", self.page_size))

        if path.endswith("/dags"):
            items = [{"dag_id": "sales_ingestion_etl"}, {"dag_id": "crm_customer_360"}]
            key = "dags"
        elif path.endswith("/dagRuns"):
            dag_id = path.split("/dags/")[1].split("/")[0]
            items = self._dag_runs(dag_id)
            key = "dag_runs"
        elif "/taskInstances/" in path and path.endswith("/xcomEntries"):
            parts = path.split("/dags/")[1].split("/")
            items = self._xcom(parts[0], parts[2], parts[4])
            key = "xcom_entries"
        elif path.endswith("/taskInstances"):
            parts = path.split("/dags/")[1].split("/")
            items = self._task_instances(parts[0], parts[2])
            key = "task_instances"
        else:
            return httpx.Response(404, json={"detail": "not found"})

        page = items[offset: offset + limit]
        return httpx.Response(
            200,
            json={key: page, "total_entries": len(items)},
            headers={"content-type": "application/json"},
        )


@pytest.fixture()
def mock_transport():
    handler = MockAirflowHandler()
    return httpx.MockTransport(handler), handler


@pytest.fixture()
def retry_transport():
    handler = MockAirflowHandler(fail_first=2)
    return httpx.MockTransport(handler), handler
