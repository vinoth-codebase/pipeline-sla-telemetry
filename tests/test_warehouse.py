"""Tests for the DuckDB warehouse, schema records and configuration loading."""

from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import pytest

from pipeline_sla.config import Settings, load_settings
from pipeline_sla.storage.schema import DashboardLineage, DagRun, TaskInstance
from pipeline_sla.storage.warehouse import TelemetryWarehouse


# --------------------------------------------------------------------------- #
# Schema records
# --------------------------------------------------------------------------- #
def test_dag_run_roundtrip():
    run = DagRun(dag_id="d", run_id="r", state="success", execution_date=datetime(2026, 1, 1))
    assert DagRun.from_dict(run.to_dict()) == run


def test_task_instance_roundtrip():
    task = TaskInstance(dag_id="d", task_id="t", run_id="r", state="success",
                        execution_date=datetime(2026, 1, 1), rows_loaded=10)
    assert TaskInstance.from_dict(task.to_dict()) == task


def test_dashboard_lineage_roundtrip():
    dash = DashboardLineage(
        dashboard_id="x", dashboard_name="X", business_tower="Finance",
        criticality_tier="TIER_1", parent_task_id="t", parent_dag_id="d",
        audience="BOARD", audience_size=100, target_sla_time="07:00", platform="Power BI",
    )
    assert DashboardLineage.from_dict(dash.to_dict()) == dash


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
def test_load_settings_reads_pipelines(settings):
    assert isinstance(settings, Settings)
    assert len(settings.pipelines) == 6
    assert "gl_financial_reconciliation" in settings.pipeline_ids
    assert len(settings.dashboards) == 10
    assert settings.sla.morning_cutoff == "07:00"
    assert settings.forecast.percentile_bands == (50, 85, 95)


def test_settings_pipeline_lookup(settings):
    pipeline = settings.pipeline("sales_ingestion_etl")
    assert pipeline is not None
    assert pipeline["business_tower"] == "Commercial"
    assert settings.pipeline("does_not_exist") is None


def test_settings_env_substitution(monkeypatch, tmp_path):
    monkeypatch.setenv("AIRFLOW_API_TOKEN", "env-token-123")
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.yaml").write_text(
        "airflow:\n  base_url: http://x\n  auth:\n    method: token\n    token: ${AIRFLOW_API_TOKEN}\n",
        encoding="utf-8",
    )
    (cfg_dir / "pipelines.yaml").write_text("pipelines: []\n", encoding="utf-8")
    (cfg_dir / "dashboards_lineage.yaml").write_text("dashboards: []\n", encoding="utf-8")
    loaded = load_settings(cfg_dir)
    assert loaded.airflow.token == "env-token-123"


def test_load_settings_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_settings(tmp_path)


# --------------------------------------------------------------------------- #
# Warehouse lifecycle
# --------------------------------------------------------------------------- #
def test_in_memory_warehouse_has_schema(empty_warehouse):
    assert set(empty_warehouse.table_names()) >= {"dag_runs", "task_instances", "dashboard_lineage"}
    assert empty_warehouse.counts() == {"dag_runs": 0, "task_instances": 0, "dashboard_lineage": 0}


def test_persistent_warehouse_file(tmp_db):
    wh = TelemetryWarehouse(tmp_db)
    assert tmp_db.exists()
    wh.execute("INSERT INTO dag_runs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
               ["d", "r", "success", datetime(2026, 1, 1), None, None, 10.0, 60, "Finance", "TIER_1", "scheduled"])
    assert wh.counts()["dag_runs"] == 1
    wh.close()

    reopened = TelemetryWarehouse(tmp_db, read_only=True)
    assert reopened.counts()["dag_runs"] == 1
    reopened.close()


def test_warehouse_context_manager(tmp_db):
    with TelemetryWarehouse(tmp_db) as wh:
        assert wh.counts()["dag_runs"] == 0


# --------------------------------------------------------------------------- #
# Ingestion and extraction
# --------------------------------------------------------------------------- #
def test_insert_and_query_dag_runs(warehouse, generated):
    df = warehouse.get_dag_runs()
    assert len(df) == len(generated.dag_runs)
    one = warehouse.get_dag_runs("crm_customer_360")
    assert set(one["dag_id"].unique()) == {"crm_customer_360"}


def test_get_task_instances_filters(warehouse):
    all_tasks = warehouse.get_task_instances()
    filtered = warehouse.get_task_instances(dag_id="inventory_sync_dag", task_id="load_inventory_mart")
    assert len(filtered) < len(all_tasks)
    assert set(filtered["task_id"].unique()) == {"load_inventory_mart"}


def test_get_task_durations_returns_success_only(warehouse):
    durations = warehouse.get_task_durations("sales_ingestion_etl", "load_sales_fact")
    assert len(durations) > 100
    assert all(d > 0 for d in durations)


def test_get_volume_series(warehouse):
    series = warehouse.get_volume_series("sales_ingestion_etl", "load_sales_fact")
    assert {"execution_date", "rows_loaded", "state"} <= set(series.columns)
    assert len(series) > 100


def test_get_dashboard_lineage(warehouse):
    lineage = warehouse.get_dashboard_lineage()
    assert len(lineage) == 10
    assert "criticality_tier" in lineage.columns


def test_insert_dataframe_directly(empty_warehouse):
    df = pd.DataFrame([{
        "dag_id": "d", "run_id": "r", "state": "success", "execution_date": datetime(2026, 1, 1),
        "start_date": None, "end_date": None, "duration_seconds": 1.0, "sla_minutes": 60,
        "business_tower": "T", "criticality_tier": "TIER_1", "run_type": "scheduled",
    }])
    assert empty_warehouse.insert_dag_runs(df) == 1
    assert empty_warehouse.insert_dag_runs([]) == 0


def test_latest_execution_date(warehouse, empty_warehouse):
    assert warehouse.latest_execution_date() is not None
    assert empty_warehouse.latest_execution_date() is None


def test_transaction_rollback(empty_warehouse):
    with pytest.raises(RuntimeError):
        with empty_warehouse.transaction() as conn:
            conn.execute("INSERT INTO dashboard_lineage VALUES (?,?,?,?,?,?,?,?,?,?)",
                         ["x", "X", "T", "TIER_1", "t", "d", "BOARD", 1, "07:00", "Power BI"])
            raise RuntimeError("boom")
    assert empty_warehouse.counts()["dashboard_lineage"] == 0


# --------------------------------------------------------------------------- #
# Parquet interchange
# --------------------------------------------------------------------------- #
def test_parquet_export_and_reingest(warehouse, empty_warehouse, tmp_path):
    path = warehouse.export_table_to_parquet("task_instances", tmp_path)
    assert path.exists()
    loaded = empty_warehouse.ingest_parquet("task_instances", path)
    assert loaded == warehouse.counts()["task_instances"]


def test_parquet_partition_scan(warehouse, tmp_path):
    warehouse.export_table_to_parquet("dag_runs", tmp_path)
    scanned = warehouse.parquet_partition_scan(
        "dag_runs", "execution_date", "2026-01-01", "2026-12-31", tmp_path
    )
    assert len(scanned) == warehouse.counts()["dag_runs"]


def test_parquet_unknown_table_raises(warehouse, tmp_path):
    with pytest.raises(ValueError):
        warehouse.export_table_to_parquet("nope", tmp_path)
    with pytest.raises(ValueError):
        warehouse.ingest_parquet("nope", tmp_path / "x.parquet")


def test_query_arrow_returns_table(warehouse):
    table = warehouse.query_arrow("SELECT COUNT(*) AS n FROM dag_runs")
    assert table.num_rows == 1
