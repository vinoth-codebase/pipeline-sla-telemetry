"""Tests for the Airflow REST client, auth providers and synthetic generator."""

from __future__ import annotations

import httpx
import pytest

from pipeline_sla.client.airflow_client import AirflowApiError, AirflowRestClient
from pipeline_sla.client.auth import (
    BasicAuthProvider,
    NoAuthProvider,
    TokenAuthProvider,
    build_auth_provider,
)
from pipeline_sla.client.synthetic_generator import (
    SyntheticPipelineGenerator,
    generate_and_persist,
)


# --------------------------------------------------------------------------- #
# Auth providers
# --------------------------------------------------------------------------- #
def test_no_auth_provider_headers():
    provider = NoAuthProvider()
    assert provider.headers()["Accept"] == "application/json"
    assert provider.describe() == "no-auth"


def test_token_auth_provider():
    provider = TokenAuthProvider(token="secret-token")
    assert provider.headers()["Authorization"] == "Bearer secret-token"
    assert "token" in provider.describe()


def test_token_auth_provider_requires_token():
    with pytest.raises(ValueError):
        TokenAuthProvider(token="")


def test_basic_auth_provider_base64():
    provider = BasicAuthProvider(username="admin", password="pw")
    header = provider.headers()["Authorization"]
    assert header.startswith("Basic ")
    assert provider.describe() == "basic(admin)"


def test_basic_auth_provider_requires_username():
    with pytest.raises(ValueError):
        BasicAuthProvider(username="", password="x")


@pytest.mark.parametrize(
    "method,expected",
    [("token", TokenAuthProvider), ("bearer", TokenAuthProvider), ("jwt", TokenAuthProvider),
     ("basic", BasicAuthProvider), ("password", BasicAuthProvider),
     ("none", NoAuthProvider), ("noauth", NoAuthProvider)],
)
def test_build_auth_provider_factory(method, expected):
    provider = build_auth_provider(method, token="t", username="u", password="p")
    assert isinstance(provider, expected)


def test_build_auth_provider_rejects_unknown():
    with pytest.raises(ValueError):
        build_auth_provider("kerberos")


# --------------------------------------------------------------------------- #
# Client extraction
# --------------------------------------------------------------------------- #
def _client(transport, **kwargs):
    return AirflowRestClient(
        "http://airflow.local",
        auth_method="token",
        token="abc",
        transport=transport,
        page_size=2,
        sleep=lambda _: None,
        **kwargs,
    )


def test_list_dags_paginates(mock_transport):
    transport, handler = mock_transport
    with _client(transport) as client:
        dags = client.list_dags()
    assert len(dags) == 2
    assert any("offset=0" in c for c in handler.calls)


def test_get_dag_runs_maps_records(mock_transport):
    transport, _ = mock_transport
    with _client(transport) as client:
        runs = client.get_dag_runs("sales_ingestion_etl")
    assert len(runs) == 5
    run = runs[0]
    assert run.dag_id == "sales_ingestion_etl"
    assert run.state == "success"
    assert run.duration_seconds == pytest.approx(1800.0)
    assert run.run_type == "scheduled"


def test_get_task_instances_attaches_xcom_metrics(mock_transport):
    transport, _ = mock_transport
    with _client(transport) as client:
        runs = client.get_dag_runs("sales_ingestion_etl")
        tasks = client.get_task_instances("sales_ingestion_etl", runs[0].run_id)
    assert len(tasks) == 2
    extract = next(t for t in tasks if t.task_id == "extract_source")
    assert extract.rows_loaded == 12345
    assert extract.bytes_processed == 6312960
    assert extract.queued_seconds == pytest.approx(60.0)
    load = next(t for t in tasks if t.task_id == "load_target")
    assert load.retry_count == 1


def test_get_xcom_metrics_returns_flat_map(mock_transport):
    transport, _ = mock_transport
    with _client(transport) as client:
        metrics = client.get_xcom_metrics("d", "r", "t")
    assert metrics["row_count_loaded"] == 12345


def test_extract_dag_history_combines_runs_and_tasks(mock_transport):
    transport, _ = mock_transport
    with _client(transport) as client:
        runs, tasks = client.extract_dag_history(["sales_ingestion_etl"])
    assert len(runs) == 5
    assert len(tasks) == 10


def test_retry_backoff_recovers(retry_transport):
    transport, handler = retry_transport
    delays: list[float] = []
    with _client(transport, max_retries=4, backoff_factor=0.1) as client:
        client._sleep = delays.append  # type: ignore[assignment]
        runs = client.get_dag_runs("sales_ingestion_etl")
    assert len(runs) == 5
    assert delays  # backoff was exercised
    assert delays[0] == pytest.approx(0.1)


def test_retry_exhaustion_raises():
    def always_fail(request):
        return httpx.Response(503, json={"detail": "down"})

    transport = httpx.MockTransport(always_fail)
    client = AirflowRestClient(
        "http://airflow.local", auth_method="none", transport=transport,
        max_retries=2, sleep=lambda _: None,
    )
    with pytest.raises(AirflowApiError):
        client.list_dags()
    client.close()


def test_client_error_status_raises():
    def not_found(request):
        return httpx.Response(404, json={"detail": "nope"})

    transport = httpx.MockTransport(not_found)
    client = AirflowRestClient("http://airflow.local", auth_method="none", transport=transport,
                               max_retries=0, sleep=lambda _: None)
    with pytest.raises(AirflowApiError) as exc:
        client.list_dags()
    assert exc.value.status_code == 404
    client.close()


def test_api_root_property():
    client = AirflowRestClient("http://airflow.local/", auth_method="none", sleep=lambda _: None)
    assert client.api_root == "http://airflow.local/api/v1"
    client.close()


# --------------------------------------------------------------------------- #
# Synthetic generator
# --------------------------------------------------------------------------- #
def test_generator_produces_expected_shape(settings):
    gen = SyntheticPipelineGenerator(settings=settings, seed=7, days=60)
    result = gen.generate(60)
    assert len(result.dag_runs) == 60 * len(settings.pipelines)
    assert len(result.dashboard_lineage) == len(settings.dashboards)
    assert result.task_instances


def test_generator_is_deterministic(settings):
    a = SyntheticPipelineGenerator(settings=settings, seed=11, days=30).generate(30)
    b = SyntheticPipelineGenerator(settings=settings, seed=11, days=30).generate(30)
    assert [r.duration_seconds for r in a.dag_runs] == [r.duration_seconds for r in b.dag_runs]


def test_generator_injects_silent_empty_runs(settings):
    gen = SyntheticPipelineGenerator(settings=settings, seed=42, days=120)
    result = gen.generate(120)
    silent = [
        t for t in result.task_instances
        if t.dag_id == "sales_ingestion_etl" and t.rows_loaded == 0 and t.state == "success"
    ]
    assert silent, "expected at least one silent zero-row ingestion run"
    assert silent[0].bytes_processed == 0


def test_generator_injects_weekend_volume_drops(settings):
    gen = SyntheticPipelineGenerator(settings=settings, seed=3, days=90)
    result = gen.generate(90)
    load = [
        t for t in result.task_instances
        if t.dag_id == "marketing_attribution" and t.task_id == "score_touchpoints" and t.rows_loaded > 0
    ]
    weekday = [t.rows_loaded for t in load if t.execution_date.weekday() < 5]
    weekend = [t.rows_loaded for t in load if t.execution_date.weekday() >= 5]
    assert weekend and sum(weekend) / len(weekend) < sum(weekday) / len(weekday)


def test_generator_marks_failed_and_retried_tasks(settings):
    result = SyntheticPipelineGenerator(settings=settings, seed=5, days=180).generate(180)
    assert any(t.retry_count > 0 for t in result.task_instances)
    assert any(t.failure_reason for t in result.task_instances)


def test_generate_and_persist(empty_warehouse, settings):
    result = generate_and_persist(empty_warehouse, days=45, seed=9, settings=settings)
    counts = empty_warehouse.counts()
    assert counts["dag_runs"] == len(result.dag_runs)
    assert counts["task_instances"] == len(result.task_instances)
    assert counts["dashboard_lineage"] == len(result.dashboard_lineage)


def test_generation_result_len(settings):
    result = SyntheticPipelineGenerator(settings=settings, seed=1, days=10).generate(10)
    assert len(result) == len(result.dag_runs)
