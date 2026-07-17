"""Resilient Apache Airflow REST API extraction client.

Extracts the operational telemetry that feeds the warehouse:

* ``/api/v1/dags``            -- DAG inventory
* ``/api/v1/dags/{id}/dagRuns``       -- run history (state, timings)
* ``/api/v1/dags/{id}/dagRuns/{rid}/taskInstances`` -- per-task execution
* ``/api/v1/dags/{id}/dagRuns/{rid}/taskInstances/{tid}/xcomEntries`` -- XCom metrics

Every list endpoint is paginated (``offset``/``limit``); transient 5xx and
connection errors are retried with exponential backoff.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

import httpx

from pipeline_sla.client.auth import AuthProvider, build_auth_provider
from pipeline_sla.storage.schema import DagRun, TaskInstance

logger = logging.getLogger(__name__)

# XCom keys we treat as volume telemetry.
XCOM_ROW_COUNT_KEY = "row_count_loaded"
XCOM_BYTES_KEY = "bytes_processed"

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class AirflowApiError(RuntimeError):
    """Raised when the Airflow API returns an unrecoverable error."""

    def __init__(self, message: str, *, status_code: int | None = None, url: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


class AirflowRestClient:
    """Thin, resilient wrapper over the Airflow stable REST API.

    Parameters
    ----------
    base_url:
        Airflow webserver root, e.g. ``http://localhost:8080``.
    auth:
        A pre-built :class:`AuthProvider`, or ``None`` to build one from the
        explicit ``auth_method`` / token / username / password arguments.
    transport:
        Optional ``httpx`` transport, used to inject a mock server in tests.
    sleep:
        Injectable sleep function so backoff can be tested without real waits.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        *,
        api_version: str = "v1",
        auth: AuthProvider | None = None,
        auth_method: str = "none",
        token: str = "",
        username: str = "",
        password: str = "",
        page_size: int = 100,
        max_retries: int = 5,
        backoff_factor: float = 0.5,
        timeout: float = 30.0,
        verify_ssl: bool = True,
        transport: httpx.BaseTransport | None = None,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self.page_size = page_size
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self._sleep = sleep
        self.auth = auth or build_auth_provider(
            auth_method, token=token, username=username, password=password
        )
        self._client = client or httpx.Client(
            timeout=timeout,
            verify=verify_ssl,
            transport=transport,
            follow_redirects=True,
        )

    # ------------------------------------------------------------------ #
    # Low-level request + retry
    # ------------------------------------------------------------------ #
    @property
    def api_root(self) -> str:
        return f"{self.base_url}/api/{self.api_version}"

    def _request(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.api_root}{path}"
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.get(url, params=params, headers=self.auth.headers())
            except httpx.TransportError as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                self._backoff(attempt, exc)
                continue

            if response.status_code in RETRYABLE_STATUS:
                last_error = AirflowApiError(
                    f"Retryable status {response.status_code} for {url}",
                    status_code=response.status_code,
                    url=url,
                )
                if attempt >= self.max_retries:
                    break
                self._backoff(attempt, last_error)
                continue

            # Non-retryable client/server error: fail fast, preserving the code.
            if response.status_code >= 400:
                raise AirflowApiError(
                    f"Airflow API error {response.status_code} for {url}",
                    status_code=response.status_code,
                    url=url,
                )
            return response.json()

        raise AirflowApiError(f"Exhausted retries for {url}: {last_error}", url=url)

    def _backoff(self, attempt: int, error: Exception) -> None:
        delay = self.backoff_factor * (2**attempt)
        logger.warning(
            "Airflow request failed (%s), retrying in %.2fs [%d/%d]",
            error, delay, attempt + 1, self.max_retries,
        )
        self._sleep(delay)

    def _paginate(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Walk ``offset``/``limit`` pages until the collection is exhausted."""
        limit = limit or self.page_size
        offset = 0
        collected: list[dict[str, Any]] = []
        while True:
            page_params = dict(params or {})
            page_params["limit"] = limit
            page_params["offset"] = offset
            payload = self._request(path, page_params)
            items = payload.get(_collection_key(payload), [])
            collected.extend(items)
            total = payload.get("total_entries")
            if not items or len(items) < limit:
                break
            if total is not None and offset + len(items) >= total:
                break
            offset += len(items)
        return collected

    # ------------------------------------------------------------------ #
    # High-level extraction
    # ------------------------------------------------------------------ #
    def list_dags(self, limit: int | None = None) -> list[dict[str, Any]]:
        return self._paginate("/dags", limit=limit)

    def get_dag_runs(
        self,
        dag_id: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
    ) -> list[DagRun]:
        params: dict[str, Any] = {"order_by": "-execution_date"}
        if start_date:
            params["execution_date_gte"] = start_date
        if end_date:
            params["execution_date_lte"] = end_date
        raw = self._paginate(f"/dags/{dag_id}/dagRuns", params=params, limit=limit)
        return [self._to_dag_run(dag_id, item) for item in raw]

    def get_task_instances(
        self,
        dag_id: str,
        run_id: str,
        *,
        limit: int | None = None,
    ) -> list[TaskInstance]:
        raw = self._paginate(
            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances", limit=limit
        )
        instances = [self._to_task_instance(dag_id, run_id, item) for item in raw]
        for instance in instances:
            xcom = self.get_xcom_metrics(dag_id, run_id, instance.task_id)
            instance.rows_loaded = int(xcom.get(XCOM_ROW_COUNT_KEY, 0) or 0)
            instance.bytes_processed = int(xcom.get(XCOM_BYTES_KEY, 0) or 0)
        return instances

    def get_xcom_metrics(self, dag_id: str, run_id: str, task_id: str) -> dict[str, Any]:
        """Return a flat {key: value} map of XCom entries for a task instance."""
        try:
            raw = self._paginate(
                f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries"
            )
        except AirflowApiError:  # pragma: no cover - defensive
            return {}
        metrics: dict[str, Any] = {}
        for entry in raw:
            key = entry.get("key")
            if key:
                metrics[key] = entry.get("value")
        return metrics

    def extract_dag_history(
        self,
        dag_ids: Iterable[str],
        *,
        limit: int | None = None,
    ) -> tuple[list[DagRun], list[TaskInstance]]:
        """Convenience: pull runs + tasks for several DAGs in one call."""
        runs: list[DagRun] = []
        tasks: list[TaskInstance] = []
        for dag_id in dag_ids:
            dag_runs = self.get_dag_runs(dag_id, limit=limit)
            runs.extend(dag_runs)
            for run in dag_runs:
                tasks.extend(self.get_task_instances(dag_id, run.run_id))
        return runs, tasks

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "AirflowRestClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Mapping helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _to_dag_run(dag_id: str, item: dict[str, Any]) -> DagRun:
        start = _parse_dt(item.get("start_date"))
        end = _parse_dt(item.get("end_date"))
        duration = None
        if start and end:
            duration = (end - start).total_seconds()
        execution = _parse_dt(item.get("execution_date")) or _parse_dt(item.get("logical_date"))
        if execution is None:
            execution = datetime.now(timezone.utc)
        return DagRun(
            dag_id=dag_id,
            run_id=item.get("dag_run_id") or item.get("run_id") or "",
            state=(item.get("state") or "unknown"),
            execution_date=execution,
            start_date=start,
            end_date=end,
            duration_seconds=duration,
            sla_minutes=item.get("sla_minutes"),
            business_tower=item.get("business_tower"),
            criticality_tier=item.get("criticality_tier"),
            run_type=item.get("run_type", "scheduled"),
        )

    @staticmethod
    def _to_task_instance(dag_id: str, run_id: str, item: dict[str, Any]) -> TaskInstance:
        start = _parse_dt(item.get("start_date"))
        end = _parse_dt(item.get("end_date"))
        duration = item.get("duration")
        if duration is None and start and end:
            duration = (end - start).total_seconds()
        execution = _parse_dt(item.get("execution_date")) or start
        if execution is None:
            execution = datetime.now(timezone.utc)
        queued = 0.0
        if start and item.get("queued_when"):
            queued_dt = _parse_dt(item.get("queued_when"))
            if queued_dt:
                queued = max(0.0, (start - queued_dt).total_seconds())
        return TaskInstance(
            dag_id=dag_id,
            task_id=item.get("task_id", ""),
            run_id=run_id,
            state=(item.get("state") or "unknown"),
            execution_date=execution,
            start_date=start,
            end_date=end,
            duration_seconds=duration,
            retry_count=int(item.get("try_number", 1) or 1) - 1,
            queued_seconds=queued,
            task_order=int(item.get("task_order", 0) or 0),
        )


def _collection_key(payload: dict[str, Any]) -> str:
    """Infer the list key from a paginated Airflow payload."""
    for key in ("dags", "dag_runs", "task_instances", "xcom_entries"):
        if key in payload:
            return key
    # Fallback: first list-valued field.
    for key, value in payload.items():
        if isinstance(value, list):
            return key
    return "items"
