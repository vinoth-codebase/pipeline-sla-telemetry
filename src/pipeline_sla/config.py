"""Configuration loading for PipelineSLA-Telemetry.

Loads the three YAML configuration documents that describe the runtime:

* ``config.yaml``               -- global thresholds, SLAs, detection tuning
* ``pipelines.yaml``            -- the enterprise pipeline estate definition
* ``dashboards_lineage.yaml``   -- downstream BI dashboard lineage map

Environment variables of the form ``${VAR}`` inside the YAML are resolved at
load time so that secrets (Airflow tokens, credentials) never live in the repo.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")

# Repository root: <root>/src/pipeline_sla/config.py -> <root>
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_DIR = PROJECT_ROOT / "config"


def _resolve_env(value: Any) -> Any:
    """Recursively substitute ``${VAR}`` / ``${VAR:-default}`` in strings."""
    if isinstance(value, str):
        def _sub(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            return os.environ.get(name, default if default is not None else "")

        return _ENV_PATTERN.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _resolve_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env(v) for v in value]
    return value


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    resolved = _resolve_env(raw)
    if not isinstance(resolved, dict):
        raise ValueError(f"Configuration root must be a mapping: {path}")
    return resolved


@dataclass(frozen=True)
class AirflowSettings:
    """Connection settings for the Airflow REST client."""

    base_url: str = "http://localhost:8080"
    api_version: str = "v1"
    auth_method: str = "token"
    token: str = ""
    username: str = ""
    password: str = ""
    page_size: int = 100
    max_retries: int = 5
    backoff_factor: float = 0.5
    timeout_seconds: float = 30.0
    verify_ssl: bool = True

    @property
    def api_root(self) -> str:
        return f"{self.base_url.rstrip('/')}/api/{self.api_version}"


@dataclass(frozen=True)
class StorageSettings:
    database_path: str = "data/telemetry.duckdb"
    parquet_dir: str = "data/parquet"
    in_memory: bool = False


@dataclass(frozen=True)
class SLASettings:
    morning_cutoff: str = "07:00"
    timezone: str = "Asia/Kolkata"
    default_attainment_target: float = 99.0
    warning_attainment_target: float = 95.0


@dataclass(frozen=True)
class ForecastSettings:
    distribution_primary: str = "weibull"
    distribution_secondary: str = "lognorm"
    min_samples_for_fit: int = 20
    breach_probability_alert: float = 0.35
    breach_probability_critical: float = 0.65
    percentile_bands: tuple[int, ...] = (50, 85, 95)


@dataclass(frozen=True)
class DetectionSettings:
    cusum_slack_k: float = 0.5
    cusum_threshold_h: float = 5.0
    cusum_two_sided: bool = True
    zscore_window: int = 14
    zscore_alert: float = 3.0
    zscore_min_periods: int = 7
    empty_run_ratio: float = 0.05
    baseline_window_days: int = 28


@dataclass(frozen=True)
class LineageSettings:
    tier_weights: dict[str, float] = field(default_factory=dict)
    audience_scale: dict[str, float] = field(default_factory=dict)
    escalation: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ReportingSettings:
    output_dir: str = "reports"
    excel_filename: str = "weekly_pipeline_health.xlsx"
    pptx_filename: str = "monday_operations_review.pptx"
    pptx_title: str = "Monday Morning Operations Review"


@dataclass(frozen=True)
class DashboardSettings:
    title: str = "PipelineSLA Telemetry Cockpit"
    refresh_seconds: int = 60
    simple_browser_url: str = "http://localhost:8501"


@dataclass
class Settings:
    """Aggregated, typed view over every configuration document."""

    airflow: AirflowSettings = field(default_factory=AirflowSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)
    sla: SLASettings = field(default_factory=SLASettings)
    forecast: ForecastSettings = field(default_factory=ForecastSettings)
    detection: DetectionSettings = field(default_factory=DetectionSettings)
    lineage: LineageSettings = field(default_factory=LineageSettings)
    reporting: ReportingSettings = field(default_factory=ReportingSettings)
    dashboard: DashboardSettings = field(default_factory=DashboardSettings)
    pipelines: list[dict[str, Any]] = field(default_factory=list)
    dashboards: list[dict[str, Any]] = field(default_factory=list)
    staging_tables: list[dict[str, Any]] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    def pipeline(self, dag_id: str) -> dict[str, Any] | None:
        for entry in self.pipelines:
            if entry.get("dag_id") == dag_id:
                return entry
        return None

    @property
    def pipeline_ids(self) -> list[str]:
        return [p["dag_id"] for p in self.pipelines]


def load_settings(config_dir: str | Path | None = None) -> Settings:
    """Load and validate the full configuration set.

    Parameters
    ----------
    config_dir:
        Directory holding the three YAML documents. Defaults to the repository
        ``config/`` directory, or the value of ``PIPELINE_SLA_CONFIG_DIR``.
    """

    if config_dir is None:
        config_dir = os.environ.get("PIPELINE_SLA_CONFIG_DIR", str(DEFAULT_CONFIG_DIR))
    base = Path(config_dir)

    core = _load_yaml(base / "config.yaml")
    pipelines_doc = _load_yaml(base / "pipelines.yaml")
    lineage_doc = _load_yaml(base / "dashboards_lineage.yaml")

    airflow_raw = core.get("airflow", {})
    auth_raw = airflow_raw.get("auth", {})
    req_raw = airflow_raw.get("request", {})
    storage_raw = core.get("storage", {})
    sla_raw = core.get("sla", {})
    forecast_raw = core.get("forecast", {})
    detection_raw = core.get("detection", {})
    cusum_raw = detection_raw.get("cusum", {})
    zscore_raw = detection_raw.get("zscore", {})
    volume_raw = detection_raw.get("volume", {})
    lineage_raw = core.get("lineage", {})
    reporting_raw = core.get("reporting", {})
    excel_raw = reporting_raw.get("excel", {})
    pptx_raw = reporting_raw.get("pptx", {})
    dashboard_raw = core.get("dashboard", {})

    settings = Settings(
        airflow=AirflowSettings(
            base_url=airflow_raw.get("base_url", "http://localhost:8080"),
            api_version=airflow_raw.get("api_version", "v1"),
            auth_method=auth_raw.get("method", "token"),
            token=auth_raw.get("token", ""),
            username=auth_raw.get("username", ""),
            password=auth_raw.get("password", ""),
            page_size=int(req_raw.get("page_size", 100)),
            max_retries=int(req_raw.get("max_retries", 5)),
            backoff_factor=float(req_raw.get("backoff_factor", 0.5)),
            timeout_seconds=float(req_raw.get("timeout_seconds", 30.0)),
            verify_ssl=bool(req_raw.get("verify_ssl", True)),
        ),
        storage=StorageSettings(
            database_path=storage_raw.get("database_path", "data/telemetry.duckdb"),
            parquet_dir=storage_raw.get("parquet_dir", "data/parquet"),
            in_memory=bool(storage_raw.get("in_memory", False)),
        ),
        sla=SLASettings(
            morning_cutoff=sla_raw.get("morning_cutoff", "07:00"),
            timezone=sla_raw.get("timezone", "Asia/Kolkata"),
            default_attainment_target=float(sla_raw.get("default_attainment_target", 99.0)),
            warning_attainment_target=float(sla_raw.get("warning_attainment_target", 95.0)),
        ),
        forecast=ForecastSettings(
            distribution_primary=forecast_raw.get("distribution_primary", "weibull"),
            distribution_secondary=forecast_raw.get("distribution_secondary", "lognorm"),
            min_samples_for_fit=int(forecast_raw.get("min_samples_for_fit", 20)),
            breach_probability_alert=float(forecast_raw.get("breach_probability_alert", 0.35)),
            breach_probability_critical=float(forecast_raw.get("breach_probability_critical", 0.65)),
            percentile_bands=tuple(int(b) for b in forecast_raw.get("percentile_bands", [50, 85, 95])),
        ),
        detection=DetectionSettings(
            cusum_slack_k=float(cusum_raw.get("slack_k", 0.5)),
            cusum_threshold_h=float(cusum_raw.get("threshold_h", 5.0)),
            cusum_two_sided=bool(cusum_raw.get("two_sided", True)),
            zscore_window=int(zscore_raw.get("window", 14)),
            zscore_alert=float(zscore_raw.get("alert_z", 3.0)),
            zscore_min_periods=int(zscore_raw.get("min_periods", 7)),
            empty_run_ratio=float(volume_raw.get("empty_run_ratio", 0.05)),
            baseline_window_days=int(volume_raw.get("baseline_window_days", 28)),
        ),
        lineage=LineageSettings(
            tier_weights=dict(lineage_raw.get("tier_weights", {})),
            audience_scale=dict(lineage_raw.get("audience_scale", {})),
            escalation=dict(lineage_raw.get("escalation", {})),
        ),
        reporting=ReportingSettings(
            output_dir=reporting_raw.get("output_dir", "reports"),
            excel_filename=excel_raw.get("filename", "weekly_pipeline_health.xlsx"),
            pptx_filename=pptx_raw.get("filename", "monday_operations_review.pptx"),
            pptx_title=pptx_raw.get("title", "Monday Morning Operations Review"),
        ),
        dashboard=DashboardSettings(
            title=dashboard_raw.get("title", "PipelineSLA Telemetry Cockpit"),
            refresh_seconds=int(dashboard_raw.get("refresh_seconds", 60)),
            simple_browser_url=dashboard_raw.get("simple_browser_url", "http://localhost:8501"),
        ),
        pipelines=list(pipelines_doc.get("pipelines", [])),
        dashboards=list(lineage_doc.get("dashboards", [])),
        staging_tables=list(lineage_doc.get("staging_tables", [])),
        raw=core,
    )
    return settings
