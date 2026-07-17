"""Client package: Airflow REST extraction + synthetic telemetry generation."""

from __future__ import annotations

from pipeline_sla.client.airflow_client import AirflowRestClient, AirflowApiError
from pipeline_sla.client.auth import (
    AuthProvider,
    BasicAuthProvider,
    NoAuthProvider,
    TokenAuthProvider,
    build_auth_provider,
)
from pipeline_sla.client.synthetic_generator import SyntheticPipelineGenerator

__all__ = [
    "AirflowRestClient",
    "AirflowApiError",
    "AuthProvider",
    "TokenAuthProvider",
    "BasicAuthProvider",
    "NoAuthProvider",
    "build_auth_provider",
    "SyntheticPipelineGenerator",
]
