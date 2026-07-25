"""Tests for the Weibull/Log-Normal SLA forecaster and operational metrics."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline_sla.analytics.metrics import (
    compute_first_run_success_rate,
    compute_mtbf,
    compute_mttr,
    compute_pipeline_metrics,
    compute_sla_attainment,
    operational_summary,
)
from pipeline_sla.analytics.weibull_forecaster import WeibullForecaster


# --------------------------------------------------------------------------- #
# Fitting
# --------------------------------------------------------------------------- #
def test_fit_returns_both_distributions(warehouse):
    durations = warehouse.get_task_durations("gl_financial_reconciliation", "reconcile_subledgers")
    forecaster = WeibullForecaster().fit(durations)
    assert set(forecaster.fits) == {"weibull", "lognorm"}
    for fit in forecaster.fits.values():
        assert fit.n_samples == len(durations)
        assert np.isfinite(fit.aic)
        assert 0.0 <= fit.ks_statistic <= 1.0


def test_best_fit_is_lowest_aic(warehouse):
    durations = warehouse.get_task_durations("risk_credit_scoring", "score_credit_models")
    forecaster = WeibullForecaster().fit(durations)
    best = forecaster.best_fit
    assert best.aic == min(f.aic for f in forecaster.fits.values())


def test_fit_requires_min_samples():
    forecaster = WeibullForecaster(min_samples=20)
    forecaster.fit([1.0, 2.0, 3.0])
    assert forecaster.fits == {}
    assert forecaster.best_fit is None
    assert forecaster.percentiles() == {}
    assert forecaster.breach_probability(10, 100) == 0.0
    assert forecaster.survival_curve().empty


def test_fit_ignores_non_positive_values():
    forecaster = WeibullForecaster(min_samples=5).fit([0.0, -1.0, None, *range(1, 40)])
    assert "weibull" in forecaster.fits
    assert forecaster.fits["weibull"].n_samples == 39


def test_distribution_functions_are_consistent(warehouse):
    forecaster = WeibullForecaster().fit(
        warehouse.get_task_durations("inventory_sync_dag", "reconcile_stock_levels")
    )
    fit = forecaster.best_fit
    median = fit.median()
    assert fit.cdf(median) == pytest.approx(0.5, abs=1e-6)
    assert fit.sf(median) == pytest.approx(0.5, abs=1e-6)
    assert fit.pdf(median) > 0
    assert fit.mean() > 0
    assert fit.std() > 0
    assert fit.ppf(0.95) > median


def test_fit_to_dict_serialisable(warehouse):
    forecaster = WeibullForecaster().fit(
        warehouse.get_task_durations("marketing_attribution", "score_touchpoints")
    )
    payload = forecaster.best_fit.to_dict()
    assert {"name", "params", "aic", "ks_statistic", "mean_seconds"} <= set(payload)


# --------------------------------------------------------------------------- #
# Percentiles & boundaries
# --------------------------------------------------------------------------- #
def test_percentiles_are_monotonic(warehouse):
    forecaster = WeibullForecaster().fit(
        warehouse.get_task_durations("sales_ingestion_etl", "load_sales_fact")
    )
    pcts = forecaster.percentiles()
    assert pcts[50] < pcts[85] < pcts[95]


def test_rolling_percentiles_shape(warehouse):
    durations = warehouse.get_task_durations("crm_customer_360", "dedupe_customer_keys")
    forecaster = WeibullForecaster()
    frame = forecaster.rolling_percentiles(durations, window=30)
    assert list(frame.columns) == ["p50", "p85", "p95"]
    assert len(frame) == len(durations)
    tail = frame.dropna()
    assert (tail["p50"] <= tail["p95"]).all()


# --------------------------------------------------------------------------- #
# Forecasting
# --------------------------------------------------------------------------- #
def test_breach_probability_increases_with_elapsed(warehouse):
    forecaster = WeibullForecaster().fit(
        warehouse.get_task_durations("gl_financial_reconciliation", "reconcile_subledgers")
    )
    early = forecaster.breach_probability(100, 1800)
    late = forecaster.breach_probability(1500, 1800)
    assert 0.0 <= early <= late <= 1.0


def test_breach_probability_is_one_past_cutoff(warehouse):
    forecaster = WeibullForecaster().fit(
        warehouse.get_task_durations("gl_financial_reconciliation", "reconcile_subledgers")
    )
    assert forecaster.breach_probability(5000, 1800) == 1.0


def test_breach_probability_remaining_method(warehouse):
    forecaster = WeibullForecaster().fit(
        warehouse.get_task_durations("gl_financial_reconciliation", "reconcile_subledgers")
    )
    conditional = forecaster.breach_probability(600, 1800, method="conditional")
    remaining = forecaster.breach_probability(600, 1800, method="remaining")
    assert 0.0 <= conditional <= 1.0
    assert 0.0 <= remaining <= 1.0


def test_risk_level_thresholds():
    forecaster = WeibullForecaster(alert_threshold=0.35, critical_threshold=0.65)
    assert forecaster.risk_level(0.10) == "LOW"
    assert forecaster.risk_level(0.50) == "ELEVATED"
    assert forecaster.risk_level(0.90) == "CRITICAL"


def test_forecast_result_payload(warehouse):
    forecaster = WeibullForecaster().fit(
        warehouse.get_task_durations("risk_credit_scoring", "publish_risk_ratings")
    )
    result = forecaster.forecast("risk_credit_scoring", "publish_risk_ratings", 600, 1800)
    payload = result.to_dict()
    assert payload["dag_id"] == "risk_credit_scoring"
    assert payload["best_distribution"] in {"weibull", "lognorm"}
    assert payload["risk_level"] in {"LOW", "ELEVATED", "CRITICAL"}
    assert result.remaining_budget_seconds == pytest.approx(1200.0)
    assert set(payload["percentiles"]) == {50, 85, 95}


def test_expected_remaining_is_positive_and_decaying(warehouse):
    forecaster = WeibullForecaster().fit(
        warehouse.get_task_durations("sales_ingestion_etl", "load_sales_fact")
    )
    remaining_early = forecaster.expected_remaining(60)
    remaining_late = forecaster.expected_remaining(3000)
    assert remaining_early > remaining_late >= 0.0


def test_expected_remaining_without_fit():
    forecaster = WeibullForecaster(min_samples=100)
    forecaster.fit([1.0] * 5)
    assert forecaster.expected_remaining(10) == 0.0


def test_survival_curve_monotonic_decreasing(warehouse):
    forecaster = WeibullForecaster().fit(
        warehouse.get_task_durations("inventory_sync_dag", "load_inventory_mart")
    )
    curve = forecaster.survival_curve(points=50)
    assert len(curve) == 50
    assert curve["survival"].iloc[0] > curve["survival"].iloc[-1]


def test_get_fit_by_name(warehouse):
    forecaster = WeibullForecaster().fit(
        warehouse.get_task_durations("gl_financial_reconciliation", "extract_gl_journal")
    )
    assert forecaster.get_fit("weibull").name == "weibull"
    assert forecaster.get_fit("nope") is None


# --------------------------------------------------------------------------- #
# Operational metrics
# --------------------------------------------------------------------------- #
def test_compute_mttr_and_mtbf():
    runs = pd.DataFrame({
        "execution_date": pd.date_range("2026-01-01", periods=6, freq="D"),
        "state": ["success", "failed", "success", "success", "failed", "success"],
        "duration_seconds": [100.0, 900.0, 110.0, 120.0, 600.0, 130.0],
        "sla_minutes": [60] * 6,
    })
    assert compute_mttr(runs) == pytest.approx(750.0)
    assert compute_mtbf(runs) == pytest.approx(72.0)


def test_compute_mttr_edge_cases():
    assert compute_mttr(pd.DataFrame()) == 0.0
    assert compute_mttr(pd.DataFrame({"state": ["success"], "duration_seconds": [1.0]})) == 0.0
    assert compute_mtbf(pd.DataFrame({"state": ["failed"], "execution_date": [pd.Timestamp("2026-01-01")]})) == 0.0


def test_first_run_success_rate():
    tasks = pd.DataFrame({"state": ["success", "success", "failed"], "retry_count": [0, 2, 0]})
    assert compute_first_run_success_rate(tasks) == pytest.approx(50.0)
    assert compute_first_run_success_rate(pd.DataFrame()) == 0.0


def test_sla_attainment():
    runs = pd.DataFrame({
        "state": ["success", "success", "failed"],
        "duration_seconds": [600.0, 7200.0, 100.0],
        "sla_minutes": [60, 60, 60],
    })
    assert compute_sla_attainment(runs) == pytest.approx(200 / 3)
    assert compute_sla_attainment(pd.DataFrame()) == 0.0


def test_compute_pipeline_metrics(warehouse):
    runs = warehouse.get_dag_runs("sales_ingestion_etl")
    tasks = warehouse.get_task_instances("sales_ingestion_etl")
    metrics = compute_pipeline_metrics("sales_ingestion_etl", runs, tasks)
    assert metrics.total_runs == len(runs)
    assert 0.0 <= metrics.sla_attainment <= 100.0
    assert metrics.total_rows_loaded > 0
    assert "sla_attainment" in metrics.to_dict()


def test_compute_pipeline_metrics_empty():
    metrics = compute_pipeline_metrics("x", pd.DataFrame())
    assert metrics.total_runs == 0


def test_operational_summary(warehouse):
    summary = operational_summary(warehouse)
    assert len(summary) == 6
    assert {"mttr_seconds", "mtbf_hours", "sla_attainment"} <= set(summary.columns)


def test_operational_summary_empty(empty_warehouse):
    assert operational_summary(empty_warehouse).empty
