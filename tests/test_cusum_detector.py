"""Tests for the Two-Sided CUSUM and rolling z-score drift detectors."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline_sla.analytics.cusum_detector import (
    CusumResult,
    VolumeAnomalyDetector,
    cusum,
    rolling_zscore,
)


# --------------------------------------------------------------------------- #
# Rolling z-score
# --------------------------------------------------------------------------- #
def test_rolling_zscore_flags_outlier():
    series = [100.0] * 40 + [5.0]
    z = rolling_zscore(series, window=20, min_periods=5)
    assert abs(z[-1]) > 3


def test_rolling_zscore_nan_prefix():
    z = rolling_zscore([1.0, 2.0, 3.0], window=5, min_periods=3)
    assert np.isnan(z[0])
    assert not np.isnan(z[-1])


# --------------------------------------------------------------------------- #
# CUSUM core
# --------------------------------------------------------------------------- #
def test_cusum_detects_downward_shift():
    baseline = list(np.random.default_rng(0).normal(1000, 40, 60))
    shifted = list(np.random.default_rng(1).normal(600, 40, 25))
    result = cusum(baseline + shifted, slack_k=0.5, threshold_h=5.0, baseline=baseline)
    assert result.n_alarms > 0
    assert result.alarms[-1] >= len(baseline)
    assert "downward" in result.direction


def test_cusum_detects_upward_shift():
    baseline = list(np.random.default_rng(2).normal(1000, 40, 60))
    shifted = list(np.random.default_rng(3).normal(1600, 40, 25))
    result = cusum(baseline + shifted, baseline=baseline)
    assert "upward" in result.direction


def test_cusum_one_sided_ignores_downward():
    values = [100.0] * 30 + [10.0] * 30
    two_sided = cusum(values, baseline=[100.0] * 30, two_sided=True)
    one_sided = cusum(values, baseline=[100.0] * 30, two_sided=False)
    assert two_sided.n_alarms >= one_sided.n_alarms


def test_cusum_flat_series_has_no_alarm():
    result = cusum([500.0] * 40, baseline=[500.0] * 40)
    assert result.n_alarms == 0
    assert result.mean == pytest.approx(500.0)


def test_cusum_empty_input():
    result = cusum([])
    assert isinstance(result, CusumResult)
    assert result.n_alarms == 0
    assert result.to_frame().empty


def test_cusum_zero_variance_fallback():
    result = cusum([100.0, 100.0, 100.0], baseline=[100.0, 100.0])
    assert result.std >= 1.0


def test_cusum_result_to_frame():
    result = cusum([1.0, 2.0, 3.0])
    frame = result.to_frame()
    assert {"cusum_pos", "cusum_neg", "alarm"} <= set(frame.columns)
    assert len(frame) == 3


# --------------------------------------------------------------------------- #
# VolumeAnomalyDetector
# --------------------------------------------------------------------------- #
def test_detect_annotates_columns(warehouse):
    series = warehouse.get_volume_series("sales_ingestion_etl", "load_sales_fact")
    detected = VolumeAnomalyDetector().detect(series)
    expected = {"zscore", "cusum_pos", "cusum_neg", "cusum_alarm", "is_silent_empty",
                "zscore_anomaly", "is_anomaly", "severity", "reason", "rolling_median"}
    assert expected <= set(detected.columns)
    assert len(detected) == len(series)


def test_detect_finds_silent_empty_runs(warehouse):
    series = warehouse.get_volume_series("sales_ingestion_etl", "load_sales_fact")
    detector = VolumeAnomalyDetector()
    silent = detector.detect_silent_empty_runs(series)
    assert not silent.empty
    assert (silent["rows_loaded"] == 0).all()
    assert silent["severity"].eq("CRITICAL").all()
    assert "silent_empty_run" in silent["reason"].iloc[0]


def test_detect_empty_frame(empty_warehouse):
    detector = VolumeAnomalyDetector()
    assert detector.detect(pd.DataFrame()).empty
    assert detector.detect(pd.DataFrame({"x": [1]})).empty
    assert detector.scan_estate(empty_warehouse).empty


def test_scan_estate_flags_across_tasks(warehouse):
    scanned = VolumeAnomalyDetector().scan_estate(warehouse)
    assert not scanned.empty
    assert {"dag_id", "task_id"} <= set(scanned.columns)
    assert scanned["is_silent_empty"].sum() > 0


def test_scan_estate_respects_dag_filter(warehouse):
    scanned = VolumeAnomalyDetector().scan_estate(warehouse, dag_ids=["crm_customer_360"])
    assert set(scanned["dag_id"].unique()) == {"crm_customer_360"}


def test_detector_handles_constant_series():
    frame = pd.DataFrame({
        "execution_date": pd.date_range("2026-01-01", periods=30, freq="D"),
        "rows_loaded": [1000] * 30,
        "state": ["success"] * 30,
    })
    detected = VolumeAnomalyDetector().detect(frame)
    assert not detected["is_anomaly"].any()


def test_severity_and_reason_text():
    detector = VolumeAnomalyDetector(zscore_alert=1.0)
    assert detector._severity(True, False, False, 0.0) == "CRITICAL"
    assert detector._severity(False, True, True, 5.0) == "HIGH"
    assert detector._severity(False, False, True, 0.0) == "MEDIUM"
    assert detector._severity(False, False, False, 0.0) == "LOW"
    assert "silent_empty_run" in detector._reason(True, False, False, "", 0.0)
    assert detector._reason(False, False, False, "", 0.0) == "nominal"


def test_sensitivity_tuning_changes_alarm_count():
    baseline = list(np.random.default_rng(5).normal(1000, 30, 60))
    shifted = list(np.random.default_rng(6).normal(850, 30, 30))
    frame = pd.DataFrame({
        "execution_date": pd.date_range("2026-01-01", periods=len(baseline + shifted), freq="D"),
        "rows_loaded": baseline + shifted,
        "state": ["success"] * len(baseline + shifted),
    })
    sensitive = VolumeAnomalyDetector(slack_k=0.25, threshold_h=2.0)
    relaxed = VolumeAnomalyDetector(slack_k=1.5, threshold_h=20.0)
    assert sensitive.detect(frame)["cusum_alarm"].sum() >= relaxed.detect(frame)["cusum_alarm"].sum()


def test_silent_empty_run_on_day_42(settings):
    from datetime import datetime, timedelta

    from pipeline_sla.client.synthetic_generator import SyntheticPipelineGenerator

    end = datetime(2026, 8, 19)
    days = 180
    day_42 = (end - timedelta(days=days - 1)) + timedelta(days=41)

    result = SyntheticPipelineGenerator(
        settings=settings, seed=42, days=days, end_date=end
    ).generate(days)
    rows = [
        t for t in result.task_instances
        if t.dag_id == "sales_ingestion_etl" and t.execution_date.date() == day_42.date()
    ]
    assert rows, f"expected task instances on {day_42.date()}"
    assert all(t.rows_loaded == 0 and t.state == "success" for t in rows)
