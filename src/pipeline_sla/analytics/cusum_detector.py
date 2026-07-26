"""Volume & drift detection: Two-Sided CUSUM + Rolling Z-Score.

A pipeline can succeed *technically* while failing *materially*. If an upstream
feed silently delivers zero rows, the task exits ``0``, Airflow paints it green,
and the executive dashboard refreshes with an empty -- or stale -- chart. Nobody
notices until a business user does.

Two complementary detectors catch this class of **silent** failure:

**Rolling Z-Score** -- point-wise sensitivity. Fast to react, but a single slow
drift hides under a moving baseline.

**Two-Sided CUSUM** -- cumulative sensitivity. It integrates small persistent
deviations, so it detects a *sustained* downward shift in throughput that no
single observation flags. The classic formulation:

.. math::

    S^{+}_i = \\max\\left(0,\\; S^{+}_{i-1} + \\frac{x_i - \\mu}{\\sigma} - k\\right)

    S^{-}_i = \\max\\left(0,\\; S^{-}_{i-1} - \\frac{x_i - \\mu}{\\sigma} - k\\right)

An alarm fires when either statistic exceeds the decision threshold :math:`h`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np
import pandas as pd


@dataclass
class CusumResult:
    """Output of a two-sided CUSUM run."""

    cusum_pos: np.ndarray
    cusum_neg: np.ndarray
    alarms: list[int]
    mean: float
    std: float
    slack_k: float
    threshold_h: float
    direction: list[str] = field(default_factory=list)

    @property
    def n_alarms(self) -> int:
        return len(self.alarms)

    def to_frame(self, index: Sequence[Any] | None = None) -> pd.DataFrame:
        idx = list(index) if index is not None else range(len(self.cusum_pos))
        return pd.DataFrame(
            {
                "cusum_pos": self.cusum_pos,
                "cusum_neg": self.cusum_neg,
                "alarm": [i in self.alarms for i in range(len(self.cusum_pos))],
            },
            index=idx,
        )


def rolling_zscore(
    values: Sequence[float] | np.ndarray | pd.Series,
    window: int = 14,
    min_periods: int | None = None,
) -> np.ndarray:
    """Rolling z-score of a series against its own trailing baseline."""
    s = pd.Series(np.asarray(values, dtype="float64"))
    min_periods = min_periods or max(2, window // 2)
    mean = s.rolling(window=window, min_periods=min_periods).mean()
    std = s.rolling(window=window, min_periods=min_periods).std(ddof=0)
    std = std.replace(0.0, np.nan)
    z = (s - mean) / std
    return z.to_numpy(dtype="float64")


def cusum(
    values: Sequence[float] | np.ndarray,
    *,
    slack_k: float = 0.5,
    threshold_h: float = 5.0,
    two_sided: bool = True,
    baseline: Sequence[float] | None = None,
) -> CusumResult:
    """Run the two-sided CUSUM change-detection algorithm.

    Parameters
    ----------
    values:
        The observed series (e.g. daily ``rows_loaded``).
    slack_k:
        Reference value -- half the shift size you want to detect, in sigma.
    threshold_h:
        Decision interval; the statistic must exceed this to raise an alarm.
    two_sided:
        Detect both upward and downward shifts (downward is the silent-drop
        case). When False only the upward statistic is maintained.
    baseline:
        Optional reference window used to estimate :math:`\\mu` and :math:`\\sigma`;
        defaults to the full ``values`` series.
    """
    x = np.asarray(values, dtype="float64")
    x = x[np.isfinite(x)]
    if x.size == 0:
        empty = np.array([], dtype="float64")
        return CusumResult(empty, empty, [], 0.0, 0.0, slack_k, threshold_h)

    ref = np.asarray(baseline, dtype="float64") if baseline is not None else x
    ref = ref[np.isfinite(ref)]
    mu = float(np.mean(ref)) if ref.size else 0.0
    sigma = float(np.std(ref, ddof=1)) if ref.size > 1 else 0.0
    if sigma <= 1e-12:
        sigma = float(np.std(x, ddof=1)) or 1.0

    pos = np.zeros(x.size, dtype="float64")
    neg = np.zeros(x.size, dtype="float64")
    alarms: list[int] = []
    directions: list[str] = []

    for i, xi in enumerate(x):
        z = (xi - mu) / sigma
        pos_prev = pos[i - 1] if i else 0.0
        neg_prev = neg[i - 1] if i else 0.0
        pos[i] = max(0.0, pos_prev + z - slack_k)
        if two_sided:
            neg[i] = max(0.0, neg_prev - z - slack_k)

        if pos[i] > threshold_h:
            alarms.append(i)
            directions.append("upward")
        elif two_sided and neg[i] > threshold_h:
            alarms.append(i)
            directions.append("downward")

    return CusumResult(
        cusum_pos=pos,
        cusum_neg=neg,
        alarms=alarms,
        mean=mu,
        std=sigma,
        slack_k=slack_k,
        threshold_h=threshold_h,
        direction=directions,
    )


class VolumeAnomalyDetector:
    """Detects silent volume drops and drift in ingestion throughput."""

    def __init__(
        self,
        *,
        slack_k: float = 0.5,
        threshold_h: float = 5.0,
        zscore_window: int = 14,
        zscore_alert: float = 3.0,
        min_periods: int = 7,
        empty_run_ratio: float = 0.05,
        baseline_window_days: int = 28,
        two_sided: bool = True,
    ) -> None:
        self.slack_k = slack_k
        self.threshold_h = threshold_h
        self.zscore_window = zscore_window
        self.zscore_alert = zscore_alert
        self.min_periods = min_periods
        self.empty_run_ratio = empty_run_ratio
        self.baseline_window_days = baseline_window_days
        self.two_sided = two_sided

    # ------------------------------------------------------------------ #
    def detect(
        self,
        frame: pd.DataFrame,
        *,
        value_column: str = "rows_loaded",
        date_column: str = "execution_date",
        state_column: str = "state",
    ) -> pd.DataFrame:
        """Annotate a volume series with z-score, CUSUM and anomaly flags."""
        if frame is None or frame.empty or value_column not in frame.columns:
            return pd.DataFrame()

        df = frame.copy().reset_index(drop=True)
        if date_column in df.columns:
            df = df.sort_values(date_column).reset_index(drop=True)
        values = pd.to_numeric(df[value_column], errors="coerce").fillna(0.0).to_numpy()

        # Seasonal baseline: trailing window median, robust to spikes.
        baseline = (
            pd.Series(values)
            .rolling(window=self.baseline_window_days, min_periods=max(3, self.baseline_window_days // 4))
            .median()
            .to_numpy()
        )
        global_median = float(np.median(values)) if values.size else 0.0

        z = rolling_zscore(values, window=self.zscore_window, min_periods=self.min_periods)
        result = cusum(
            values,
            slack_k=self.slack_k,
            threshold_h=self.threshold_h,
            two_sided=self.two_sided,
            baseline=values,
        )

        df["rolling_median"] = np.where(np.isnan(baseline), global_median, baseline)
        df["zscore"] = z
        df["cusum_pos"] = result.cusum_pos
        df["cusum_neg"] = result.cusum_neg
        df["cusum_alarm"] = [i in result.alarms for i in range(len(values))]
        df["cusum_direction"] = [""] * len(values)
        for idx, direction in zip(result.alarms, result.direction):
            df.loc[idx, "cusum_direction"] = direction

        # Silent empty run: technically successful, materially empty.
        states = df[state_column].astype(str) if state_column in df.columns else pd.Series(["success"] * len(df))
        empty_mask = (values <= df["rolling_median"].to_numpy() * self.empty_run_ratio) & (
            states.str.lower() == "success"
        )
        zscore_mask = np.abs(np.nan_to_num(z)) > self.zscore_alert

        df["is_silent_empty"] = empty_mask
        df["zscore_anomaly"] = zscore_mask
        df["is_anomaly"] = empty_mask | zscore_mask | df["cusum_alarm"]

        df["severity"] = [
            self._severity(bool(e), bool(zm), bool(ca), float(zv) if not np.isnan(zv) else 0.0)
            for e, zm, ca, zv in zip(
                df["is_silent_empty"], df["zscore_anomaly"], df["cusum_alarm"], df["zscore"]
            )
        ]
        df["reason"] = [
            self._reason(bool(e), bool(zm), bool(ca), direction, zv if not np.isnan(zv) else 0.0)
            for e, zm, ca, direction, zv in zip(
                df["is_silent_empty"],
                df["zscore_anomaly"],
                df["cusum_alarm"],
                df["cusum_direction"],
                df["zscore"],
            )
        ]
        return df

    # ------------------------------------------------------------------ #
    def detect_silent_empty_runs(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Return only the silent-empty-run rows (the highest-severity class)."""
        detected = self.detect(frame)
        if detected.empty:
            return detected
        return detected[detected["is_silent_empty"]].reset_index(drop=True)

    def scan_estate(
        self,
        warehouse: Any,
        *,
        dag_ids: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Run the detector across every task in the estate's warehouse."""
        tasks = warehouse.get_task_instances()
        if tasks.empty:
            return pd.DataFrame()
        if dag_ids:
            tasks = tasks[tasks["dag_id"].isin(list(dag_ids))]

        frames: list[pd.DataFrame] = []
        for (dag_id, task_id), group in tasks.groupby(["dag_id", "task_id"]):
            detected = self.detect(group)
            if detected.empty:
                continue
            detected["dag_id"] = dag_id
            detected["task_id"] = task_id
            frames.append(detected)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ------------------------------------------------------------------ #
    def _severity(self, empty: bool, zscore: bool, alarm: bool, z: float) -> str:
        if empty:
            return "CRITICAL"
        if alarm and zscore:
            return "HIGH"
        if alarm or abs(z) > self.zscore_alert:
            return "MEDIUM"
        return "LOW"

    def _reason(self, empty: bool, zscore: bool, alarm: bool, direction: str, z: float) -> str:
        if empty:
            return "silent_empty_run: task succeeded with ~0 rows loaded"
        parts = []
        if zscore:
            parts.append(f"rolling z-score {z:+.2f} beyond +/-{self.zscore_alert}")
        if alarm:
            parts.append(f"cusum {direction or 'shift'} alarm")
        return "; ".join(parts) if parts else "nominal"
