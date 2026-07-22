"""Probabilistic SLA breach forecaster.

Fits parametric duration distributions -- **Weibull** and **Log-Normal** -- to
historical task runtimes and, while an overnight batch is still executing,
returns the exact probability that it will breach the morning business SLA.

Why these two families? Operational latencies are strictly positive and
right-skewed: a minority of runs drag on for hours because of lock contention,
retry storms or oversized month-end volumes. A Normal fit understates that tail
catastrophically. Weibull's shape parameter ``k`` captures the hazard regime
(``k < 1`` infant-mortality, ``k = 1`` memoryless, ``k > 1`` wear-out) while the
Log-Normal is the natural model for multiplicative, many-small-cause delays.

Core probability
----------------
Let ``T`` be the total runtime of the in-flight run and ``c`` the SLA cutoff.
Given the run has already been executing for ``e`` seconds, the conditional
probability of breaching the SLA is

.. math::

    P(\\text{breach} \\mid T > e) = \\frac{S(c)}{S(e)}

where :math:`S(t) = P(T > t)` is the survival function.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd
from scipy import stats


@dataclass
class DistributionFit:
    """A fitted parametric distribution with goodness-of-fit diagnostics."""

    name: str
    params: tuple[float, ...]
    aic: float
    ks_statistic: float
    ks_pvalue: float
    n_samples: int
    _dist: Any = field(default=None, repr=False, compare=False)

    # -- distribution functions -------------------------------------------
    def pdf(self, t: float | np.ndarray) -> float | np.ndarray:
        return self._dist.pdf(t)

    def cdf(self, t: float | np.ndarray) -> float | np.ndarray:
        return self._dist.cdf(t)

    def sf(self, t: float | np.ndarray) -> float | np.ndarray:
        """Survival function ``S(t) = P(T > t)``."""
        return self._dist.sf(t)

    def ppf(self, q: float) -> float:
        """Percent point function (inverse CDF)."""
        return float(self._dist.ppf(q))

    def mean(self) -> float:
        return float(self._dist.mean())

    def median(self) -> float:
        return float(self._dist.median())

    def std(self) -> float:
        return float(self._dist.std())

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "params": [round(float(p), 4) for p in self.params],
            "aic": round(self.aic, 2),
            "ks_statistic": round(self.ks_statistic, 4),
            "ks_pvalue": round(self.ks_pvalue, 4),
            "n_samples": self.n_samples,
            "mean_seconds": round(self.mean(), 2),
            "median_seconds": round(self.median(), 2),
        }


@dataclass
class ForecastResult:
    """Output of a single in-flight SLA forecast."""

    dag_id: str
    task_id: str
    elapsed_seconds: float
    cutoff_seconds: float
    breach_probability: float
    risk_level: str
    best_distribution: str
    expected_remaining_seconds: float
    percentiles: dict[int, float] = field(default_factory=dict)
    distribution_fits: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def remaining_budget_seconds(self) -> float:
        return max(0.0, self.cutoff_seconds - self.elapsed_seconds)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "cutoff_seconds": round(self.cutoff_seconds, 2),
            "remaining_budget_seconds": round(self.remaining_budget_seconds, 2),
            "expected_remaining_seconds": round(self.expected_remaining_seconds, 2),
            "breach_probability": round(self.breach_probability, 4),
            "risk_level": self.risk_level,
            "best_distribution": self.best_distribution,
            "percentiles": {k: round(v, 2) for k, v in self.percentiles.items()},
        }


class WeibullForecaster:
    """Fit duration distributions and forecast SLA breach probability.

    Parameters
    ----------
    min_samples:
        Minimum history required to attempt a fit.
    bands:
        Percentile bands to report (default P50/P85/P95).
    alert_threshold / critical_threshold:
        Probability cut-offs mapping to ``ELEVATED`` / ``CRITICAL`` risk.
    """

    def __init__(
        self,
        *,
        min_samples: int = 20,
        bands: Sequence[int] = (50, 85, 95),
        alert_threshold: float = 0.35,
        critical_threshold: float = 0.65,
    ) -> None:
        self.min_samples = min_samples
        self.bands = tuple(bands)
        self.alert_threshold = alert_threshold
        self.critical_threshold = critical_threshold
        self.fits: dict[str, DistributionFit] = {}
        self._durations: np.ndarray | None = None

    # ------------------------------------------------------------------ #
    # Fitting
    # ------------------------------------------------------------------ #
    def fit(self, durations: Iterable[float] | np.ndarray) -> "WeibullForecaster":
        """Fit Weibull and Log-Normal distributions to a duration sample.

        Returns ``self`` so calls can be chained, e.g.
        ``WeibullForecaster().fit(history).best_fit``. The fitted models are
        available on :attr:`fits` and :attr:`best_fit`.
        """
        data = np.asarray([float(d) for d in durations if d is not None], dtype=float)
        data = data[np.isfinite(data) & (data > 0)]
        self._durations = data
        self.fits = {}
        if data.size < self.min_samples:
            return self

        self.fits["weibull"] = self._fit_weibull(data)
        self.fits["lognorm"] = self._fit_lognorm(data)
        return self

    @staticmethod
    def _aic(log_likelihood: float, n_params: int) -> float:
        return 2 * n_params - 2 * log_likelihood

    def _fit_weibull(self, data: np.ndarray) -> DistributionFit:
        shape, loc, scale = stats.weibull_min.fit(data, floc=0)
        frozen = stats.weibull_min(c=shape, loc=0, scale=scale)
        loglik = float(np.sum(frozen.logpdf(data)))
        ks = stats.kstest(data, frozen.cdf)
        return DistributionFit(
            name="weibull",
            params=(float(shape), float(loc), float(scale)),
            aic=self._aic(loglik, 2),
            ks_statistic=float(ks.statistic),
            ks_pvalue=float(ks.pvalue),
            n_samples=int(data.size),
            _dist=frozen,
        )

    def _fit_lognorm(self, data: np.ndarray) -> DistributionFit:
        shape, loc, scale = stats.lognorm.fit(data, floc=0)
        frozen = stats.lognorm(s=shape, loc=0, scale=scale)
        loglik = float(np.sum(frozen.logpdf(data)))
        ks = stats.kstest(data, frozen.cdf)
        return DistributionFit(
            name="lognorm",
            params=(float(shape), float(loc), float(scale)),
            aic=self._aic(loglik, 2),
            ks_statistic=float(ks.statistic),
            ks_pvalue=float(ks.pvalue),
            n_samples=int(data.size),
            _dist=frozen,
        )

    @property
    def best_fit(self) -> DistributionFit | None:
        """Distribution with the lowest AIC (best trade-off of fit vs complexity)."""
        if not self.fits:
            return None
        return min(self.fits.values(), key=lambda f: f.aic)

    def get_fit(self, name: str | None = None) -> DistributionFit | None:
        if name is None:
            return self.best_fit
        return self.fits.get(name)

    # ------------------------------------------------------------------ #
    # Percentiles & boundaries
    # ------------------------------------------------------------------ #
    def percentiles(
        self,
        bands: Sequence[int] | None = None,
        distribution: str | None = None,
    ) -> dict[int, float]:
        """Return completion-time percentiles (seconds) from the fitted model."""
        fit = self.get_fit(distribution)
        if fit is None:
            return {}
        bands = bands or self.bands
        return {int(b): float(fit.ppf(b / 100.0)) for b in bands}

    def rolling_percentiles(
        self,
        series: pd.Series | Sequence[float],
        window: int = 30,
        bands: Sequence[int] | None = None,
    ) -> pd.DataFrame:
        """Rolling P50/P85/P95 completion boundaries across a duration series."""
        bands = bands or self.bands
        s = pd.Series(list(series), dtype="float64")
        out = pd.DataFrame(index=s.index)
        for b in bands:
            out[f"p{b}"] = s.rolling(window=window, min_periods=max(3, window // 3)).quantile(b / 100.0)
        return out

    # ------------------------------------------------------------------ #
    # Forecasting
    # ------------------------------------------------------------------ #
    def expected_remaining(self, elapsed_seconds: float, distribution: str | None = None) -> float:
        """Expected additional runtime given the run has already taken ``e``.

        Uses :math:`E[T \\mid T > e] - e` computed by numerical integration of the
        conditional survival function.
        """
        fit = self.get_fit(distribution)
        if fit is None:
            return 0.0
        e = max(0.0, float(elapsed_seconds))
        s_e = float(fit.sf(e))
        if s_e <= 1e-12:
            return 0.0
        upper = float(fit.ppf(0.999999)) + 1.0
        grid = np.linspace(e, max(upper, e + 1.0), 2000)
        conditional_survival = np.clip(fit.sf(grid) / s_e, 0.0, 1.0)
        tail = np.trapezoid(conditional_survival, grid)
        return float(max(0.0, tail))

    def breach_probability(
        self,
        elapsed_seconds: float,
        cutoff_seconds: float,
        *,
        distribution: str | None = None,
        method: str = "conditional",
    ) -> float:
        """Probability that an in-flight run breaches the SLA cutoff.

        ``method='conditional'`` (default) uses :math:`S(c)/S(e)` -- the correct
        posterior given the run has survived to ``e``. ``method='remaining'``
        models the *remaining* time directly as :math:`S(c - e)`.
        """
        fit = self.get_fit(distribution)
        if fit is None:
            return 0.0
        e = max(0.0, float(elapsed_seconds))
        c = float(cutoff_seconds)
        if e >= c:
            return 1.0
        if method == "remaining":
            return float(np.clip(fit.sf(c - e), 0.0, 1.0))
        s_e = float(fit.sf(e))
        s_c = float(fit.sf(c))
        if s_e <= 1e-12:
            return 1.0
        return float(np.clip(s_c / s_e, 0.0, 1.0))

    def risk_level(self, probability: float) -> str:
        if probability >= self.critical_threshold:
            return "CRITICAL"
        if probability >= self.alert_threshold:
            return "ELEVATED"
        return "LOW"

    def forecast(
        self,
        dag_id: str,
        task_id: str,
        elapsed_seconds: float,
        cutoff_seconds: float,
        *,
        distribution: str | None = None,
    ) -> ForecastResult:
        """Produce a full forecast object for an in-flight pipeline."""
        best = self.get_fit(distribution)
        probability = self.breach_probability(
            elapsed_seconds, cutoff_seconds, distribution=distribution
        )
        return ForecastResult(
            dag_id=dag_id,
            task_id=task_id,
            elapsed_seconds=float(elapsed_seconds),
            cutoff_seconds=float(cutoff_seconds),
            breach_probability=probability,
            risk_level=self.risk_level(probability),
            best_distribution=best.name if best else "none",
            expected_remaining_seconds=self.expected_remaining(elapsed_seconds, distribution),
            percentiles=self.percentiles(distribution=distribution),
            distribution_fits={name: fit.to_dict() for name, fit in self.fits.items()},
        )

    def survival_curve(
        self,
        distribution: str | None = None,
        points: int = 200,
    ) -> pd.DataFrame:
        """Sampled survival + density curve for plotting the Weibull fit."""
        fit = self.get_fit(distribution)
        if fit is None:
            return pd.DataFrame(columns=["t", "survival", "density"])
        upper = float(fit.ppf(0.999))
        grid = np.linspace(0.0, upper, points)
        return pd.DataFrame(
            {"t": grid, "survival": fit.sf(grid), "density": fit.pdf(grid)}
        )
