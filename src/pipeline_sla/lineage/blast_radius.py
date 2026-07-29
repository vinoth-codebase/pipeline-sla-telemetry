"""Downstream Blast Radius scoring.

When an upstream task stalls, the operational question is not "which task
failed" but "how much of the business is now flying blind". The Blast Radius
Index answers it with a single weighted number:

.. math::

    \\text{BlastRadius}(n) = \\sum_{d \\in \\text{ImpactedDashboards}(n)}
        \\text{TierWeight}(d) \\times \\text{AudienceScale}(d) \\times
        \\frac{1}{\\text{Depth}(n, d)}

* **TierWeight** -- how mission-critical the dashboard is (TIER_1 = 5.0).
* **AudienceScale** -- how many people / how senior the consumers are
  (BOARD = 10000, ANALYST = 50).
* **1 / Depth** -- a direct feed hits harder than one mediated by three
  intermediate tables; distance discounts impact.

The resulting score is bucketed into ``LOW``, ``ELEVATED`` and
``CRITICAL BUSINESS ESCALATION``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Sequence

from pipeline_sla.config import LineageSettings, Settings, load_settings
from pipeline_sla.lineage.dag_graph import LineageGraph, NodeKind

DEFAULT_TIER_WEIGHTS = {"TIER_1": 5.0, "TIER_2": 3.0, "TIER_3": 1.5, "TIER_4": 0.75}
DEFAULT_AUDIENCE_SCALE = {
    "BOARD": 10000.0, "EXECUTIVE": 5000.0, "MANAGEMENT": 1000.0,
    "OPERATIONAL": 250.0, "ANALYST": 50.0,
}
DEFAULT_ESCALATION = {"low": 0.0, "elevated": 5000.0, "critical": 25000.0}


@dataclass
class ImpactedDashboard:
    """A single dashboard caught in the blast radius."""

    dashboard_id: str
    dashboard_name: str
    criticality_tier: str
    audience: str
    audience_size: int
    depth: int
    tier_weight: float
    audience_scale: float
    contribution: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BlastRadiusResult:
    """Full blast radius assessment for one stalled node."""

    node_id: str
    score: float
    category: str
    n_dashboards: int
    total_audience: int
    max_depth: int
    impacted: list[ImpactedDashboard] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["impacted"] = [d.to_dict() for d in self.impacted]
        return data

    def top_dashboards(self, n: int = 5) -> list[ImpactedDashboard]:
        return sorted(self.impacted, key=lambda d: d.contribution, reverse=True)[:n]


class BlastRadiusCalculator:
    """Compute downstream business impact for stalled lineage nodes."""

    def __init__(
        self,
        graph: LineageGraph,
        settings: Settings | LineageSettings | None = None,
    ) -> None:
        self.graph = graph
        if isinstance(settings, Settings):
            lineage_settings = settings.lineage
        elif isinstance(settings, LineageSettings):
            lineage_settings = settings
        else:
            lineage_settings = load_settings().lineage

        self.tier_weights = {**DEFAULT_TIER_WEIGHTS, **lineage_settings.tier_weights}
        self.audience_scale = {**DEFAULT_AUDIENCE_SCALE, **lineage_settings.audience_scale}
        self.escalation = {**DEFAULT_ESCALATION, **lineage_settings.escalation}

    # ------------------------------------------------------------------ #
    def categorize(self, score: float) -> str:
        if score >= self.escalation["critical"]:
            return "CRITICAL BUSINESS ESCALATION"
        if score >= self.escalation["elevated"]:
            return "ELEVATED"
        return "LOW"

    def compute(self, node: str) -> BlastRadiusResult:
        """Score the downstream impact of a stalled ``node``."""
        impacted_ids = self.graph.impacted_dashboards(node)
        impacted: list[ImpactedDashboard] = []
        for dash_id in impacted_ids:
            meta = self.graph.node_metadata(dash_id)
            tier = meta.get("criticality_tier", "TIER_3")
            audience = meta.get("audience", "ANALYST")
            tier_weight = float(self.tier_weights.get(tier, 1.0))
            scale = float(self.audience_scale.get(audience, float(meta.get("audience_size", 1) or 1)))
            depth = max(1, self.graph.depth_to(node, dash_id))
            contribution = tier_weight * scale * (1.0 / depth)
            impacted.append(
                ImpactedDashboard(
                    dashboard_id=dash_id,
                    dashboard_name=meta.get("label", dash_id),
                    criticality_tier=tier,
                    audience=audience,
                    audience_size=int(meta.get("audience_size", 0) or 0),
                    depth=depth,
                    tier_weight=tier_weight,
                    audience_scale=scale,
                    contribution=round(contribution, 2),
                )
            )

        score = round(sum(d.contribution for d in impacted), 2)
        return BlastRadiusResult(
            node_id=node,
            score=score,
            category=self.categorize(score),
            n_dashboards=len(impacted),
            total_audience=sum(d.audience_size for d in impacted),
            max_depth=max((d.depth for d in impacted), default=0),
            impacted=sorted(impacted, key=lambda d: d.contribution, reverse=True),
        )

    def compute_many(self, nodes: Sequence[str]) -> list[BlastRadiusResult]:
        return [self.compute(n) for n in nodes]

    def rank_nodes(self, *, kind: NodeKind | str = NodeKind.TASK) -> list[BlastRadiusResult]:
        """Rank every node of a kind by blast radius, highest impact first."""
        kind_value = kind.value if isinstance(kind, NodeKind) else kind
        nodes = self.graph.nodes_of_kind(kind_value)
        results = [self.compute(n) for n in nodes]
        return sorted(results, key=lambda r: r.score, reverse=True)

    def escalate_for_stalled_tasks(self, stalled_tasks: Sequence[str]) -> dict[str, Any]:
        """Aggregate escalation view across a set of currently-stalled tasks."""
        results = self.compute_many(stalled_tasks)
        worst = max(results, key=lambda r: r.score, default=None)
        union: set[str] = set()
        for r in results:
            union.update(d.dashboard_id for d in r.impacted)
        return {
            "stalled_tasks": list(stalled_tasks),
            "max_score": worst.score if worst else 0.0,
            "category": worst.category if worst else "LOW",
            "worst_node": worst.node_id if worst else None,
            "unique_dashboards_at_risk": sorted(union),
            "results": [r.to_dict() for r in results],
        }
