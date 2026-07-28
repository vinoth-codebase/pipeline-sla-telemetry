"""Directed Acyclic Graph lineage model.

Builds a single NetworkX ``DiGraph`` that stitches together the whole estate:

``task -> task``               intra-DAG execution order
``task -> staging table``      the task's materialised output
``staging table -> task``      consumers of that table
``task -> BI dashboard``       the client-facing surface it ultimately refreshes

This is what lets an operations engineer answer the only question that matters
at 06:40 in the morning: *if this task is stalled, whose dashboards are dark?*
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable

import networkx as nx

from pipeline_sla.config import Settings, load_settings

logger = logging.getLogger(__name__)


class NodeKind(str, Enum):
    TASK = "task"
    STAGING = "staging_table"
    DASHBOARD = "dashboard"


@dataclass(frozen=True)
class LineageNode:
    node_id: str
    kind: NodeKind
    label: str
    dag_id: str = ""
    metadata: dict[str, Any] | None = None


class LineageGraph:
    """Wrapper around a NetworkX DiGraph describing ETL -> BI lineage."""

    def __init__(self, graph: nx.DiGraph | None = None) -> None:
        self.graph: nx.DiGraph = graph if graph is not None else nx.DiGraph()

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #
    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> "LineageGraph":
        settings = settings or load_settings()
        instance = cls()
        instance.build(settings)
        return instance

    def build(self, settings: Settings) -> "LineageGraph":
        for pipeline in settings.pipelines:
            self._add_pipeline(pipeline)
        self._add_staging_tables(settings.staging_tables)
        self._add_dashboards(settings.dashboards)
        if not nx.is_directed_acyclic_graph(self.graph):
            raise ValueError("Lineage graph contains a cycle; expected a DAG")
        logger.debug(
            "Lineage graph built: %d nodes, %d edges", self.graph.number_of_nodes(), self.graph.number_of_edges()
        )
        return self

    def _add_pipeline(self, pipeline: dict[str, Any]) -> None:
        dag_id = pipeline["dag_id"]
        tasks = sorted(pipeline["tasks"], key=lambda t: t["order"])
        prev: str | None = None
        for task in tasks:
            task_id = task["task_id"]
            self.graph.add_node(
                task_id,
                kind=NodeKind.TASK.value,
                label=task_id,
                dag_id=dag_id,
                task_order=int(task["order"]),
                business_tower=pipeline.get("business_tower", ""),
                criticality_tier=pipeline.get("criticality_tier", "TIER_3"),
            )
            if prev is not None:
                self.graph.add_edge(prev, task_id, kind="task_order")
            prev = task_id

    def _add_staging_tables(self, staging: Iterable[dict[str, Any]]) -> None:
        for table in staging:
            table_id = table["table_id"]
            producer = table.get("produced_by")
            dag_id = table.get("dag_id", "")
            self.graph.add_node(
                table_id, kind=NodeKind.STAGING.value, label=table_id, dag_id=dag_id
            )
            if producer and self.graph.has_node(producer):
                self.graph.add_edge(producer, table_id, kind="produces")
            for consumer in table.get("feeds", []):
                if self.graph.has_node(consumer):
                    self.graph.add_edge(table_id, consumer, kind="consumed_by")

    def _add_dashboards(self, dashboards: Iterable[dict[str, Any]]) -> None:
        for dash in dashboards:
            dash_id = dash["dashboard_id"]
            self.graph.add_node(
                dash_id,
                kind=NodeKind.DASHBOARD.value,
                label=dash["dashboard_name"],
                dag_id=dash.get("parent_dag_id", ""),
                criticality_tier=dash.get("criticality_tier", "TIER_3"),
                audience=dash.get("audience", "ANALYST"),
                audience_size=int(dash.get("audience_size", 0)),
                platform=dash.get("platform", "Power BI"),
                target_sla_time=dash.get("target_sla_time", "07:00"),
            )
            parent = dash.get("parent_task_id")
            if parent and self.graph.has_node(parent):
                self.graph.add_edge(parent, dash_id, kind="feeds_dashboard")

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #
    def nodes_of_kind(self, kind: NodeKind | str) -> list[str]:
        kind_value = kind.value if isinstance(kind, NodeKind) else kind
        return [n for n, d in self.graph.nodes(data=True) if d.get("kind") == kind_value]

    @property
    def tasks(self) -> list[str]:
        return self.nodes_of_kind(NodeKind.TASK)

    @property
    def dashboards(self) -> list[str]:
        return self.nodes_of_kind(NodeKind.DASHBOARD)

    def downstream_of(self, node: str) -> set[str]:
        if node not in self.graph:
            return set()
        return nx.descendants(self.graph, node)

    def upstream_of(self, node: str) -> set[str]:
        if node not in self.graph:
            return set()
        return nx.ancestors(self.graph, node)

    def impacted_dashboards(self, node: str) -> list[str]:
        """Dashboard nodes reachable downstream of ``node`` (inclusive)."""
        if node not in self.graph:
            return []
        reachable = self.downstream_of(node)
        if self.graph.nodes[node].get("kind") == NodeKind.DASHBOARD.value:
            reachable = reachable | {node}
        return sorted(n for n in reachable if self.graph.nodes[n].get("kind") == NodeKind.DASHBOARD.value)

    def depth_to(self, source: str, target: str) -> int:
        """Shortest dependency depth from ``source`` to ``target``.

        Returns 0 when unreachable. A task feeding a dashboard directly has
        depth 1; via one staging table, depth 2.
        """
        if source not in self.graph or target not in self.graph:
            return 0
        if source == target:
            return 0
        try:
            return int(nx.shortest_path_length(self.graph, source, target))
        except nx.NetworkXNoPath:
            return 0

    def shortest_path(self, source: str, target: str) -> list[str]:
        try:
            return list(nx.shortest_path(self.graph, source, target))
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return []

    def critical_path(self, node: str) -> list[str]:
        """Longest downstream path from ``node`` (the deepest dependency chain)."""
        if node not in self.graph:
            return []
        longest: list[str] = []
        for target in self.downstream_of(node):
            path = self.shortest_path(node, target)
            if len(path) > len(longest):
                longest = path
        return longest or [node]

    def node_metadata(self, node: str) -> dict[str, Any]:
        return dict(self.graph.nodes.get(node, {}))

    def edges(self) -> list[tuple[str, str, str]]:
        return [(u, v, d.get("kind", "")) for u, v, d in self.graph.edges(data=True)]

    def to_node_link(self) -> dict[str, list[dict[str, Any]]]:
        """Serialise the graph to the node-link format (Plotly / D3 friendly)."""
        nodes = [
            {"id": n, "kind": d.get("kind"), "label": d.get("label", n), **{
                k: v for k, v in d.items() if k not in {"kind", "label"}
            }}
            for n, d in self.graph.nodes(data=True)
        ]
        links = [{"source": u, "target": v, "kind": d.get("kind", "")} for u, v, d in self.graph.edges(data=True)]
        return {"nodes": nodes, "links": links}

    def summary(self) -> dict[str, int]:
        return {
            "tasks": len(self.tasks),
            "dashboards": len(self.dashboards),
            "staging_tables": len(self.nodes_of_kind(NodeKind.STAGING)),
            "edges": self.graph.number_of_edges(),
            "nodes": self.graph.number_of_nodes(),
        }
