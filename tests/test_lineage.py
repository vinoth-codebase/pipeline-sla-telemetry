"""Tests for the NetworkX lineage graph and blast radius scoring."""

from __future__ import annotations

import pytest

from pipeline_sla.config import load_settings
from pipeline_sla.lineage.blast_radius import BlastRadiusCalculator, BlastRadiusResult
from pipeline_sla.lineage.dag_graph import LineageGraph, NodeKind


# --------------------------------------------------------------------------- #
# Graph construction
# --------------------------------------------------------------------------- #
def test_graph_builds_from_settings(lineage_graph, settings):
    summary = lineage_graph.summary()
    assert summary["dashboards"] == len(settings.dashboards)
    assert summary["tasks"] == sum(len(p["tasks"]) for p in settings.pipelines)
    assert summary["edges"] > 0
    assert summary["nodes"] == summary["tasks"] + summary["dashboards"] + summary["staging_tables"]


def test_graph_is_acyclic(lineage_graph):
    import networkx as nx

    assert nx.is_directed_acyclic_graph(lineage_graph.graph)


def test_graph_rejects_cycles():
    settings = load_settings()
    # load_sales_fact -> stg_a -> stage_sales_orders -> ... -> load_sales_fact
    settings.staging_tables = [{
        "table_id": "stg_cycle",
        "produced_by": "load_sales_fact",
        "dag_id": "sales_ingestion_etl",
        "feeds": ["stage_sales_orders"],
    }]
    with pytest.raises(ValueError):
        LineageGraph().build(settings)


def test_node_kinds(lineage_graph):
    assert lineage_graph.tasks
    assert lineage_graph.dashboards
    assert lineage_graph.nodes_of_kind(NodeKind.STAGING)
    assert lineage_graph.nodes_of_kind("dashboard") == lineage_graph.dashboards


def test_node_metadata_and_edges(lineage_graph):
    meta = lineage_graph.node_metadata("load_sales_fact")
    assert meta["kind"] == NodeKind.TASK.value
    assert meta["dag_id"] == "sales_ingestion_etl"
    assert lineage_graph.edges()


# --------------------------------------------------------------------------- #
# Traversal
# --------------------------------------------------------------------------- #
def test_downstream_and_upstream(lineage_graph):
    downstream = lineage_graph.downstream_of("extract_gl_journal")
    assert "dash_cfo_finance_close" in downstream
    upstream = lineage_graph.upstream_of("load_finance_mart")
    assert "extract_gl_journal" in upstream


def test_downstream_of_unknown_node(lineage_graph):
    assert lineage_graph.downstream_of("nope") == set()
    assert lineage_graph.upstream_of("nope") == set()
    assert lineage_graph.impacted_dashboards("nope") == []


def test_impacted_dashboards_for_task(lineage_graph):
    impacted = lineage_graph.impacted_dashboards("reconcile_subledgers")
    assert "dash_cfo_finance_close" in impacted


def test_impacted_dashboards_includes_self_for_dashboard(lineage_graph):
    assert lineage_graph.impacted_dashboards("dash_cfo_finance_close") == ["dash_cfo_finance_close"]


def test_depth_to(lineage_graph):
    direct = lineage_graph.depth_to("load_finance_mart", "dash_cfo_finance_close")
    indirect = lineage_graph.depth_to("extract_gl_journal", "dash_cfo_finance_close")
    assert direct == 1
    assert indirect > direct
    assert lineage_graph.depth_to("load_finance_mart", "load_finance_mart") == 0
    assert lineage_graph.depth_to("nope", "dash_cfo_finance_close") == 0


def test_shortest_path(lineage_graph):
    path = lineage_graph.shortest_path("extract_gl_journal", "dash_cfo_finance_close")
    assert path[0] == "extract_gl_journal"
    assert path[-1] == "dash_cfo_finance_close"
    assert lineage_graph.shortest_path("nope", "dash_cfo_finance_close") == []


def test_critical_path_is_longest(lineage_graph):
    path = lineage_graph.critical_path("extract_pos_stream")
    assert len(path) > 2
    assert lineage_graph.critical_path("nope") == []


def test_to_node_link_serialisation(lineage_graph):
    payload = lineage_graph.to_node_link()
    assert {"nodes", "links"} <= set(payload)
    assert len(payload["nodes"]) == lineage_graph.graph.number_of_nodes()
    assert all({"id", "kind"} <= set(n) for n in payload["nodes"])


def test_from_settings_defaults():
    graph = LineageGraph.from_settings()
    assert graph.summary()["dashboards"] == 10


# --------------------------------------------------------------------------- #
# Blast radius
# --------------------------------------------------------------------------- #
def test_blast_radius_compute(lineage_graph, settings):
    calc = BlastRadiusCalculator(lineage_graph, settings)
    result = calc.compute("load_sales_fact")
    assert isinstance(result, BlastRadiusResult)
    assert result.n_dashboards >= 1
    assert result.score > 0
    assert result.category in {"LOW", "ELEVATED", "CRITICAL BUSINESS ESCALATION"}
    assert result.total_audience > 0


def test_blast_radius_formula(lineage_graph, settings):
    calc = BlastRadiusCalculator(lineage_graph, settings)
    result = calc.compute("load_sales_fact")
    expected = sum(
        calc.tier_weights.get(d.criticality_tier, 1.0)
        * calc.audience_scale.get(d.audience, 1.0)
        * (1.0 / d.depth)
        for d in result.impacted
    )
    assert result.score == pytest.approx(expected, rel=1e-6)


def test_blast_radius_deeper_nodes_score_lower(lineage_graph, settings):
    calc = BlastRadiusCalculator(lineage_graph, settings)
    upstream = calc.compute("extract_pos_stream")   # far from the dashboard
    downstream = calc.compute("load_sales_fact")    # feeds it directly
    assert downstream.score > upstream.score


def test_categorisation_thresholds(lineage_graph):
    calc = BlastRadiusCalculator(lineage_graph)
    assert calc.categorize(0.0) == "LOW"
    assert calc.categorize(6000.0) == "ELEVATED"
    assert calc.categorize(30000.0) == "CRITICAL BUSINESS ESCALATION"


def test_rank_nodes_orders_by_score(lineage_graph):
    ranked = BlastRadiusCalculator(lineage_graph).rank_nodes()
    scores = [r.score for r in ranked]
    assert scores == sorted(scores, reverse=True)


def test_compute_many_and_top_dashboards(lineage_graph):
    calc = BlastRadiusCalculator(lineage_graph)
    results = calc.compute_many(["load_sales_fact", "load_finance_mart"])
    assert len(results) == 2
    top = results[0].top_dashboards(1)
    assert len(top) == 1


def test_escalate_for_stalled_tasks(lineage_graph):
    calc = BlastRadiusCalculator(lineage_graph)
    escalation = calc.escalate_for_stalled_tasks(["reconcile_subledgers", "load_sales_fact"])
    assert escalation["max_score"] > 0
    assert escalation["unique_dashboards_at_risk"]
    assert len(escalation["results"]) == 2


def test_escalate_with_no_tasks(lineage_graph):
    escalation = BlastRadiusCalculator(lineage_graph).escalate_for_stalled_tasks([])
    assert escalation["max_score"] == 0.0
    assert escalation["category"] == "LOW"
    assert escalation["worst_node"] is None


def test_calculator_accepts_lineage_settings_only(lineage_graph):
    calc = BlastRadiusCalculator(lineage_graph, load_settings().lineage)
    assert calc.tier_weights["TIER_1"] == 5.0


def test_blast_radius_result_serialisation(lineage_graph):
    result = BlastRadiusCalculator(lineage_graph).compute("load_sales_fact")
    payload = result.to_dict()
    assert isinstance(payload["impacted"], list)
    assert payload["impacted"][0]["dashboard_id"]
