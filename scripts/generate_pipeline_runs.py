#!/usr/bin/env python
"""Seed the telemetry warehouse with 180 days of synthetic Airflow history.

Usage
-----
    python scripts/generate_pipeline_runs.py --days 180
    python scripts/generate_pipeline_runs.py --days 365 --seed 7 --db data/telemetry.duckdb

This is the first step of the quickstart: it fabricates a realistic multi-year
estate (weekend drops, month-end surges, DB-lock stalls, silent zero-row runs
and network-timeout retries) and loads it straight into DuckDB.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Make `src/` importable when the package is not pip-installed.
_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

from pipeline_sla.client.synthetic_generator import SyntheticPipelineGenerator  # noqa: E402
from pipeline_sla.config import load_settings  # noqa: E402
from pipeline_sla.storage.warehouse import TelemetryWarehouse  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic pipeline telemetry.")
    parser.add_argument("--days", type=int, default=180, help="Number of daily batch cycles.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument("--db", type=str, default=None, help="DuckDB path (defaults to config).")
    parser.add_argument("--parquet", action="store_true", help="Also export Parquet snapshots.")
    parser.add_argument("--reset", action="store_true", help="Delete an existing DB before seeding.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_settings()

    db_path = Path(args.db) if args.db else _ROOT / settings.storage.database_path
    if not db_path.is_absolute():
        db_path = _ROOT / db_path

    if args.reset and db_path.exists():
        db_path.unlink()
        print(f"Removed existing warehouse: {db_path}")

    print(f"PipelineSLA-Telemetry :: synthetic generator")
    print(f"  horizon   : {args.days} days")
    print(f"  pipelines : {len(settings.pipelines)}")
    print(f"  seed      : {args.seed}")
    print(f"  warehouse : {db_path}")

    warehouse = TelemetryWarehouse(db_path)
    generator = SyntheticPipelineGenerator(settings=settings, seed=args.seed, days=args.days)
    result = generator.generate(args.days)

    warehouse.insert_dag_runs(result.dag_runs)
    warehouse.insert_task_instances(result.task_instances)
    warehouse.insert_dashboard_lineage(result.dashboard_lineage)

    counts = warehouse.counts()
    print("\nLoaded into DuckDB:")
    for table, count in counts.items():
        print(f"  {table:<20} {count:>8,}")

    silent = sum(1 for t in result.task_instances if t.rows_loaded == 0 and t.state == "success")
    print(f"\n  silent empty task runs : {silent:,}")
    print(f"  failed task runs       : {sum(1 for t in result.task_instances if t.state == 'failed'):,}")

    if args.parquet:
        parquet_dir = _ROOT / settings.storage.parquet_dir
        for table in ("dag_runs", "task_instances", "dashboard_lineage"):
            path = warehouse.export_table_to_parquet(table, parquet_dir)
            print(f"  parquet -> {path}")

    warehouse.close()
    print("\nDone. Next: `pipeline-sla analyze --all`")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
