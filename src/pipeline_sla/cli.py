"""PipelineSLA-Telemetry command line interface.

Subcommands
-----------
``generate``   seed the warehouse with synthetic multi-year telemetry
``analyze``    run the KPI / drift / forecast analytics over the estate
``forecast``   one-off SLA breach probability for an in-flight pipeline
``lineage``    inspect downstream blast radius for a task
``export``     write the Excel ledger and PowerPoint operations review
``dashboard``  launch the Streamlit cockpit (VS Code Simple Browser friendly)
"""

from __future__ import annotations

import logging
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from pipeline_sla import __version__
from pipeline_sla.analytics.cusum_detector import VolumeAnomalyDetector
from pipeline_sla.analytics.metrics import operational_summary
from pipeline_sla.analytics.weibull_forecaster import WeibullForecaster
from pipeline_sla.client.synthetic_generator import SyntheticPipelineGenerator
from pipeline_sla.config import PROJECT_ROOT, load_settings
from pipeline_sla.lineage.blast_radius import BlastRadiusCalculator
from pipeline_sla.lineage.dag_graph import LineageGraph
from pipeline_sla.reporting.excel_exporter import ExcelExporter
from pipeline_sla.reporting.pptx_exporter import PptxExporter
from pipeline_sla.storage.warehouse import TelemetryWarehouse

console = Console()
logger = logging.getLogger(__name__)

BANNER = f"[bold cyan]PipelineSLA-Telemetry[/bold cyan] [dim]v{__version__}[/dim]"


def _resolve_path(path: str | None, default: str) -> Path:
    p = Path(path or default)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p


def _open_warehouse(db: str | None, *, in_memory: bool = False) -> TelemetryWarehouse:
    settings = load_settings()
    if in_memory:
        return TelemetryWarehouse(in_memory=True)
    path = _resolve_path(db, settings.storage.database_path)
    return TelemetryWarehouse(path)


def _status_colour(value: float) -> str:
    if value >= 99:
        return "green"
    if value >= 95:
        return "yellow"
    return "red"


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="pipeline-sla")
def main() -> None:
    """PipelineSLA-Telemetry: upstream pipeline health & downstream business impact."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")


# --------------------------------------------------------------------------- #
# generate
# --------------------------------------------------------------------------- #
@main.command()
@click.option("--days", default=180, show_default=True, help="Number of daily batch cycles to simulate.")
@click.option("--seed", default=42, show_default=True, help="Random seed for reproducibility.")
@click.option("--db", default=None, help="Target DuckDB path (defaults to config).")
@click.option("--output", default=None, help="Alias for --db.")
def generate(days: int, seed: int, db: str | None, output: str | None) -> None:
    """Generate synthetic Airflow telemetry and load it into the warehouse."""
    console.print(Panel(f"{BANNER}\n[bold]Synthetic telemetry generation[/bold]", expand=False))
    settings = load_settings()
    target = output or db
    warehouse = _open_warehouse(target)

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        task = progress.add_task("Simulating enterprise ETL estate...", total=None)
        generator = SyntheticPipelineGenerator(settings=settings, seed=seed, days=days)
        result = generator.generate(days)
        progress.update(task, description="Loading DuckDB warehouse...")
        warehouse.insert_dag_runs(result.dag_runs)
        warehouse.insert_task_instances(result.task_instances)
        warehouse.insert_dashboard_lineage(result.dashboard_lineage)

    counts = warehouse.counts()
    table = Table(title="Warehouse Contents", show_header=True, header_style="bold cyan")
    table.add_column("Table")
    table.add_column("Rows", justify="right")
    for name, count in counts.items():
        table.add_row(name, f"{count:,}")
    console.print(table)
    console.print(f"[green]✔[/green] Generated [bold]{days}[/bold] days across "
                  f"[bold]{len(settings.pipelines)}[/bold] pipelines → [dim]{warehouse.database_path}[/dim]")
    warehouse.close()


# --------------------------------------------------------------------------- #
# analyze
# --------------------------------------------------------------------------- #
@main.command()
@click.option("--all", "analyze_all", is_flag=True, help="Analyse every pipeline in the estate.")
@click.option("--dag", default=None, help="Restrict the analysis to a single DAG id.")
@click.option("--db", default=None, help="DuckDB path (defaults to config).")
def analyze(analyze_all: bool, dag: str | None, db: str | None) -> None:
    """Compute reliability KPIs, volume anomalies and SLA forecasts."""
    console.print(Panel(f"{BANNER}\n[bold]Estate analytics[/bold]", expand=False))
    warehouse = _open_warehouse(db)

    summary = operational_summary(warehouse)
    if summary.empty:
        console.print("[yellow]No telemetry found. Run `pipeline-sla generate` first.[/yellow]")
        warehouse.close()
        return
    if dag:
        summary = summary[summary["dag_id"] == dag]

    table = Table(title="Reliability Scorecard", header_style="bold cyan")
    for col, justify in [("Pipeline", "left"), ("Runs", "right"), ("Failed", "right"),
                         ("MTTR (s)", "right"), ("MTBF (h)", "right"),
                         ("First-Run %", "right"), ("SLA %", "right")]:
        table.add_column(col, justify=justify)
    for _, row in summary.iterrows():
        sla = float(row["sla_attainment"])
        table.add_row(
            str(row["dag_id"]), f"{int(row['total_runs']):,}", f"{int(row['failed_runs']):,}",
            f"{float(row['mttr_seconds']):,.0f}", f"{float(row['mtbf_hours']):,.1f}",
            f"{float(row['first_run_success_rate']):.1f}",
            f"[{_status_colour(sla)}]{sla:.1f}[/{_status_colour(sla)}]",
        )
    console.print(table)

    detector = VolumeAnomalyDetector()
    scanned = detector.scan_estate(warehouse, dag_ids=[dag] if dag else None)
    if not scanned.empty:
        flagged = scanned[scanned["is_anomaly"]]
        console.print(f"\n[bold]Volume anomalies:[/bold] [red]{int(scanned['is_silent_empty'].sum())}[/red] "
                      f"silent empty runs, [yellow]{len(flagged)}[/yellow] total flagged observations.")
    warehouse.close()


# --------------------------------------------------------------------------- #
# forecast
# --------------------------------------------------------------------------- #
@main.command()
@click.option("--dag", required=True, help="DAG id to forecast.")
@click.option("--task", required=True, help="Task id to forecast.")
@click.option("--elapsed", type=float, default=0.0, help="Elapsed runtime in seconds.")
@click.option("--cutoff", type=float, default=60.0, help="SLA cutoff in minutes.")
@click.option("--db", default=None, help="DuckDB path (defaults to config).")
def forecast(dag: str, task: str, elapsed: float, cutoff: float, db: str | None) -> None:
    """Compute the Weibull/Log-Normal SLA breach probability for an in-flight run."""
    warehouse = _open_warehouse(db)
    durations = warehouse.get_task_durations(dag, task)
    forecaster = WeibullForecaster()
    forecaster.fit(durations)
    if forecaster.best_fit is None:
        console.print("[red]Insufficient successful history to fit a distribution.[/red]")
        warehouse.close()
        return

    result = forecaster.forecast(dag, task, elapsed, cutoff * 60)
    colour = {"LOW": "green", "ELEVATED": "yellow", "CRITICAL": "red"}[result.risk_level]
    table = Table(title=f"SLA Breach Forecast — {dag}.{task}", header_style="bold cyan")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Samples fitted", f"{len(durations):,}")
    table.add_row("Best distribution (AIC)", result.best_distribution)
    table.add_row("Elapsed", f"{elapsed/60:.1f} min")
    table.add_row("SLA cutoff", f"{cutoff:.1f} min")
    table.add_row("Expected remaining", f"{result.expected_remaining_seconds/60:.1f} min")
    table.add_row("P50 / P85 / P95", " / ".join(f"{v/60:.1f}m" for v in result.percentiles.values()))
    table.add_row("Breach probability", f"[{colour}]{result.breach_probability:.1%}[/{colour}]")
    table.add_row("Risk level", f"[{colour}]{result.risk_level}[/{colour}]")
    console.print(table)
    warehouse.close()


# --------------------------------------------------------------------------- #
# lineage
# --------------------------------------------------------------------------- #
@main.command()
@click.option("--task", default=None, help="Task to score; omit to rank the whole estate.")
@click.option("--top", default=10, show_default=True, help="How many tasks to rank.")
def lineage(task: str | None, top: int) -> None:
    """Show the downstream blast radius of a stalled task."""
    graph = LineageGraph.from_settings()
    calc = BlastRadiusCalculator(graph)

    if task:
        result = calc.compute(task)
        console.print(Panel(
            f"[bold]{task}[/bold]\nScore: [bold]{result.score:,.0f}[/bold]  |  "
            f"Category: [bold red]{result.category}[/bold red]\n"
            f"Dashboards at risk: {result.n_dashboards}  |  Audience: {result.total_audience:,}",
            title="Blast Radius", expand=False,
        ))
        table = Table(header_style="bold cyan")
        table.add_column("Dashboard")
        table.add_column("Tier")
        table.add_column("Audience")
        table.add_column("Depth", justify="right")
        table.add_column("Contribution", justify="right")
        for d in result.top_dashboards(top):
            table.add_row(d.dashboard_name, d.criticality_tier, d.audience,
                          str(d.depth), f"{d.contribution:,.0f}")
        console.print(table)
        return

    results = calc.rank_nodes()[:top]
    table = Table(title="Top Blast Radius Tasks", header_style="bold cyan")
    table.add_column("Task")
    table.add_column("Score", justify="right")
    table.add_column("Category")
    table.add_column("Dashboards", justify="right")
    for r in results:
        colour = "red" if r.category.startswith("CRITICAL") else ("yellow" if r.category == "ELEVATED" else "green")
        table.add_row(r.node_id, f"{r.score:,.0f}", f"[{colour}]{r.category}[/{colour}]", str(r.n_dashboards))
    console.print(table)


# --------------------------------------------------------------------------- #
# export
# --------------------------------------------------------------------------- #
@main.command()
@click.option("--excel", is_flag=True, help="Generate the Excel operations ledger.")
@click.option("--pptx", is_flag=True, help="Generate the PowerPoint operations review.")
@click.option("--outdir", default=None, help="Output directory (defaults to config reports/).")
@click.option("--db", default=None, help="DuckDB path (defaults to config).")
def export(excel: bool, pptx: bool, outdir: str | None, db: str | None) -> None:
    """Produce the automated MS Office reporting artefacts."""
    if not (excel or pptx):
        excel = pptx = True
    settings = load_settings()
    warehouse = _open_warehouse(db)
    out_dir = _resolve_path(outdir, settings.reporting.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if excel:
        path = ExcelExporter().export(warehouse, out_dir / settings.reporting.excel_filename)
        console.print(f"[green]✔[/green] Excel ledger → [bold]{path}[/bold]")
    if pptx:
        path = PptxExporter().export(
            warehouse, out_dir / settings.reporting.pptx_filename, title=settings.reporting.pptx_title
        )
        console.print(f"[green]✔[/green] PowerPoint review → [bold]{path}[/bold]")
    warehouse.close()


# --------------------------------------------------------------------------- #
# dashboard
# --------------------------------------------------------------------------- #
@main.command()
@click.option("--port", default=8501, show_default=True, help="Streamlit server port.")
def dashboard(port: int) -> None:
    """Launch the Streamlit cockpit (open in VS Code Simple Browser)."""
    import subprocess
    import sys

    app_path = Path(__file__).parent / "dashboard" / "app.py"
    console.print(Panel(
        f"{BANNER}\nLaunching cockpit on [bold]http://localhost:{port}[/bold]\n"
        "Open in VS Code: Cmd/Ctrl+Shift+P → 'Simple Browser: Show'",
        expand=False,
    ))
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(app_path),
         "--server.port", str(port), "--server.headless", "true"],
        check=False,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
