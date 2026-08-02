"""PowerPoint operations review deck.

Builds the three-slide ``monday_operations_review.pptx`` that an operations lead
walks a Monday standup through:

1. **Weekly Pipeline Health Summary** -- executive KPI scorecard.
2. **Top 5 Bottleneck Tasks** -- the upstream tasks dragging downstream analytics.
3. **Ingestion Volume Anomalies** -- silent drops flagged for DE triage.

Built with ``python-pptx`` and a hand-rolled dark-header layout so it renders
identically whether opened in PowerPoint, LibreOffice or Keynote.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Emu, Inches, Pt

from pipeline_sla.analytics.cusum_detector import VolumeAnomalyDetector
from pipeline_sla.analytics.metrics import operational_summary

logger = logging.getLogger(__name__)

# ---- theme -----------------------------------------------------------------
NAVY = RGBColor(0x1F, 0x38, 0x64)
ACCENT = RGBColor(0x2E, 0x75, 0xB6)
LIGHT = RGBColor(0xF2, 0xF2, 0xF2)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
DARK_TEXT = RGBColor(0x26, 0x26, 0x26)
GREEN = RGBColor(0x37, 0x8D, 0x3E)
AMBER = RGBColor(0xBF, 0x8F, 0x00)
RED = RGBColor(0xC0, 0x39, 0x2B)
GREY = RGBColor(0x59, 0x59, 0x59)

_SLIDE_W = Inches(13.333)
_SLIDE_H = Inches(7.5)


@dataclass
class DeckContent:
    overview: pd.DataFrame
    bottlenecks: pd.DataFrame
    anomalies: pd.DataFrame
    title: str = "Monday Morning Operations Review"
    subtitle: str = "Upstream pipeline health & downstream business impact"


class PptxExporter:
    """Generates the executive Monday operations review deck."""

    def __init__(self, detector: VolumeAnomalyDetector | None = None) -> None:
        self.detector = detector or VolumeAnomalyDetector()

    # ------------------------------------------------------------------ #
    def export(
        self,
        warehouse: Any,
        output_path: str | Path,
        *,
        title: str | None = None,
    ) -> Path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)

        content = DeckContent(
            overview=operational_summary(warehouse),
            bottlenecks=self._top_bottlenecks(warehouse),
            anomalies=self._volume_anomalies(warehouse),
            title=title or "Monday Morning Operations Review",
        )

        prs = Presentation()
        prs.slide_width = _SLIDE_W
        prs.slide_height = _SLIDE_H
        blank = prs.slide_layouts[6]

        self._slide_kpi(prs.slides.add_slide(blank), content)
        self._slide_bottlenecks(prs.slides.add_slide(blank), content)
        self._slide_anomalies(prs.slides.add_slide(blank), content)

        prs.save(out)
        logger.info("PowerPoint review written to %s", out)
        return out

    # ------------------------------------------------------------------ #
    # Data builders
    # ------------------------------------------------------------------ #
    def _top_bottlenecks(self, warehouse: Any, top_n: int = 5) -> pd.DataFrame:
        tasks = warehouse.get_task_instances()
        if tasks.empty:
            return pd.DataFrame()
        tasks = tasks[tasks["state"] == "success"]
        records = []
        for (dag_id, task_id), group in tasks.groupby(["dag_id", "task_id"]):
            durations = pd.to_numeric(group["duration_seconds"], errors="coerce").dropna()
            if durations.empty:
                continue
            records.append(
                {
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "p50_seconds": round(float(durations.quantile(0.50)), 1),
                    "p95_seconds": round(float(durations.quantile(0.95)), 1),
                    "max_seconds": round(float(durations.max()), 1),
                    "runs": int(len(durations)),
                }
            )
        df = pd.DataFrame(records)
        if df.empty:
            return df
        return df.sort_values("p95_seconds", ascending=False).head(top_n).reset_index(drop=True)

    def _volume_anomalies(self, warehouse: Any, top_n: int = 6) -> pd.DataFrame:
        scanned = self.detector.scan_estate(warehouse)
        if scanned.empty:
            return scanned
        flagged = scanned[scanned["is_anomaly"]].copy()
        if flagged.empty:
            return flagged
        flagged = flagged.sort_values(
            by=["is_silent_empty", "severity"], ascending=[False, True]
        )
        cols = ["dag_id", "task_id", "execution_date", "rows_loaded", "rolling_median",
                "zscore", "severity", "reason"]
        return flagged[[c for c in cols if c in flagged.columns]].head(top_n).reset_index(drop=True)

    # ------------------------------------------------------------------ #
    # Slide builders
    # ------------------------------------------------------------------ #
    def _header(self, slide: Any, title: str, subtitle: str) -> None:
        bar = slide.shapes.add_shape(1, 0, 0, _SLIDE_W, Inches(1.15))
        bar.fill.solid()
        bar.fill.fore_color.rgb = NAVY
        bar.line.fill.background()

        tb = slide.shapes.add_textbox(Inches(0.5), Inches(0.12), Inches(12.3), Inches(0.6))
        p = tb.text_frame.paragraphs[0]
        p.text = title
        p.font.size = Pt(26)
        p.font.bold = True
        p.font.color.rgb = WHITE

        sb = slide.shapes.add_textbox(Inches(0.5), Inches(0.68), Inches(12.3), Inches(0.4))
        sp = sb.text_frame.paragraphs[0]
        sp.text = subtitle
        sp.font.size = Pt(13)
        sp.font.color.rgb = RGBColor(0xBD, 0xD7, 0xEE)

    def _footer(self, slide: Any, page: int, total: int = 3) -> None:
        tb = slide.shapes.add_textbox(Inches(11.8), Inches(7.0), Inches(1.3), Inches(0.35))
        p = tb.text_frame.paragraphs[0]
        p.text = f"PipelineSLA-Telemetry  |  {page}/{total}"
        p.font.size = Pt(9)
        p.font.color.rgb = GREY
        p.alignment = PP_ALIGN.RIGHT

    def _slide_kpi(self, slide: Any, content: DeckContent) -> None:
        self._header(slide, content.title, content.subtitle)
        overview = content.overview

        if overview.empty:
            self._body_text(slide, "No telemetry available for the reporting window.", 2.0)
            self._footer(slide, 1)
            return

        # --- KPI tiles ---------------------------------------------------
        avg_sla = float(overview["sla_attainment"].mean())
        total_runs = int(overview["total_runs"].sum())
        total_failed = int(overview["failed_runs"].sum())
        avg_first_run = float(overview["first_run_success_rate"].mean())

        tiles = [
            ("Estate SLA Attainment", f"{avg_sla:.1f}%", self._kpi_colour(avg_sla)),
            ("Total Runs (window)", f"{total_runs:,}", ACCENT),
            ("Failed Runs", f"{total_failed:,}", RED if total_failed else GREEN),
            ("First-Run Success", f"{avg_first_run:.1f}%", self._kpi_colour(avg_first_run)),
        ]
        x = Inches(0.5)
        for label, value, colour in tiles:
            self._kpi_tile(slide, x, Inches(1.5), label, value, colour)
            x += Inches(3.2)

        # --- per-pipeline scorecard -------------------------------------
        self._body_text(slide, "Per-Pipeline Reliability Scorecard", 3.1, size=15, bold=True)
        headers = ["Pipeline", "Runs", "Failed", "MTTR (s)", "First-Run %", "SLA %"]
        rows = []
        for _, r in overview.iterrows():
            rows.append([
                str(r["dag_id"]), f"{int(r['total_runs']):,}", f"{int(r['failed_runs']):,}",
                f"{float(r['mttr_seconds']):,.0f}", f"{float(r['first_run_success_rate']):.1f}",
                f"{float(r['sla_attainment']):.1f}",
            ])
        self._table(slide, headers, rows, Inches(0.5), Inches(3.6), Inches(12.3), highlight_col=5)
        self._footer(slide, 1)

    def _slide_bottlenecks(self, slide: Any, content: DeckContent) -> None:
        self._header(
            slide,
            "Top 5 Bottleneck Tasks",
            "Upstream tasks whose P95 runtime is delaying downstream analytics refreshes",
        )
        df = content.bottlenecks
        if df.empty:
            self._body_text(slide, "No bottleneck data available.", 2.0)
            self._footer(slide, 2)
            return

        headers = ["#", "Pipeline", "Task", "P50 (s)", "P95 (s)", "Max (s)", "Runs"]
        rows = []
        for i, (_, r) in enumerate(df.iterrows(), start=1):
            rows.append([
                str(i), str(r["dag_id"]), str(r["task_id"]),
                f"{r['p50_seconds']:,.0f}", f"{r['p95_seconds']:,.0f}",
                f"{r['max_seconds']:,.0f}", f"{int(r['runs']):,}",
            ])
        self._table(slide, headers, rows, Inches(0.5), Inches(1.6), Inches(12.3), highlight_col=4)

        worst = df.iloc[0]
        note = (
            f"Highest-impact bottleneck: {worst['task_id']} in {worst['dag_id']} "
            f"(P95 {worst['p95_seconds']:,.0f}s, max {worst['max_seconds']:,.0f}s). "
            "Recommend DB lock contention review and parallelisation of the transform stage."
        )
        self._callout(slide, note, Inches(0.5), Inches(6.0), Inches(12.3))
        self._footer(slide, 2)

    def _slide_anomalies(self, slide: Any, content: DeckContent) -> None:
        self._header(
            slide,
            "Ingestion Volume Anomalies",
            "Silent zero-row runs and CUSUM drift alarms flagged for Data Engineering triage",
        )
        df = content.anomalies
        if df.empty:
            self._body_text(slide, "No volume anomalies detected. Throughput nominal.", 2.0)
            self._footer(slide, 3)
            return

        headers = ["Pipeline", "Task", "Date", "Rows", "Expected", "Z-Score", "Severity", "Diagnosis"]
        rows = []
        for _, r in df.iterrows():
            date = pd.to_datetime(r["execution_date"]).strftime("%Y-%m-%d")
            z = float(r["zscore"]) if pd.notna(r["zscore"]) else 0.0
            rows.append([
                str(r["dag_id"]), str(r["task_id"]), date,
                f"{int(r['rows_loaded']):,}", f"{int(r['rolling_median']):,}",
                f"{z:+.2f}", str(r["severity"]), str(r["reason"])[:58],
            ])
        self._table(slide, headers, rows, Inches(0.5), Inches(1.6), Inches(12.3), highlight_col=6)

        silent = int((df.get("severity", pd.Series(dtype=str)) == "CRITICAL").sum())
        note = (
            f"{silent} silent empty run(s) require immediate triage: the task reported SUCCESS "
            "while loading ~0 rows, so the dashboard refreshed with stale or empty data. "
            "Action: validate upstream extract filters and re-run the affected partitions."
        )
        self._callout(slide, note, Inches(0.5), Inches(6.0), Inches(12.3))
        self._footer(slide, 3)

    # ------------------------------------------------------------------ #
    # Primitives
    # ------------------------------------------------------------------ #
    @staticmethod
    def _kpi_colour(value: float) -> RGBColor:
        if value >= 99:
            return GREEN
        if value >= 95:
            return AMBER
        return RED

    def _kpi_tile(self, slide: Any, x: Emu, y: Emu, label: str, value: str, colour: RGBColor) -> None:
        card = slide.shapes.add_shape(1, x, y, Inches(3.0), Inches(1.25))
        card.fill.solid()
        card.fill.fore_color.rgb = LIGHT
        card.line.color.rgb = colour
        card.line.width = Pt(2)

        tf = card.text_frame
        tf.word_wrap = True
        tf.margin_left = Inches(0.15)
        tf.margin_top = Inches(0.08)
        p = tf.paragraphs[0]
        p.text = value
        p.font.size = Pt(30)
        p.font.bold = True
        p.font.color.rgb = colour
        p.alignment = PP_ALIGN.LEFT

        p2 = tf.add_paragraph()
        p2.text = label
        p2.font.size = Pt(12)
        p2.font.color.rgb = GREY

    def _table(
        self,
        slide: Any,
        headers: Sequence[str],
        rows: Sequence[Sequence[str]],
        x: Emu,
        y: Emu,
        width: Emu,
        *,
        highlight_col: int | None = None,
    ) -> None:
        n_rows = len(rows) + 1
        n_cols = len(headers)
        height = Inches(0.4 * n_rows)
        shape = slide.shapes.add_table(n_rows, n_cols, x, y, width, height)
        table = shape.table

        for c, header in enumerate(headers):
            cell = table.cell(0, c)
            cell.text = str(header)
            self._style_cell(cell, fill=NAVY, colour=WHITE, bold=True, size=11)
        for r, row in enumerate(rows, start=1):
            for c, value in enumerate(row):
                cell = table.cell(r, c)
                cell.text = str(value)
                fill = WHITE if r % 2 else RGBColor(0xF7, 0xF9, 0xFC)
                colour = DARK_TEXT
                bold = False
                if highlight_col is not None and c == highlight_col:
                    colour = self._severity_colour(str(value))
                    bold = True
                self._style_cell(cell, fill=fill, colour=colour, bold=bold, size=10)

    @staticmethod
    def _severity_colour(value: str) -> RGBColor:
        v = value.strip().upper()
        if v in {"CRITICAL", "HIGH"}:
            return RED
        if v == "MEDIUM":
            return AMBER
        try:
            numeric = float(value)
            if numeric >= 99:
                return GREEN
            if numeric >= 95:
                return AMBER
            return RED
        except (ValueError, TypeError):
            return DARK_TEXT

    @staticmethod
    def _style_cell(cell: Any, *, fill: RGBColor, colour: RGBColor, bold: bool, size: int) -> None:
        cell.fill.solid()
        cell.fill.fore_color.rgb = fill
        cell.margin_left = Inches(0.06)
        cell.margin_right = Inches(0.06)
        for paragraph in cell.text_frame.paragraphs:
            paragraph.font.size = Pt(size)
            paragraph.font.bold = bold
            paragraph.font.color.rgb = colour

    def _body_text(self, slide: Any, text: str, y_in: float, *, size: int = 16, bold: bool = False) -> None:
        tb = slide.shapes.add_textbox(Inches(0.5), Inches(y_in), Inches(12.3), Inches(0.5))
        p = tb.text_frame.paragraphs[0]
        p.text = text
        p.font.size = Pt(size)
        p.font.bold = bold
        p.font.color.rgb = DARK_TEXT

    def _callout(self, slide: Any, text: str, x: Emu, y: Emu, width: Emu) -> None:
        box = slide.shapes.add_shape(1, x, y, width, Inches(0.85))
        box.fill.solid()
        box.fill.fore_color.rgb = RGBColor(0xFD, 0xF2, 0xE3)
        box.line.color.rgb = AMBER
        tf = box.text_frame
        tf.word_wrap = True
        tf.margin_left = Inches(0.2)
        tf.margin_top = Inches(0.1)
        p = tf.paragraphs[0]
        p.text = text
        p.font.size = Pt(12)
        p.font.color.rgb = DARK_TEXT
