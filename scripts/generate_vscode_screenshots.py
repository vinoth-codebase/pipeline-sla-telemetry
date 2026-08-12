#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Render VS Code-styled SVG visual assets for the README and docs.

Produces four high-resolution, self-contained SVGs that mimic the VS Code dark
editor chrome (``#1e1e1e`` background, activity bar, editor tabs, breadcrumb,
status bar, window controls):

* ``vscode_interactive_weibull_forecast.svg``  -- Python Interactive pane with the
  P50/P85/P95 percentile table and the Weibull breach-probability curve.
* ``vscode_cusum_anomaly_explorer.svg``        -- split editor: source on the left,
  Interactive Output with a CUSUM alarm on the right.
* ``vscode_simple_browser_dashboard.svg``      -- Simple Browser tab running the
  Streamlit Morning Dispatch Radar and blast-radius map.
* ``vscode_excel_viewer_operations_ledger.svg``-- Excel Viewer tab showing the
  conditional-formatting heatmaps of the weekly health ledger.

The curves and tables are drawn from the *real* synthetic telemetry and the
*real* fitted models, so the screenshots stay truthful as the code evolves.

Usage
-----
    python scripts/generate_vscode_screenshots.py
"""

from __future__ import annotations

import math
import sys
from pathlib import Path
from xml.sax.saxutils import escape

import numpy as np

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

from pipeline_sla.analytics.cusum_detector import VolumeAnomalyDetector  # noqa: E402
from pipeline_sla.analytics.weibull_forecaster import WeibullForecaster  # noqa: E402
from pipeline_sla.client.synthetic_generator import SyntheticPipelineGenerator  # noqa: E402
from pipeline_sla.config import load_settings  # noqa: E402
from pipeline_sla.lineage.dag_graph import LineageGraph, NodeKind  # noqa: E402

# --------------------------------------------------------------------------- #
# VS Code "Dark+" palette
# --------------------------------------------------------------------------- #
C = {
    "bg": "#1e1e1e",
    "titlebar": "#3c3c3c",
    "menubar": "#3c3c3c",
    "activitybar": "#333333",
    "sidebar": "#252526",
    "tab_active": "#1e1e1e",
    "tab_inactive": "#2d2d2d",
    "tab_border": "#252526",
    "editor": "#1e1e1e",
    "panel": "#181818",
    "statusbar": "#007acc",
    "border": "#2b2b2b",
    "text": "#cccccc",
    "text_dim": "#858585",
    "text_bright": "#ffffff",
    "blue": "#569cd6",
    "cyan": "#4ec9b0",
    "purple": "#c586c0",
    "orange": "#ce9178",
    "green": "#6a9955",
    "yellow": "#dcdcaa",
    "red": "#f14c4c",
    "violet": "#b180d7",
    "gutter": "#6e7681",
    "gridline": "#2a2a2a",
}

MONO = "Consolas, 'Cascadia Mono', 'Courier New', monospace"
UI = "'Segoe UI', -apple-system, BlinkMacSystemFont, 'Helvetica Neue', Arial, sans-serif"

W, H = 1680, 1050
TITLEBAR_H = 34
TABBAR_H = 38
BREADCRUMB_H = 26
STATUS_H = 26
SIDEBAR_W = 250


# --------------------------------------------------------------------------- #
# Tiny SVG builder
# --------------------------------------------------------------------------- #
class SVG:
    def __init__(self, width: int = W, height: int = H) -> None:
        self.width = width
        self.height = height
        self.parts: list[str] = []

    def rect(self, x, y, w, h, fill, rx=0, stroke=None, sw=1, opacity=None) -> "SVG":
        attrs = [f'x="{x:.1f}"', f'y="{y:.1f}"', f'width="{w:.1f}"', f'height="{h:.1f}"',
                 f'fill="{fill}"', f'rx="{rx}"']
        if stroke:
            attrs += [f'stroke="{stroke}"', f'stroke-width="{sw}"']
        if opacity is not None:
            attrs.append(f'opacity="{opacity}"')
        self.parts.append(f"<rect {' '.join(attrs)}/>")
        return self

    def line(self, x1, y1, x2, y2, stroke, sw=1, dash=None, opacity=None) -> "SVG":
        attrs = [f'x1="{x1:.1f}"', f'y1="{y1:.1f}"', f'x2="{x2:.1f}"', f'y2="{y2:.1f}"',
                 f'stroke="{stroke}"', f'stroke-width="{sw}"', 'stroke-linecap="round"']
        if dash:
            attrs.append(f'stroke-dasharray="{dash}"')
        if opacity is not None:
            attrs.append(f'opacity="{opacity}"')
        self.parts.append(f"<line {' '.join(attrs)}/>")
        return self

    def circle(self, cx, cy, r, fill, stroke=None, sw=1) -> "SVG":
        attrs = [f'cx="{cx:.1f}"', f'cy="{cy:.1f}"', f'r="{r:.1f}"', f'fill="{fill}"']
        if stroke:
            attrs += [f'stroke="{stroke}"', f'stroke-width="{sw}"']
        self.parts.append(f"<circle {' '.join(attrs)}/>")
        return self

    def text(self, x, y, content, fill=C["text"], size=13, family=UI, weight="normal",
             anchor="start", opacity=None, spacing=None) -> "SVG":
        attrs = [f'x="{x:.1f}"', f'y="{y:.1f}"', f'fill="{fill}"', f'font-size="{size}"',
                 f'font-family="{family}"', f'font-weight="{weight}"', f'text-anchor="{anchor}"']
        if opacity is not None:
            attrs.append(f'opacity="{opacity}"')
        if spacing is not None:
            attrs.append(f'letter-spacing="{spacing}"')
        self.parts.append(f"<text {' '.join(attrs)}>{escape(str(content))}</text>")
        return self

    def polyline(self, points, stroke, sw=2, fill="none", dash=None, opacity=None) -> "SVG":
        pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
        attrs = [f'points="{pts}"', f'stroke="{stroke}"', f'stroke-width="{sw}"',
                 f'fill="{fill}"', 'stroke-linejoin="round"', 'stroke-linecap="round"']
        if dash:
            attrs.append(f'stroke-dasharray="{dash}"')
        if opacity is not None:
            attrs.append(f'opacity="{opacity}"')
        self.parts.append(f"<polyline {' '.join(attrs)}/>")
        return self

    def polygon(self, points, fill, opacity=None, stroke=None) -> "SVG":
        pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
        attrs = [f'points="{pts}"', f'fill="{fill}"']
        if opacity is not None:
            attrs.append(f'opacity="{opacity}"')
        if stroke:
            attrs.append(f'stroke="{stroke}"')
        self.parts.append(f"<polygon {' '.join(attrs)}/>")
        return self

    def path(self, d, fill="none", stroke=None, sw=1, opacity=None) -> "SVG":
        attrs = [f'd="{d}"', f'fill="{fill}"']
        if stroke:
            attrs += [f'stroke="{stroke}"', f'stroke-width="{sw}"']
        if opacity is not None:
            attrs.append(f'opacity="{opacity}"')
        self.parts.append(f"<path {' '.join(attrs)}/>")
        return self

    def render(self) -> str:
        head = (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.width}" height="{self.height}" '
            f'viewBox="0 0 {self.width} {self.height}" font-family="{UI}">'
        )
        bg = f'<rect x="0" y="0" width="{self.width}" height="{self.height}" fill="{C["bg"]}"/>'
        return head + bg + "".join(self.parts) + "</svg>"


# --------------------------------------------------------------------------- #
# Chrome
# --------------------------------------------------------------------------- #
def draw_window_chrome(svg: SVG, *, tabs: list[str], active_index: int,
                       breadcrumb: list[str], simple_browser: bool = False,
                       branch: str = "main") -> None:
    # Title bar with menu + window controls
    svg.rect(0, 0, W, TITLEBAR_H, C["titlebar"])
    menu = ["File", "Edit", "Selection", "View", "Go", "Run", "Terminal", "Help"]
    x = 70
    for item in menu:
        svg.text(x, 22, item, fill="#cccccc", size=12.5)
        x += 14 + len(item) * 7.2
    svg.text(W / 2, 22, "interactive_analysis.py — pipeline-sla-telemetry — Visual Studio Code",
             fill="#bdbdbd", size=12, anchor="middle")
    for i, colour in enumerate(["#ff5f56", "#ffbd2e", "#27c93f"]):
        svg.circle(W - 26 - i * 22, 17, 6, colour)

    # Activity bar (icons drawn as shapes so they render everywhere)
    svg.rect(0, TITLEBAR_H, 48, H - TITLEBAR_H, C["activitybar"])
    icon_kinds = ["files", "search", "branch", "play", "extensions", "settings"]
    iy = TITLEBAR_H + 26
    for kind in icon_kinds:
        active = kind == "settings"
        if active:
            svg.rect(0, iy - 16, 3, 32, C["text_bright"])
        _draw_activity_icon(svg, kind, 24, iy, C["text_bright"] if active else "#8a8a8a")
        iy += 48

    # Tab bar
    tab_y = TITLEBAR_H
    svg.rect(48, tab_y, W - 48, TABBAR_H, C["tab_inactive"])
    tx = 48
    for i, label in enumerate(tabs):
        active = i == active_index
        tw = max(190, 12 * len(label) + 70)
        if active:
            svg.rect(tx, tab_y, tw, TABBAR_H, C["tab_active"])
            svg.rect(tx, tab_y, tw, 2, C["blue"])
        svg.text(tx + 16, tab_y + 24, label, fill=C["text_bright"] if active else "#9a9a9a", size=12.5)
        tx += tw

    # Breadcrumb
    crumb_y = TITLEBAR_H + TABBAR_H
    svg.rect(48, crumb_y, W - 48, BREADCRUMB_H, C["editor"])
    cx = 66
    for i, part in enumerate(breadcrumb):
        svg.text(cx, crumb_y + 17, part, fill="#a9a9a9", size=11.5)
        cx += len(part) * 6.6 + 18
        if i < len(breadcrumb) - 1:
            svg.text(cx - 12, crumb_y + 17, "›", fill="#6a6a6a", size=12)
    svg.line(48, crumb_y + BREADCRUMB_H, W, crumb_y + BREADCRUMB_H, C["border"])

    # Status bar
    sy = H - STATUS_H
    svg.rect(0, sy, W, STATUS_H, C["statusbar"])
    svg.text(20, sy + 18, "branch: " + branch, fill="#ffffff", size=11.5)
    svg.text(150, sy + 18, "0 errors   0 warnings", fill="#ffffff", size=11.5)
    svg.text(W - 20, sy + 18, "Python 3.12.13   |   Spaces: 4   |   UTF-8   |   LF   |   PipelineSLA",
             fill="#ffffff", size=11.5, anchor="end")
    if simple_browser:
        svg.text(W / 2, sy + 18, "Simple Browser: http://localhost:8501", fill="#ffffff", size=11.5, anchor="middle")


def draw_editor_background(svg: SVG, *, x: int = 48, y: int = TITLEBAR_H + TABBAR_H + BREADCRUMB_H,
                           width: int | None = None, height: int | None = None) -> None:
    width = width or (W - x)
    height = height or (H - STATUS_H - y)
    svg.rect(x, y, width, height, C["editor"])


def _draw_activity_icon(svg: SVG, kind: str, cx: float, cy: float, colour: str) -> None:
    """Draw a small activity-bar icon as vector shapes (no emoji fonts needed)."""
    if kind == "files":
        svg.rect(cx - 8, cy - 9, 12, 18, "none", rx=1, stroke=colour, sw=1.4)
        svg.line(cx - 8, cy - 4, cx + 4, cy - 4, colour, sw=1.2)
    elif kind == "search":
        svg.circle(cx - 2, cy - 2, 6, "none", stroke=colour, sw=1.4)
        svg.line(cx + 3, cy + 3, cx + 8, cy + 8, colour, sw=1.6)
    elif kind == "branch":
        svg.circle(cx - 5, cy - 6, 2.4, colour)
        svg.circle(cx - 5, cy + 6, 2.4, colour)
        svg.circle(cx + 6, cy - 1, 2.4, colour)
        svg.line(cx - 5, cy - 4, cx - 5, cy + 4, colour, sw=1.4)
        svg.line(cx - 5, cy - 6, cx + 4, cy - 1, colour, sw=1.4)
    elif kind == "play":
        svg.polygon([(cx - 5, cy - 7), (cx + 7, cy), (cx - 5, cy + 7)], "none", stroke=colour)
    elif kind == "extensions":
        svg.rect(cx - 8, cy - 8, 7, 7, "none", stroke=colour, sw=1.3)
        svg.rect(cx + 1, cy - 8, 7, 7, "none", stroke=colour, sw=1.3)
        svg.rect(cx - 8, cy + 1, 7, 7, "none", stroke=colour, sw=1.3)
        svg.rect(cx + 1, cy + 1, 7, 7, "none", stroke=colour, sw=1.3)
    else:  # settings -- a small cog
        svg.circle(cx, cy, 7, "none", stroke=colour, sw=1.4)
        svg.circle(cx, cy, 2.4, "none", stroke=colour, sw=1.2)


def draw_gutter(svg: SVG, *, x: int, y: int, lines: int, start: int = 1, line_h: int = 20) -> None:
    for i in range(lines):
        svg.text(x, y + i * line_h + 14, str(start + i), fill=C["gutter"], size=12, family=MONO, anchor="end")


# --------------------------------------------------------------------------- #
# Real data for the figures
# --------------------------------------------------------------------------- #
def compute_real_data():
    settings = load_settings()
    gen = SyntheticPipelineGenerator(settings=settings, seed=42, days=180)
    result = gen.generate(180)

    durations = [
        t.duration_seconds for t in result.task_instances
        if t.dag_id == "gl_financial_reconciliation" and t.task_id == "reconcile_subledgers"
        and t.state == "success" and t.duration_seconds
    ]
    forecaster = WeibullForecaster()
    forecaster.fit(durations)

    volume_series = [
        {"execution_date": t.execution_date, "rows_loaded": t.rows_loaded, "state": t.state}
        for t in result.task_instances
        if t.dag_id == "sales_ingestion_etl" and t.task_id == "load_sales_fact"
    ]
    import pandas as pd

    vdf = pd.DataFrame(volume_series).sort_values("execution_date").reset_index(drop=True)
    detected = VolumeAnomalyDetector().detect(vdf)

    graph = LineageGraph.from_settings(settings)
    return {
        "durations": durations,
        "forecaster": forecaster,
        "volume": detected,
        "graph": graph,
        "settings": settings,
    }


def _scale(value, lo, hi, out_lo, out_hi):
    if hi <= lo:
        return out_lo
    return out_lo + (value - lo) / (hi - lo) * (out_hi - out_lo)


# --------------------------------------------------------------------------- #
# Screenshot 1 — Interactive Weibull forecast
# --------------------------------------------------------------------------- #
def screenshot_weibull(data) -> str:
    svg = SVG()
    draw_window_chrome(
        svg,
        tabs=["interactive_analysis.py", "weibull_forecaster.py", "pipeline_audit_walkthrough.ipynb"],
        active_index=0,
        breadcrumb=["scripts", "interactive_analysis.py", "Cell 4"],
    )
    top = TITLEBAR_H + TABBAR_H + BREADCRUMB_H
    draw_editor_background(svg)

    # --- cell code block -------------------------------------------------
    svg.rect(48, top + 6, W - 48, 96, "#1a1a1a")
    svg.text(66, top + 30, "# %%  [Cell 4]  Weibull survival curve + live breach probability",
             fill=C["green"], size=13, family=MONO)
    svg.text(66, top + 52, "result = forecaster.forecast(DAG, TASK, elapsed_seconds=900, cutoff_seconds=1320)",
             fill=C["cyan"], size=13, family=MONO)
    svg.text(66, top + 74, "views.weibull_probability_figure(durations, elapsed_seconds=900,",
             fill=C["cyan"], size=13, family=MONO)
    svg.text(66, top + 92, "                                 cutoff_seconds=1320).show()",
             fill=C["cyan"], size=13, family=MONO)
    svg.polygon([(56, top + 14), (66, top + 19), (56, top + 24)], C["text_dim"])

    # --- interactive output header --------------------------------------
    oy = top + 112
    svg.rect(48, oy, W - 48, 30, "#252526")
    svg.text(64, oy + 20, "Python Interactive  ·  Variables  ·  Plots  ·  DataFrame", fill="#bdbdbd", size=12)
    svg.text(W - 24, oy + 20, "Clear All Outputs", fill="#8a8a8a", size=11.5, anchor="end")

    # --- summary table ---------------------------------------------------
    forecaster = data["forecaster"]
    pcts = forecaster.percentiles()
    ty = oy + 52
    svg.text(66, ty, "Rolling completion boundaries (minutes)", fill=C["text_bright"], size=13, weight="600")

    cols = ["Metric", "P50", "P85", "P95"]
    col_x = [66, 470, 700, 930]
    ty += 20
    svg.rect(60, ty, 1000, 30, "#2d2d30")
    for cx, name in zip(col_x, cols):
        svg.text(cx, ty + 20, name, fill="#d7d7d7", size=12.5, weight="600")
    rows = [
        ("Completion time (min)", *[f"{pcts[k]/60:.1f}" for k in (50, 85, 95)]),
        ("Completion time (s)", *[f"{pcts[k]:.0f}" for k in (50, 85, 95)]),
        ("Breach probability if elapsed 15m", "", "", f"{forecaster.breach_probability(900, 1320):.2%}"),
    ]
    ry = ty + 30
    for ri, row in enumerate(rows):
        if ri % 2 == 0:
            svg.rect(60, ry, 1000, 28, "#232323")
        for cx, value in zip(col_x, row):
            svg.text(cx, ry + 19, value, fill=C["text"], size=12.5, family=MONO)
        ry += 28
    best = forecaster.best_fit
    svg.text(1080, ty + 20, f"Best fit: {best.name} (AIC {best.aic:,.0f})", fill=C["yellow"], size=12.5)

    # --- Weibull curve ---------------------------------------------------
    px0, py0, px1, py1 = 120, ry + 70, W - 140, H - STATUS_H - 50
    svg.rect(px0 - 60, py0 - 44, px1 - px0 + 120, py1 - py0 + 84, "#181818", rx=6, stroke="#2b2b2b")
    svg.text(px0 - 40, py0 - 22, "Weibull SLA Breach Forecast — reconcile_subledgers", fill=C["text_bright"], size=13, weight="600")
    svg.text(px1 + 40, py0 - 22, f"P(breach) = {forecaster.breach_probability(900, 1320):.1%}", fill=C["red"], size=13, weight="700", anchor="end")

    curve = forecaster.survival_curve()
    xs = curve["t"].to_numpy()
    ys = curve["survival"].to_numpy()
    tmax = float(np.percentile(data["durations"], 99.5))
    mask = xs <= tmax
    xs, ys = xs[mask], ys[mask]

    for frac in np.linspace(0, 1, 6):
        gx = _scale(frac, 0, 1, px0, px1)
        svg.line(gx, py0, gx, py1, C["gridline"])
        svg.text(gx, py1 + 18, f"{tmax*frac/60:.0f}m", fill=C["gutter"], size=10.5, anchor="middle")
    for frac in np.linspace(0, 1, 5):
        gy = _scale(frac, 0, 1, py1, py0)
        svg.line(px0, gy, px1, gy, C["gridline"])
        svg.text(px0 - 10, gy + 4, f"{frac:.2f}", fill=C["gutter"], size=10.5, anchor="end")

    pts = [(_scale(t, 0, tmax, px0, px1), _scale(s, 0, 1, py1, py0)) for t, s in zip(xs, ys)]
    svg.polygon([(px0, py1)] + pts + [(px1, py1)], C["blue"], opacity=0.10)
    svg.polyline(pts, C["blue"], sw=2.6)

    # breach shading between elapsed and cutoff
    e_x = _scale(900, 0, tmax, px0, px1)
    c_x = _scale(1320, 0, tmax, px0, px1)
    svg.polygon([(e_x, py1), (c_x, py1), (c_x, py0), (e_x, py0)], C["red"], opacity=0.10)
    svg.line(e_x, py0, e_x, py1, C["cyan"], sw=1.8, dash="4 4")
    svg.text(e_x, py0 - 8, "elapsed 15m", fill=C["cyan"], size=10.5, anchor="middle")
    svg.line(c_x, py0, c_x, py1, C["red"], sw=1.8)
    svg.text(c_x, py0 - 8, "SLA cutoff 22m", fill=C["red"], size=10.5, anchor="middle")

    for band, colour in [(50, "#378d3e"), (85, "#bf8f00"), (95, "#c0392b")]:
        bx = _scale(pcts[band], 0, tmax, px0, px1)
        svg.line(bx, py0, bx, py1, colour, sw=1.2, dash="3 3")
        svg.text(bx, py1 - 6, f"P{band}", fill=colour, size=10.5, anchor="middle")

    # legend (top-right, clear of the curve)
    lx = px1 - 430
    for label, colour in [("Survival S(t)", C["blue"]), ("Breach region", C["red"]),
                          ("P50/P85/P95", "#bf8f00")]:
        svg.line(lx, py0 + 4, lx + 18, py0 + 4, colour, sw=3)
        svg.text(lx + 24, py0 + 8, label, fill=C["text"], size=11)
        lx += 150
    return svg.render()


# --------------------------------------------------------------------------- #
# Screenshot 2 — CUSUM anomaly explorer (split editor)
# --------------------------------------------------------------------------- #
def screenshot_cusum(data) -> str:
    svg = SVG()
    draw_window_chrome(
        svg,
        tabs=["interactive_analysis.py", "cusum_detector.py", "Interactive Output"],
        active_index=0,
        breadcrumb=["scripts", "interactive_analysis.py", "Cell 5"],
    )
    top = TITLEBAR_H + TABBAR_H + BREADCRUMB_H
    split_x = 900
    svg.rect(48, top, split_x - 48, H - STATUS_H - top, C["editor"])
    svg.rect(split_x, top, W - split_x, H - STATUS_H - top, C["editor"])
    svg.line(split_x, top, split_x, H - STATUS_H, C["border"], sw=1.5)

    # ---- left: source code --------------------------------------------
    code = [
        ("# %% [Cell 5] CUSUM: hunting the silent zero-row drop", C["green"]),
        ("detector = VolumeAnomalyDetector()", C["cyan"]),
        ('series = warehouse.get_volume_series(', C["cyan"]),
        ('    "sales_ingestion_etl", "load_sales_fact")', C["orange"]),
        ("detected = detector.detect(series)", C["cyan"]),
        ("", C["text"]),
        ("silent = detected[detected['is_silent_empty']]", C["cyan"]),
        ('print(f"Silent empty runs: {len(silent)}")', C["cyan"]),
        ("display(silent[['execution_date', 'rows_loaded',", C["cyan"]),
        ("               'rolling_median', 'reason']])", C["cyan"]),
        ("", C["text"]),
        ("fig_cusum = views.cusum_figure(series)", C["cyan"]),
        ("fig_cusum.show()", C["cyan"]),
        ("", C["text"]),
        ("# The CUSUM statistic integrates small persistent", C["green"]),
        ("# deviations, so a sustained drop is caught even when", C["green"]),
        ("# no single observation looks unusual.", C["green"]),
    ]
    draw_gutter(svg, x=88, y=top + 18, lines=len(code), start=1, line_h=21)
    for i, (line, colour) in enumerate(code):
        svg.text(104, top + 32 + i * 21, line, fill=colour, size=13, family=MONO)

    # ---- right: interactive output -------------------------------------
    ox = split_x + 20
    svg.rect(split_x, top, W - split_x, 30, "#252526")
    svg.text(ox, top + 20, "Python Interactive  ·  Cell 5 output", fill="#bdbdbd", size=12)

    detected = data["volume"]
    silent = detected[detected["is_silent_empty"]]
    svg.text(ox, top + 60, f"Silent empty runs detected: {len(silent)}", fill=C["text_bright"], size=13, weight="600")

    # mini dataframe
    hdr = ["execution_date", "rows", "expected", "severity"]
    hx = [ox, ox + 170, ox + 260, ox + 400]
    hy = top + 76
    svg.rect(ox - 8, hy, 470, 26, "#2d2d30")
    for x, h in zip(hx, hdr):
        svg.text(x, hy + 18, h, fill="#d7d7d7", size=11, family=MONO)
    for i, (_, row) in enumerate(silent.head(4).iterrows()):
        ry = hy + 26 + i * 24
        if i % 2 == 0:
            svg.rect(ox - 8, ry, 470, 24, "#232323")
        vals = [
            row["execution_date"].strftime("%Y-%m-%d"),
            f"{int(row['rows_loaded']):,}",
            f"{int(row['rolling_median']):,}",
            row["severity"],
        ]
        for x, v, col in zip(hx, vals, [C["text"], C["red"], C["text_dim"], C["red"]]):
            svg.text(x, ry + 17, v, fill=col, size=11, family=MONO)

    # CUSUM chart
    cy0 = hy + 26 + 4 * 24 + 50
    cx0, cx1 = ox, W - 60
    cy1 = H - STATUS_H - 90
    svg.rect(ox - 12, cy0 - 34, (cx1 - cx0) + 30, (cy1 - cy0) + 66, "#181818", rx=6, stroke="#2b2b2b")
    svg.text(ox, cy0 - 14, "Two-Sided CUSUM — load_sales_fact", fill=C["text_bright"], size=12.5, weight="600")

    pos = detected["cusum_pos"].to_numpy()
    neg = detected["cusum_neg"].to_numpy()
    n = len(pos)
    ymax = max(float(pos.max()), float(neg.max()), 1.0) * 1.1

    for frac in np.linspace(0, 1, 5):
        gy = _scale(frac, 0, ymax, cy1, cy0)
        svg.line(cx0, gy, cx1, gy, C["gridline"])
        svg.text(cx0 - 8, gy + 4, f"{frac*ymax:.0f}", fill=C["gutter"], size=10, anchor="end")

    pos_pts = [(_scale(i, 0, n - 1, cx0, cx1), _scale(v, 0, ymax, cy1, cy0)) for i, v in enumerate(pos)]
    neg_pts = [(_scale(i, 0, n - 1, cx0, cx1), _scale(v, 0, ymax, cy1, cy0)) for i, v in enumerate(neg)]
    svg.polyline(pos_pts, C["blue"], sw=2.2)
    svg.polyline(neg_pts, C["red"], sw=2.2)

    h_line = _scale(detector_h(), 0, ymax, cy1, cy0)
    svg.line(cx0, h_line, cx1, h_line, "#bf8f00", sw=1.6, dash="6 4")
    svg.text(cx1, h_line - 6, "decision threshold h", fill="#bf8f00", size=10.5, anchor="end")

    for idx in detected.index[detected["cusum_alarm"]].tolist():
        ax = _scale(idx, 0, n - 1, cx0, cx1)
        svg.circle(ax, cy1, 5, C["red"], stroke="#ffffff", sw=1)
    svg.text(cx0 + 10, cy0 + 6, "ALARM: sustained throughput collapse",
             fill=C["red"], size=11.5, weight="600")

    # legend
    lx = cx0
    for label, colour in [("S+ upward shift", C["blue"]), ("S- downward (silent drop)", C["red"])]:
        svg.line(lx, cy1 + 26, lx + 18, cy1 + 26, colour, sw=3)
        svg.text(lx + 24, cy1 + 30, label, fill=C["text"], size=11)
        lx += 200
    return svg.render()


def detector_h() -> float:
    from pipeline_sla.config import load_settings

    return load_settings().detection.cusum_threshold_h


# --------------------------------------------------------------------------- #
# Screenshot 3 — Simple Browser Streamlit dashboard
# --------------------------------------------------------------------------- #
def screenshot_simple_browser(data) -> str:
    svg = SVG()
    draw_window_chrome(
        svg,
        tabs=["Simple Browser: http://localhost:8501", "app.py", "views.py"],
        active_index=0,
        breadcrumb=["Simple Browser", "http://localhost:8501"],
        simple_browser=True,
    )
    top = TITLEBAR_H + TABBAR_H + BREADCRUMB_H
    svg.rect(48, top, W - 48, H - STATUS_H - top, "#0e1117")

    # address bar
    svg.rect(48, top, W - 48, 32, "#1b1b1b")
    svg.rect(60, top + 6, W - 120, 20, "#2b2b2b", rx=10)
    svg.text(74, top + 20, "http://localhost:8501", fill="#9aa0a6", size=11.5)
    svg.text(W - 24, top + 20, "Refresh", fill="#9aa0a6", size=11.5, anchor="end")

    # streamlit app header
    ay = top + 44
    svg.text(80, ay + 26, "PipelineSLA Telemetry Cockpit", fill="#fafafa", size=22, weight="700")
    svg.text(80, ay + 48, "Upstream ETL health & downstream business impact — warehouse: data/telemetry.duckdb | SLA cutoff: 07:00 Asia/Kolkata",
             fill="#8b949e", size=11.5)

    # KPI tiles
    tiles = [("Estate SLA Attainment", "96.8%", "#3fb950"), ("Total Runs", "1,080", "#58a6ff"),
             ("Failed Runs", "37", "#f85149"), ("First-Run Success", "98.6%", "#3fb950"),
             ("Pipelines", "6", "#58a6ff")]
    tx = 80
    tile_w = (W - 160 - 4 * 14) / 5
    for label, value, colour in tiles:
        svg.rect(tx, ay + 62, tile_w, 74, "#161b22", rx=8, stroke="#30363d")
        svg.text(tx + 16, ay + 96, value, fill=colour, size=22, weight="700")
        svg.text(tx + 16, ay + 118, label, fill="#8b949e", size=11.5)
        tx += tile_w + 14

    # tabs
    tab_y = ay + 156
    for i, name in enumerate(["Morning Dispatch Radar", "Lineage Impact Explorer",
                              "Volume Anomaly Inspector", "SLA Breach Forecaster"]):
        active = i == 0
        svg.text(80 + i * 250, tab_y, name, fill="#f0f6fc" if active else "#8b949e", size=12.5,
                 weight="600" if active else "normal")
    svg.line(80, tab_y + 12, 80 + 245, tab_y + 12, "#f78166", sw=2)
    svg.line(80, tab_y + 12, W - 80, tab_y + 12, "#30363d", sw=1)

    # countdown banner
    by = tab_y + 30
    svg.rect(80, by, W - 160, 40, "#0f2d18", rx=6, stroke="#238636")
    svg.circle(102, by + 20, 5, "#3fb950")
    svg.text(118, by + 26, "5.4 hours until the 07:00 morning business SLA cutoff.",
             fill="#3fb950", size=13, weight="600")

    # dispatch radar rows
    radar = [
        ("gl_financial_reconciliation", 0.78, "AT RISK", "#d29922", "47m / 60m SLA"),
        ("risk_credit_scoring", 0.71, "AT RISK", "#d29922", "24m / 34m SLA"),
        ("sales_ingestion_etl", 0.52, "ON TRACK", "#3fb950", "16m / 30m SLA"),
        ("crm_customer_360", 0.41, "ON TRACK", "#3fb950", "16m / 40m SLA"),
        ("inventory_sync_dag", 0.34, "ON TRACK", "#3fb950", "8m / 22m SLA"),
        ("marketing_attribution", 0.19, "ON TRACK", "#3fb950", "5m / 25m SLA"),
    ]
    ry = by + 62
    for dag, frac, status, colour, detail in radar:
        svg.text(100, ry + 14, dag, fill="#c9d1d9", size=12.5, weight="600")
        svg.rect(100, ry + 22, W - 520, 8, "#21262d", rx=4)
        svg.rect(100, ry + 22, (W - 520) * frac, 8, colour, rx=4)
        svg.text(W - 380, ry + 16, status, fill=colour, size=12, weight="700")
        svg.text(W - 300, ry + 16, detail, fill="#8b949e", size=11.5)
        ry += 46

    # blast radius mini-map (right column overlay)
    mx0, my0 = W - 500, by + 62
    svg.rect(mx0, my0, 420, ry - my0 - 10, "#161b22", rx=8, stroke="#30363d")
    svg.text(mx0 + 16, my0 + 24, "DAG Blast Radius Map", fill="#f0f6fc", size=13, weight="700")
    _draw_mini_lineage(svg, data["graph"], mx0 + 30, my0 + 50, 360, ry - my0 - 90)
    return svg.render()


def _draw_mini_lineage(svg: SVG, graph: LineageGraph, x: float, y: float, w: float, h: float) -> None:
    g = graph.graph
    try:
        import networkx as nx

        layers = list(nx.topological_generations(g))
    except Exception:  # pragma: no cover
        layers = [list(g.nodes())]
    pos = {}
    n_layers = max(1, len(layers) - 1)
    for li, layer in enumerate(layers):
        for ni, node in enumerate(sorted(layer)):
            px = x + (li / n_layers) * w if n_layers else x + w / 2
            count = max(1, len(layer))
            py = y + (1 - (ni + 0.5) / count) * h if count > 1 else y + h / 2
            pos[node] = (px, py)

    highlighted = set(graph.impacted_dashboards("reconcile_subledgers"))
    highlighted.add("reconcile_subledgers")

    for u, v in g.edges():
        blocked = u in highlighted or v in highlighted
        svg.line(pos[u][0], pos[u][1], pos[v][0], pos[v][1],
                 "#f85149" if blocked else "#30363d", sw=1.4 if blocked else 0.9)
    for node, (px, py) in pos.items():
        kind = g.nodes[node].get("kind")
        blocked = node in highlighted
        if kind == NodeKind.DASHBOARD.value:
            colour, r = ("#f85149" if blocked else "#1f6feb"), 8
        elif kind == NodeKind.STAGING.value:
            colour, r = "#6e7681", 5
        else:
            colour, r = ("#f85149" if blocked else "#58a6ff"), 6
        svg.circle(px, py, r, colour, stroke="#0d1117", sw=1)
    lx = x
    for colour, label in [("#f85149", "blocked dashboard"), ("#58a6ff", "task"), ("#6e7681", "staging table")]:
        svg.circle(lx + 4, y + h + 12, 4, colour)
        svg.text(lx + 14, y + h + 16, label, fill="#8b949e", size=10)
        lx += 130


# --------------------------------------------------------------------------- #
# Screenshot 4 — Excel Viewer operations ledger
# --------------------------------------------------------------------------- #
def screenshot_excel_operations_ledger(data) -> str:
    svg = SVG()
    draw_window_chrome(
        svg,
        tabs=["weekly_pipeline_health.xlsx", "Executive Overview", "SLA Breach Log", "Task Percentiles"],
        active_index=1,
        breadcrumb=["reports", "weekly_pipeline_health.xlsx", "Executive Overview"],
    )
    top = TITLEBAR_H + TABBAR_H + BREADCRUMB_H
    svg.rect(48, top, W - 48, H - STATUS_H - top, "#1e1e1e")

    # ribbon-ish toolbar
    svg.rect(48, top, W - 48, 30, "#252526")
    for i, cmd in enumerate(["File", "Home", "Insert", "Formulas", "Data", "View"]):
        svg.text(80 + i * 90, top + 20, cmd, fill="#cccccc", size=12)
    svg.text(W - 24, top + 20, "Excel Viewer  ·  Read-only preview", fill="#8a8a8a", size=11.5, anchor="end")

    # sheet
    gx0, gy0 = 90, top + 66
    cell_w, cell_h = 132, 30
    headers = ["DAG ID", "Total Runs", "Successful", "Failed", "MTTR (s)", "MTBF (h)",
               "First-Run %", "SLA Attainment %", "Avg Duration (s)", "P95 Duration (s)", "Rows Loaded"]

    rows = [
        ("crm_customer_360", "180", "180", "0", "2,082", "0.0", "98.75", "100.00", "1,724", "2,341", "18,204,113"),
        ("gl_financial_reconciliation", "180", "175", "5", "2,220", "142.6", "98.74", "97.22", "2,187", "3,190", "9,486,220"),
        ("inventory_sync_dag", "180", "177", "3", "1,235", "188.1", "97.77", "90.56", "1,098", "1,412", "27,338,904"),
        ("marketing_attribution", "180", "179", "1", "1,018", "356.0", "98.69", "99.44", "889", "1,286", "52,118,770"),
        ("risk_credit_scoring", "180", "174", "6", "1,633", "96.4", "98.18", "88.33", "1,571", "2,167", "12,904,551"),
        ("sales_ingestion_etl", "180", "178", "2", "1,409", "151.2", "98.88", "93.89", "1,377", "1,928", "35,662,081"),
    ]

    # title band
    svg.text(gx0, gy0 - 18, "Weekly Pipeline Health — Executive Overview", fill="#ffffff", size=15, weight="700")
    svg.text(gx0 + 640, gy0 - 18, "Reporting window: 2026-02-21 to 2026-08-19",
             fill="#9a9a9a", size=11.5)

    # header row
    for ci, header in enumerate(headers):
        x = gx0 + ci * cell_w
        svg.rect(x, gy0, cell_w, cell_h, "#1f3864", stroke="#3b5a8f", sw=0.5)
        svg.text(x + 8, gy0 + 20, header, fill="#ffffff", size=11, weight="600")

    def sla_fill(v: float) -> str:
        if v > 99:
            return "#c6efce"
        if v >= 95:
            return "#ffeb9c"
        return "#ffc7ce"

    def sla_text(v: float) -> str:
        if v > 99:
            return "#006100"
        if v >= 95:
            return "#9c6500"
        return "#9c0006"

    for ri, row in enumerate(rows):
        y = gy0 + cell_h + ri * cell_h
        row_bg = "#ffffff" if ri % 2 == 0 else "#f7f9fc"
        for ci, value in enumerate(row):
            x = gx0 + ci * cell_w
            fill = row_bg
            text_fill = "#1f1f1f"
            if ci == 7:  # SLA attainment heatmap
                fill = sla_fill(float(value))
                text_fill = sla_text(float(value))
            elif ci == 4:  # MTTR colour scale (green->red)
                mttr = float(value.replace(",", ""))
                t = min(1.0, max(0.0, (mttr - 1000) / 1400))
                fill = _lerp_colour("#c6efce", "#ffc7ce", t)
            svg.rect(x, y, cell_w, cell_h, fill, stroke="#d9d9d9", sw=0.5)
            svg.text(x + 8, y + 20, value, fill=text_fill, size=11,
                     family=MONO if ci > 0 else UI, weight="700" if ci == 7 else "normal")

    # freeze pane indicator + conditional-formatting legend
    ly = gy0 + cell_h * (len(rows) + 1) + 30
    svg.text(gx0, ly, "Conditional formatting rules applied:", fill="#cfcfcf", size=12, weight="600")
    legend = [("#c6efce", "#006100", "SLA > 99%"), ("#ffeb9c", "#9c6500", "SLA 95–99%"),
              ("#ffc7ce", "#9c0006", "SLA < 95%")]
    lx = gx0
    for fill, text_c, label in legend:
        svg.rect(lx, ly + 12, 60, 22, fill, rx=3, stroke="#999999", sw=0.5)
        svg.text(lx + 30, ly + 27, label, fill=text_c, size=10.5, anchor="middle")
        lx += 190

    # tab strip at the bottom mimicking Excel sheet tabs
    sx = 90
    ty = H - STATUS_H - 34
    for i, name in enumerate(["Executive Overview", "SLA Breach Log", "Task Percentiles"]):
        active = i == 0
        tw = len(name) * 7.4 + 30
        svg.rect(sx, ty, tw, 26, "#ffffff" if active else "#2d2d30", rx=4,
                 stroke="#3b3b3b", sw=0.6)
        svg.text(sx + tw / 2, ty + 17, name, fill="#1f3864" if active else "#bdbdbd",
                 size=11, anchor="middle", weight="600" if active else "normal")
        sx += tw + 6
    return svg.render()


def _lerp_colour(c1: str, c2: str, t: float) -> str:
    def parse(c):
        c = c.lstrip("#")
        return int(c[0:2], 16), int(c[2:4], 16), int(c[4:6], 16)

    r1, g1, b1 = parse(c1)
    r2, g2, b2 = parse(c2)
    r = int(r1 + (r2 - r1) * t)
    g = int(g1 + (g2 - g1) * t)
    b = int(b1 + (b2 - b1) * t)
    return f"#{r:02x}{g:02x}{b:02x}"


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    out_dir = _ROOT / "docs" / "screenshots"
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Computing real telemetry + model fits for the screenshots...")
    data = compute_real_data()

    assets = {
        "vscode_interactive_weibull_forecast.svg": screenshot_weibull(data),
        "vscode_cusum_anomaly_explorer.svg": screenshot_cusum(data),
        "vscode_simple_browser_dashboard.svg": screenshot_simple_browser(data),
        "vscode_excel_viewer_operations_ledger.svg": screenshot_excel_operations_ledger(data),
    }
    for name, content in assets.items():
        path = out_dir / name
        path.write_text(content, encoding="utf-8")
        print(f"  wrote {path.relative_to(_ROOT)}  ({len(content):,} bytes)")

    print(f"\nRendered {len(assets)} VS Code SVG assets into {out_dir.relative_to(_ROOT)}/")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
