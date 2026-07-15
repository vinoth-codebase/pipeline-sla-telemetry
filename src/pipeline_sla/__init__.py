"""PipelineSLA-Telemetry.

Bridging the gap between upstream Data Engineering ETL stability and
downstream Data Analytics morning dashboard SLAs.

Public entry points are intentionally thin; the substantive logic lives in the
sub-packages:

* :mod:`pipeline_sla.client`    -- Airflow REST extraction + synthetic telemetry
* :mod:`pipeline_sla.storage`   -- DuckDB / Arrow analytical warehouse
* :mod:`pipeline_sla.analytics` -- Weibull SLA forecasting + CUSUM drift detection
* :mod:`pipeline_sla.lineage`   -- NetworkX DAG lineage + blast radius scoring
* :mod:`pipeline_sla.reporting` -- Excel ledger + PowerPoint operations review
* :mod:`pipeline_sla.dashboard` -- Streamlit visual management cockpit
"""

from __future__ import annotations

__version__ = "1.0.0"
__author__ = "Vinothkumar N"
__email__ = "vinothofficial14@gmail.com"

__all__ = ["__version__", "__author__", "__email__"]
