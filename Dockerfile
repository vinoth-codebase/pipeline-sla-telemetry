# syntax=docker/dockerfile:1
# =============================================================================
# PipelineSLA-Telemetry :: production container image
# -----------------------------------------------------------------------------
# Multi-stage build. Builder compiles wheels for the heavy scientific stack
# (scipy / pyarrow / duckdb); the runtime layer installs only the wheels and
# the application, keeping the final image lean and reproducible.
# =============================================================================

FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --upgrade pip && pip wheel --wheel-dir /wheels .

# -----------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    PIPELINE_SLA_CONFIG=/app/config/config.yaml

RUN groupadd --system app && useradd --system --gid app --create-home app

WORKDIR /app
COPY --from=builder /wheels /wheels
COPY pyproject.toml README.md ./
COPY src ./src
COPY config ./config
COPY scripts ./scripts

RUN pip install --no-cache-dir --no-index --find-links=/wheels pipeline-sla-telemetry \
    && pip install --no-cache-dir streamlit \
    && rm -rf /wheels \
    && mkdir -p /app/data /app/reports /app/docs/screenshots \
    && chown -R app:app /app

USER app
EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import pipeline_sla; print('ok')" || exit 1

# Default: bring up the visual management cockpit.
CMD ["streamlit", "run", "src/pipeline_sla/dashboard/app.py", \
     "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
