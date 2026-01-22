# backend/app/main.py
from fastapi import FastAPI, Response

from .api.routes import router
from app.logging import setup_logging
from app.config import settings
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from app.telemetry.tracing import setup_tracing
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

setup_logging(service_name="backend")

app = FastAPI(
    title="PySpark Intelligence Platform",
    description="""
## Overview

A distributed system for analyzing PySpark code through AST parsing, DAG construction,
data lineage tracking, anti-pattern detection, and LLM-assisted explanations using Google Gemini.

## Features

* **Syntax Validation**: Fast-fail AST parsing for immediate syntax error detection
* **DAG Analysis**: Builds operation DAGs with stage assignment and shuffle boundary detection
* **Data Lineage Tracking**: Tracks DataFrame dependencies across transformations
* **Anti-pattern Detection**: Identifies performance issues (multiple actions, early shuffles, missing cache)
* **LLM Explanations**: Generates human-readable explanations using Google Gemini
* **Caching**: Redis-backed result caching with 1-hour TTL
* **Async Processing**: Celery-based background job processing
* **Observability**: OpenTelemetry tracing (Jaeger) and Prometheus metrics

## Architecture

FastAPI → Redis (cache/state/broker) → Celery workers → Analysis Pipeline → Gemini LLM

## Workflow

1. Submit PySpark code via `POST /explain/pyspark`
2. System validates syntax and checks cache
3. Cache hit → immediate response | Cache miss → enqueue Celery task
4. Worker runs DAG pipeline analysis + LLM explanation
5. Results cached in Redis
6. Poll `GET /status/{job_id}` until complete
    """,
    version=settings.app_version,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    contact={
        "name": "PySpark Intelligence Platform",
    },
    license_info={
        "name": "Internal Use",
    },
)
app.include_router(router)

tracer = setup_tracing("pyspark-llm-backend")
FastAPIInstrumentor.instrument_app(app)


@app.get(
    "/",
    tags=["System"],
    summary="API root",
    description="Welcome endpoint showing the API is running.",
)
def root():
    return {"message": "PySpark LLM Explainer API running"}


@app.get(
    "/metrics",
    tags=["Observability"],
    summary="Prometheus metrics",
    description="""
    Prometheus scrape endpoint exposing application metrics.

    Metrics include:
    - HTTP request counts by endpoint and status
    - HTTP request latency histograms
    - Cache hit/miss counters
    - LLM API call metrics (success/failure/rate limits)
    - Job processing metrics

    Format: Prometheus text-based exposition format
    """,
)
def metrics():
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
