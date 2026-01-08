# backend/app/main.py
import logging
from fastapi import FastAPI, Response

from .api.routes import router
from app.logging import setup_logging
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from app.telemetry.tracing import setup_tracing
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

setup_logging(service_name="backend")

app = FastAPI(title="PySpark Intelligence Platform", version="1.0.0")
app.include_router(router)

tracer = setup_tracing("pyspark-llm-backend")
FastAPIInstrumentor.instrument_app(app)

@app.get("/")
def root():
    return {"message": "PySpark LLM Explainer API running"}

@app.get("/metrics")
def metrics():
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
