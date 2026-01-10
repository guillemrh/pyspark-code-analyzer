# backend/app/tasks.py
import time
from celery import Celery
from celery.utils.log import get_task_logger

from ..config import settings, CACHE_TTL
from app.services.llm import explain_with_fallback
from ..services.cache import set_result
from ..services.dag_pipeline import run_dag_pipeline

from app.metrics import (
    JOBS_STARTED_TOTAL,
    JOBS_FAILED_TOTAL,
    JOB_DURATION_SECONDS,
)


from opentelemetry import trace
from app.telemetry.context import extract_trace_context
from app.telemetry.tracing import setup_tracing

setup_tracing(service_name="pyspark-llm-worker")


logger = get_task_logger(__name__)
tracer = trace.get_tracer(__name__)

celery = Celery(
    "tasks",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

@celery.task(
    bind=True,
    autoretry_for=(),
)

def explain_code_task(self, job_id: str, code: str, cache_key: str, trace_ctx: dict | None = None,):
    """
    Background task that:
    1. Executes the LLM call
    2. Stores reusable result in cache
    3. Stores execution-specific job status
    """
    # Extract trace context if provided
    parent_ctx = None
    if trace_ctx:
        parent_ctx = extract_trace_context(trace_ctx)
    with tracer.start_as_current_span(
        "celery.explain_code_task",
        context=parent_ctx,
        attributes={
            "job_id": job_id,
            "component": "celery_worker",
        },
    ):    
        # Update job status to running
        job_key = f"job:{job_id}"
        
        def update(status, result=None):
            set_result(
                job_key,
                {
                    "job_id": job_id,
                    "status": status,
                    "result": result,
                    "cached": False,
                },
                ttl=CACHE_TTL,
            )
            
        logger.info(
            "task_started",
            extra={
                "event": "task_started",
                "job_id": job_id,
                "component": "celery_worker",
            },
        )
        
        JOBS_STARTED_TOTAL.inc()
        task_start = time.time()

        update("running")
        
        # simulate queue / scheduling delay
        time.sleep(1)
        
        try: 
            # --- DAG / Analysis ---
            # Build DAG and generate DOT representation
            dag_result = run_dag_pipeline(code)
            analysis_cache_key = f"{cache_key}:analysis"
            set_result(analysis_cache_key, dag_result, ttl=CACHE_TTL)

            update(
                "analysis_complete",
                {
                    "analysis": dag_result,
                    "llm": None,
                }
            )
            
            # --- LLM ---
            llm_result = explain_with_fallback(code)

            # Cache only successful LLM outputs
            if "explanation" in llm_result:
                set_result(cache_key, llm_result, ttl=CACHE_TTL)

            update(
                "finished",
                {
                    "analysis": dag_result,
                    "llm": llm_result,
                },
            )
            
            job_duration_ms = int((time.time() - task_start) * 1000)
            
            logger.info(
                "task_finished",
                extra={
                    "event": "task_finished",
                    "job_id": job_id,
                    "duration_ms": job_duration_ms,
                    "component": "celery_worker",
                },
            )

        except Exception as e:
            JOBS_FAILED_TOTAL.inc()
            logger.exception(
                "task_failed",
                extra={
                    "event": "task_failed",
                    "job_id": job_id,
                    "component": "celery_worker",
                },
            )

            update(
                    "failed",
                    {
                        "error": {
                            "type": type(e).__name__,
                            "message": str(e),
                        }
                    },
                )

        job_payload = {
            "job_id": job_id,
            "status": "finished",
            "job_duration_ms": job_duration_ms,
            "cached": False,
        }
        
        JOB_DURATION_SECONDS.observe(
        time.time() - task_start
        )

        return job_payload