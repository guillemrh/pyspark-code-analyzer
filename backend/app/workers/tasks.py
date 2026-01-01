# backend/app/tasks.py
import time
from celery import Celery
from celery.utils.log import get_task_logger

from ..config import settings, CACHE_TTL
from ..services.llm import GeminiClient, LLMRateLimitError
from ..services.cache import set_result
from ..services.dag_pipeline import run_dag_pipeline

logger = get_task_logger(__name__)

celery = Celery(
    "tasks",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

@celery.task(
    bind=True,
    autoretry_for=(),
)

def explain_code_task(self, job_id: str, code: str, cache_key: str):
    """
    Background task that:
    1. Executes the LLM call
    2. Stores reusable result in cache
    3. Stores execution-specific job status
    """
    
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
        
    logger.info(f"Task start job_id={job_id}")
    update("running")
    
    # Measure FULL job duration (queue + execution)
    task_start = time.time()
    
    # simulate queue / scheduling delay
    time.sleep(1)
    
    try: 
        # --- DAG / Analysis ---
        # Build DAG and generate DOT representation
        dag_result = run_dag_pipeline(code)
        
        update(
            "analysis_complete",
            {
                "analysis": dag_result,
                "llm": None,
            }
        )
        
        # --- LLM ---
        try:
            llm_client = GeminiClient()
            llm_result = llm_client.explain_pyspark(code)

        except LLMRateLimitError as e:
            logger.warning(f"llm_rate_limited job_id={job_id}")
            llm_result = {
                "error": "rate_limited",
                "message": str(e),
            }

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
            f"task_finished job_id={job_id}"
            f" duration_ms={job_duration_ms}"
        )

        
    except Exception as e:
        logger.exception(f"Task failed job_id={job_id}")
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

    return job_payload

