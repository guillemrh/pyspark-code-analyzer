# backend/app/tasks.py
import time
from celery import Celery
from celery.utils.log import get_task_logger

from .config import settings, CACHE_TTL
from .services.llm import GeminiClient
from .services.cache import set_result
from .services.dag_pipeline import run_dag_pipeline

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

    logger.info(f"Task start job_id={job_id}")
    
    # Measure FULL job duration (queue + execution)
    task_start = time.time()
    
    # simulate queue / scheduling delay
    time.sleep(1)
    
    try: 
        client = GeminiClient()
        llm_result = client.explain_pyspark(code)
        
        # Build DAG and generate DOT representation
        dag_result = run_dag_pipeline(code)

        status = "failed" if "error" in llm_result else "finished"

        # Cache key (reusable content)
        set_result(cache_key, llm_result, ttl=CACHE_TTL)
        
        result_payload = {
            **llm_result,
            **dag_result,
        }
        
    except Exception as e:
        logger.exception(f"Task failed job_id={job_id}")

        status = "failed"
        result_payload = {
            "error": {
                "type": type(e).__name__,
                "message": str(e),
            }
        }
    
    job_duration_ms = int((time.time() - task_start) * 1000)
    
    # Job key (execution metadata + content)
    job_payload = {
        "job_id": job_id,
        "status": status,
        "result": result_payload,
        "job_duration_ms": job_duration_ms,
        "cached": False
    }
    
    # ALWAYS write job state
    set_result(f"job:{job_id}", job_payload, ttl=CACHE_TTL)
    
    logger.info(
        f"Task finished job_id={job_id} "
        f"status={status} "
        f"job_duration_ms={job_duration_ms}"
    )

    return job_payload
