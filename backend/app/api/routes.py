# backend/app/routes.py
# backend/app/api/routes.py
from fastapi import APIRouter, HTTPException, Depends
from uuid import uuid4
import time
import ast
import logging

from ..services.cache import make_cache_key_for_code, get_result, set_result
from ..workers.tasks import explain_code_task
from ..config import CACHE_TTL
from ..rate_limit import rate_limit
from .schemas import CodeRequest, JobResponse

from app.metrics import (
    HTTP_REQUESTS_TOTAL,
    HTTP_REQUEST_LATENCY,
    CACHE_HIT_TOTAL,
    CACHE_MISS_TOTAL,
)


router = APIRouter()
logger = logging.getLogger(__name__)


@router.post(
    "/explain/pyspark",
    response_model=JobResponse,
    dependencies=[Depends(rate_limit)],
)
async def explain_pyspark(request: CodeRequest):
    start = time.time()
    status = "error" 
    endpoint = "/explain/pyspark"
    
    logger.info(
        "request_received",
        extra={
            "event": "request_received",
            "endpoint": "/explain/pyspark",
        },
    )

    # 1) FAST FAIL: syntax validation
    try:
        code = request.code
        ast.parse(code)
    except SyntaxError as e:
        status = "bad_request"
        raise HTTPException(
            status_code=400,
            detail={
                "type": "SyntaxError",
                "message": str(e),
                "lineno": e.lineno,
                "offset": e.offset,
            },
        )
    
    # 2) Check cache
    try: 
        cache_key = make_cache_key_for_code(code)
        cached = get_result(cache_key)
        
        if cached:
            CACHE_HIT_TOTAL.inc()
            status = "cached"
            
            logger.info(
                "cache_hit",
                extra={
                    "event": "cache_hit",
                    "cache_key": cache_key,
                    "endpoint": "/explain/pyspark",
                },
            )

            cache_job_id = f"cached:{cache_key}"
            cached_analysis = get_result(f"{cache_key}:analysis") or {}

            payload = {
                "job_id": cache_job_id,
                "status": "finished",
                "result": {
                    "llm": cached,
                    "analysis": cached_analysis,
                },
                "job_duration_ms": 0,
                "cached": True,
            }
            
            set_result(f"job:{cache_job_id}", payload, ttl=CACHE_TTL)

            return {
                "job_id": cache_job_id,
                "status": "finished",
                "cached": True
            }
        # 3) Enqueue background job
        CACHE_MISS_TOTAL.inc()
        status = "queued"
        
        job_id = str(uuid4())
        explain_code_task.apply_async(
                    kwargs={
                        "job_id": job_id,
                        "code": code,
                        "cache_key": cache_key,
                    }
                )  
        
        return {"job_id": job_id, "status": "pending", "cached": False}
    
    finally:
        # 4) Metrics
        HTTP_REQUESTS_TOTAL.labels(
            method="POST",
            endpoint=endpoint,
            status=status,
        ).inc()

        HTTP_REQUEST_LATENCY.labels(
            endpoint=endpoint,
        ).observe(time.time() - start)

@router.get("/status/{job_id}")
async def get_status(job_id: str):
    job_key = f"job:{job_id}"
    payload = get_result(job_key)
    
    if not payload:
        return {
            "job_id": job_id,
            "status": "pending",
            "result": None,
            "job_duration_ms": None,
            "cached": False
        }
    return {
        "job_id": payload.get("job_id"),
        "status": payload.get("status", "pending"),
        "result": payload.get("result"),
        "job_duration_ms": payload.get("job_duration_ms"),
        "cached": payload.get("cached", False),
    }

@router.get("/health")
async def health():
    return {"status": "ok"}
