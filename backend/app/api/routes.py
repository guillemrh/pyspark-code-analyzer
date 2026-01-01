# backend/app/routes.py
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from uuid import uuid4
from ..services.cache import make_cache_key_for_code, get_result
from ..workers.tasks import explain_code_task
from .schemas import CodeRequest, ExplanationResponse, JobResponse
import logging
from fastapi import Depends
from ..rate_limit import rate_limit
import time
from ..services.cache import set_result
from ..config import settings, CACHE_TTL
import ast 

router = APIRouter()

@router.post("/explain/pyspark", response_model=JobResponse, dependencies=[Depends(rate_limit)])
async def explain_pyspark(request: CodeRequest):
    code = request.code
    
    # 🔴 FAST FAIL: syntax validation
    try:
        ast.parse(code)
    except SyntaxError as e:
        raise HTTPException(
            status_code=400,
            detail={
                "type": "SyntaxError",
                "message": str(e),
                "lineno": e.lineno,
                "offset": e.offset,
            },
        )

    
    cache_key = make_cache_key_for_code(code)
    
    # 1) Check cache
    request_start = time.time()
    cached = get_result(cache_key)
    if cached:
        logging.info("CACHE HIT for pyspark explanation")
        request_duration_ms = int((time.time() - request_start) * 1000)
        cache_job_id = f"cached:{cache_key}"
        payload = {
            "job_id": cache_job_id,
            "status": "finished",
            "result": {
                "llm": cached,
                "analysis": None,
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
    # 2) Enqueue background job
    job_id = str(uuid4())
    explain_code_task.apply_async(kwargs={"job_id": job_id, "code": code, "cache_key": cache_key})
    
    return {"job_id": job_id, "status": "pending", "cached": False}

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
