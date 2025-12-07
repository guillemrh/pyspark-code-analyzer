# backend/app/routes.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from .llm import GeminiClient

router = APIRouter()
client = GeminiClient()

class CodeRequest(BaseModel):
    code: str

@router.post("/explain/pyspark")
async def explain_pyspark(request: CodeRequest):
    result = client.explain_pyspark(request.code)
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return result

@router.get("/health")
async def health():
    return {"status": "ok"}
