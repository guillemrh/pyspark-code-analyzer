from pydantic import BaseModel
from typing import Optional


class CodeRequest(BaseModel):
    code: str


class ExplanationResult(BaseModel):
    explanation: str | None = None
    latency_ms: int
    tokens_used: int | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    error: str | None = None


class JobResponse(BaseModel):
    job_id: str
    status: str
    cached: bool = False


class ExplanationResponse(BaseModel):
    job_id: str
    status: str
    result: Optional[ExplanationResult] = None
    job_duration_ms: Optional[int] = None
    cached: bool = False
