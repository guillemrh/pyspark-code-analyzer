from pydantic import BaseModel, Field, field_validator
from typing import Optional


class CodeRequest(BaseModel):
    code: str = Field(..., min_length=1, max_length=100_000)

    @field_validator("code")
    @classmethod
    def validate_code_not_empty(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("Code cannot be empty or whitespace only")
        return stripped


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
