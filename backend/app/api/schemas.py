from pydantic import BaseModel, Field, field_validator
from typing import Optional


class CodeRequest(BaseModel):
    code: str = Field(
        ...,
        min_length=1,
        max_length=100_000,
        description="PySpark code to analyze (max 100,000 characters)",
        examples=["""df = spark.read.parquet("data.parquet")
df_filtered = df.filter(df.age > 25)
df_filtered.show()"""],
    )

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
    job_id: str = Field(
        description="Unique job identifier for polling status",
        examples=["550e8400-e29b-41d4-a716-446655440000"],
    )
    status: str = Field(
        description="Job status: 'pending' or 'finished'",
        examples=["pending"],
    )
    cached: bool = Field(
        default=False,
        description="Whether the result was returned from cache",
    )


class ExplanationResponse(BaseModel):
    job_id: str
    status: str
    result: Optional[ExplanationResult] = None
    job_duration_ms: Optional[int] = None
    cached: bool = False
