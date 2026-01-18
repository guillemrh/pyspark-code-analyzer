import time
import google.generativeai as genai
from app.config import settings
import logging

from app.metrics import (
    LLM_CALLS_TOTAL,
    LLM_RATE_LIMIT_TOTAL,
    LLM_LATENCY_SECONDS,
)

from opentelemetry import trace

tracer = trace.get_tracer(__name__)
logger = logging.getLogger("llm")


class LLMRateLimitError(Exception):
    """Raised when the LLM hits a rate or quota limit."""

    pass


class GeminiClient:
    def __init__(self, model: str | None = None):
        genai.configure(api_key=settings.gemini_api_key)
        self.model_name = model or settings.gemini_model
        self.model = genai.GenerativeModel(self.model_name)

    def explain_pyspark(self, code: str) -> dict:

        with tracer.start_as_current_span(
            "llm.explain_pyspark",
            attributes={
                "model": self.model_name,
                "provider": "gemini",
            },
        ):

            start = time.time()
            logger.info(
                "llm_request",
                extra={
                    "event": "llm_request",
                    "model": self.model_name,
                },
            )
            try:

                prompt = f"""
                Explain the following PySpark code:\n\n{code}
                Be concise and clear in your explanation, don't expand too much. 
                Talk about the tradeoffs if any.
                Add a paragraph at the end with suggestions to improve the code performance.
                """
                LLM_CALLS_TOTAL.labels(model=self.model_name).inc()
                response = self.model.generate_content(prompt)
                explanation = response.text

                # Statistics
                latency_ms = int((time.time() - start) * 1000)

                # Extract usage stats safely
                raw = response.to_dict()
                usage = raw.get("usage_metadata", {})

                tokens_used = usage.get("total_token_count", 0)
                prompt_tokens = usage.get("prompt_token_count", 0)
                completion_tokens = usage.get("candidates_token_count", 0)

                return {
                    "model": self.model_name,
                    "explanation": explanation,
                    "tokens_used": tokens_used,
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "latency_ms": latency_ms,
                }
            except Exception as e:
                msg = str(e).lower()

                if "quota" in msg or "rate" in msg or "429" in msg:
                    LLM_RATE_LIMIT_TOTAL.labels(model=self.model_name).inc()
                    logger.warning(
                        "llm_rate_limited",
                        extra={
                            "event": "llm_rate_limited",
                            "model": self.model_name,
                        },
                    )
                    raise LLMRateLimitError(str(e))

                return {
                    "model": self.model_name,
                    "error": str(e),
                    "latency_ms": int((time.time() - start) * 1000),
                }
            finally:
                LLM_LATENCY_SECONDS.labels(model=self.model_name).observe(
                    time.time() - start
                )


def explain_with_fallback(code: str) -> dict:
    models = [
        settings.gemini_model,
        settings.gemini_fallback_model,
    ]

    last_error = None

    for model in filter(None, models):
        try:
            client = GeminiClient(model=model)
            return client.explain_pyspark(code)
        except LLMRateLimitError as e:
            last_error = e
            continue

    raise LLMRateLimitError(f"All models exhausted. Last error: {last_error}")
