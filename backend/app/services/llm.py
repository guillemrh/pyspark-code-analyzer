import time
import google.generativeai as genai
from app.config import settings


class LLMRateLimitError(Exception):
    """Raised when the LLM hits a rate or quota limit."""
    pass

class GeminiClient:
    def __init__(self, model: str | None = None):
        genai.configure(api_key=settings.gemini_api_key)
        self.model_name = model or settings.gemini_model
        self.model = genai.GenerativeModel(self.model_name)


    def explain_pyspark(self, code: str) -> dict:
        start = time.time()
        try:
            
            prompt = f"""
            Explain the following PySpark code:\n\n{code}
            Be concise and clear in your explanation, don't expand too much. 
            Talk about the tradeoffs if any.
            Add a paragraph at the end with suggestions to improve the code performance.
            """

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
                "explanation": explanation,
                "tokens_used": tokens_used,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "latency_ms": latency_ms
            }
        except Exception as e:
            msg = str(e).lower()

            if "quota" in msg or "rate" in msg or "429" in msg:
                raise LLMRateLimitError(str(e))

            return {
                "error": str(e),
                "latency_ms": int((time.time() - start) * 1000),
            }
