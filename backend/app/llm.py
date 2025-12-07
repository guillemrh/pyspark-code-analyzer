import time
from google.generativeai import Client
from .config import settings

class GeminiClient:
    def __init__(self):
        self.client = Client(api_key=settings.gemini_api_key)

    def explain_pyspark(self, code: str) -> dict:
        start = time.time()
        try:
            response = self.client.chat(
                model="gemini-2.5-flash-lite",
                messages=[{"role": "user", "content": f"Explain this PySpark code:\n{code}"}],
                timeout=settings.timeout_seconds
            )
            explanation = response.last or "No explanation generated"
            latency_ms = int((time.time() - start) * 1000)
            tokens_used = response.metadata.get("token_count", 0)
            
            return {
                "explanation": explanation,
                "tokens_used": tokens_used,
                "latency_ms": latency_ms
            }
        except Exception as e:
            return {"error": str(e), "latency_ms": int((time.time() - start) * 1000)}
