import time
import google.generativeai as genai
from app.config import settings

class GeminiClient:
    def __init__(self):
        genai.configure(api_key=settings.gemini_api_key)
        self.model = genai.GenerativeModel("gemini-2.5-flash-lite")


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
            return {
                "error": str(e), 
                "latency_ms": int((time.time() - start) * 1000)
                }
