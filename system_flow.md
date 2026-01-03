User (Browser)
    |
    |  PySpark code submission
    v
Streamlit Frontend
    |
    |  HTTP POST /explain/pyspark
    v
FastAPI Backend
    |
    |  Syntax validation (AST)
    |
    |  Cache key generation
    v
Redis (Cache)
    |        ^
    |        |
    |  HIT   |  MISS
    |        |
    v        |
Return      |
cached      |
response    |
             \
              \
               v
            Celery Queue
                 |
                 v
           Celery Worker
                 |
      -------------------------
      |                       |
      v                       v
DAG / Lineage Pipeline      LLM Client
(AST → DAG → DOT)           (Gemini → fallback)
      |                       |
      -----------+-------------
                  |
                  v
              Redis Cache
           (LLM + Analysis)
                  |
                  v
           Job Status Update
                  |
                  v
            FastAPI /status
                  |
                  v
          Streamlit UI renders