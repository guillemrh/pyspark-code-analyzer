from prometheus_client import Counter, Histogram

# -------------------
# HTTP / API metrics
# -------------------

HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

HTTP_REQUEST_LATENCY = Histogram(
    "http_request_latency_seconds",
    "HTTP request latency",
    ["endpoint"],
)

# -------------------
# Job / Celery metrics
# -------------------

JOBS_STARTED_TOTAL = Counter(
    "jobs_started_total",
    "Total background jobs started",
)

JOBS_FAILED_TOTAL = Counter(
    "jobs_failed_total",
    "Total background jobs failed",
)

JOB_DURATION_SECONDS = Histogram(
    "job_duration_seconds",
    "Background job duration",
)

# -------------------
# LLM metrics
# -------------------

LLM_CALLS_TOTAL = Counter(
    "llm_calls_total",
    "Total LLM calls",
    ["model"],
)

LLM_RATE_LIMIT_TOTAL = Counter(
    "llm_rate_limit_total",
    "LLM rate limit events",
    ["model"],
)

LLM_LATENCY_SECONDS = Histogram(
    "llm_latency_seconds",
    "LLM call latency",
    ["model"],
)

# -------------------
# Cache metrics
# -------------------

CACHE_HIT_TOTAL = Counter(
    "cache_hit_total",
    "Cache hits",
)

CACHE_MISS_TOTAL = Counter(
    "cache_miss_total",
    "Cache misses",
)
