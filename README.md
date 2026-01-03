# PySpark Intelligence Platform

A distributed, observable system for **static analysis, execution graph extraction, lineage tracking, and LLM-assisted explanation of PySpark code**.

This project goes beyond simple code explanation and evolves into a **Spark ETL intelligence layer** capable of understanding transformations, execution stages, data lineage, and performance anti-patterns — all exposed through a production-grade architecture.

---

## Overview

This system allows users to submit PySpark code and receive:

- Natural-language explanations (LLM-powered)
- Logical operation DAGs
- Stage-level execution summaries
- Data lineage graphs
- Anti-pattern and performance insights
- Cached and versioned analysis artifacts

The platform is designed with **scalability, observability, and fault tolerance** in mind, separating real-time request handling from heavy analysis workloads.

---

## High-Level Architecture

The system follows a **request → cache → async execution → aggregation** model:

- FastAPI handles validation, orchestration, and status tracking
- Redis provides caching, rate limiting, and job state
- Celery workers execute CPU- and LLM-heavy tasks
- Streamlit provides an interactive UI
- Prometheus and structured logs provide observability

---

## Core Capabilities

- 🔍 Static analysis of PySpark code via AST parsing
- 🧠 Logical DAG construction (transformations & actions)
- 🧬 Data lineage graph generation
- ⚠️ Detection of Spark performance anti-patterns
- 🤖 LLM-powered explanations with fallback models
- ♻️ Redis-backed caching for LLM and analysis results
- 📊 Structured logging and metrics (production-ready)
- 🧵 Asynchronous execution with Celery workers

---

## 🧱 Project Structure

```text
.
├── backend/
│   ├── app/
│   │   ├── main.py                 # FastAPI app initialization and lifecycle
│   │   ├── api/
│   │   │   ├── routes.py           # API endpoints (/explain, /status, /health)
│   │   │   └── schemas.py          # Request/response Pydantic models
│   │   ├── services/
│   │   │   ├── llm.py              # LLM abstraction (Gemini + fallback logic)
│   │   │   ├── dag_pipeline.py     # End-to-end DAG & lineage construction
│   │   │   ├── cache.py            # Redis helpers (LLM + analysis caching)
│   │   │   ├── dag_service_deprecated.py # Legacy DAG service (for reference)
│   │   │   └── documentation/      # Summarization logic for various components
│   │   │       ├── stage_summary.py
│   │   │       ├── lineage_summary.py
│   │   │       ├── dag_summary.py
│   │   │       └── antipattern_summary.py
│   │   ├── parsers/
│   │   │   ├── ast_parser.py       # AST parsing logic
│   │   │   ├── spark_semantics.py  # Spark-specific semantics
│   │   │   └── dag_nodes.py        # DAGNode and ASTNode definitions
│   │   ├── graphs/                 # Core graph construction and pattern logic
│   │   │   ├── antipatterns/       # Spark performance anti-pattern detection
│   │   │   │   ├── registry.py
│   │   │   │   ├── base.py
│   │   │   │   └── rules/
│   │   │   │       ├── multiple_actions.py
│   │   │   │       ├── repartition_misuse.py
│   │   │   │       ├── action_without_cache.py
│   │   │   │       └── early_shuffle.py
│   │   │   ├── lineage/
│   │   │   │   └── lineage_graph_builder.py
│   │   │   └── operation/
│   │   │       ├── operation_graph_builder.py
│   │   │       └── stage_assignment.py
│   │   ├── visualizers/
│   │   │   ├── lineage_graph_visualizer.py   # DOT rendering for lineage
│   │   │   └── operation_graph_visualizer.py # DOT rendering for operations
│   │   ├── workers/
│   │   │   └── tasks.py            # Celery background tasks
│   │   ├── tests/                  # Unit and integration tests
│   │   │   ├── test_ast_parser.py
│   │   │   ├── test_dag_visualizer.py
│   │   │   └── test_dag_builder.py
│   │   ├── rate_limit.py           # API rate limiting
│   │   ├── config.py               # Environment-based configuration
│   │   ├── logging.py              # Centralized logging configuration
│   │   └── debug_run.py            # Local debugging entry point
│   ├── Dockerfile
│   ├── requirements.txt
│   └── README.md
├── frontend/
│   ├── streamlit_app.py            # Streamlit UI
│   ├── Dockerfile
│   └── README.md
├── docker-compose.yml              # Multi-service orchestration
└── README.md                       # Project-level documentation
```

---

## Running the Project

### Prerequisites

- Docker
- Docker Compose
- Gemini API key

### Environment Configuration

Create a `.env` file in `backend/` with:

- `GEMINI_API_KEY`
- `GEMINI_MODEL`
- `GEMINI_FALLBACK_MODEL`
- `REDIS_URL`

---

### Start the Application

- `docker compose up --build`
- Streamlit UI: http://localhost:8501
- FastAPI backend: http://localhost:8000
- Prometheus metrics: http://localhost:8000/metrics

---

## API Overview

### POST /explain/pyspark

Submits PySpark code for analysis.

- Performs syntax validation
- Checks Redis cache
- Enqueues Celery job if needed

### GET /status/{job_id}

Returns job status and results, including:

- LLM explanation
- DAG and lineage graphs
- Stage summaries
- Anti-pattern detection

---

## Observability

### Logging
- Structured JSON logs
- Correlation via job_id
- Separate logs for API, workers, and cache

### Metrics
- HTTP request rates & latency
- LLM latency and rate-limit events
- Cache hit/miss ratios
- Celery job duration and failures

### Tracing (planned)
- End-to-end request tracing via OpenTelemetry

---

## Technology Stack

| Layer | Tools |
|----|----|
| API | FastAPI, Pydantic |
| Async | Celery |
| Cache & State | Redis |
| Frontend | Streamlit |
| LLM | Gemini (with fallback models) |
| Observability | Structured logs, Prometheus |
| Infra | Docker, Docker Compose |


## Project Roadmap

This project is structured as a multi-stage system that grows into a **Spark ETL intelligence platform**.

### 🟦 Stage 1 — Core Functionality

- PySpark code submission
- LLM-based explanation
- Structured API responses
- Basic UI

### 🟩 Stage 2 — Distributed Architecture

- Redis caching
- Background workers
- Job status API
- Rate limiting
- Fault-tolerant execution

### 🟧 Stage 3 — ETL + Spark Intelligence Layer

- Parse PySpark code into a logical DAG
- Detect transformations and actions
- Identify shuffles and wide dependencies
- Detect performance anti-patterns
- Auto-generate documentation
- Build data lineage graphs

### 🟨 Stage 4 — System Integration & UX

- Wire DAG + lineage + antipatterns into Celery
- Job lifecycle & status tracking
- Frontend graph rendering
- Streaming results / progressive explanation
- Failure handling
- Versioned analysis artifacts

### 🟥 Stage 5 — Production Deployment

- Production Docker builds
- Structured logging
- Prometheus metrics
- OpenTelemetry tracing
- CI/CD pipelines
- Deployment-ready configuration

---

## Future Improvements

- Visual DAG rendering
- Multi-file project analysis
- Version comparison
- Interactive lineage graphs
- Performance recommendations

---

## 📜 License

MIT