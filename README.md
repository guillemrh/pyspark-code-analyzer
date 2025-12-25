## PySpark Code Explainer

A lightweight, containerized AI system for analyzing and explaining PySpark code, designed to evolve into a full **Spark ETL intelligence layer**.

---

## Overview

This project provides a web interface where users can submit PySpark code and receive structured explanations and execution metadata.

The system is built with scalability and observability in mind, separating **request handling**, **background execution**, and **analysis logic**.

### Architecture Components

| Component | Responsibility |
|--------|----------------|
| **FastAPI Backend** | API endpoints, job orchestration, validation |
| **Redis** | Caching, job state storage, rate limiting |
| **Celery Workers** | Background execution of long-running tasks |
| **LLM (Gemini)** | Natural language explanation layer |
| **Streamlit Frontend** | User interface |
| **Docker Compose** | Local orchestration |

---

## 🧱 Project Structure
```text
.
├── backend/
│   ├── app/
│   │   ├── main.py                 # FastAPI app initialization and lifecycle
│   │   ├── api/
│   │   │   ├── routes.py           # /explain/pyspark and /status/{job_id} endpoints
│   │   │   └── schemas.py          # Request/response Pydantic models
│   │   ├── services/
│   │   │   ├── llm_service.py      # Gemini LLM client abstraction
│   │   │   ├── dag_service.py      # OUTDATED
│   │   │   ├── dag_pipeline.py     
│   │   │   ├── operation_dag_builder.py
│   │   │   ├── stage_builder.py
│   │   │   └── cache_service.py    # Redis helpers for LLM and DAG caching
│   │   ├── parsers/
│   │   │   ├── ast_parser.py       # AST parsing logic (visitor pattern)
│   │   │   ├── spark_semantics.py      
│   │   │   └── dag_nodes.py        # DAGNode and ASTNode classes
│   │   ├── visualizers/
│   │   │   └── dag_visualizer.py   # Graphviz DAG rendering
│   │   ├── tests/
│   │   ├── config.py               # Environment-based settings (Pydantic)
│   │   ├── debug_run.py            
│   │   ├── workers/
│   │   │   └── tasks.py            # Celery background tasks (LLM execution & DAG processing)
│   │   └── rate_limit.py           # API rate limiting logic
│   ├── Dockerfile
│   ├── requirements.txt
│   └── README.md
├── frontend/
│   ├── streamlit_app.py            # Streamlit UI
│   ├── Dockerfile
│   └── README.md
├── docker-compose.yml               # Multi-service orchestration
└── README.md                        # Project-level documentation
```

---

## Running the Project

### Prerequisites

- **Docker**
- **Docker Compose**
- **Gemini API key**

### Environment Configuration

Create a `.env` file in the backend directory with:

- **GEMINI_API_KEY** — Gemini API key

### Start the Application

- Run `docker compose up --build`
- Open **http://localhost:8501** in your browser

---

## API

### POST /explain/pyspark

Submits PySpark code for analysis.  
If the same code was previously analyzed, the cached result is reused.

**Request Fields**

| Field | Type | Description |
|-----|-----|-------------|
| **code** | string | PySpark source code |

**Response Fields**

| Field | Description |
|-----|-------------|
| **job_id** | Unique identifier for the background job |
| **status** | `pending`, `finished`, or `failed` |
| **cached** | Whether the result came from cache |

---

### GET /status/{job_id}

Returns the job result when background processing is complete.

---

## Technology Stack

| Category | Tools |
|------|------|
| **Backend** | FastAPI, Pydantic |
| **Async Processing** | Celery |
| **Caching & State** | Redis |
| **Frontend** | Streamlit |
| **LLM** | Gemini |
| **Infrastructure** | Docker, Docker Compose |

---

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

### 🟥 Stage 4 — Production Deployment

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