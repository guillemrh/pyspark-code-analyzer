# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PySpark Intelligence Platform - a distributed system for analyzing PySpark code through AST parsing, DAG construction, data lineage tracking, anti-pattern detection, and LLM-assisted explanations using Google Gemini.

**Architecture**: FastAPI → Redis (cache/state/broker) → Celery workers → Streamlit UI, with OpenTelemetry tracing to Jaeger and Prometheus metrics.

## Common Commands

```bash
# Start all services
docker compose up --build

# Run tests
cd backend && pytest app/tests

# Run single test
cd backend && pytest app/tests/test_ast_parser.py::test_unary_chain

# Linting
cd backend && ruff check app --fix

# Formatting
cd backend && black app

# Check formatting (CI mode)
cd backend && black --check app
```

## Service URLs (when running locally)

- Streamlit UI: http://localhost:8501
- FastAPI: http://localhost:8050
- Prometheus: http://localhost:9090
- Jaeger: http://localhost:16686

## Architecture

### Request Flow

1. User submits PySpark code via Streamlit
2. FastAPI `/explain/pyspark` validates syntax, checks cache
3. Cache hit → return immediately; miss → enqueue Celery task
4. Worker runs `run_dag_pipeline()` for analysis, then LLM explanation
5. Results cached in Redis, UI polls `/status/{job_id}` until complete

### Core Processing Pipeline (backend/app/services/dag_pipeline.py)

1. **AST Parsing** (`parsers/ast_parser.py`) → extracts DataFrame operations
2. **Operation DAG** (`graphs/operation/operation_graph_builder.py`) → wires execution dependencies
3. **Stage Assignment** (`graphs/operation/stage_assignment.py`) → groups by shuffle boundaries
4. **Lineage Graph** (`graphs/lineage/lineage_graph_builder.py`) → tracks DataFrame dependencies
5. **Anti-patterns** (`graphs/antipatterns/registry.py`) → detects performance issues
6. **Visualization** (`visualizers/`) → Graphviz DOT output
7. **Summarization** (`services/documentation/`) → JSON + Markdown output

### Key Data Structures

- `SparkOperationNode` (parsers/dag_nodes.py): Parsed operation with df_name, operation, parents, op_type
- `OperationDAGNode` (graphs/operation/operation_graph_builder.py): DAG node with stage_id, parents/children sets
- `OpType` enum: TRANSFORMATION | ACTION
- `SHUFFLE_OPS`: {"groupBy", "join", "distinct", "repartition"}

### Parser Features (parsers/ast_parser.py)

- **60+ PySpark operations** supported (see `spark_semantics.py`)
- **spark.read.\*** patterns: parquet, csv, json, jdbc, table, etc.
- **spark.sql()** queries
- **df.write.\*** patterns with chaining
- **Variable aliasing**: tracks `alias = df` and resolves through chains
- **Nested expressions**: handles `df1.join(df2.filter(...), on="id")`

### Anti-pattern Rules (graphs/antipatterns/rules/)

Extensible via registry pattern (10 rules):
- `MultipleActionsRule` (HIGH): Multiple actions on same lineage
- `EarlyShuffleRule` (HIGH): Shuffle before necessary
- `CartesianJoinRule` (HIGH): crossJoin without filters
- `CollectInsideLoopRule` (HIGH): Multiple collect() calls
- `ActionWithoutCacheRule` (MEDIUM): Action without caching
- `RepartitionMisuseRule` (MEDIUM): Repartition before actions
- `UnpersistMissingRule` (MEDIUM): cache() without unpersist()
- `CollectOnLargeDataRule` (WARNING): collect() on potentially large data
- `UDFUsageRule` (INFO): Suggests native functions over UDFs
- `BroadcastJoinHintRule` (INFO): Suggests broadcast for small tables

## API Endpoints

- `POST /explain/pyspark`: Submit code, returns job_id
- `GET /status/{job_id}`: Poll for results
- `GET /health`: Liveness probe
- `GET /ready`: Readiness probe (checks Redis + env vars)
- `GET /metrics`: Prometheus scrape endpoint
- `GET /docs`: Interactive Swagger UI documentation
- `GET /redoc`: Alternative API documentation

## Configuration

Environment variables via `.env` (see `.env.example`):
- `GEMINI_API_KEY`: Required for LLM explanations
- `GEMINI_MODEL` / `GEMINI_FALLBACK_MODEL`: Primary and fallback models
- `REDIS_URL`: Redis connection string
- `APP_ENV`: "local" | "docker" | "prod"

Config loaded via pydantic_settings in `backend/app/config.py`.

## Development Notes

- Always use Docker + docker-compose (no devcontainers)
- Default LLM is Gemini (never OpenAI unless specified)
- Redis must be accessible before backend starts
- Celery worker concurrency is set to 1
- Local debug entry point: `backend/app/debug_run.py`

---

## Agent Routing

Delegate complex tasks to specialized subagents:

| Request Pattern | Agent | Model | Examples |
|-----------------|-------|-------|----------|
| Backend code changes | `backend` | opus | "fix the parser", "add API endpoint", "new antipattern rule" |
| Frontend/UI changes | `frontend` | sonnet | "update the UI", "fix streamlit", "change theme" |
| Docker/Infra issues | `devops` | sonnet | "fix docker", "update compose", "check logs" |

### When to Use Agents

- **Use agents** for: multi-file changes, architectural decisions, new features, debugging complex issues
- **Don't use agents** for: simple questions, single-line fixes, running commands

## Skills (Quick Commands)

| Skill | Description | Model |
|-------|-------------|-------|
| `/test` | Run backend pytest tests | haiku |
| `/lint` | Run ruff + black on backend | haiku |
| `/build` | Rebuild Docker services | haiku |
| `/review` | Review recent code changes | haiku |

### Specialized Skills

| Skill | Description |
|-------|-------------|
| `/pyspark-antipattern-rule` | Guided workflow for creating new anti-pattern detection rules |
