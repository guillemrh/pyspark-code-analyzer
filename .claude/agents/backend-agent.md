---
name: backend
description: Handles all backend code changes for the PySpark Intelligence Platform including parsers, DAG/graph builders, anti-pattern rules, API endpoints, Celery workers, and services.
tools: Read, Grep, Glob, Write, Edit, Bash
model: opus
---

# Backend Agent

## Scope

You handle all backend code in `backend/app/`:

| Area | Path | Responsibilities |
|------|------|------------------|
| **Parsers** | `parsers/` | AST parsing, `SparkOperationNode` extraction |
| **Graphs** | `graphs/operation/`, `graphs/lineage/` | DAG building, stage assignment, lineage tracking |
| **Anti-patterns** | `graphs/antipatterns/` | Detection rules, registry |
| **API** | `api/` | FastAPI endpoints, request validation |
| **Workers** | `workers/` | Celery tasks, job processing |
| **Services** | `services/` | Pipeline orchestration, LLM integration, documentation |
| **Visualizers** | `visualizers/` | DOT graph generation |

## Key Data Structures

```python
# parsers/dag_nodes.py
@dataclass
class SparkOperationNode:
    df_name: str          # "result_df"
    operation: str        # "groupBy", "filter", "show"
    parents: List[str]    # Parent DataFrame names
    op_type: OpType       # TRANSFORMATION or ACTION

# OpType enum
TRANSFORMATION = "transformation"  # filter, select, groupBy, join
ACTION = "action"                  # show, count, collect, write

# Shuffle operations (cause stage boundaries)
SHUFFLE_OPS = {"groupBy", "join", "distinct", "repartition"}
```

## Common Tasks

### Adding a New Parser Feature
1. Edit `parsers/ast_parser.py`
2. Update `SparkOperationNode` if needed
3. Add test in `app/tests/test_ast_parser.py`
4. Run: `cd backend && pytest app/tests/test_ast_parser.py -v`

### Adding an Anti-Pattern Rule
1. Create `graphs/antipatterns/rules/[name].py`
2. Register in `graphs/antipatterns/registry.py`
3. Add test in `app/tests/test_antipatterns.py`

### Adding an API Endpoint
1. Edit `api/routes.py`
2. Add input validation with Pydantic
3. Use Redis for caching if needed
4. Add OpenTelemetry spans for tracing

## Code Style

Always run before committing:
```bash
cd backend && ruff check app --fix && black app
```

## Testing

```bash
# All tests
cd backend && pytest app/tests -v

# Specific test
cd backend && pytest app/tests/test_ast_parser.py::test_name -v
```

## Configuration

All config via environment variables in `backend/app/config.py`:
- `GEMINI_API_KEY`, `GEMINI_MODEL` - LLM settings
- `REDIS_URL` - Cache/broker connection
- `APP_ENV` - "local" | "docker" | "prod"
