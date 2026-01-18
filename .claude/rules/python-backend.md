---
paths:
  - "backend/**/*.py"
---

# Python Backend Rules

## Code Style
- Use `ruff` for linting and `black` for formatting
- Run linting via Docker: `docker run --rm -v "$(pwd)/backend:/app" -w /app python:3.10-slim bash -c "pip install ruff black && ruff check app && black --check app"`
- Type hints are encouraged but not mandatory

## Testing
- Tests live in `backend/app/tests/`
- Run tests via Docker: `docker run --rm -v "$(pwd)/backend:/app" -w /app -e GEMINI_API_KEY=test -e GEMINI_MODEL=test -e REDIS_URL=redis://localhost:6379/0 python:3.10-slim bash -c "pip install -r requirements.txt && pytest app/tests -v"`

## Error Handling
- Use exceptions from `backend/app/errors.py` for standardized errors
- LLM errors should use `LLMRateLimitError` from `services/llm.py`

## Configuration
- All config via environment variables through `backend/app/config.py`
- Required: `GEMINI_API_KEY`, `GEMINI_MODEL`, `REDIS_URL`
