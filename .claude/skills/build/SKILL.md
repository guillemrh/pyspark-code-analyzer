---
name: build
description: Build and restart Docker services
model: haiku
allowed-tools: Bash
---

# Build Skill

Build and restart Docker Compose services.

## Instructions

1. Build the specified service(s) or all if none specified
2. Restart the service(s)
3. Verify they're running

## Commands

Build all:
```bash
docker compose build && docker compose up -d
```

Build specific service:
```bash
docker compose build [service] && docker compose up -d [service]
```

Check status:
```bash
docker compose ps
```

## Services

- `backend` - FastAPI server
- `worker` - Celery worker
- `frontend` - Streamlit UI
- `redis` - Cache/broker
- `prometheus` - Metrics
- `jaeger` - Tracing
