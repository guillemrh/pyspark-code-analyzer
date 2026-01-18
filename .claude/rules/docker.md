---
paths:
  - "docker-compose.yml"
  - "Dockerfile"
  - "**/Dockerfile"
---

# Docker Rules

## Resource Limits
- All services must have `deploy.resources.limits` defined
- Backend/Worker: 512M-1G memory, 1.0 cpu max
- Supporting services (Redis, Prometheus): 256M memory, 0.5 cpu max

## Health Checks
- Backend must have healthcheck with interval, timeout, retries
- Frontend depends on backend with `condition: service_healthy`

## Environment Variables
- Never hardcode secrets in docker-compose.yml
- Use `${VAR}` syntax to reference from .env file
- Frontend should get `BACKEND_URL` from environment
