---
name: lint
description: Run ruff linter and black formatter on backend code
model: haiku
allowed-tools: Bash
---

# Lint Skill

Run linting and formatting on the backend code.

## Instructions

1. Run ruff to check and fix linting issues
2. Run black to format code
3. Report any issues that couldn't be auto-fixed

## Commands

```bash
cd backend && ruff check app --fix && black app
```

For check-only mode (CI):
```bash
cd backend && ruff check app && black --check app
```
