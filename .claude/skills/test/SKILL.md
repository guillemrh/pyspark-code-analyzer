---
name: test
description: Run backend pytest tests
model: haiku
allowed-tools: Bash
---

# Test Skill

Run the backend test suite.

## Instructions

1. Run pytest on the backend tests
2. Report results clearly
3. If tests fail, summarize the failures

## Command

```bash
cd backend && pytest app/tests -v
```

If a specific test file or pattern is mentioned, use:
```bash
cd backend && pytest app/tests/[pattern] -v
```
