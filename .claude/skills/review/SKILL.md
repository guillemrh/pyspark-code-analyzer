---
name: review
description: Review recent code changes
model: haiku
allowed-tools: Bash, Read, Glob, Grep
---

# Review Skill

Review recent code changes for quality and correctness.

## Instructions

1. Check git status and recent changes
2. Review the diff for issues
3. Provide a summary with any concerns

## Commands

```bash
# See what changed
git status
git diff --stat

# See recent commits
git log --oneline -5

# See full diff
git diff
```

## Review Checklist

- [ ] Code passes linting (ruff, black)
- [ ] No hardcoded secrets or config
- [ ] Error handling is present
- [ ] Changes are focused (not mixing concerns)

## Output Format

```markdown
## Changes Reviewed
[List of files changed]

## Summary
[Brief description]

## Issues Found
- [Issue 1]
- [Issue 2]

## Recommendation
[LGTM | Needs fixes]
```
