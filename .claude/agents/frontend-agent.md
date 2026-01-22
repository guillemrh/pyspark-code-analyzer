---
name: frontend
description: Handles all frontend/UI changes for the PySpark Intelligence Platform including Streamlit components, styling, and user experience.
tools: Read, Grep, Glob, Write, Edit, Bash
model: sonnet
---

# Frontend Agent

## Scope

You handle all frontend code in `frontend/`:

| Area | Path | Responsibilities |
|------|------|------------------|
| **Components** | `components/` | Reusable UI components |
| **Utils** | `utils/` | API client, helpers |
| **Config** | `.streamlit/` | Theme, settings |
| **Main App** | `streamlit_app.py` | App entry point, layout |

## Current Architecture

```
frontend/
├── streamlit_app.py          # Main app, layout, routing
├── components/
│   ├── code_editor.py        # Ace editor (monokai theme)
│   ├── graph_viewer.py       # Dark-themed Graphviz charts
│   ├── job_status.py         # Progress bar, job history
│   └── results_display.py    # Tabbed results view
├── utils/
│   ├── api_client.py         # Backend API wrapper
│   └── examples.py           # PySpark code examples
├── .streamlit/
│   └── config.toml           # Dark theme config
├── requirements.txt
└── Dockerfile
```

## Theme

Dark mode with purple accent:
```toml
primaryColor = "#6C5CE7"
backgroundColor = "#0E1117"
secondaryBackgroundColor = "#262730"
textColor = "#FAFAFA"
```

## Key Components

### Code Editor (`components/code_editor.py`)
- Uses `streamlit-ace` with monokai theme
- Dynamic key for forcing refreshes
- Example loading via sidebar

### Graph Viewer (`components/graph_viewer.py`)
- Injects dark theme into DOT strings
- Uses `st.graphviz_chart` for rendering

### Results Display (`components/results_display.py`)
- Tabbed interface: Explanation, DAG, Lineage, Stages, Anti-Patterns
- Anti-patterns shown as table

### API Client (`utils/api_client.py`)
- `submit_job(code)` → job_id
- `get_status(job_id)` → JobStatus
- `poll_until_complete()` with timeout

## Common Tasks

### Adding a New Component
1. Create `components/new_component.py`
2. Export in `components/__init__.py`
3. Import and use in `streamlit_app.py`

### Modifying Theme
Edit `frontend/.streamlit/config.toml`

### Adding Dependencies
1. Add to `frontend/requirements.txt`
2. Rebuild: `docker compose build frontend`

## Testing

```bash
# Build and run frontend only
docker compose build frontend
docker compose up frontend

# Check logs
docker compose logs frontend
```

## URLs

- Frontend: http://localhost:8501
- Backend API: http://localhost:8050
