---
paths:
  - "frontend/**/*.py"
  - "frontend/.streamlit/*"
---

# Streamlit Frontend Rules

## Component Structure
- All reusable components go in `frontend/components/`
- Export functions in `components/__init__.py`
- Utility functions go in `frontend/utils/`

## Session State
- Initialize all state in `init_session_state()` in `streamlit_app.py`
- Use descriptive keys: `st.session_state.job_id`, not `st.session_state.jid`

## Styling
- Dark theme is default - use colors that contrast with `#0E1117` background
- Primary color: `#6C5CE7` (purple)
- Text color: `#FAFAFA` (white)

## Streamlit-Ace Editor
- Always use dynamic key to force re-renders: `key=f"editor_{st.session_state.editor_key}"`
- Dark theme: `theme="monokai"`

## Graphviz Charts
- Inject dark theme attributes into DOT strings before rendering
- Use `st.graphviz_chart(dot, use_container_width=True)`

## Dependencies
- Keep `frontend/requirements.txt` minimal
- Current deps: streamlit, streamlit-ace, requests
