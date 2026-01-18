"""
PySpark Intelligence Platform - Streamlit Frontend

A professional UI for analyzing PySpark code through AST parsing,
DAG visualization, and LLM-powered explanations.
"""

import streamlit as st

from components.code_editor import render_code_editor
from components.job_status import (
    render_job_status,
    render_job_history,
    add_to_history,
)
from components.results_display import render_results, render_error
from utils.api_client import APIClient


# Page configuration
st.set_page_config(
    page_title="PySpark Intelligence Platform",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)


def init_session_state():
    """Initialize session state variables."""
    defaults = {
        "code": "",
        "job_id": None,
        "job_status": None,
        "results": None,
        "history": [],
        "api_client": APIClient(),
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def main():
    """Main application entry point."""
    init_session_state()

    # Header
    st.title("🔍 PySpark Intelligence Platform")
    st.caption(
        "Analyze your PySpark code with AST parsing, DAG visualization, "
        "and AI-powered explanations"
    )

    # Sidebar: job history
    render_job_history()

    # Main content area
    col_editor, col_spacer, col_button = st.columns([8, 1, 2])

    with col_editor:
        code = render_code_editor()

    with col_button:
        st.write("")  # Spacer
        st.write("")  # Spacer
        st.write("")  # Spacer
        explain_clicked = st.button(
            "Explain Code",
            type="primary",
            use_container_width=True,
            disabled=not code.strip(),
        )

    # Handle submission
    if explain_clicked and code.strip():
        handle_submission(code)

    # Display results
    st.divider()

    if st.session_state.results:
        render_results(st.session_state.results)
    else:
        st.info(
            "Enter your PySpark code above and click **Explain Code** to get started."
        )


def handle_submission(code: str):
    """Handle code submission and job polling."""
    api_client = st.session_state.api_client

    # Clear previous results
    st.session_state.results = None
    st.session_state.job_id = None
    st.session_state.job_status = None

    # Submit job
    with st.spinner("Submitting job..."):
        response = api_client.submit_job(code)

    if not response.success:
        st.error(f"Failed to submit job: {response.error}")
        return

    job_id = response.job_id
    st.session_state.job_id = job_id
    add_to_history(job_id, "pending")

    st.info(f"Job submitted: `{job_id}`")

    # Poll for results
    final_status = render_job_status(job_id, api_client)

    if final_status is None:
        # Cancelled
        add_to_history(job_id, "cancelled")
        return

    # Update history and state
    add_to_history(job_id, final_status.status)
    st.session_state.job_status = final_status.status

    if final_status.status == "finished":
        st.session_state.results = final_status.result
        st.success("Analysis complete!")
        st.rerun()

    elif final_status.status == "failed":
        render_error(final_status.error or {}, job_id)

    elif final_status.status == "timeout":
        st.warning(
            f"Job timed out. It may still be processing in the background. "
            f"Job ID: `{job_id}`"
        )

    elif final_status.status == "error":
        render_error(
            final_status.error or {"message": "Connection error"},
            job_id,
        )


if __name__ == "__main__":
    main()
