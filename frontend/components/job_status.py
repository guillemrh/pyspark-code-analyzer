"""Job status and progress display component."""

import streamlit as st
import time

from utils.api_client import APIClient, JobStatus


# Progress stages with their weights (0-100)
STAGES = {
    "pending": {"label": "Queued", "progress": 10},
    "running": {"label": "Analyzing", "progress": 40},
    "analysis_complete": {"label": "Generating Explanation", "progress": 70},
    "finished": {"label": "Complete", "progress": 100},
    "failed": {"label": "Failed", "progress": 100},
    "error": {"label": "Error", "progress": 100},
    "timeout": {"label": "Timed Out", "progress": 100},
}


def get_progress_info(status: str) -> tuple[int, str]:
    """Get progress percentage and label for a status."""
    info = STAGES.get(status, {"label": "Processing", "progress": 50})
    return info["progress"], info["label"]


def render_job_status(job_id: str, api_client: APIClient) -> JobStatus | None:
    """
    Render job status with progress bar and poll until completion.

    Args:
        job_id: The job ID to track
        api_client: API client instance

    Returns:
        Final JobStatus when complete, or None if cancelled
    """
    status_container = st.container()
    progress_bar = st.progress(0)
    status_text = st.empty()
    time_text = st.empty()
    cancel_col, _ = st.columns([1, 4])

    # Initialize tracking
    start_time = time.time()
    cancelled = False

    with cancel_col:
        if st.button("Cancel", type="secondary", key="cancel_job"):
            cancelled = True

    if cancelled:
        progress_bar.empty()
        status_text.empty()
        time_text.empty()
        st.warning("Job cancelled by user")
        return None

    # Poll for status
    last_status = None
    while True:
        status = api_client.get_status(job_id)
        elapsed = time.time() - start_time

        # Update progress
        progress, label = get_progress_info(status.status)
        progress_bar.progress(progress / 100)
        status_text.markdown(f"**Status:** {label}")
        time_text.caption(f"Elapsed: {elapsed:.1f}s")

        # Check for completion
        if status.status in ("finished", "failed", "error", "timeout"):
            progress_bar.empty()
            status_text.empty()
            time_text.empty()
            return status

        # Update tracking
        if status.status != last_status:
            last_status = status.status

        time.sleep(1.0)


def render_job_history():
    """Render recent job history (call this inside a sidebar context)."""
    if "history" not in st.session_state:
        st.session_state.history = []

    st.subheader("Recent Jobs")
    if not st.session_state.history:
        st.caption("No jobs yet")
    else:
        for job in st.session_state.history[-5:]:
            status_emoji = {
                "finished": "✅",
                "failed": "❌",
                "running": "🔄",
                "pending": "⏳",
            }.get(job.get("status", ""), "❓")

            st.caption(f"{status_emoji} {job.get('job_id', 'Unknown')[:8]}...")


def add_to_history(job_id: str, status: str):
    """Add a job to the history."""
    if "history" not in st.session_state:
        st.session_state.history = []

    # Update existing or add new
    for job in st.session_state.history:
        if job.get("job_id") == job_id:
            job["status"] = status
            return

    st.session_state.history.append({"job_id": job_id, "status": status})

    # Keep only last 10
    if len(st.session_state.history) > 10:
        st.session_state.history = st.session_state.history[-10:]
