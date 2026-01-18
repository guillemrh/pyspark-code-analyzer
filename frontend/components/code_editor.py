"""Code editor component with syntax highlighting and examples."""

import streamlit as st
from streamlit_ace import st_ace

from utils.examples import EXAMPLES, DEFAULT_EXAMPLE


def render_sidebar_examples():
    """Render the examples section in the sidebar (call inside sidebar context)."""
    st.subheader("Examples")
    example_names = list(EXAMPLES.keys())
    selected_example = st.selectbox(
        "Load example",
        options=["-- Select --"] + example_names,
        key="example_selector",
        help="Select an example to load into the editor",
    )

    if selected_example != "-- Select --":
        if st.button("Load Example", type="primary", use_container_width=True):
            st.session_state.code = EXAMPLES[selected_example]
            st.session_state.editor_key += 1  # Force editor refresh
            st.rerun()

    st.divider()

    # Clear button
    if st.button("Clear Editor", use_container_width=True):
        st.session_state.code = ""
        st.session_state.editor_key += 1  # Force editor refresh
        st.rerun()


def render_code_editor() -> str:
    """
    Render the code editor.

    Returns:
        The current code in the editor
    """
    # Initialize session state
    if "code" not in st.session_state:
        st.session_state.code = EXAMPLES[DEFAULT_EXAMPLE]
    if "editor_key" not in st.session_state:
        st.session_state.editor_key = 0

    # Main code editor
    st.subheader("PySpark Code")

    # Use dynamic key to force re-render when loading examples
    code = st_ace(
        value=st.session_state.code,
        language="python",
        theme="monokai",  # Dark theme
        font_size=14,
        tab_size=4,
        show_gutter=True,
        show_print_margin=False,
        wrap=True,
        auto_update=True,
        min_lines=15,
        max_lines=30,
        key=f"ace_editor_{st.session_state.editor_key}",
        placeholder="Enter your PySpark code here...",
    )

    # Update session state
    if code != st.session_state.code:
        st.session_state.code = code

    return code or ""
