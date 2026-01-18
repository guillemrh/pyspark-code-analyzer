"""Graph viewer component using Graphviz."""

import streamlit as st


# Dark theme styling for Graphviz
DARK_THEME_ATTRS = '''
  bgcolor="#0E1117"
  node [fontcolor="white", color="white"]
  edge [color="#888888"]
'''


def apply_dark_theme(dot_string: str) -> str:
    """Inject dark theme attributes into a DOT string."""
    # Find the opening brace and insert dark theme attributes after it
    if "{" in dot_string:
        idx = dot_string.index("{") + 1
        return dot_string[:idx] + DARK_THEME_ATTRS + dot_string[idx:]
    return dot_string


def render_dag_graph(dot_string: str) -> None:
    """Render the Operation DAG graph using Graphviz."""
    if not dot_string:
        st.info("No DAG data available")
        return

    st.markdown("**Operation DAG** - Shows the execution flow of Spark operations")
    themed_dot = apply_dark_theme(dot_string)
    st.graphviz_chart(themed_dot, use_container_width=True)


def render_lineage_graph(dot_string: str) -> None:
    """Render the Data Lineage graph using Graphviz."""
    if not dot_string:
        st.info("No lineage data available")
        return

    st.markdown(
        "**Data Lineage** - Shows DataFrame dependencies and transformations"
    )
    themed_dot = apply_dark_theme(dot_string)
    st.graphviz_chart(themed_dot, use_container_width=True)
