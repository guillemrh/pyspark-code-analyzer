"""Graph viewer component with interactive visualization using streamlit-agraph."""

import streamlit as st
from streamlit_agraph import agraph

from utils.graph_converter import (
    parse_dot_to_agraph,
    get_dag_config,
    get_lineage_config,
    COLORS,
)


def render_legend():
    """Render a color legend for the graph."""
    st.markdown(
        f"""
        <div style="display: flex; gap: 15px; flex-wrap: wrap; margin-bottom: 10px;">
            <span><span style="background-color: {COLORS['input']}; color: white; padding: 2px 8px; border-radius: 3px;">●</span> Input/Read</span>
            <span><span style="background-color: {COLORS['transformation']}; color: white; padding: 2px 8px; border-radius: 3px;">●</span> Transformation</span>
            <span><span style="background-color: {COLORS['shuffle']}; color: white; padding: 2px 8px; border-radius: 3px;">●</span> Shuffle</span>
            <span><span style="background-color: {COLORS['action']}; color: white; padding: 2px 8px; border-radius: 3px;">●</span> Action</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dag_graph(dot_string: str) -> None:
    """Render the Operation DAG graph using interactive agraph."""
    if not dot_string:
        st.info("No DAG data available")
        return

    st.markdown("**Operation DAG** - Shows the execution flow of Spark operations")
    st.caption("Drag nodes to rearrange. Scroll to zoom. Click and drag background to pan.")
    render_legend()

    try:
        nodes, edges = parse_dot_to_agraph(dot_string, graph_type="dag")
        if nodes:
            agraph(nodes=nodes, edges=edges, config=get_dag_config())
        else:
            st.warning("No nodes found in the DAG")
    except Exception as e:
        st.warning(f"Interactive graph failed, falling back to static view: {e}")
        _render_static_graph(dot_string)


def render_lineage_graph(dot_string: str) -> None:
    """Render the Data Lineage graph using interactive agraph."""
    if not dot_string:
        st.info("No lineage data available")
        return

    st.markdown(
        "**Data Lineage** - Shows DataFrame dependencies and transformations"
    )
    st.caption("Drag nodes to rearrange. Scroll to zoom. Click and drag background to pan.")

    try:
        nodes, edges = parse_dot_to_agraph(dot_string, graph_type="lineage")
        if nodes:
            agraph(nodes=nodes, edges=edges, config=get_lineage_config())
        else:
            st.warning("No nodes found in the lineage graph")
    except Exception as e:
        st.warning(f"Interactive graph failed, falling back to static view: {e}")
        _render_static_graph(dot_string)


# Dark theme styling for static Graphviz fallback
DARK_THEME_ATTRS = '''
  bgcolor="#0E1117"
  node [fontcolor="white", color="white"]
  edge [color="#888888"]
'''


def _apply_dark_theme(dot_string: str) -> str:
    """Inject dark theme attributes into a DOT string."""
    if "{" in dot_string:
        idx = dot_string.index("{") + 1
        return dot_string[:idx] + DARK_THEME_ATTRS + dot_string[idx:]
    return dot_string


def _render_static_graph(dot_string: str) -> None:
    """Fallback to static Graphviz rendering."""
    themed_dot = _apply_dark_theme(dot_string)
    st.graphviz_chart(themed_dot, use_container_width=True)
