"""Graph viewer component using Graphviz."""

import streamlit as st

from utils.graph_converter import get_graph_legend


def render_legend() -> None:
    """Render the color legend for the graph (always visible)."""
    legend = get_graph_legend()

    st.markdown("**Legend:**")
    cols = st.columns(len(legend))
    for col, (label, color) in zip(cols, legend.items()):
        with col:
            st.markdown(
                f'<div style="display: flex; align-items: center;">'
                f'<span style="background-color: {color}; '
                f'width: 14px; height: 14px; border-radius: 50%; '
                f'display: inline-block; margin-right: 6px;"></span>'
                f'<span style="font-size: 0.8em;">{label}</span>'
                f"</div>",
                unsafe_allow_html=True,
            )


def render_dag_graph(dot_string: str) -> None:
    """Render the Operation DAG graph using Graphviz."""
    if not dot_string:
        st.info("No DAG data available")
        return

    st.markdown("**Operation DAG** - Shows the execution flow of Spark operations")
    render_legend()
    st.graphviz_chart(dot_string, use_container_width=True)


def render_lineage_graph(dot_string: str) -> None:
    """Render the Data Lineage graph using Graphviz."""
    if not dot_string:
        st.info("No lineage data available")
        return

    st.markdown(
        "**Data Lineage** - Shows DataFrame dependencies and transformations"
    )
    st.graphviz_chart(dot_string, use_container_width=True)
