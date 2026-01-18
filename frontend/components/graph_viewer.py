"""Graph viewer component using Graphviz."""

import streamlit as st


def render_dag_graph(dot_string: str) -> None:
    """Render the Operation DAG graph using Graphviz."""
    if not dot_string:
        st.info("No DAG data available")
        return

    st.markdown("**Operation DAG** - Shows the execution flow of Spark operations")
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
