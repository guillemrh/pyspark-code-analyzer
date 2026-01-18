"""Interactive graph viewer component using streamlit-agraph."""

import streamlit as st
from streamlit_agraph import agraph, Config

from utils.graph_converter import dot_to_agraph, get_graph_legend


def render_graph(dot_string: str, graph_type: str = "dag") -> None:
    """
    Render an interactive graph from DOT format.

    Args:
        dot_string: DOT format graph string
        graph_type: Type of graph ("dag" or "lineage")
    """
    if not dot_string:
        st.info("No graph data available")
        return

    # Convert DOT to agraph format
    nodes, edges = dot_to_agraph(dot_string)

    if not nodes:
        st.warning("Could not parse graph data")
        return

    # Configure graph layout
    if graph_type == "dag":
        config = Config(
            width="100%",
            height=500,
            directed=True,
            physics=True,
            hierarchical=True,
            hierarchicalType="directed",
            sortMethod="directed",
            nodeHighlightBehavior=True,
            highlightColor="#F7F7F7",
            collapsible=False,
            node={
                "labelProperty": "label",
                "renderLabel": True,
            },
            link={
                "highlightColor": "#FF6B6B",
            },
        )
    else:  # lineage
        config = Config(
            width="100%",
            height=400,
            directed=True,
            physics=True,
            hierarchical=False,
            nodeHighlightBehavior=True,
            highlightColor="#F7F7F7",
            collapsible=False,
        )

    # Render the graph
    try:
        agraph(nodes=nodes, edges=edges, config=config)
    except Exception as e:
        st.error(f"Error rendering graph: {e}")
        # Fallback to graphviz
        st.graphviz_chart(dot_string)

    # Render legend
    render_legend()


def render_legend() -> None:
    """Render the color legend for the graph."""
    legend = get_graph_legend()

    with st.expander("Legend", expanded=False):
        cols = st.columns(len(legend))
        for col, (label, color) in zip(cols, legend.items()):
            with col:
                st.markdown(
                    f'<div style="display: flex; align-items: center;">'
                    f'<span style="background-color: {color}; '
                    f'width: 16px; height: 16px; border-radius: 50%; '
                    f'display: inline-block; margin-right: 8px;"></span>'
                    f'<span style="font-size: 0.85em;">{label}</span>'
                    f"</div>",
                    unsafe_allow_html=True,
                )


def render_dag_graph(dot_string: str) -> None:
    """Render the Operation DAG graph."""
    st.markdown("**Operation DAG** - Shows the execution flow of Spark operations")
    render_graph(dot_string, graph_type="dag")


def render_lineage_graph(dot_string: str) -> None:
    """Render the Data Lineage graph."""
    st.markdown(
        "**Data Lineage** - Shows DataFrame dependencies and transformations"
    )
    render_graph(dot_string, graph_type="lineage")
