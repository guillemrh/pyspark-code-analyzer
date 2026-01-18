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
        st.warning("Could not parse graph data. Showing raw DOT:")
        st.graphviz_chart(dot_string)
        return

    # Render legend first (always visible)
    render_legend()

    # Configure graph layout
    if graph_type == "dag":
        config = Config(
            width=800,
            height=500,
            directed=True,
            physics=False,  # Disable physics for stable layout
            hierarchical=True,
            levelSeparation=100,
            nodeSpacing=150,
            treeSpacing=200,
            blockShifting=True,
            edgeMinimization=True,
            parentCentralization=True,
            direction="UD",  # Up-Down direction
            sortMethod="directed",
            nodeHighlightBehavior=True,
            highlightColor="#FF6B6B",
            collapsible=False,
        )
    else:  # lineage
        config = Config(
            width=800,
            height=400,
            directed=True,
            physics=False,  # Disable physics for stable layout
            hierarchical=True,
            levelSeparation=80,
            nodeSpacing=120,
            direction="LR",  # Left-Right for lineage
            nodeHighlightBehavior=True,
            highlightColor="#FF6B6B",
            collapsible=False,
        )

    # Render the graph
    try:
        agraph(nodes=nodes, edges=edges, config=config)
    except Exception as e:
        st.error(f"Error rendering interactive graph: {e}")
        # Fallback to graphviz
        st.graphviz_chart(dot_string)


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
    """Render the Operation DAG graph."""
    st.markdown("**Operation DAG** - Shows the execution flow of Spark operations")
    render_graph(dot_string, graph_type="dag")


def render_lineage_graph(dot_string: str) -> None:
    """Render the Data Lineage graph."""
    st.markdown(
        "**Data Lineage** - Shows DataFrame dependencies and transformations"
    )
    render_graph(dot_string, graph_type="lineage")
