"""Graph viewer component using Graphviz for reliable DAG rendering."""

import streamlit as st

# Color scheme
COLORS = {
    "shuffle": "#C0392B",
    "action": "#E67E22",
    "transformation": "#3498DB",
    "input": "#27AE60",
    "default": "#7F8C8D",
}


def render_dag_legend():
    """Render a color legend for the DAG graph."""
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


def render_lineage_legend():
    """Render a color legend for the lineage graph."""
    st.markdown(
        f"""
        <div style="display: flex; gap: 15px; flex-wrap: wrap; margin-bottom: 10px;">
            <span><span style="background-color: {COLORS['input']}; color: white; padding: 2px 8px; border-radius: 3px;">●</span> Source</span>
            <span><span style="background-color: {COLORS['transformation']}; color: white; padding: 2px 8px; border-radius: 3px;">●</span> Intermediate</span>
            <span><span style="background-color: {COLORS['shuffle']}; color: white; padding: 2px 8px; border-radius: 3px;">●</span> Join</span>
            <span><span style="background-color: {COLORS['action']}; color: white; padding: 2px 8px; border-radius: 3px;">●</span> Output</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dag_graph(dot_string: str) -> None:
    """Render the Operation DAG graph using Graphviz."""
    if not dot_string:
        st.info("No DAG data available")
        return

    st.markdown("**Operation DAG** - Shows the execution flow of Spark operations")
    render_dag_legend()

    styled_dot = _style_dag_dot(dot_string)
    st.graphviz_chart(styled_dot, use_container_width=True)


def render_lineage_graph(dot_string: str) -> None:
    """Render the Data Lineage graph using Graphviz."""
    if not dot_string:
        st.info("No lineage data available")
        return

    st.markdown("**Data Lineage** - Shows DataFrame dependencies")
    render_lineage_legend()

    styled_dot = _style_lineage_dot(dot_string)
    st.graphviz_chart(styled_dot, use_container_width=True)


def _style_dag_dot(dot_string: str) -> str:
    """Apply dark theme styling to DAG DOT string."""
    dark_theme = '''
  bgcolor="#0E1117"
  node [fontcolor="white", color="white", style="filled", fillcolor="#3498DB"]
  edge [color="#888888", penwidth=1.5]
'''
    if "{" in dot_string:
        idx = dot_string.index("{") + 1
        return dot_string[:idx] + dark_theme + dot_string[idx:]
    return dot_string


def _style_lineage_dot(dot_string: str) -> str:
    """Apply styling to lineage DOT string with colored nodes based on position."""
    import re

    # Parse edges to determine node positions
    edge_pattern = r'"([^"]+)"\s*->\s*"([^"]+)"'
    edges = re.findall(edge_pattern, dot_string)

    in_degree = {}
    out_degree = {}
    for source, target in edges:
        in_degree[target] = in_degree.get(target, 0) + 1
        out_degree[source] = out_degree.get(source, 0) + 1

    # Get all nodes
    all_nodes = set()
    for s, t in edges:
        all_nodes.add(s)
        all_nodes.add(t)

    # Build new DOT with styled nodes
    lines = [
        "digraph DataLineage {",
        "  rankdir=LR;",
        '  bgcolor="#0E1117";',
        '  node [fontcolor="white", style="filled"];',
        '  edge [color="#888888", penwidth=1.5];',
    ]

    for node in all_nodes:
        node_in = in_degree.get(node, 0)
        node_out = out_degree.get(node, 0)

        if node_in == 0:
            # Source node
            color = COLORS["input"]
        elif node_in > 1:
            # Join node (multiple parents)
            color = COLORS["shuffle"]
        elif node_out == 0:
            # Output node
            color = COLORS["action"]
        else:
            # Intermediate
            color = COLORS["transformation"]

        lines.append(f'  "{node}" [fillcolor="{color}"];')

    for source, target in edges:
        lines.append(f'  "{source}" -> "{target}";')

    lines.append("}")
    return "\n".join(lines)
