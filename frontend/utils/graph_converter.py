"""Convert DOT format to streamlit-agraph nodes and edges."""

import re
from typing import Tuple, List
from streamlit_agraph import Node, Edge, Config


# Color scheme for different node types
COLORS = {
    "shuffle": "#C0392B",  # Red - shuffle operations
    "action": "#E67E22",  # Orange - actions
    "transformation": "#3498DB",  # Blue - transformations
    "input": "#27AE60",  # Green - read/input
    "default": "#7F8C8D",  # Gray - default
}


def parse_dot_to_agraph(
    dot_string: str, graph_type: str = "dag"
) -> Tuple[List[Node], List[Edge]]:
    """
    Parse a DOT string and return agraph nodes and edges.

    Args:
        dot_string: The DOT format string from the backend
        graph_type: "dag" for Operation DAG, "lineage" for Data Lineage

    Returns:
        Tuple of (nodes, edges) for use with streamlit-agraph
    """
    nodes = []
    edges = []

    # Extract node definitions: "node_id" [attr1=val1, attr2=val2];
    node_pattern = r'"([^"]+)"\s*\[([^\]]*)\];'
    edge_pattern = r'"([^"]+)"\s*->\s*"([^"]+)";'

    # Parse nodes
    node_matches = re.findall(node_pattern, dot_string)
    parsed_node_ids = set()

    for node_id, attrs_str in node_matches:
        parsed_node_ids.add(node_id)

        # Parse attributes
        attrs = {}
        if attrs_str:
            # Match key="value" or key=value patterns
            attr_pattern = r'(\w+)=(?:"([^"]*)"|([^\s,]+))'
            for match in re.findall(attr_pattern, attrs_str):
                key = match[0]
                value = match[1] if match[1] else match[2]
                attrs[key] = value

        # Determine node styling
        label = attrs.get("label", node_id).replace("\\n", "\n")
        color = COLORS["default"]
        size = 25

        # Check for shuffle (filled with shuffle color)
        if "fillcolor" in attrs and attrs.get("fillcolor") == COLORS["shuffle"]:
            color = COLORS["shuffle"]
            size = 30

        # Check for action (box shape)
        elif attrs.get("shape") == "box":
            color = COLORS["action"]
            size = 28

        # Input/read operations
        elif any(
            keyword in label.lower()
            for keyword in ["read", "parquet", "csv", "json", "load"]
        ):
            color = COLORS["input"]

        # Default transformations
        else:
            color = COLORS["transformation"]

        nodes.append(
            Node(
                id=node_id,
                label=label,
                size=size,
                color=color,
                font={"color": "white", "size": 12},
            )
        )

    # Parse edges
    edge_matches = re.findall(edge_pattern, dot_string)
    for source, target in edge_matches:
        # Create nodes for any edges that reference nodes not yet defined
        for node_id in [source, target]:
            if node_id not in parsed_node_ids:
                parsed_node_ids.add(node_id)
                nodes.append(
                    Node(
                        id=node_id,
                        label=node_id,
                        size=25,
                        color=COLORS["default"],
                        font={"color": "white", "size": 12},
                    )
                )

        edges.append(Edge(source=source, target=target))

    return nodes, edges


def get_dag_config() -> Config:
    """Get configuration for Operation DAG display."""
    return Config(
        width="100%",
        height=500,
        directed=True,
        physics=True,
        hierarchical=True,
        nodeHighlightBehavior=True,
        highlightColor="#F1C40F",
        collapsible=False,
        node={"labelProperty": "label"},
        link={"highlightColor": "#F1C40F"},
    )


def get_lineage_config() -> Config:
    """Get configuration for Data Lineage display."""
    return Config(
        width="100%",
        height=400,
        directed=True,
        physics=True,
        hierarchical=True,
        nodeHighlightBehavior=True,
        highlightColor="#F1C40F",
        collapsible=False,
        node={"labelProperty": "label"},
        link={"highlightColor": "#F1C40F"},
    )
