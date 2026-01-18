"""Convert DOT format graphs to streamlit-agraph format."""

import re
from streamlit_agraph import Node, Edge


# Color scheme for different operation types
COLORS = {
    "read": "#4A90D9",  # Blue - Input operations
    "transform": "#5CB85C",  # Green - Transformations
    "shuffle": "#D9534F",  # Red - Shuffle operations
    "action": "#F0AD4E",  # Orange - Actions
    "default": "#777777",  # Gray - Default
}

# Operations that trigger shuffles
SHUFFLE_OPS = {"groupby", "join", "distinct", "repartition", "coalesce", "orderby"}

# Action operations
ACTION_OPS = {"show", "count", "collect", "write", "save", "take", "first"}

# Read/input operations
READ_OPS = {"read", "load", "parquet", "csv", "json", "jdbc", "table"}


def get_op_type(label: str) -> str:
    """Determine operation type from label."""
    label_lower = label.lower()

    for op in READ_OPS:
        if op in label_lower:
            return "read"

    for op in SHUFFLE_OPS:
        if op in label_lower:
            return "shuffle"

    for op in ACTION_OPS:
        if op in label_lower:
            return "action"

    return "transform"


def get_node_color(label: str) -> str:
    """Get node color based on operation type."""
    op_type = get_op_type(label)
    return COLORS.get(op_type, COLORS["default"])


def clean_label(label: str) -> str:
    """Clean up node label by removing newlines and extra whitespace."""
    # Replace literal \n and actual newlines with space
    cleaned = label.replace("\\n", " ").replace("\n", " ")
    # Collapse multiple spaces
    cleaned = " ".join(cleaned.split())
    return cleaned


def dot_to_agraph(
    dot_string: str, node_size: int = 25
) -> tuple[list[Node], list[Edge]]:
    """
    Parse DOT format and return agraph nodes and edges.

    Args:
        dot_string: DOT format graph string
        node_size: Size of nodes in the visualization

    Returns:
        Tuple of (nodes list, edges list) for streamlit-agraph
    """
    nodes = []
    edges = []
    seen_nodes = set()

    # Pattern 1: Nodes with explicit labels: "node_id" [label="Label", ...]
    labeled_pattern = r'"([^"]+)"\s*\[([^\]]*label="([^"]+)"[^\]]*)\]'
    for match in re.finditer(labeled_pattern, dot_string):
        node_id = match.group(1)
        label = clean_label(match.group(3))
        if node_id not in seen_nodes:
            seen_nodes.add(node_id)
            color = get_node_color(label)
            nodes.append(
                Node(
                    id=node_id,
                    label=label,
                    size=node_size,
                    color=color,
                    font={"color": "#FAFAFA"},  # Light text for dark mode
                )
            )

    # Pattern 2: Nodes without labels: "node_id" [shape=...] or "node_id" [...]
    # Use node_id as the label
    unlabeled_pattern = r'"([^"]+)"\s*\[([^\]]*)\];'
    for match in re.finditer(unlabeled_pattern, dot_string):
        node_id = match.group(1)
        attrs = match.group(2)
        # Skip if we already have this node (from labeled pattern)
        # or if this line contains a label (already handled above)
        if node_id not in seen_nodes and 'label=' not in attrs:
            seen_nodes.add(node_id)
            label = node_id  # Use node_id as label
            color = get_node_color(label)
            nodes.append(
                Node(
                    id=node_id,
                    label=label,
                    size=node_size,
                    color=color,
                    font={"color": "#FAFAFA"},  # Light text for dark mode
                )
            )

    # Parse edge definitions: "source" -> "target"
    edge_pattern = r'"([^"]+)"\s*->\s*"([^"]+)"'
    for match in re.finditer(edge_pattern, dot_string):
        source = match.group(1)
        target = match.group(2)
        edges.append(
            Edge(
                source=source,
                target=target,
                color="#AAAAAA",  # Lighter edges for dark mode
            )
        )

    return nodes, edges


def get_graph_legend() -> dict[str, str]:
    """Return the color legend for the graph."""
    return {
        "Input/Read": COLORS["read"],
        "Transformation": COLORS["transform"],
        "Shuffle": COLORS["shuffle"],
        "Action": COLORS["action"],
    }
