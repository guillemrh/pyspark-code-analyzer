"""Graph utilities for the frontend."""

# Color scheme for different operation types (for legend display)
COLORS = {
    "read": "#4A90D9",  # Blue - Input operations
    "transform": "#5CB85C",  # Green - Transformations
    "shuffle": "#D9534F",  # Red - Shuffle operations
    "action": "#F0AD4E",  # Orange - Actions
}


def get_graph_legend() -> dict[str, str]:
    """Return the color legend for the graph."""
    return {
        "Input/Read": COLORS["read"],
        "Transformation": COLORS["transform"],
        "Shuffle": COLORS["shuffle"],
        "Action": COLORS["action"],
    }
