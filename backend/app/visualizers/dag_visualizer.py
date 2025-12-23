from app.services.dag_service import SparkDAG


def render_dag_to_dot(dag: SparkDAG) -> str:
    """
    Convert a SparkDAG into a Graphviz DOT representation.
    """
    lines = []
    lines.append("digraph SparkDAG {")
    lines.append("  rankdir=LR;")  # Left-to-right layout

    # Emit nodes
    for node_id, node in dag.nodes.items():
        label = node.label
        lines.append(f'  "{node_id}" [label="{label}"];')

    # Emit edges
    for node_id, node in dag.nodes.items():
        for child_id in node.children:
            lines.append(f'  "{node_id}" -> "{child_id}";')

    lines.append("}")
    return "\n".join(lines)
