from app.graphs.lineage.lineage_graph_builder import DataLineageGraph


def render_data_lineage_to_dot(lineage: DataLineageGraph) -> str:
    lines = [
        "digraph DataLineage {",
        "  rankdir=LR;",
    ]

    all_nodes = set(lineage.parents) | set(lineage.children)

    for df in all_nodes:
        lines.append(f'  "{df}" [shape=ellipse];')

    for parent, children in lineage.children.items():
        for child in children:
            lines.append(f'  "{parent}" -> "{child}";')

    lines.append("}")
    return "\n".join(lines)
