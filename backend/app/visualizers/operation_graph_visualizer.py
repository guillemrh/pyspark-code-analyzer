from app.graphs.operation.operation_graph_builder import OperationDAG
from app.parsers.spark_semantics import OpType, DependencyType


def render_operation_dag_to_dot(dag: OperationDAG) -> str:
    """
    Convert an OperationDAG into a Graphviz DOT representation.

    This graph answers:
    - What executes?
    - In what order?
    - Where are actions?
    - Where are shuffles?
    """

    lines = [
        "digraph SparkDAG {",
        "  rankdir=LR;",
    ]

    for node in dag.nodes.values():
        attrs = []

        # Label
        attrs.append(f'label="{node.label}\\nStage {node.stage_id}"')

        # Actions as boxes
        if node.op_type == OpType.ACTION:
            attrs.append("shape=box")

        if node.dependency_type == DependencyType.WIDE:
            attrs.append("penwidth=2")

        # Shuffles highlighted
        if node.causes_shuffle:
            attrs.append("color=red")
            attrs.append("style=filled")
            attrs.append("fillcolor=mistyrose")

        attr_str = ", ".join(attrs)
        lines.append(f'  "{node.id}" [{attr_str}];')

    # Edges
    for node in dag.nodes.values():
        for child_id in node.children:
            lines.append(f'  "{node.id}" -> "{child_id}";')

    lines.append("}")
    return "\n".join(lines)
