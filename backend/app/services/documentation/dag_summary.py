from typing import Dict, Any
from app.graphs.operation.operation_graph_builder import OperationDAG
from app.parsers.spark_semantics import OpType, DependencyType


def build_dag_summary(dag: OperationDAG) -> Dict[str, Any]:
    """
    Build a structural summary of the Spark operation DAG.
    This is descriptive, not analytical.
    """

    nodes = dag.nodes.values()

    total_nodes = len(nodes)

    transformations = sum(
        1 for n in nodes if n.op_type == OpType.TRANSFORMATION
    )
    actions = sum(
        1 for n in nodes if n.op_type == OpType.ACTION
    )

    wide_ops = sum(
        1 for n in nodes if n.dependency_type == DependencyType.WIDE
    )

    root_nodes = [
        n.id for n in nodes if not n.parents
    ]

    leaf_nodes = [
        n.id for n in nodes if not n.children
    ]

    max_stage = max(
        (n.stage_id for n in nodes if n.stage_id is not None),
        default=0
    )

    return {
        "total_operations": total_nodes,
        "transformations": transformations,
        "actions": actions,
        "wide_operations": wide_ops,
        "stages": max_stage + 1,
        "roots": root_nodes,
        "leaves": leaf_nodes,
    }


def dag_summary_markdown(summary: Dict[str, Any]) -> str:
    """
    Render DAG summary as Markdown.
    """

    return f"""## Spark DAG Summary

**Total operations:** {summary["total_operations"]}  
**Transformations:** {summary["transformations"]}  
**Actions:** {summary["actions"]}  
**Wide (shuffle) operations:** {summary["wide_operations"]}  
**Estimated Spark stages:** {summary["stages"]}

### Entry points
{", ".join(summary["roots"]) or "None"}

### Terminal operations
{", ".join(summary["leaves"]) or "None"}
"""
