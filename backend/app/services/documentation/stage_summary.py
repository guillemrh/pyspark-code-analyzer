from typing import Dict, Any, List
from collections import defaultdict

from app.graphs.operation.operation_graph_builder import OperationDAG
from app.parsers.spark_semantics import DependencyType


def stage_summary_json(dag: OperationDAG) -> Dict[str, Any]:
    """
    Summarize Spark stages inferred from wide dependencies.
    """

    stages: Dict[int, List[str]] = defaultdict(list)
    wide_ops_per_stage: Dict[int, List[str]] = defaultdict(list)

    for node in dag.nodes.values():
        if node.stage_id is None:
            continue

        stages[node.stage_id].append(node.id)

        if node.dependency_type == DependencyType.WIDE:
            wide_ops_per_stage[node.stage_id].append(node.id)

    stage_summaries = []

    for stage_id in sorted(stages.keys()):
        stage_summaries.append(
            {
                "stage_id": stage_id,
                "operations": stages[stage_id],
                "wide_operations": wide_ops_per_stage.get(stage_id, []),
                "operation_count": len(stages[stage_id]),
            }
        )

    return {
        "total_stages": len(stage_summaries),
        "stages": stage_summaries,
    }


def stage_summary_markdown(summary: Dict[str, Any]) -> str:
    """
    Render stage summary as Markdown.
    """

    lines = [
        "## Spark Stage Summary",
        f"**Total stages:** {summary['total_stages']}",
        "",
    ]

    for stage in summary["stages"]:
        lines.append(f"### Stage {stage['stage_id']}")
        lines.append(f"- Operations: {stage['operation_count']}")
        lines.append(
            f"- Wide dependencies: {', '.join(stage['wide_operations']) or 'None'}"
        )
        lines.append(
            f"- Nodes: {', '.join(stage['operations'])}"
        )
        lines.append("")

    return "\n".join(lines)
