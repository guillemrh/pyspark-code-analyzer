from __future__ import annotations
from app.parsers.spark_semantics import DependencyType
from collections import deque
from app.graphs.operation.operation_graph_builder import OperationDAG


def topological_sort(dag: OperationDAG):
    """
    Returns nodes in topological order (parents before children)
    """
    in_degree = {node_id: len(node.parents) for node_id, node in dag.nodes.items()}
    queue = deque([dag.nodes[n] for n, deg in in_degree.items() if deg == 0])

    ordered = []

    while queue:
        node = queue.popleft()
        ordered.append(node)

        for child_id in node.children:
            in_degree[child_id] -= 1
            if in_degree[child_id] == 0:
                queue.append(dag.nodes[child_id])

    return ordered


def assign_stages(dag: OperationDAG) -> OperationDAG:
    """
    Assign Spark stage IDs based on wide dependencies.

    Rule:
    - Start at stage 0
    - Increment stage whenever a node has a WIDE dependency
    """

    stage_id = 0
    nodes_in_order = topological_sort(dag)

    for node in nodes_in_order:
        # Root nodes start stage 0
        if not node.parents:
            node.stage_id = stage_id
            continue

        # Wide dependency starts a new stage
        parent_stages = {
            dag.nodes[p].stage_id
            for p in node.parents
            if dag.nodes[p].stage_id is not None
        }
        node.stage_id = max(parent_stages, default=0)

        if node.dependency_type == DependencyType.WIDE:
            node.stage_id += 1

    return dag
