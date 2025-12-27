from typing import List, Dict
from app.parsers.dag_nodes import SparkOperationNode
from app.parsers.spark_semantics import OpType, DependencyType, SHUFFLE_OPS

class OperationDAGNode:
    """
    Represents a single Spark operation in the execution DAG
    """
    def __init__(
        self,
        id: str,
        label: str,
        op_type: OpType | None,
        causes_shuffle: bool,
        lineno: int,
    ):
        self.id = id
        self.label = label
        self.op_type = op_type
        self.causes_shuffle = causes_shuffle
        self.lineno = lineno
        self.dependency_type = (
            DependencyType.WIDE if causes_shuffle else DependencyType.NARROW
        )
        self.stage_id: int | None = None
        self.parents = set()
        self.children = set()


class OperationDAG:
    """
    Pure execution DAG (operation → operation)
    """
    def __init__(self):
        self.nodes: Dict[str, OperationDAGNode] = {}

    def add_node(self, node: OperationDAGNode):
        self.nodes[node.id] = node

    def add_edge(self, parent_id: str, child_id: str):
        self.nodes[parent_id].children.add(child_id)
        self.nodes[child_id].parents.add(parent_id)


def build_operation_dag(
    operations: List[SparkOperationNode],
) -> OperationDAG:
    """
    Builds a Spark *execution* DAG.

    Rules:
    - Nodes are operations
    - Edges represent execution dependencies
    - Parents MUST be operation IDs (not DataFrame names)
    """
    dag = OperationDAG()
    
    # Track last operation producing each DataFrame
    df_last_op: Dict[str, str] = {}
    assignment_last_op: Dict[int, str] = {}

    # Create nodes
    for op in operations:
        dag.add_node(
            OperationDAGNode(
                id=op.id,
                label=op.operation,
                op_type=op.op_type,
                causes_shuffle=op.operation in SHUFFLE_OPS,
                lineno=op.lineno,
            )
        )
        # Record this operation as the latest producer of its DataFrame
        df_last_op[op.df_name] = op.id

    # Wire execution dependencies
    for op in operations:
        # Prefer chaining within same assignment
        if op.lineno in assignment_last_op:
            dag.add_edge(assignment_last_op[op.lineno], op.id)

        # Otherwise, wire DataFrame lineage
        else:
            for parent_df in op.parents:
                if parent_df in df_last_op:
                    dag.add_edge(df_last_op[parent_df], op.id)

        # Update trackers
        assignment_last_op[op.lineno] = op.id
        df_last_op[op.df_name] = op.id

    return dag
