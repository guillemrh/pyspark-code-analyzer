from typing import Dict, Set, List
from collections import defaultdict

from app.parsers.dag_nodes import SparkOperationNode
from app.parsers.spark_semantics import OpType


class DataLineageGraph:
    """
    Dataset-level lineage graph (DataFrame → DataFrame)
    """

    def __init__(self):
        self.parents: Dict[str, Set[str]] = defaultdict(set)
        self.children: Dict[str, Set[str]] = defaultdict(set)

    def add_edge(self, parent_df: str, child_df: str):
        # Skip self-loops
        if parent_df == child_df:
            return
        self.parents[child_df].add(parent_df)
        self.children[parent_df].add(child_df)


def build_data_lineage_graph(
    operations: List[SparkOperationNode],
) -> DataLineageGraph:
    """
    Build dataset lineage graph from Spark operations.

    Only includes transformations that produce new DataFrames.
    Actions (show, count, collect, write) are excluded as they
    don't create new DataFrames in the lineage.
    """

    lineage = DataLineageGraph()

    for op in operations:
        # Skip actions - they don't produce new DataFrames
        if op.op_type == OpType.ACTION:
            continue

        child_df = op.df_name

        for parent_df in op.parents:
            lineage.add_edge(parent_df, child_df)

    return lineage
