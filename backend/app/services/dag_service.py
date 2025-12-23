# backend/app/services/dag.py

from app.parsers.dag_nodes import SparkOperationNode

# This class represents a single node in the DAG and tracks its relationships with other operations
class DAGNode:
    def __init__(self, id, label):
        self.id = id
        self.label = label
        self.parents = set()
        self.children = set() # set of child node IDs that depend on this node

# This class represents the entire DAG structure to store and manage the relationships between operations
class SparkDAG:
    def __init__(self):
        self.nodes = {} # A dictionary where the keys are node IDs and the values are DAGNode objects

    def add_node(self, node: DAGNode):
        self.nodes[node.id] = node

    def add_edge(self, parent_id, child_id):
        self.nodes[parent_id].children.add(child_id)
        self.nodes[child_id].parents.add(parent_id)


def build_dag(operations: list[SparkOperationNode]) -> SparkDAG:
    """
    This function constructs a SparkDAG from a list of SparkOperationNode objects. 
    Each SparkOperationNode represents a Spark operation extracted from the AST
    """
    # Initialize the DAG
    dag = SparkDAG()

    # Tracks the latest DAG node producing each DataFrame
    df_producers = {}

    for op in operations:
        # For each SparkOperationNode in the list, a corresponding DAGNode is created.
        dag_node = DAGNode(
            id=op.id,
            label=op.operation
        )
        dag.add_node(dag_node)

        # Connect parents
        for parent_df in op.parents:
            if parent_df in df_producers:
                dag.add_edge(df_producers[parent_df], dag_node.id)

        # Update to record that the current node is the latest producer of the DataFrame
        df_producers[op.df_name] = dag_node.id

    return dag
