from app.graphs.operation.operation_graph_builder import build_operation_dag
from app.parsers.dag_nodes import SparkOperationNode


def test_join_dag_construction():
    ops = [
        SparkOperationNode(
            id="n1",
            df_name="df1",
            operation="select",
            parents=["df"],
            lineno=1,
        ),
        SparkOperationNode(
            id="n2",
            df_name="df2",
            operation="join",
            parents=["df1", "df_other"],
            lineno=2,
        ),
    ]

    dag = build_operation_dag(ops)

    assert len(dag.nodes) == 2

    join_node = next(
        node for node in dag.nodes.values()
        if node.label == "join"
    )

    parent_labels = {
        dag.nodes[parent_id].label
        for parent_id in join_node.parents
    }

    assert "select" in parent_labels
