from backend.app.services.dag_service import build_dag
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

    dag = build_dag(ops)

    assert len(dag.nodes) == 2
    assert "n1" in dag.nodes["n2"].parents
