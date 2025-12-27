# DEPRECATED
from app.services.dag_service_deprecated import SparkDAG, DAGNode
from app.visualizers.operation_graph_visualizer import render_dag_to_dot


def test_render_simple_dag():
    dag = SparkDAG()

    n1 = DAGNode(id="n1", label="select")
    n2 = DAGNode(id="n2", label="filter")

    dag.add_node(n1)
    dag.add_node(n2)
    dag.add_edge("n1", "n2")

    dot = render_dag_to_dot(dag)

    assert "digraph SparkDAG" in dot
    assert '"n1" -> "n2"' in dot
