from app.services.dag_pipeline import run_dag_pipeline


def test_render_simple_operation_dag():
    code = """
    df2 = df.select("a")
    df3 = df2.filter("a > 1")
    """

    result = run_dag_pipeline(code)

    dot = result["dag_dot"]

    # Graph header
    assert "digraph SparkDAG" in dot

    # Nodes
    assert "select" in dot
    assert "filter" in dot

    # Edge: select -> filter
    # We don't hardcode node IDs, but we assert that an edge exists
    assert "->" in dot
