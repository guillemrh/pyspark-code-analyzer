import textwrap
from app.services.dag_pipeline import run_dag_pipeline


def test_render_simple_operation_dag():
    code = """
    df2 = df.select("a")
    df3 = df2.filter("a > 1")
    """

    result = run_dag_pipeline(textwrap.dedent(code))
    dot = result["dag_dot"]

    assert "digraph SparkDAG" in dot
    assert "select" in dot
    assert "filter" in dot
    assert "->" in dot
