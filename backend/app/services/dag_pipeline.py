import ast
from app.parsers.ast_parser import PySparkASTParser
from app.services.dag_service import build_dag
from app.visualizers.dag_visualizer import render_dag_to_dot


def run_dag_pipeline(code: str) -> dict:
    """
    Runs:
    PySpark code -> AST -> DAG -> Graphviz DOT
    """
    tree = ast.parse(code)

    parser = PySparkASTParser()
    parser.visit(tree)

    dag = build_dag(parser.operations)
    dag_dot = render_dag_to_dot(dag)

    return {
        "dag_dot": dag_dot,
    }
