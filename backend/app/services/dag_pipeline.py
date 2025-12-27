import ast
import traceback
from app.parsers.ast_parser import PySparkASTParser
from app.graphs.operation.operation_graph_builder import build_operation_dag
from app.visualizers.operation_graph_visualizer import render_operation_dag_to_dot
from app.graphs.operation.stage_assignment import assign_stages
from app.services.antipatterns.registry import detect_antipatterns


def run_dag_pipeline(code: str) -> dict:
    """
    Runs the OPERATION-LEVEL pipeline only:

    PySpark code
        -> AST parsing
        -> Operation DAG (execution / performance view)
        -> Graphviz DOT
    """
    try:
        # Parse code into AST
        tree = ast.parse(code)

        # Extract Spark operations
        parser = PySparkASTParser()
        parser.visit(tree)

        # Build execution / operation DAG
        operation_dag = build_operation_dag(parser.operations)
        
        # Assign stages based on wide dependencies
        operation_dag = assign_stages(operation_dag)
        
        # Detect anti-patterns (multiple actions on the same lineage)
        findings = detect_antipatterns(operation_dag)

        # Render DAG to Graphviz DOT
        dag_dot = render_operation_dag_to_dot(operation_dag)

        return {
            "dag_dot": dag_dot,
            "findings": findings
        }
    except Exception as e:
        print(f"ERROR in run_dag_pipeline: {e}")
        traceback.print_exc()
        raise