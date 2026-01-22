import ast
import traceback
from app.parsers.ast_parser import PySparkASTParser
from app.graphs.operation.operation_graph_builder import build_operation_dag
from app.visualizers.operation_graph_visualizer import render_operation_dag_to_dot
from app.graphs.operation.stage_assignment import assign_stages
from app.graphs.antipatterns.registry import detect_antipatterns
from app.graphs.lineage.lineage_graph_builder import build_data_lineage_graph
from app.visualizers.lineage_graph_visualizer import render_data_lineage_to_dot

from app.services.documentation.dag_summary import (
    dag_summary_json,
    dag_summary_markdown,
)
from app.services.documentation.stage_summary import (
    stage_summary_json,
    stage_summary_markdown,
)
from app.services.documentation.antipattern_summary import (
    antipatterns_summary_json,
    antipattern_summary_markdown,
)
from app.services.documentation.lineage_summary import (
    lineage_summary_json,
    lineage_summary_markdown,
)

from opentelemetry import trace

tracer = trace.get_tracer(__name__)


def run_dag_pipeline(code: str) -> dict:
    """
    Runs the OPERATION-LEVEL pipeline only:

    PySpark code
        -> AST parsing
        -> Operation DAG (execution / performance view)
        -> Graphviz DOT
    """
    with tracer.start_as_current_span(
        "dag_pipeline",
        attributes={
            "component": "analysis",
            "language": "pyspark",
        },
    ):
        try:
            # Parse code into AST
            tree = ast.parse(code)

            # Extract Spark operations
            parser = PySparkASTParser()
            parser.visit(tree)

            print("=== PARSER OPERATIONS ===")
            for op in parser.operations:
                print(op)

            # Build execution / operation DAG
            operation_dag = build_operation_dag(parser.operations)

            print("=== DAG NODES ===")
            for node_id, node in operation_dag.nodes.items():
                print(f"{node_id}: {type(node)}, {getattr(node, 'parents', None)}")

            # Build data lineage graph
            lineage_graph = build_data_lineage_graph(parser.operations)

            # Assign stages based on wide dependencies
            assign_stages(operation_dag)

            # Detect anti-patterns (multiple actions on the same lineage)
            findings = detect_antipatterns(operation_dag, source_code=code)

            # Render lineage and operation DAG to Graphviz DOT
            dag_dot = render_operation_dag_to_dot(operation_dag)
            lineage_dot = render_data_lineage_to_dot(lineage_graph)

            # DAG summaries
            dag_summary_dict = dag_summary_json(operation_dag)
            stage_summary_dict = stage_summary_json(operation_dag)
            lineage_summary_dict = lineage_summary_json(lineage_graph)
            antipattern_summary_dict = antipatterns_summary_json(findings)

            return {
                "dag_dot": dag_dot,
                "lineage_dot": lineage_dot,
                "dag_summary": {
                    "json": dag_summary_dict,
                    "markdown": dag_summary_markdown(dag_summary_dict),
                },
                "stage_summary": {
                    "json": stage_summary_dict,
                    "markdown": stage_summary_markdown(stage_summary_dict),
                },
                "lineage_summary": {
                    "json": lineage_summary_dict,
                    "markdown": lineage_summary_markdown(lineage_graph),
                },
                "antipatterns": {
                    "json": antipattern_summary_dict,
                    "markdown": antipattern_summary_markdown(antipattern_summary_dict),
                },
            }
        except Exception as e:
            print(f"ERROR in run_dag_pipeline: {e}")
            traceback.print_exc()
            raise
