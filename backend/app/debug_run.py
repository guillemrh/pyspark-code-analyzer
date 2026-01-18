import ast

from app.parsers.ast_parser import PySparkASTParser
from app.graphs.operation.operation_graph_builder import build_operation_dag
from app.visualizers.operation_graph_visualizer import render_operation_dag_to_dot
from app.graphs.operation.stage_assignment import assign_stages
from app.graphs.antipatterns.registry import detect_antipatterns
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
from app.graphs.lineage.lineage_graph_builder import build_data_lineage_graph
from app.visualizers.lineage_graph_visualizer import render_data_lineage_to_dot

"""df_base = df.select("user_id", "value").filter("value > 10")

df_rep = df_base.repartition(200)

df_grouped = df_rep.groupBy("user_id").count().show()

df_grouped.show()
df_grouped.collect()

df_joined = df_grouped.join(df_base, on="user_id")
df_joined.count()
"""


code = """df_base = df.select("user_id", "value").filter("value > 10")"""

# --------------------
# AST PARSING
# --------------------
tree = ast.parse(code)
parser = PySparkASTParser()
parser.visit(tree)

print("\n=== AST OUTPUT ===")
for op in parser.operations:
    print(op)

# --------------------
# DAG BUILDING
# --------------------
dag = build_operation_dag(parser.operations)

print("\n=== DAG NODES ===")
for node_id, node in dag.nodes.items():
    print(
        f"Node {node_id}: "
        f"label={node.label}, "
        f"parents={node.parents}, "
        f"children={node.children}"
    )

# --------------------
# ASSIGN STAGES
# --------------------
stages = assign_stages(dag)

print("\n=== ASSIGNED STAGES ===")
for node_id, node in stages.nodes.items():
    print(
        f"Node {node_id}: "
        f"op={node.label}, "
        f"stage_id={node.stage_id}, "
        f"dependency={node.dependency_type}"
    )

# --------------------
# ANTIPATTERNS
# --------------------
print("\n=== ANTIPATTERNS ===")
findings = detect_antipatterns(dag)
for f in findings:
    print(f"[{f.severity}] {f.rule_id}: {f.message} → nodes={f.nodes}")

# --------------------
# GRAPHVIZ DOT
# --------------------
dot = render_operation_dag_to_dot(dag)

print("\n=== GRAPHVIZ DOT ===")
print(dot)

# --------------------
# DATA LINEAGE
# --------------------
lineage = build_data_lineage_graph(parser.operations)
lineage_dot = render_data_lineage_to_dot(lineage)

print("\n=== DATA LINEAGE GRAPH ===")
print(lineage_dot)

# --------------------
# DOCUMENTATION
# --------------------
summary = dag_summary_json(dag)
stage_summary = stage_summary_json(dag)
antipattern_summary = antipatterns_summary_json(findings)

print("\n=== DAG SUMMARY (JSON) ===")
print(summary)

print("\n=== DAG SUMMARY (MARKDOWN) ===")
print(dag_summary_markdown(summary))

print("\n=== STAGE SUMMARY (JSON) ===")
print(stage_summary)

print("\n=== STAGE SUMMARY (MARKDOWN) ===")
print(stage_summary_markdown(stage_summary))

print("\n=== ANTIPATTERN SUMMARY (JSON) ===")
print(antipattern_summary)

print("\n=== ANTIPATTERN SUMMARY (MARKDOWN) ===")
print(antipattern_summary_markdown(antipattern_summary))
