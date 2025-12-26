import ast

from app.parsers.ast_parser import PySparkASTParser
from app.services.operation_dag_builder import build_operation_dag
from app.visualizers.dag_visualizer import render_operation_dag_to_dot
from app.services.stage_builder import assign_stages
from app.services.antipatterns.registry import detect_antipatterns


code = """
df2 = df.select("a").filter("a > 5")
df3 = df2.groupBy("a").count().show()
df4 = df3.join(df2, on="a", how="inner")
"""

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
    print(
        f"[{f.severity}] {f.rule_id}: {f.message} → nodes={f.nodes}"
    )
    
# --------------------
# GRAPHVIZ DOT
# --------------------
dot = render_operation_dag_to_dot(dag)

print("\n=== GRAPHVIZ DOT ===")
print(dot)
