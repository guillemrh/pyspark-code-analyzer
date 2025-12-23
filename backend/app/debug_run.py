import ast

from app.parsers.ast_parser import PySparkASTParser
from app.services.dag_service import build_dag
from app.visualizers.dag_visualizer import render_dag_to_dot


code = """
df2 = df.select("a").filter("a > 5")
df3 = df2.join(df_other, on="id").select("a")
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
dag = build_dag(parser.operations)

print("\n=== DAG NODES ===")
for node_id, node in dag.nodes.items():
    print(
        f"Node {node_id}: "
        f"label={node.label}, "
        f"parents={node.parents}, "
        f"children={node.children}"
    )

# --------------------
# GRAPHVIZ DOT
# --------------------
dot = render_dag_to_dot(dag)

print("\n=== GRAPHVIZ DOT ===")
print(dot)
