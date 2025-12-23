# backend/app/parsers/ast_parser.py

import ast
from typing import List, Dict
from .dag_nodes import SparkOperationNode


class PySparkASTParser(ast.NodeVisitor):
    """
    AST parser that extracts Spark DataFrame operations
    from single-file PySpark scripts.
    """
    
    # Multi-parent operations that can have multiple parent DataFrames
    MULTI_PARENT_OPS = {"join", "union", "unionAll", "intersect", "except"}

    def __init__(self):
        self.operations: List[SparkOperationNode] = []
        self.variable_lineage: Dict[str, List[str]] = {}

    def visit_Assign(self, node: ast.Assign):
        """
        Handles patterns like:
        df2 = df1.select(...).filter(...)
        """
        
        # Checking if the right-hand side of the assignment (node.value) is a function call (ast.Call). 
        # If it is not, the method exits early
        if not isinstance(node.value, ast.Call):
            return

        # Ensures that the left-hand side of the assignment (node.targets[0]) is a simple variable name (ast.Name). 
        # This restriction ensures that only straightforward assignments like df2 = ... are processed, excluding more complex patterns like tuple unpacking.
        if not isinstance(node.targets[0], ast.Name):
            return

        # Variable name on the left-hand side of the assignment
        target_df = node.targets[0].id
        
        # Invoked on the right-hand side of the assignment to retrieve the sequence of method calls
        call_chain = self._extract_call_chain(node.value)
        if not call_chain:
            return

        # First element of the call chain is assumed to represent the "parent dataframe"
        parent_df = call_chain[0]["parents"]

        # Dictionary is updated to record the lineage of the target_df
        self.variable_lineage[target_df] = [parent_df]

        # Unique node identifier
        node_id = f"{target_df}_{call_chain[-1]['op']}_{node.lineno}"

        # Iterates over the extracted call chain, creating a SparkOperationNode for each operation
        for call in call_chain:
            op_node = SparkOperationNode(
                id=node_id,
                df_name=target_df,
                operation=call["op"],
                parents=call["parents"],
                lineno=node.lineno,
            )
            self.operations.append(op_node)

        # It ensures that the traversal of the AST continues for any child nodes of the current node
        # Example: df2 = df1.select("col1").filter("col2 > 10") 
        # Here, filter is a child node of the select call
        self.generic_visit(node)

    def _extract_call_chain(self, call: ast.Call) -> List[dict]:
        """
        Extracting the sequence of method calls (e.g., select, filter, groupBy, join)
        from a chained operation on a DataFrame.

        Returns:
        [
        {"op": "select", "parents": ["df"]},
        {"op": "filter", "parents": ["df"]},
        {"op": "join", "parents": ["df1", "df2"]}
        ]
        """
        chain = []  # will store the extracted operations
        current = call  # starts as the call node (e.g., the filter or join call)

        while isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute):
                op_name = current.func.attr  # represents the method (select, filter, join, etc.)

                # Default parent extraction (unary operations)
                parents = []

                # The object on which the method is called (e.g., df1.join(...))
                if isinstance(current.func.value, ast.Name):
                    parents.append(current.func.value.id)

                # Special handling for multi-parent operations like join
                if op_name in self.MULTI_PARENT_OPS:
                    # Extract additional DataFrame arguments (e.g., df2 in df1.join(df2))
                    if current.args:
                        for arg in current.args:
                            if isinstance(arg, ast.Name):
                                parents.append(arg.id)

                chain.append(
                    {
                        "op": op_name,
                        "parents": parents,
                    }
                )

                # Move to the next call in the chain (left side)
                current = current.func.value
            else:
                break

        # The chain is reversed to maintain the original order of operations
        return list(reversed(chain))


    def _get_base_name(self, node) -> str:
        """
        Extract base DataFrame variable name.
        """
        if isinstance(node, ast.Name):
            return node.id
        return "UNKNOWN"
