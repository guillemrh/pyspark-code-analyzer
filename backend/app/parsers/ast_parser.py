# backend/app/parsers/ast_parser.py

import ast
from typing import List, Dict, Optional, Tuple
from .dag_nodes import SparkOperationNode
from app.parsers.spark_semantics import SPARK_OPS, OpType


class PySparkASTParser(ast.NodeVisitor):
    """
    AST parser that extracts Spark DataFrame operations
    from single-file PySpark scripts.
    """

    MULTI_PARENT_OPS = {
        "join",
        "union",
        "unionAll",
        "unionByName",
        "intersect",
        "intersectAll",
        "except",
        "exceptAll",
        "subtract",
        "crossJoin",
    }

    def __init__(self):
        self.operations: List[SparkOperationNode] = []
        self.variable_lineage: Dict[str, List[str]] = {}

    def _is_spark_read_pattern(self, node: ast.Call) -> Tuple[bool, Optional[str]]:
        """
        Check if this is a spark.read.* pattern like:
        - spark.read.parquet("file")
        - spark.read.csv("file", header=True)
        - spark.read.format("parquet").load("file")

        Returns (is_spark_read, operation_name)
        """
        current = node
        chain = []

        # Walk the call chain to find spark.read.*
        while isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute):
                chain.append(current.func.attr)
                current = current.func.value
            else:
                break

        # Check if we have a spark.read.* pattern
        if isinstance(current, ast.Attribute):
            if current.attr == "read":
                if isinstance(current.value, ast.Name) and current.value.id in (
                    "spark",
                    "session",
                ):
                    # The operation is the last item in the reversed chain
                    if chain:
                        return True, chain[0]

        return False, None

    def _is_spark_sql_pattern(self, node: ast.Call) -> bool:
        """
        Check if this is a spark.sql("...") pattern.
        """
        if isinstance(node.func, ast.Attribute):
            if node.func.attr == "sql":
                if isinstance(node.func.value, ast.Name) and node.func.value.id in (
                    "spark",
                    "session",
                ):
                    return True
        return False

    def _is_write_pattern(
        self, node: ast.Call
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Check if this is a df.write.* pattern like:
        - df.write.parquet("output")
        - df.write.mode("overwrite").parquet("output")
        - df.write.format("parquet").save("output")

        Returns (is_write, base_df, operation_name)
        """
        current = node
        chain = []
        base_df = None

        # Walk the call chain to find *.write.*
        while isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute):
                chain.append(current.func.attr)
                current = current.func.value
            else:
                break

        # Check for attribute chain ending in .write
        while isinstance(current, ast.Attribute):
            chain.append(current.attr)
            current = current.value

        if isinstance(current, ast.Name):
            base_df = current.id

        # Check if "write" is in the chain
        if "write" in chain:
            # Get the final operation (first in chain since we built it backwards)
            if chain:
                return True, base_df, chain[0]

        return False, None, None

    def visit_Assign(self, node: ast.Assign):
        """
        Handles patterns like:
        df2 = df1.select(...).filter(...)
        df = spark.read.parquet("file")
        result = spark.sql("SELECT * FROM table")
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

        # Check for spark.read.* pattern
        is_spark_read, read_op = self._is_spark_read_pattern(node.value)
        if is_spark_read and read_op:
            node_id = f"{target_df}_{read_op}_{node.lineno}"
            op_type = SPARK_OPS.get(read_op, OpType.TRANSFORMATION)

            op_node = SparkOperationNode(
                id=node_id,
                df_name=target_df,
                operation=read_op,
                parents=[],  # Data source has no parents
                lineno=node.lineno,
                op_type=op_type,
            )
            self.operations.append(op_node)
            self.generic_visit(node)
            return

        # Check for spark.sql() pattern
        if self._is_spark_sql_pattern(node.value):
            node_id = f"{target_df}_sql_{node.lineno}"
            op_node = SparkOperationNode(
                id=node_id,
                df_name=target_df,
                operation="sql",
                parents=[],  # SQL query creates a new DataFrame
                lineno=node.lineno,
                op_type=OpType.TRANSFORMATION,
            )
            self.operations.append(op_node)
            self.generic_visit(node)
            return

        # Invoked on the right-hand side of the assignment to retrieve the sequence of method calls
        call_chain = self._extract_call_chain(node.value)
        if not call_chain:
            return

        # Iterates over the extracted call chain, creating a SparkOperationNode for each operation
        for idx, call in enumerate(call_chain):
            # Generate a unique node ID for each operation
            node_id = f"{target_df}_{call['op']}_{node.lineno}"

            # Determine operation type for each call in the chain
            op_type = SPARK_OPS.get(call["op"], OpType.TRANSFORMATION)

            op_node = SparkOperationNode(
                id=node_id,
                df_name=target_df,
                operation=call["op"],
                parents=call["parents"],
                lineno=node.lineno,
                op_type=op_type,
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
        base_df = None  # Tracks the base DataFrame for the entire chain
        tmp = current

        while isinstance(tmp, ast.Call):
            if isinstance(tmp.func, ast.Attribute):
                if isinstance(tmp.func.value, ast.Name):
                    base_df = tmp.func.value.id
                    break
                tmp = tmp.func.value
            else:
                break

        while isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute):
                op_name = (
                    current.func.attr
                )  # represents the method (select, filter, join, etc.)

                # Default parent extraction (unary operations)
                parents = []

                # Detect base DataFrame only once
                if base_df is None:
                    if isinstance(current.func.value, ast.Name):
                        base_df = current.func.value.id
                        print(f"Detected base DataFrame: {base_df}")

                # Unary operations inherit the base DataFrame
                if op_name not in self.MULTI_PARENT_OPS:
                    if base_df:
                        parents.append(base_df)
                else:
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

    def visit_Expr(self, node: ast.Expr):
        """
        Handles standalone calls like:
        df.show()
        df.collect()
        df1.filter(...)
        df.write.parquet("output")
        df.write.mode("overwrite").parquet("output")
        """
        if not isinstance(node.value, ast.Call):
            return

        # Check for write pattern (df.write.*)
        is_write, base_df, write_op = self._is_write_pattern(node.value)
        if is_write and base_df and write_op:
            # For write patterns, create a single write action node
            node_id = f"{base_df}_write_{node.lineno}"
            op_node = SparkOperationNode(
                id=node_id,
                df_name=base_df,
                operation="write",
                parents=[base_df],
                lineno=node.lineno,
                op_type=OpType.ACTION,
            )
            self.operations.append(op_node)
            self.generic_visit(node)
            return

        call_chain = self._extract_call_chain(node.value)
        if not call_chain:
            return

        # Try to infer the DF name from the base call
        base_df = None
        current = node.value
        while isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute):
                if isinstance(current.func.value, ast.Name):
                    base_df = current.func.value.id
                    break
                current = current.func.value
            else:
                break

        for call in call_chain:
            node_id = f"{base_df}_{call['op']}_{node.lineno}"

            op_type = SPARK_OPS.get(call["op"], OpType.TRANSFORMATION)

            op_node = SparkOperationNode(
                id=node_id,
                df_name=base_df or "UNKNOWN",
                operation=call["op"],
                parents=call["parents"],
                lineno=node.lineno,
                op_type=op_type,
            )

            self.operations.append(op_node)

        self.generic_visit(node)

    def _get_base_name(self, node) -> str:
        """
        Extract base DataFrame variable name.
        """
        if isinstance(node, ast.Name):
            return node.id
        return "UNKNOWN"
