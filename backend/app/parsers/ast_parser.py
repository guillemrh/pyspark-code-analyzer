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
        self.alias_map: Dict[str, str] = {}  # Maps alias -> original variable
        self._anonymous_counter: int = 0

    def _generate_anonymous_df_name(self) -> str:
        """Generate a unique anonymous DataFrame name for nested expressions."""
        self._anonymous_counter += 1
        return f"_anon_df_{self._anonymous_counter}"

    def _is_dataframe_method_chain(self, node: ast.AST) -> bool:
        """
        Check if a node represents a DataFrame method chain (e.g., df.filter(...).select(...)).
        Returns True if the node is a Call with an Attribute func that could be a DF operation.
        """
        if not isinstance(node, ast.Call):
            return False
        if not isinstance(node.func, ast.Attribute):
            return False
        # Check if the method name is a known Spark operation
        op_name = node.func.attr
        return op_name in SPARK_OPS

    def _extract_operations_from_expr(
        self, expr: ast.AST, lineno: int
    ) -> Tuple[List[SparkOperationNode], Optional[str]]:
        """
        Extract all DataFrame operations from an expression that may contain
        a method chain (e.g., df2.filter(col("x") > 10).select("id")).

        This method handles nested DataFrame operation chains that appear as
        arguments to multi-parent operations like join() or union().

        Args:
            expr: An AST expression that may contain DataFrame operations
            lineno: Line number for the operations

        Returns:
            A tuple of (list of SparkOperationNode objects, final DataFrame name)
            The final DataFrame name is either the base variable name (if simple)
            or an anonymous name representing the result of the chain.
        """
        # Base case: simple variable name
        if isinstance(expr, ast.Name):
            return [], expr.id

        # Not a method call chain - return empty
        if not isinstance(expr, ast.Call):
            return [], None

        # Check if this is a DataFrame method chain
        if not self._is_dataframe_method_chain(expr):
            return [], None

        # Extract the call chain from this expression
        chain = []
        current = expr
        base_df = None

        # First, find the base DataFrame name
        tmp = current
        while isinstance(tmp, ast.Call):
            if isinstance(tmp.func, ast.Attribute):
                if isinstance(tmp.func.value, ast.Name):
                    base_df = self._resolve_alias(tmp.func.value.id)
                    break
                tmp = tmp.func.value
            else:
                break

        if base_df is None:
            return [], None

        # Now extract all operations in the chain
        while isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute):
                op_name = current.func.attr

                # Skip if not a Spark operation
                if op_name not in SPARK_OPS:
                    current = current.func.value
                    continue

                parents = [base_df]

                # For multi-parent operations, also extract from arguments
                if op_name in self.MULTI_PARENT_OPS and current.args:
                    for arg in current.args:
                        if isinstance(arg, ast.Name):
                            parents.append(self._resolve_alias(arg.id))
                        elif isinstance(arg, ast.Call):
                            # Recursively extract from nested call
                            nested_ops, nested_df = self._extract_operations_from_expr(
                                arg, lineno
                            )
                            if nested_ops:
                                # Add nested operations to the chain
                                for op in nested_ops:
                                    chain.append(
                                        {
                                            "op": op.operation,
                                            "parents": op.parents,
                                            "df_name": op.df_name,
                                        }
                                    )
                            if nested_df:
                                parents.append(nested_df)
                            elif (
                                not nested_ops
                                and isinstance(arg.func, ast.Name)
                                and arg.args
                                and isinstance(arg.args[0], ast.Name)
                            ):
                                # Handle wrapper functions like broadcast(df)
                                parents.append(
                                    self._resolve_alias(arg.args[0].id)
                                )

                chain.append({"op": op_name, "parents": parents, "df_name": None})
                current = current.func.value
            else:
                break

        # Reverse to get correct order
        chain = list(reversed(chain))

        # Generate anonymous DataFrame name for this chain's result
        anon_df_name = self._generate_anonymous_df_name()

        # Create SparkOperationNode objects
        extracted_ops = []
        for call_info in chain:
            op_type = SPARK_OPS.get(call_info["op"], OpType.TRANSFORMATION)
            # Use the df_name from the call_info if it was from a nested op,
            # otherwise use the anonymous name for the current chain
            df_name = call_info["df_name"] if call_info["df_name"] else anon_df_name
            node_id = f"{df_name}_{call_info['op']}_{lineno}"

            op_node = SparkOperationNode(
                id=node_id,
                df_name=df_name,
                operation=call_info["op"],
                parents=call_info["parents"],
                lineno=lineno,
                op_type=op_type,
            )
            extracted_ops.append(op_node)

        return extracted_ops, anon_df_name

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
        alias = df  # Variable aliasing
        result = df1.join(df2.filter(...), on="id")  # Nested expressions
        """

        # Ensures that the left-hand side of the assignment (node.targets[0]) is a simple variable name (ast.Name).
        # This restriction ensures that only straightforward assignments like df2 = ... are processed, excluding more complex patterns like tuple unpacking.
        if not isinstance(node.targets[0], ast.Name):
            return

        target_name = node.targets[0].id

        # Check for variable aliasing: other = df (right side is just a variable name)
        if isinstance(node.value, ast.Name):
            # This is an alias assignment like: alias = df
            source_name = node.value.id
            self.alias_map[target_name] = source_name
            self.generic_visit(node)
            return

        # Checking if the right-hand side of the assignment (node.value) is a function call (ast.Call).
        # If it is not, the method exits early
        if not isinstance(node.value, ast.Call):
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
        call_chain, nested_ops = self._extract_call_chain(node.value, node.lineno)
        if not call_chain and not nested_ops:
            return

        # First, add any nested operations that were extracted from arguments
        for nested_op in nested_ops:
            self.operations.append(nested_op)

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

    def _extract_call_chain(
        self, call: ast.Call, lineno: int
    ) -> Tuple[List[dict], List[SparkOperationNode]]:
        """
        Extracting the sequence of method calls (e.g., select, filter, groupBy, join)
        from a chained operation on a DataFrame.

        Also extracts nested operations from arguments to multi-parent operations.

        Args:
            call: The AST Call node to extract from
            lineno: Line number for generating unique IDs

        Returns:
            A tuple of:
            - List of operation dicts: [{"op": "select", "parents": ["df"]}, ...]
            - List of SparkOperationNode: Nested operations extracted from arguments
        """
        chain = []  # will store the extracted operations
        nested_operations = []  # operations extracted from nested expressions
        current = call  # starts as the call node (e.g., the filter or join call)
        base_df = None  # Tracks the base DataFrame for the entire chain
        tmp = current

        while isinstance(tmp, ast.Call):
            if isinstance(tmp.func, ast.Attribute):
                if isinstance(tmp.func.value, ast.Name):
                    # Resolve alias to get the original DataFrame
                    base_df = self._resolve_alias(tmp.func.value.id)
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
                        # Resolve alias to get the original DataFrame
                        base_df = self._resolve_alias(current.func.value.id)

                # Unary operations inherit the base DataFrame
                if op_name not in self.MULTI_PARENT_OPS:
                    if base_df:
                        parents.append(base_df)
                else:
                    # The object on which the method is called (e.g., df1.join(...))
                    if isinstance(current.func.value, ast.Name):
                        # Resolve alias to get the original DataFrame
                        parents.append(self._resolve_alias(current.func.value.id))

                    # Special handling for multi-parent operations like join
                    if op_name in self.MULTI_PARENT_OPS:
                        # Extract additional DataFrame arguments (e.g., df2 in df1.join(df2))
                        if current.args:
                            for arg in current.args:
                                if isinstance(arg, ast.Name):
                                    # Resolve alias for each argument
                                    parents.append(self._resolve_alias(arg.id))
                                elif isinstance(arg, ast.Call):
                                    # Check if this is a nested DataFrame operation chain
                                    extracted_ops, result_df = (
                                        self._extract_operations_from_expr(arg, lineno)
                                    )
                                    if extracted_ops:
                                        # Add the extracted operations to our list
                                        nested_operations.extend(extracted_ops)
                                    if result_df:
                                        # Use the anonymous df name as parent
                                        parents.append(result_df)
                                    elif (
                                        not extracted_ops
                                        and isinstance(arg.func, ast.Name)
                                        and arg.args
                                        and isinstance(arg.args[0], ast.Name)
                                    ):
                                        # Handle wrapper functions like broadcast(df)
                                        parents.append(
                                            self._resolve_alias(arg.args[0].id)
                                        )

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
        return list(reversed(chain)), nested_operations

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
            # Resolve alias to get the original DataFrame
            resolved_df = self._resolve_alias(base_df)
            # For write patterns, create a single write action node
            node_id = f"{resolved_df}_write_{node.lineno}"
            op_node = SparkOperationNode(
                id=node_id,
                df_name=resolved_df,
                operation="write",
                parents=[resolved_df],
                lineno=node.lineno,
                op_type=OpType.ACTION,
            )
            self.operations.append(op_node)
            self.generic_visit(node)
            return

        call_chain, nested_ops = self._extract_call_chain(node.value, node.lineno)
        if not call_chain and not nested_ops:
            return

        # Try to infer the DF name from the base call
        base_df = None
        current = node.value
        while isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute):
                if isinstance(current.func.value, ast.Name):
                    # Resolve alias to get the original DataFrame
                    base_df = self._resolve_alias(current.func.value.id)
                    break
                current = current.func.value
            else:
                break

        # First, add any nested operations
        for nested_op in nested_ops:
            self.operations.append(nested_op)

        for call in call_chain:
            node_id = f"{base_df}_{call['op']}_{node.lineno}"

            op_type = SPARK_OPS.get(call["op"], OpType.TRANSFORMATION)

            op_node = SparkOperationNode(
                id=node_id,
                df_name=base_df or "UNKNOWN",
                operation=call["op"],
                parents=call["parents"],  # Already resolved in _extract_call_chain
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

    def _resolve_alias(self, name: str) -> str:
        """
        Resolve a variable name through the alias chain to find the original DataFrame.

        Handles chains like: a = df; b = a; c = b
        _resolve_alias("c") -> "df"

        Includes cycle detection to prevent infinite loops.
        """
        visited = set()
        current = name

        while current in self.alias_map:
            if current in visited:
                # Cycle detected, return the current name to avoid infinite loop
                break
            visited.add(current)
            current = self.alias_map[current]

        return current
