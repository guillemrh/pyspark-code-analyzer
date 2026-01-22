from typing import List, Optional
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding


class UDFUsageRule(AntiPatternRule):
    """
    Detects potential UDF usage patterns.

    User Defined Functions (UDFs) in PySpark require serialization between
    the JVM and Python, which adds significant overhead. Native Spark SQL
    functions are optimized and run entirely in the JVM, making them much
    faster.

    This rule flags operations that commonly use UDFs:
    - withColumn: Often used with UDFs to create new columns
    - Operations with 'udf' in the label

    Note: Full UDF detection would require deeper AST analysis of function
    arguments. This rule provides guidance for common patterns.
    """

    rule_id = "UDF_USAGE"
    severity = "INFO"

    # Operations that commonly use UDFs
    UDF_PRONE_OPS = {"withColumn", "withColumns"}

    def detect(
        self, dag, source_code: Optional[str] = None
    ) -> List[AntiPatternFinding]:
        findings = []

        for node in dag.nodes.values():
            # Direct UDF operation detection (if label contains 'udf')
            if "udf" in node.label.lower():
                suggestion = self._build_udf_suggestion(node, source_code)
                findings.append(
                    AntiPatternFinding(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            "UDF detected. Consider using native Spark SQL functions "
                            "when possible for better performance. UDFs require "
                            "serialization between JVM and Python."
                        ),
                        nodes=[node.id],
                        suggestion=suggestion,
                    )
                )
            # Flag withColumn operations as potential UDF usage points
            elif node.label in self.UDF_PRONE_OPS:
                suggestion = self._build_withcolumn_suggestion(node, source_code)
                findings.append(
                    AntiPatternFinding(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            f"{node.label}() detected. If using a UDF here, consider "
                            "replacing with native Spark SQL functions (from "
                            "pyspark.sql.functions) for better performance."
                        ),
                        nodes=[node.id],
                        suggestion=suggestion,
                    )
                )

        return findings

    def _build_udf_suggestion(self, node, source_code: Optional[str]) -> str:
        """Build a contextual suggestion for UDF usage."""
        df_name = None
        lineno = node.lineno
        if node.op_node:
            df_name = node.op_node.df_name

        source_line = self._get_source_line(source_code, lineno)

        if df_name:
            suggestion = f"# Line {lineno}: UDF detected on DataFrame '{df_name}'"
            if source_line:
                suggestion = f"# Line {lineno}: {source_line}"
            suggestion += f"""

# UDFs require serialization between JVM and Python, causing significant overhead.
# Check if a native Spark function exists:

from pyspark.sql import functions as F

# Common native function replacements:
# String operations:
{df_name}.withColumn('col', F.upper(F.col('col')))     # Instead of custom upper UDF
{df_name}.withColumn('col', F.lower(F.col('col')))     # Instead of custom lower UDF
{df_name}.withColumn('col', F.trim(F.col('col')))      # Instead of custom trim UDF

# Math operations:
{df_name}.withColumn('col', F.abs(F.col('col')))       # Instead of abs UDF
{df_name}.withColumn('col', F.round(F.col('col'), 2))  # Instead of round UDF

# Date operations:
{df_name}.withColumn('col', F.to_date(F.col('col')))   # Instead of date parsing UDF"""
        else:
            suggestion = (
                "Check if a native Spark function exists:\n"
                "  from pyspark.sql import functions as F\n"
                "  # Instead of: @udf def upper(s): return s.upper()\n"
                "  #             df.withColumn('col', upper(df.col))\n"
                "  # Use native: df.withColumn('col', F.upper(df.col))\n"
                "  # Common replacements:\n"
                "  #   String: F.upper, F.lower, F.trim, F.regexp_replace\n"
                "  #   Math: F.abs, F.round, F.ceil, F.floor\n"
                "  #   Date: F.date_format, F.datediff, F.to_date"
            )

        return suggestion

    def _build_withcolumn_suggestion(self, node, source_code: Optional[str]) -> str:
        """Build a contextual suggestion for withColumn usage."""
        df_name = None
        lineno = node.lineno
        operation = node.label
        if node.op_node:
            df_name = node.op_node.df_name

        source_line = self._get_source_line(source_code, lineno)

        if df_name:
            suggestion = f"# Line {lineno}: {operation}() on DataFrame '{df_name}'"
            if source_line:
                suggestion = f"# Line {lineno}: {source_line}"
            suggestion += f"""

# If using a UDF in this {operation}, consider native Spark functions instead:

from pyspark.sql import functions as F

# Instead of a custom UDF:
# {df_name}.{operation}('new_col', my_udf(F.col('col')))

# Try native functions:
{df_name}.{operation}('new_col', F.when(F.col('col') > 0, 'positive').otherwise('negative'))
{df_name}.{operation}('new_col', F.concat(F.col('a'), F.lit('-'), F.col('b')))
{df_name}.{operation}('new_col', F.regexp_replace(F.col('col'), 'pattern', 'replacement'))"""
        else:
            suggestion = (
                f"If using a UDF in this {operation}, check for native alternatives:\n"
                "  from pyspark.sql import functions as F\n"
                "  # Instead of custom UDF:\n"
                "  # df.withColumn('new_col', my_udf(df.col))\n"
                "  # Try native functions:\n"
                "  df.withColumn('new_col', F.when(F.col('col') > 0, 'positive')"
                ".otherwise('negative'))\n"
                "  df.withColumn('new_col', F.concat(F.col('a'), F.lit('-'), F.col('b')))"
            )

        return suggestion
