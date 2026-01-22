from typing import List
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

    def detect(self, dag) -> List[AntiPatternFinding]:
        findings = []

        for node in dag.nodes.values():
            # Direct UDF operation detection (if label contains 'udf')
            if "udf" in node.label.lower():
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
                    )
                )
            # Flag withColumn operations as potential UDF usage points
            elif node.label in self.UDF_PRONE_OPS:
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
                    )
                )

        return findings
