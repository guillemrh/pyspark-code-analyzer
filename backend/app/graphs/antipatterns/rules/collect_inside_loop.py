from typing import List, Optional
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding
from app.parsers.spark_semantics import OpType


class CollectInsideLoopRule(AntiPatternRule):
    """
    Detects patterns where collect() is called multiple times,
    which is a common anti-pattern that can indicate collect() being
    called inside loops or repeated unnecessary data collection.
    """

    rule_id = "MULTIPLE_COLLECT_CALLS"
    severity = "HIGH"

    # Threshold for number of collect() calls to trigger warning
    COLLECT_THRESHOLD = 2

    def detect(
        self, dag, source_code: Optional[str] = None
    ) -> List[AntiPatternFinding]:
        findings = []

        # Find all collect() action nodes
        collect_nodes = [
            node
            for node in dag.nodes.values()
            if node.label == "collect" and node.op_type == OpType.ACTION
        ]

        # If more than COLLECT_THRESHOLD collect() calls, flag it
        if len(collect_nodes) > self.COLLECT_THRESHOLD:
            node_ids = [node.id for node in collect_nodes]
            suggestion = self._build_contextual_suggestion(
                dag, collect_nodes, source_code
            )
            findings.append(
                AntiPatternFinding(
                    rule_id=self.rule_id,
                    severity=self.severity,
                    message=(
                        f"Multiple collect() calls detected ({len(collect_nodes)} calls). "
                        "This pattern often indicates collect() inside a loop or "
                        "unnecessary data collection to the driver."
                    ),
                    nodes=node_ids,
                    suggestion=suggestion,
                )
            )

        return findings

    def _build_contextual_suggestion(
        self, dag, collect_nodes: List, source_code: Optional[str]
    ) -> str:
        """Build a contextual suggestion using actual variable names."""
        # Collect details about each collect() call
        collect_details = []
        df_names = set()

        for node in collect_nodes:
            detail = {
                "lineno": node.lineno,
                "df_name": None,
            }
            if node.op_node:
                detail["df_name"] = node.op_node.df_name
                if node.op_node.df_name:
                    df_names.add(node.op_node.df_name)
            collect_details.append(detail)

        # Build suggestion with specific line references
        suggestion = f"# {len(collect_nodes)} collect() calls detected:\n"

        for detail in collect_details:
            source_line = self._get_source_line(source_code, detail["lineno"])
            if source_line:
                suggestion += f"#   Line {detail['lineno']}: {source_line}\n"
            elif detail["df_name"]:
                suggestion += (
                    f"#   Line {detail['lineno']}: {detail['df_name']}.collect()\n"
                )
            else:
                suggestion += f"#   Line {detail['lineno']}: .collect()\n"

        suggestion += """
# Multiple collect() calls bring data to the driver, which can cause:
# - Out of memory errors on the driver
# - Network bottlenecks transferring large amounts of data
# - Loss of parallelism benefits

# Consider these alternatives:

# 1. Combine operations and collect once:
# Instead of:
#   result1 = df.filter(cond1).collect()
#   result2 = df.filter(cond2).collect()
# Use:
#   combined = df.withColumn("group", F.when(cond1, "group1").when(cond2, "group2"))
#   results = combined.filter(F.col("group").isNotNull()).collect()

# 2. Use broadcast variables for lookups:
# Instead of:
#   lookup_data = lookup_df.collect()  # Collecting for lookup
#   result = main_df.filter(...)  # Then filtering
# Use:
#   from pyspark.sql.functions import broadcast
#   result = main_df.join(broadcast(lookup_df), on="key")

# 3. Use Spark aggregations instead of collecting and processing:
# Instead of:
#   data = df.collect()
#   total = sum(row['value'] for row in data)
# Use:
#   total = df.agg(F.sum('value')).collect()[0][0]

# 4. If iterating over collect() results, use foreachPartition:
# Instead of:
#   for row in df.collect():
#       process(row)
# Use:
#   df.foreachPartition(lambda partition: [process(row) for row in partition])"""

        return suggestion
