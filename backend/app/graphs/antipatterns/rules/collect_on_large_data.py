from typing import List, Optional
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding


class CollectOnLargeDataRule(AntiPatternRule):
    """
    Detects collect() calls which can crash the driver if data is large.

    The collect() action brings all data from executors to the driver node,
    which can cause OutOfMemoryError if the dataset is large. This is a
    common anti-pattern, especially when working with production-scale data.
    """

    rule_id = "COLLECT_ON_LARGE_DATA"
    severity = "WARNING"

    def detect(
        self, dag, source_code: Optional[str] = None
    ) -> List[AntiPatternFinding]:
        findings = []

        for node in dag.nodes.values():
            if node.label == "collect":
                suggestion = self._build_contextual_suggestion(node, source_code)
                findings.append(
                    AntiPatternFinding(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            "collect() brings all data to the driver and can cause "
                            "OutOfMemoryError on large datasets. Consider using "
                            "take(n), show(), or write() instead."
                        ),
                        nodes=[node.id],
                        suggestion=suggestion,
                    )
                )

        return findings

    def _build_contextual_suggestion(self, node, source_code: Optional[str]) -> str:
        """Build a contextual suggestion using actual variable names."""
        # Extract variable names from the operation node
        df_name = None
        parent_name = None
        lineno = node.lineno

        if node.op_node:
            df_name = node.op_node.df_name
            if node.op_node.parents:
                parent_name = node.op_node.parents[0]

        # Get the actual source line if available
        source_line = self._get_source_line(source_code, lineno)

        # Build contextual suggestion
        if df_name and parent_name:
            suggestion = f"# Line {lineno}: {df_name} = {parent_name}.collect()"
            if source_line:
                suggestion = f"# Line {lineno}: {source_line}"
            suggestion += f"""
# Suggested fix - use one of these alternatives:
{df_name} = {parent_name}.take(100)           # Get first 100 rows as list
{df_name} = {parent_name}.limit(100).collect() # Collect only 100 rows
{parent_name}.show(20)                        # Display 20 rows in console
{parent_name}.write.parquet('output/')        # Write to storage instead"""
        elif df_name:
            suggestion = f"# Line {lineno}"
            if source_line:
                suggestion = f"# Line {lineno}: {source_line}"
            suggestion += f"""
# Suggested fix - use one of these alternatives:
{df_name}.take(100)           # Get first 100 rows as list
{df_name}.limit(100).collect() # Collect only 100 rows
{df_name}.show(20)            # Display 20 rows in console
{df_name}.write.parquet('output/')  # Write to storage instead"""
        else:
            # Fall back to generic suggestion
            suggestion = (
                "Replace collect() with a safer alternative:\n"
                "  # Instead of: df.collect()\n"
                "  df.take(100)      # Get first 100 rows as list\n"
                "  df.show(20)       # Display 20 rows in console\n"
                "  df.limit(n).collect()  # Collect only n rows\n"
                "  df.write.parquet('output/')  # Write to storage"
            )

        return suggestion
