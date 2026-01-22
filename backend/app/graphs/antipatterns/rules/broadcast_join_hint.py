from typing import List, Optional
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding


class BroadcastJoinHintRule(AntiPatternRule):
    """
    Detects join operations and suggests considering broadcast hints.

    When joining a large DataFrame with a small one, broadcasting the smaller
    DataFrame to all executors avoids expensive shuffle operations. This is
    done using broadcast() hint: df1.join(broadcast(df2), ...).

    Since static analysis cannot determine DataFrame sizes, this rule flags
    all joins as informational suggestions to consider broadcast joins when
    one table is significantly smaller than the other.

    Broadcast joins are beneficial when:
    - One table is small enough to fit in executor memory (typically < 10MB)
    - You want to avoid shuffle overhead
    - The small table is used in multiple joins
    """

    rule_id = "BROADCAST_JOIN_HINT"
    severity = "INFO"

    def detect(
        self, dag, source_code: Optional[str] = None
    ) -> List[AntiPatternFinding]:
        findings = []

        for node in dag.nodes.values():
            if node.label == "join":
                suggestion = self._build_contextual_suggestion(node, source_code)
                findings.append(
                    AntiPatternFinding(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            "Join operation detected. If one table is significantly "
                            "smaller than the other (< 10MB), consider using "
                            "broadcast(small_df) to avoid shuffle overhead. "
                            "Example: df1.join(broadcast(small_df), ...)."
                        ),
                        nodes=[node.id],
                        suggestion=suggestion,
                    )
                )

        return findings

    def _build_contextual_suggestion(self, node, source_code: Optional[str]) -> str:
        """Build a contextual suggestion for join operations."""
        df_name = None
        lineno = node.lineno
        parents = []
        if node.op_node:
            df_name = node.op_node.df_name
            parents = node.op_node.parents or []

        source_line = self._get_source_line(source_code, lineno)

        if df_name and parents:
            # The first parent is typically the left side of the join
            left_df = parents[0] if len(parents) > 0 else "left_df"
            right_df = parents[1] if len(parents) > 1 else "right_df"

            suggestion = f"# Line {lineno}: Join operation on '{df_name}'"
            if source_line:
                suggestion = f"# Line {lineno}: {source_line}"
            suggestion += f"""

# If one of the DataFrames ('{left_df}' or '{right_df}') is small (< 10MB),
# consider using broadcast() to avoid shuffle overhead:

from pyspark.sql.functions import broadcast

# If '{right_df}' is the smaller DataFrame:
{df_name} = {left_df}.join(broadcast({right_df}), 'key_column')

# If '{left_df}' is the smaller DataFrame:
{df_name} = {right_df}.join(broadcast({left_df}), 'key_column')

# Broadcast joins are beneficial when:
# - The small table fits in executor memory
# - You want to avoid shuffle overhead
# - The small table is used in multiple joins"""
        elif df_name:
            suggestion = f"# Line {lineno}"
            if source_line:
                suggestion = f"# Line {lineno}: {source_line}"
            suggestion += f"""

# If one DataFrame in this join is small (< 10MB), use broadcast():

from pyspark.sql.functions import broadcast

# Wrap the smaller DataFrame with broadcast():
{df_name} = large_df.join(broadcast(small_df), 'key_column')"""
        else:
            suggestion = (
                "Use broadcast() for the smaller DataFrame:\n"
                "  from pyspark.sql.functions import broadcast\n"
                "  # Instead of: large_df.join(small_df, 'key')\n"
                "  # Use: large_df.join(broadcast(small_df), 'key')\n"
                "  \n"
                "  # Full example:\n"
                "  result = orders_df.join(\n"
                "      broadcast(countries_df),  # small lookup table\n"
                "      orders_df.country_id == countries_df.id,\n"
                "      'left'\n"
                "  )"
            )

        return suggestion
