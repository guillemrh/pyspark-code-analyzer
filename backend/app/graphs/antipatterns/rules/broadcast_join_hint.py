from typing import List
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

    def detect(self, dag) -> List[AntiPatternFinding]:
        findings = []

        for node in dag.nodes.values():
            if node.label == "join":
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
                        suggestion=(
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
                        ),
                    )
                )

        return findings
