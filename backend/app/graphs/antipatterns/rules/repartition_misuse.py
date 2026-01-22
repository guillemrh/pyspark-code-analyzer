from typing import List
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding
from app.parsers.spark_semantics import DependencyType


class RepartitionMisuseRule(AntiPatternRule):
    """
    Detects repartition() followed by another shuffle,
    indicating redundant or unnecessary repartitioning.
    """

    rule_id = "REPARTITION_MISUSE"
    severity = "MEDIUM"

    def detect(self, dag) -> List[AntiPatternFinding]:
        findings = []

        for node in dag.nodes.values():
            if node.label != "repartition":
                continue

            # repartition itself is always wide
            if node.dependency_type != DependencyType.WIDE:
                continue

            for child_id in node.children:
                child = dag.nodes[child_id]

                # If repartition is followed immediately by another shuffle
                if child.dependency_type == DependencyType.WIDE:
                    findings.append(
                        AntiPatternFinding(
                            rule_id=self.rule_id,
                            severity=self.severity,
                            message=(
                                "repartition() followed by another shuffle operation. "
                                "This repartition is likely unnecessary."
                            ),
                            nodes=[node.id, child.id],
                            suggestion=(
                                "Remove the unnecessary repartition or use coalesce:\n"
                                "  # Redundant pattern (two shuffles):\n"
                                "  df.repartition(100).groupBy('key').count()\n"
                                "  \n"
                                "  # Better: let groupBy handle partitioning:\n"
                                "  df.groupBy('key').count()\n"
                                "  \n"
                                "  # To reduce partitions (no shuffle), use coalesce:\n"
                                "  df.coalesce(10)  # Reduces partitions without full shuffle\n"
                                "  \n"
                                "  # Only use repartition when you need to:\n"
                                "  # - Increase partitions (coalesce can only decrease)\n"
                                "  # - Partition by specific columns for subsequent joins"
                            ),
                        )
                    )

        return findings
