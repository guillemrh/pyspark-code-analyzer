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
                        )
                    )

        return findings
