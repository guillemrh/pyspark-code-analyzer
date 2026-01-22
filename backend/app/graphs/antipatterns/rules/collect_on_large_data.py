from typing import List
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

    def detect(self, dag) -> List[AntiPatternFinding]:
        findings = []

        for node in dag.nodes.values():
            if node.label == "collect":
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
                    )
                )

        return findings
