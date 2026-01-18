from typing import List, Set
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding
from app.parsers.spark_semantics import OpType


class ActionWithoutCacheRule(AntiPatternRule):
    """
    Detects Spark actions executed without caching
    when their lineage is reused.
    """

    rule_id = "ACTION_WITHOUT_CACHE"
    severity = "MEDIUM"

    def detect(self, dag) -> List[AntiPatternFinding]:
        findings = []

        for node in dag.nodes.values():
            if node.op_type != OpType.ACTION:
                continue

            ancestors = self._get_ancestors(dag, node.id)

            # If any ancestor has multiple children, lineage is reused
            reused = any(len(dag.nodes[a].children) > 1 for a in ancestors)

            if reused:
                findings.append(
                    AntiPatternFinding(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            "Action executed on a reused lineage without caching. "
                            "Consider using cache() or persist()."
                        ),
                        nodes=[node.id],
                    )
                )

        return findings

    def _get_ancestors(self, dag, node_id: str) -> Set[str]:
        visited = set()
        stack = [node_id]

        while stack:
            current = stack.pop()
            for parent in dag.nodes[current].parents:
                if parent not in visited:
                    visited.add(parent)
                    stack.append(parent)

        return visited
