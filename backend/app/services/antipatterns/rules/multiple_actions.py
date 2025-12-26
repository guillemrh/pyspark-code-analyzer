from app.services.antipatterns.base import AntiPatternRule, AntiPatternFinding
from app.parsers.spark_semantics import OpType


class MultipleActionsRule(AntiPatternRule):
    rule_id = "MULTIPLE_ACTIONS_SAME_LINEAGE"
    severity = "HIGH"

    def detect(self, dag):
        findings = []

        actions = [
            node for node in dag.nodes.values()
            if node.op_type == OpType.ACTION
        ]

        for i, a1 in enumerate(actions):
            for a2 in actions[i + 1:]:
                if self._share_ancestor(dag, a1.id, a2.id):
                    findings.append(
                        AntiPatternFinding(
                            rule_id=self.rule_id,
                            severity=self.severity,
                            message="Multiple actions detected on the same lineage without caching",
                            nodes=[a1.id, a2.id],
                        )
                    )

        return findings

    def _share_ancestor(self, dag, id1, id2):
        def ancestors(node_id):
            seen = set()
            stack = list(dag.nodes[node_id].parents)
            while stack:
                cur = stack.pop()
                if cur not in seen:
                    seen.add(cur)
                    stack.extend(dag.nodes[cur].parents)
            return seen

        return bool(ancestors(id1) & ancestors(id2))
