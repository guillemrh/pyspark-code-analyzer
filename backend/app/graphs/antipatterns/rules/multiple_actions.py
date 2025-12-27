from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding
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

        lineage_map = {}

        for action in actions:
            root = self._lineage_root(dag, action.id)
            lineage_map.setdefault(root, []).append(action.id)

        for root, action_ids in lineage_map.items():
            if len(action_ids) > 1:
                findings.append(
                    AntiPatternFinding(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            "Multiple actions detected on the same lineage without caching"
                        ),
                        nodes=action_ids,
                    )
                )

        return findings

    def _lineage_root(self, dag, node_id):
        """
        Walk upstream until we reach the first transformation
        (or root of DAG).
        """
        current = node_id

        while True:
            parents = dag.nodes[current].parents
            if not parents:
                return current

            parent_id = next(iter(parents))
            parent = dag.nodes[parent_id]

            if parent.op_type == OpType.ACTION:
                current = parent_id
                continue

            return parent_id
