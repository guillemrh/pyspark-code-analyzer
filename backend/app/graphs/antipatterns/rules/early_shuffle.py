from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding
from app.parsers.spark_semantics import DependencyType


class EarlyShuffleRule(AntiPatternRule):
    """
    Detects shuffles occurring early in a lineage.
    Note: Uses logical stage ordering, not Spark runtime stages.
    """
    rule_id = "EARLY_SHUFFLE"
    severity = "HIGH"

    def detect(self, dag):
        findings = []

        for node in dag.nodes.values():
            # Shuffle = wide dependency
            if node.dependency_type != DependencyType.WIDE:
                continue
            
            # Early = happens in stage 0 or 1
            parent_stages = [
                dag.nodes[p].stage_id
                for p in node.parents
                if dag.nodes[p].stage_id is not None
            ]

            if parent_stages:
                min_parent_stage = min(parent_stages)
                if node.stage_id - min_parent_stage <= 1:
                    findings.append(
                        AntiPatternFinding(
                            rule_id=self.rule_id,
                            severity=self.severity,
                            message=(
                                f"Shuffle happens early at stage {node.stage_id}. "
                                "Consider filtering or reducing data before this operation."
                            ),
                            nodes=[node.id],
                        )
                    )

        return findings
