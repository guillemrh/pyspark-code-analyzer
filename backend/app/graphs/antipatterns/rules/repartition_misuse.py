from typing import List, Optional
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding
from app.parsers.spark_semantics import DependencyType


class RepartitionMisuseRule(AntiPatternRule):
    """
    Detects repartition() followed by another shuffle,
    indicating redundant or unnecessary repartitioning.
    """

    rule_id = "REPARTITION_MISUSE"
    severity = "MEDIUM"

    def detect(
        self, dag, source_code: Optional[str] = None
    ) -> List[AntiPatternFinding]:
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
                    suggestion = self._build_contextual_suggestion(
                        node, child, source_code
                    )
                    findings.append(
                        AntiPatternFinding(
                            rule_id=self.rule_id,
                            severity=self.severity,
                            message=(
                                "repartition() followed by another shuffle operation. "
                                "This repartition is likely unnecessary."
                            ),
                            nodes=[node.id, child.id],
                            suggestion=suggestion,
                        )
                    )

        return findings

    def _build_contextual_suggestion(
        self, repartition_node, shuffle_node, source_code: Optional[str]
    ) -> str:
        """Build a contextual suggestion using actual variable names."""
        # Get repartition node info
        repartition_df_name = None
        repartition_lineno = repartition_node.lineno
        repartition_parent = None
        if repartition_node.op_node:
            repartition_df_name = repartition_node.op_node.df_name
            if repartition_node.op_node.parents:
                repartition_parent = repartition_node.op_node.parents[0]

        # Get shuffle node info
        shuffle_label = shuffle_node.label
        shuffle_df_name = None
        shuffle_lineno = shuffle_node.lineno
        if shuffle_node.op_node:
            shuffle_df_name = shuffle_node.op_node.df_name

        # Get source lines if available
        repartition_source_line = self._get_source_line(source_code, repartition_lineno)
        shuffle_source_line = self._get_source_line(source_code, shuffle_lineno)

        if repartition_df_name and shuffle_df_name:
            suggestion = f"""# Line {repartition_lineno}: repartition() followed by '{shuffle_label}' (line {shuffle_lineno})"""
            if repartition_source_line:
                suggestion += f"""
# {repartition_source_line}"""
            if shuffle_source_line:
                suggestion += f"""
# {shuffle_source_line}"""

            parent_ref = (
                repartition_parent if repartition_parent else repartition_df_name
            )
            suggestion += f"""

# The repartition is unnecessary because '{shuffle_label}' will shuffle the data anyway.
# Suggested fix - remove the repartition:

# Instead of:
# {parent_ref}.repartition(...).{shuffle_label}(...)

# Just use:
{shuffle_df_name} = {parent_ref}.{shuffle_label}(...)

# If you need to reduce partitions, use coalesce (no shuffle):
{shuffle_df_name} = {parent_ref}.{shuffle_label}(...).coalesce(10)"""
        else:
            # Fall back to generic suggestion
            suggestion = (
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
            )

        return suggestion
