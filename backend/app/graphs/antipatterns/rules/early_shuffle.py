from typing import List, Optional
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding
from app.parsers.spark_semantics import DependencyType


class EarlyShuffleRule(AntiPatternRule):
    """
    Detects shuffles occurring early in a lineage.
    Note: Uses logical stage ordering, not Spark runtime stages.
    """

    rule_id = "EARLY_SHUFFLE"
    severity = "HIGH"

    def detect(
        self, dag, source_code: Optional[str] = None
    ) -> List[AntiPatternFinding]:
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
                    suggestion = self._build_contextual_suggestion(
                        dag, node, source_code
                    )
                    findings.append(
                        AntiPatternFinding(
                            rule_id=self.rule_id,
                            severity=self.severity,
                            message=(
                                f"Shuffle happens early at stage {node.stage_id}. "
                                "Consider filtering or reducing data before this operation."
                            ),
                            nodes=[node.id],
                            suggestion=suggestion,
                        )
                    )

        return findings

    def _build_contextual_suggestion(
        self, dag, node, source_code: Optional[str]
    ) -> str:
        """Build a contextual suggestion using actual variable names."""
        # Extract variable names from the operation node
        df_name = None
        parent_name = None
        lineno = node.lineno
        operation = node.label

        if node.op_node:
            df_name = node.op_node.df_name
            if node.op_node.parents:
                parent_name = node.op_node.parents[0]

        # Get the actual source line if available
        source_line = self._get_source_line(source_code, lineno)

        if df_name and parent_name:
            suggestion = (
                f"# Line {lineno}: Shuffle operation '{operation}' on '{parent_name}'"
            )
            if source_line:
                suggestion = f"# Line {lineno}: {source_line}"
            suggestion += f"""

# The shuffle operation '{operation}' happens early in the pipeline.
# Consider filtering or reducing data before this operation:

# Suggested fix - filter and select before the shuffle:
{df_name} = {parent_name}.filter(F.col('column') > value)  # Reduce rows first
{df_name} = {df_name}.select('col1', 'col2')               # Select only needed columns
{df_name} = {df_name}.{operation}(...)                      # Then perform shuffle"""
        elif df_name:
            suggestion = f"# Line {lineno}"
            if source_line:
                suggestion = f"# Line {lineno}: {source_line}"
            suggestion += f"""

# The shuffle operation '{operation}' happens early in the pipeline.
# Consider filtering or reducing data before this operation:

# Suggested fix:
{df_name} = {df_name}.filter(F.col('column') > value)  # Reduce rows first
{df_name} = {df_name}.select('col1', 'col2')           # Select only needed columns"""
        else:
            # Fall back to generic suggestion
            suggestion = (
                "Move filter/select operations before the shuffle:\n"
                f"  # Current: shuffle ({operation}) happens early\n"
                "  # Instead of:\n"
                "  df.groupBy('category').agg(F.sum('amount'))\n"
                "  \n"
                "  # Filter first to reduce shuffle data:\n"
                "  df.filter(F.col('date') > '2024-01-01')  # Reduce rows\n"
                "    .select('category', 'amount')          # Reduce columns\n"
                "    .groupBy('category').agg(F.sum('amount'))"
            )

        return suggestion
