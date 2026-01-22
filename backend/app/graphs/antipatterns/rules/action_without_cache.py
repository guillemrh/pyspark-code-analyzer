from typing import List, Set, Optional
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding
from app.parsers.spark_semantics import OpType


class ActionWithoutCacheRule(AntiPatternRule):
    """
    Detects Spark actions executed without caching
    when their lineage is reused.
    """

    rule_id = "ACTION_WITHOUT_CACHE"
    severity = "MEDIUM"

    def detect(
        self, dag, source_code: Optional[str] = None
    ) -> List[AntiPatternFinding]:
        findings = []

        for node in dag.nodes.values():
            if node.op_type != OpType.ACTION:
                continue

            ancestors = self._get_ancestors(dag, node.id)

            # If any ancestor has multiple children, lineage is reused
            reused_ancestors = [a for a in ancestors if len(dag.nodes[a].children) > 1]

            if reused_ancestors:
                suggestion = self._build_contextual_suggestion(
                    dag, node, reused_ancestors, source_code
                )
                findings.append(
                    AntiPatternFinding(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            "Action executed on a reused lineage without caching. "
                            "Consider using cache() or persist()."
                        ),
                        nodes=[node.id],
                        suggestion=suggestion,
                    )
                )

        return findings

    def _build_contextual_suggestion(
        self, dag, node, reused_ancestors: List[str], source_code: Optional[str]
    ) -> str:
        """Build a contextual suggestion using actual variable names."""
        # Get the action node info
        action_name = node.label
        action_lineno = node.lineno
        action_df_name = None
        if node.op_node:
            action_df_name = node.op_node.df_name

        # Get the branching point (first reused ancestor)
        branch_node = dag.nodes[reused_ancestors[0]] if reused_ancestors else None
        branch_df_name = None
        branch_lineno = None
        if branch_node and branch_node.op_node:
            branch_df_name = branch_node.op_node.df_name
            branch_lineno = branch_node.lineno

        # Get source lines if available
        action_source_line = self._get_source_line(source_code, action_lineno)
        branch_source_line = (
            self._get_source_line(source_code, branch_lineno) if branch_lineno else None
        )

        if branch_df_name and action_df_name:
            suggestion = (
                f"# Line {action_lineno}: Action '{action_name}' on reused DataFrame"
            )
            if action_source_line:
                suggestion = f"# Line {action_lineno}: {action_source_line}"
            suggestion += f"""

# The DataFrame '{branch_df_name}' (line {branch_lineno}) is used multiple times.
# Add .cache() at the branching point to avoid recomputation:"""
            if branch_source_line:
                suggestion += f"""

# Current code:
# {branch_source_line}

# Suggested fix - add .cache():
# {branch_source_line.rstrip()}.cache()"""
            else:
                suggestion += f"""

# Suggested fix:
{branch_df_name} = {branch_df_name}.cache()  # Cache before branching"""

            suggestion += f"""

# Alternative - use persist() for more control:
from pyspark import StorageLevel
{branch_df_name} = {branch_df_name}.persist(StorageLevel.MEMORY_AND_DISK)"""
        else:
            # Fall back to generic suggestion
            suggestion = (
                "Add .cache() or .persist() at the branching point:\n"
                "  # Instead of:\n"
                "  base_df = df.filter(...).select(...)\n"
                "  result1 = base_df.groupBy('a').count()  # Recomputes base_df\n"
                "  result2 = base_df.groupBy('b').sum()    # Recomputes base_df again!\n"
                "  \n"
                "  # Use cache() for memory storage:\n"
                "  base_df = df.filter(...).select(...).cache()\n"
                "  result1 = base_df.groupBy('a').count()  # Computes and caches\n"
                "  result2 = base_df.groupBy('b').sum()    # Reads from cache\n"
                "  \n"
                "  # Or persist() for more control:\n"
                "  from pyspark import StorageLevel\n"
                "  base_df = base_df.persist(StorageLevel.MEMORY_AND_DISK)"
            )

        return suggestion

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
