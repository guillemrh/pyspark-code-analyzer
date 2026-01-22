from typing import List, Optional
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding
from app.parsers.spark_semantics import OpType


class MultipleActionsRule(AntiPatternRule):
    rule_id = "MULTIPLE_ACTIONS_SAME_LINEAGE"
    severity = "HIGH"

    def detect(
        self, dag, source_code: Optional[str] = None
    ) -> List[AntiPatternFinding]:
        findings = []

        actions = [node for node in dag.nodes.values() if node.op_type == OpType.ACTION]

        lineage_map = {}

        for action in actions:
            root = self._lineage_root(dag, action.id)
            lineage_map.setdefault(root, []).append(action.id)

        for root, action_ids in lineage_map.items():
            if len(action_ids) > 1:
                suggestion = self._build_contextual_suggestion(
                    dag, root, action_ids, source_code
                )
                findings.append(
                    AntiPatternFinding(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            "Multiple actions detected on the same lineage without caching"
                        ),
                        nodes=action_ids,
                        suggestion=suggestion,
                    )
                )

        return findings

    def _build_contextual_suggestion(
        self,
        dag,
        root_id: str,
        action_ids: List[str],
        source_code: Optional[str],
    ) -> str:
        """Build a contextual suggestion using actual variable names."""
        # Get the root node info (where cache should be added)
        root_node = dag.nodes[root_id]
        root_df_name = None
        root_lineno = root_node.lineno
        if root_node.op_node:
            root_df_name = root_node.op_node.df_name

        # Get info about the action nodes
        action_details = []
        for action_id in action_ids:
            action_node = dag.nodes[action_id]
            action_info = {
                "label": action_node.label,
                "lineno": action_node.lineno,
                "df_name": None,
            }
            if action_node.op_node:
                action_info["df_name"] = action_node.op_node.df_name
            action_details.append(action_info)

        # Get source lines if available
        root_source_line = self._get_source_line(source_code, root_lineno)

        if root_df_name and action_details:
            suggestion = f"# Multiple actions on DataFrame '{root_df_name}' (line {root_lineno}):\n"
            for detail in action_details:
                action_source_line = self._get_source_line(
                    source_code, detail["lineno"]
                )
                if action_source_line:
                    suggestion += f"#   Line {detail['lineno']}: {action_source_line}\n"
                else:
                    suggestion += f"#   Line {detail['lineno']}: {detail['label']}()\n"

            suggestion += f"""
# Each action recomputes the entire lineage. Add .cache() to avoid recomputation:

# Current code (line {root_lineno}):"""
            if root_source_line:
                suggestion += f"""
# {root_source_line}

# Suggested fix - add .cache():
# {root_source_line.rstrip()}.cache()"""
            else:
                suggestion += f"""
# {root_df_name} = ...

# Suggested fix:
{root_df_name} = {root_df_name}.cache()"""

            suggestion += f"""

# This ensures '{root_df_name}' is computed once and reused for all actions."""
        else:
            # Fall back to generic suggestion
            suggestion = (
                "Add .cache() before the first action to avoid recomputation:\n"
                "  # Instead of:\n"
                "  df = spark.read.parquet('data/')\n"
                "  df = df.filter(...).groupBy(...).agg(...)\n"
                "  df.count()    # Action 1 - computes full lineage\n"
                "  df.show()     # Action 2 - recomputes full lineage!\n"
                "  \n"
                "  # Use:\n"
                "  df = spark.read.parquet('data/')\n"
                "  df = df.filter(...).groupBy(...).agg(...).cache()\n"
                "  df.count()    # Action 1 - computes and caches\n"
                "  df.show()     # Action 2 - reads from cache"
            )

        return suggestion

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
