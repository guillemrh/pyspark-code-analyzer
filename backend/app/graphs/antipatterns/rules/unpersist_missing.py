from typing import List, Optional, Dict, Set
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding


class UnpersistMissingRule(AntiPatternRule):
    """
    Detects when cache() or persist() is called on a DataFrame
    but unpersist() is never called, which can lead to memory leaks.
    """

    rule_id = "UNPERSIST_MISSING"
    severity = "MEDIUM"

    # Operations that cache data
    CACHE_OPS = {"cache", "persist"}

    def detect(
        self, dag, source_code: Optional[str] = None
    ) -> List[AntiPatternFinding]:
        findings = []

        # Track DataFrames that are cached/persisted
        cached_dfs: Dict[str, dict] = {}  # df_name -> {node_id, lineno}

        # Track DataFrames that are unpersisted
        unpersisted_dfs: Set[str] = set()

        for node in dag.nodes.values():
            df_name = None
            if node.op_node:
                df_name = node.op_node.df_name

            # Check for cache/persist operations
            if node.label in self.CACHE_OPS:
                if df_name:
                    cached_dfs[df_name] = {
                        "node_id": node.id,
                        "lineno": node.lineno,
                        "operation": node.label,
                    }
                else:
                    # If we can't determine the df_name, use the node id
                    cached_dfs[node.id] = {
                        "node_id": node.id,
                        "lineno": node.lineno,
                        "operation": node.label,
                    }

            # Check for unpersist operations
            if node.label == "unpersist":
                if df_name:
                    unpersisted_dfs.add(df_name)
                # Also check parent DataFrame names
                if node.op_node and node.op_node.parents:
                    for parent in node.op_node.parents:
                        unpersisted_dfs.add(parent)

        # Find cached DataFrames that were never unpersisted
        missing_unpersist = {
            df_name: info
            for df_name, info in cached_dfs.items()
            if df_name not in unpersisted_dfs
        }

        if missing_unpersist:
            node_ids = [info["node_id"] for info in missing_unpersist.values()]
            suggestion = self._build_contextual_suggestion(
                dag, missing_unpersist, source_code
            )
            findings.append(
                AntiPatternFinding(
                    rule_id=self.rule_id,
                    severity=self.severity,
                    message=(
                        f"DataFrame(s) cached but never unpersisted: "
                        f"{', '.join(missing_unpersist.keys())}. "
                        "This can lead to memory leaks."
                    ),
                    nodes=node_ids,
                    suggestion=suggestion,
                )
            )

        return findings

    def _build_contextual_suggestion(
        self,
        dag,
        missing_unpersist: Dict[str, dict],
        source_code: Optional[str],
    ) -> str:
        """Build a contextual suggestion using actual variable names."""
        suggestion = "# Cached DataFrames without unpersist():\n"

        for df_name, info in missing_unpersist.items():
            source_line = self._get_source_line(source_code, info["lineno"])
            if source_line:
                suggestion += f"#   Line {info['lineno']}: {source_line}\n"
            else:
                suggestion += (
                    f"#   Line {info['lineno']}: {df_name}.{info['operation']}()\n"
                )

        suggestion += """
# Cached DataFrames consume cluster memory until explicitly released.
# Without unpersist(), memory is held until the SparkContext ends or
# the DataFrame is garbage collected (which may not happen promptly).

# Best practice: Always call unpersist() when done with cached data:
"""

        # Add specific suggestions for each cached DataFrame
        for df_name, info in missing_unpersist.items():
            suggestion += f"""
# For '{df_name}':
{df_name} = {df_name}.{info['operation']}()  # Cache the DataFrame
# ... use {df_name} for computations ...
{df_name}.unpersist()  # Free memory when done
"""

        suggestion += """
# Alternative patterns:

# 1. Use a context manager pattern (custom implementation):
# with cached(df) as cached_df:
#     result1 = cached_df.filter(...).collect()
#     result2 = cached_df.groupBy(...).count()
# # unpersist() called automatically

# 2. Unpersist in a finally block:
# cached_df = df.cache()
# try:
#     result = cached_df.filter(...).collect()
# finally:
#     cached_df.unpersist()

# 3. Chain operations to avoid caching:
# If you only need the cached data once, avoid caching altogether:
# result = df.filter(...).groupBy(...).agg(...).collect()"""

        return suggestion
