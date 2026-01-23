from typing import List, Optional
from app.graphs.antipatterns.base import AntiPatternRule, AntiPatternFinding


class CartesianJoinRule(AntiPatternRule):
    """
    Detects cartesian join operations (crossJoin) which create
    expensive cartesian products between DataFrames.
    """

    rule_id = "CARTESIAN_JOIN"
    severity = "HIGH"

    def detect(
        self, dag, source_code: Optional[str] = None
    ) -> List[AntiPatternFinding]:
        findings = []

        for node in dag.nodes.values():
            # Detect crossJoin operations
            if node.label == "crossJoin":
                suggestion = self._build_contextual_suggestion(dag, node, source_code)
                findings.append(
                    AntiPatternFinding(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=(
                            "Cartesian join (crossJoin) detected. This creates a "
                            "cartesian product which can be extremely expensive "
                            "for large DataFrames."
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
        df_name = None
        parent_names = []
        lineno = node.lineno

        if node.op_node:
            df_name = node.op_node.df_name
            parent_names = node.op_node.parents if node.op_node.parents else []

        # Get the actual source line if available
        source_line = self._get_source_line(source_code, lineno)

        if df_name and len(parent_names) >= 2:
            suggestion = f"# Line {lineno}: Cartesian join detected"
            if source_line:
                suggestion = f"# Line {lineno}: {source_line}"
            suggestion += f"""

# crossJoin creates a cartesian product between '{parent_names[0]}' and '{parent_names[1]}'.
# For large DataFrames, this can explode the result size (rows1 * rows2).

# Consider these alternatives:

# 1. Add a join condition if a relationship exists:
{df_name} = {parent_names[0]}.join({parent_names[1]}, on="common_key", how="inner")

# 2. Filter DataFrames before the crossJoin to reduce size:
{parent_names[0]}_filtered = {parent_names[0]}.filter(F.col("column") == value)
{parent_names[1]}_filtered = {parent_names[1]}.filter(F.col("column") == value)
{df_name} = {parent_names[0]}_filtered.crossJoin({parent_names[1]}_filtered)

# 3. Use broadcast for small DataFrames:
from pyspark.sql.functions import broadcast
{df_name} = {parent_names[0]}.crossJoin(broadcast({parent_names[1]}))"""
        elif df_name and len(parent_names) == 1:
            # Single parent case (the other DataFrame is in the crossJoin call)
            suggestion = f"# Line {lineno}: Cartesian join detected"
            if source_line:
                suggestion = f"# Line {lineno}: {source_line}"
            suggestion += f"""

# crossJoin creates a cartesian product which can be extremely expensive.
# The result size is (rows in left DataFrame) * (rows in right DataFrame).

# Consider these alternatives:

# 1. Add a join condition if a relationship exists:
{df_name} = {parent_names[0]}.join(other_df, on="common_key", how="inner")

# 2. Filter DataFrames before the crossJoin to reduce size:
{parent_names[0]}_filtered = {parent_names[0]}.filter(F.col("column") == value)
{df_name} = {parent_names[0]}_filtered.crossJoin(other_df_filtered)

# 3. Use broadcast for small DataFrames:
from pyspark.sql.functions import broadcast
{df_name} = {parent_names[0]}.crossJoin(broadcast(small_df))"""
        else:
            # Fall back to generic suggestion
            suggestion = (
                "Cartesian join (crossJoin) detected.\n\n"
                "# crossJoin creates a cartesian product which can be extremely expensive.\n"
                "# Result size = (rows in df1) * (rows in df2)\n\n"
                "# Consider these alternatives:\n\n"
                "# 1. Add a join condition if a relationship exists:\n"
                "result = df1.join(df2, on='common_key', how='inner')\n\n"
                "# 2. Filter DataFrames before the crossJoin:\n"
                "df1_small = df1.filter(F.col('column') == value)\n"
                "df2_small = df2.filter(F.col('column') == value)\n"
                "result = df1_small.crossJoin(df2_small)\n\n"
                "# 3. Use broadcast for small DataFrames:\n"
                "from pyspark.sql.functions import broadcast\n"
                "result = df1.crossJoin(broadcast(small_df))"
            )

        return suggestion
