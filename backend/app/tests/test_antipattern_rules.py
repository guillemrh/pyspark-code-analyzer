"""
Tests for anti-pattern detection rules.
"""

from app.graphs.operation.operation_graph_builder import build_operation_dag
from app.parsers.dag_nodes import SparkOperationNode
from app.parsers.spark_semantics import OpType
from app.graphs.antipatterns.rules.cartesian_join import CartesianJoinRule
from app.graphs.antipatterns.rules.collect_inside_loop import CollectInsideLoopRule
from app.graphs.antipatterns.rules.unpersist_missing import UnpersistMissingRule


class TestCartesianJoinRule:
    """Tests for CartesianJoinRule anti-pattern detection."""

    def test_detects_cross_join(self):
        """Test that crossJoin operations are detected."""
        ops = [
            SparkOperationNode(
                id="n1",
                df_name="df1",
                operation="parquet",
                parents=[],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n2",
                df_name="df2",
                operation="parquet",
                parents=[],
                lineno=2,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n3",
                df_name="cartesian",
                operation="crossJoin",
                parents=["df1", "df2"],
                lineno=3,
                op_type=OpType.TRANSFORMATION,
            ),
        ]

        dag = build_operation_dag(ops)
        rule = CartesianJoinRule()
        findings = rule.detect(dag)

        assert len(findings) == 1
        assert findings[0].rule_id == "CARTESIAN_JOIN"
        assert findings[0].severity == "HIGH"
        assert "crossJoin" in findings[0].message

    def test_no_findings_for_regular_join(self):
        """Test that regular join operations are not flagged."""
        ops = [
            SparkOperationNode(
                id="n1",
                df_name="df1",
                operation="parquet",
                parents=[],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n2",
                df_name="df2",
                operation="parquet",
                parents=[],
                lineno=2,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n3",
                df_name="joined",
                operation="join",
                parents=["df1", "df2"],
                lineno=3,
                op_type=OpType.TRANSFORMATION,
            ),
        ]

        dag = build_operation_dag(ops)
        rule = CartesianJoinRule()
        findings = rule.detect(dag)

        assert len(findings) == 0

    def test_contextual_suggestion_includes_variable_names(self):
        """Test that suggestions include actual variable names."""
        source_code = """df1 = spark.read.parquet("big_table.parquet")
df2 = spark.read.parquet("other_big.parquet")
cartesian = df1.crossJoin(df2)"""

        ops = [
            SparkOperationNode(
                id="n1",
                df_name="df1",
                operation="parquet",
                parents=[],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n2",
                df_name="df2",
                operation="parquet",
                parents=[],
                lineno=2,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n3",
                df_name="cartesian",
                operation="crossJoin",
                parents=["df1", "df2"],
                lineno=3,
                op_type=OpType.TRANSFORMATION,
            ),
        ]

        dag = build_operation_dag(ops)
        rule = CartesianJoinRule()
        findings = rule.detect(dag, source_code)

        assert len(findings) == 1
        # Check that variable names appear in suggestion
        assert "df1" in findings[0].suggestion
        assert "df2" in findings[0].suggestion


class TestCollectInsideLoopRule:
    """Tests for CollectInsideLoopRule anti-pattern detection."""

    def test_detects_multiple_collects(self):
        """Test that multiple collect() calls are detected."""
        ops = [
            SparkOperationNode(
                id="n1",
                df_name="df",
                operation="parquet",
                parents=[],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n2",
                df_name="result1",
                operation="filter",
                parents=["df"],
                lineno=2,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n3",
                df_name="result1",
                operation="collect",
                parents=["result1"],
                lineno=3,
                op_type=OpType.ACTION,
            ),
            SparkOperationNode(
                id="n4",
                df_name="result2",
                operation="filter",
                parents=["df"],
                lineno=4,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n5",
                df_name="result2",
                operation="collect",
                parents=["result2"],
                lineno=5,
                op_type=OpType.ACTION,
            ),
            SparkOperationNode(
                id="n6",
                df_name="result3",
                operation="groupBy",
                parents=["df"],
                lineno=6,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n7",
                df_name="result3",
                operation="collect",
                parents=["result3"],
                lineno=7,
                op_type=OpType.ACTION,
            ),
        ]

        dag = build_operation_dag(ops)
        rule = CollectInsideLoopRule()
        findings = rule.detect(dag)

        assert len(findings) == 1
        assert findings[0].rule_id == "MULTIPLE_COLLECT_CALLS"
        assert findings[0].severity == "HIGH"
        assert "3 calls" in findings[0].message
        assert len(findings[0].nodes) == 3

    def test_no_findings_for_few_collects(self):
        """Test that 2 or fewer collect() calls are not flagged."""
        ops = [
            SparkOperationNode(
                id="n1",
                df_name="df",
                operation="parquet",
                parents=[],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n2",
                df_name="result1",
                operation="collect",
                parents=["df"],
                lineno=2,
                op_type=OpType.ACTION,
            ),
            SparkOperationNode(
                id="n3",
                df_name="result2",
                operation="collect",
                parents=["df"],
                lineno=3,
                op_type=OpType.ACTION,
            ),
        ]

        dag = build_operation_dag(ops)
        rule = CollectInsideLoopRule()
        findings = rule.detect(dag)

        assert len(findings) == 0

    def test_contextual_suggestion_includes_line_numbers(self):
        """Test that suggestions include line numbers."""
        source_code = """cached = df.cache()
result1 = cached.filter(col("x") > 10).collect()
result2 = cached.filter(col("y") > 20).collect()
result3 = cached.groupBy("z").count().collect()"""

        ops = [
            SparkOperationNode(
                id="n1",
                df_name="cached",
                operation="cache",
                parents=["df"],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n2",
                df_name="result1",
                operation="collect",
                parents=["cached"],
                lineno=2,
                op_type=OpType.ACTION,
            ),
            SparkOperationNode(
                id="n3",
                df_name="result2",
                operation="collect",
                parents=["cached"],
                lineno=3,
                op_type=OpType.ACTION,
            ),
            SparkOperationNode(
                id="n4",
                df_name="result3",
                operation="collect",
                parents=["cached"],
                lineno=4,
                op_type=OpType.ACTION,
            ),
        ]

        dag = build_operation_dag(ops)
        rule = CollectInsideLoopRule()
        findings = rule.detect(dag, source_code)

        assert len(findings) == 1
        # Check that line numbers appear in suggestion
        assert "Line 2" in findings[0].suggestion
        assert "Line 3" in findings[0].suggestion
        assert "Line 4" in findings[0].suggestion


class TestUnpersistMissingRule:
    """Tests for UnpersistMissingRule anti-pattern detection."""

    def test_detects_cache_without_unpersist(self):
        """Test that cache() without unpersist() is detected."""
        ops = [
            SparkOperationNode(
                id="n1",
                df_name="df",
                operation="parquet",
                parents=[],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n2",
                df_name="cached",
                operation="cache",
                parents=["df"],
                lineno=2,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n3",
                df_name="result",
                operation="filter",
                parents=["cached"],
                lineno=3,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n4",
                df_name="result",
                operation="collect",
                parents=["result"],
                lineno=4,
                op_type=OpType.ACTION,
            ),
        ]

        dag = build_operation_dag(ops)
        rule = UnpersistMissingRule()
        findings = rule.detect(dag)

        assert len(findings) == 1
        assert findings[0].rule_id == "UNPERSIST_MISSING"
        assert findings[0].severity == "MEDIUM"
        assert "cached" in findings[0].message
        assert "memory leaks" in findings[0].message

    def test_detects_persist_without_unpersist(self):
        """Test that persist() without unpersist() is detected."""
        ops = [
            SparkOperationNode(
                id="n1",
                df_name="df",
                operation="parquet",
                parents=[],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n2",
                df_name="persisted",
                operation="persist",
                parents=["df"],
                lineno=2,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n3",
                df_name="result",
                operation="show",
                parents=["persisted"],
                lineno=3,
                op_type=OpType.ACTION,
            ),
        ]

        dag = build_operation_dag(ops)
        rule = UnpersistMissingRule()
        findings = rule.detect(dag)

        assert len(findings) == 1
        assert "persisted" in findings[0].message

    def test_no_findings_when_unpersist_called(self):
        """Test that cache() with unpersist() is not flagged."""
        ops = [
            SparkOperationNode(
                id="n1",
                df_name="df",
                operation="parquet",
                parents=[],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n2",
                df_name="cached",
                operation="cache",
                parents=["df"],
                lineno=2,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n3",
                df_name="result",
                operation="filter",
                parents=["cached"],
                lineno=3,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n4",
                df_name="result",
                operation="collect",
                parents=["result"],
                lineno=4,
                op_type=OpType.ACTION,
            ),
            SparkOperationNode(
                id="n5",
                df_name="cached",
                operation="unpersist",
                parents=["cached"],
                lineno=5,
                op_type=OpType.TRANSFORMATION,
            ),
        ]

        dag = build_operation_dag(ops)
        rule = UnpersistMissingRule()
        findings = rule.detect(dag)

        assert len(findings) == 0

    def test_detects_multiple_cached_without_unpersist(self):
        """Test that multiple cached DataFrames without unpersist are detected."""
        ops = [
            SparkOperationNode(
                id="n1",
                df_name="df1",
                operation="parquet",
                parents=[],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n2",
                df_name="cached1",
                operation="cache",
                parents=["df1"],
                lineno=2,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n3",
                df_name="df2",
                operation="parquet",
                parents=[],
                lineno=3,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n4",
                df_name="cached2",
                operation="persist",
                parents=["df2"],
                lineno=4,
                op_type=OpType.TRANSFORMATION,
            ),
        ]

        dag = build_operation_dag(ops)
        rule = UnpersistMissingRule()
        findings = rule.detect(dag)

        assert len(findings) == 1
        assert "cached1" in findings[0].message
        assert "cached2" in findings[0].message

    def test_contextual_suggestion_includes_variable_names(self):
        """Test that suggestions include actual variable names."""
        source_code = """df = spark.read.parquet("data.parquet")
cached = df.cache()
result = cached.filter(col("x") > 10).collect()"""

        ops = [
            SparkOperationNode(
                id="n1",
                df_name="df",
                operation="parquet",
                parents=[],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n2",
                df_name="cached",
                operation="cache",
                parents=["df"],
                lineno=2,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n3",
                df_name="result",
                operation="collect",
                parents=["cached"],
                lineno=3,
                op_type=OpType.ACTION,
            ),
        ]

        dag = build_operation_dag(ops)
        rule = UnpersistMissingRule()
        findings = rule.detect(dag, source_code)

        assert len(findings) == 1
        # Check that variable name and suggestion to unpersist appear
        assert "cached" in findings[0].suggestion
        assert "unpersist()" in findings[0].suggestion


class TestIntegrationWithTestCase:
    """Integration test using the example from the task description."""

    def test_example_code_detects_all_antipatterns(self):
        """Test that the example code detects all expected anti-patterns."""
        source_code = """df1 = spark.read.parquet("big_table.parquet")
df2 = spark.read.parquet("other_big.parquet")
cartesian = df1.crossJoin(df2)
cartesian.show()

cached = df1.cache()
result1 = cached.filter(col("x") > 10).collect()
result2 = cached.filter(col("y") > 20).collect()
result3 = cached.groupBy("z").count().collect()
# Missing unpersist!"""

        ops = [
            # df1 = spark.read.parquet(...)
            SparkOperationNode(
                id="n1",
                df_name="df1",
                operation="parquet",
                parents=[],
                lineno=1,
                op_type=OpType.TRANSFORMATION,
            ),
            # df2 = spark.read.parquet(...)
            SparkOperationNode(
                id="n2",
                df_name="df2",
                operation="parquet",
                parents=[],
                lineno=2,
                op_type=OpType.TRANSFORMATION,
            ),
            # cartesian = df1.crossJoin(df2)
            SparkOperationNode(
                id="n3",
                df_name="cartesian",
                operation="crossJoin",
                parents=["df1", "df2"],
                lineno=3,
                op_type=OpType.TRANSFORMATION,
            ),
            # cartesian.show()
            SparkOperationNode(
                id="n4",
                df_name="cartesian",
                operation="show",
                parents=["cartesian"],
                lineno=4,
                op_type=OpType.ACTION,
            ),
            # cached = df1.cache()
            SparkOperationNode(
                id="n5",
                df_name="cached",
                operation="cache",
                parents=["df1"],
                lineno=6,
                op_type=OpType.TRANSFORMATION,
            ),
            # result1 = cached.filter(col("x") > 10).collect()
            SparkOperationNode(
                id="n6",
                df_name="result1",
                operation="filter",
                parents=["cached"],
                lineno=7,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n7",
                df_name="result1",
                operation="collect",
                parents=["result1"],
                lineno=7,
                op_type=OpType.ACTION,
            ),
            # result2 = cached.filter(col("y") > 20).collect()
            SparkOperationNode(
                id="n8",
                df_name="result2",
                operation="filter",
                parents=["cached"],
                lineno=8,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n9",
                df_name="result2",
                operation="collect",
                parents=["result2"],
                lineno=8,
                op_type=OpType.ACTION,
            ),
            # result3 = cached.groupBy("z").count().collect()
            SparkOperationNode(
                id="n10",
                df_name="result3",
                operation="groupBy",
                parents=["cached"],
                lineno=9,
                op_type=OpType.TRANSFORMATION,
            ),
            SparkOperationNode(
                id="n11",
                df_name="result3",
                operation="count",
                parents=["result3"],
                lineno=9,
                op_type=OpType.ACTION,
            ),
            SparkOperationNode(
                id="n12",
                df_name="result3",
                operation="collect",
                parents=["result3"],
                lineno=9,
                op_type=OpType.ACTION,
            ),
        ]

        dag = build_operation_dag(ops)

        # Test CartesianJoinRule
        cartesian_rule = CartesianJoinRule()
        cartesian_findings = cartesian_rule.detect(dag, source_code)
        assert len(cartesian_findings) == 1
        assert cartesian_findings[0].rule_id == "CARTESIAN_JOIN"

        # Test CollectInsideLoopRule
        collect_rule = CollectInsideLoopRule()
        collect_findings = collect_rule.detect(dag, source_code)
        assert len(collect_findings) == 1
        assert collect_findings[0].rule_id == "MULTIPLE_COLLECT_CALLS"
        assert "3 calls" in collect_findings[0].message

        # Test UnpersistMissingRule
        unpersist_rule = UnpersistMissingRule()
        unpersist_findings = unpersist_rule.detect(dag, source_code)
        assert len(unpersist_findings) == 1
        assert unpersist_findings[0].rule_id == "UNPERSIST_MISSING"
        assert "cached" in unpersist_findings[0].message
