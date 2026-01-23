import ast
import textwrap
from app.parsers.ast_parser import PySparkASTParser


def parse_ops(code: str):
    tree = ast.parse(textwrap.dedent(code))
    parser = PySparkASTParser()
    parser.visit(tree)
    return parser.operations


def test_unary_chain():
    code = """
    df2 = df.select("a").filter("b > 1")
    """
    ops = parse_ops(code)

    select_op = ops[0]
    filter_op = ops[1]

    assert select_op.operation == "select"
    assert select_op.parents == ["df"]

    assert filter_op.operation == "filter"
    assert filter_op.parents == ["df"]


def test_multiple_assignments():
    code = """
    df2 = df.select("a")
    df3 = df2.groupBy("a").count()
    """
    ops = parse_ops(code)

    select_op = ops[0]
    groupby_op = ops[1]

    assert select_op.operation == "select"
    assert select_op.parents == ["df"]

    assert groupby_op.operation == "groupBy"
    assert groupby_op.parents == ["df2"]


def test_join_parsing():
    code = """
    df3 = df1.join(df2, on="id").select("a")
    """
    ops = parse_ops(code)

    join_op = ops[0]

    assert join_op.operation == "join"
    assert set(join_op.parents) == {"df1", "df2"}


def test_spark_read_parquet():
    """Test spark.read.parquet() pattern"""
    code = """
    df = spark.read.parquet("sales.parquet")
    """
    ops = parse_ops(code)

    assert len(ops) == 1
    assert ops[0].operation == "parquet"
    assert ops[0].df_name == "df"
    assert ops[0].parents == []


def test_spark_read_csv():
    """Test spark.read.csv() pattern with options"""
    code = """
    df = spark.read.csv("data.csv", header=True)
    """
    ops = parse_ops(code)

    assert len(ops) == 1
    assert ops[0].operation == "csv"
    assert ops[0].df_name == "df"
    assert ops[0].parents == []


def test_spark_read_json():
    """Test spark.read.json() pattern"""
    code = """
    df = spark.read.json("data.json")
    """
    ops = parse_ops(code)

    assert len(ops) == 1
    assert ops[0].operation == "json"
    assert ops[0].df_name == "df"


def test_spark_sql():
    """Test spark.sql() pattern"""
    code = """
    result = spark.sql("SELECT * FROM table")
    """
    ops = parse_ops(code)

    assert len(ops) == 1
    assert ops[0].operation == "sql"
    assert ops[0].df_name == "result"
    assert ops[0].parents == []


def test_write_pattern():
    """Test df.write.* patterns"""
    code = """
    df.write.parquet("output")
    """
    ops = parse_ops(code)

    assert len(ops) == 1
    assert ops[0].operation == "write"
    assert ops[0].df_name == "df"
    assert ops[0].parents == ["df"]


def test_write_pattern_with_mode():
    """Test df.write.mode().parquet() pattern"""
    code = """
    df.write.mode("overwrite").parquet("output")
    """
    ops = parse_ops(code)

    assert len(ops) == 1
    assert ops[0].operation == "write"
    assert ops[0].df_name == "df"
    assert ops[0].parents == ["df"]


def test_aggregation_operations():
    """Test orderBy, limit, agg operations"""
    code = """
    result = df.orderBy("date").limit(100).groupBy("cat").agg(sum("amount"))
    """
    ops = parse_ops(code)

    op_names = [op.operation for op in ops]
    assert "orderBy" in op_names
    assert "limit" in op_names
    assert "groupBy" in op_names
    assert "agg" in op_names


def test_cache_operation():
    """Test cache() operation"""
    code = """
    cached = df.cache()
    """
    ops = parse_ops(code)

    assert len(ops) == 1
    assert ops[0].operation == "cache"
    assert ops[0].df_name == "cached"


def test_full_pipeline():
    """Test a complete data processing pipeline"""
    code = """
    df = spark.read.parquet("sales.parquet")
    df2 = spark.read.csv("data.csv", header=True)
    result = spark.sql("SELECT * FROM table")
    filtered = df.filter(col("x") > 10)
    sorted_df = filtered.orderBy("date")
    limited = sorted_df.limit(100)
    grouped = limited.groupBy("category").agg(sum("amount"))
    cached = grouped.cache()
    cached.show()
    """
    ops = parse_ops(code)

    # Should parse all operations
    op_names = [op.operation for op in ops]
    assert "parquet" in op_names
    assert "csv" in op_names
    assert "sql" in op_names
    assert "filter" in op_names
    assert "orderBy" in op_names
    assert "limit" in op_names
    assert "groupBy" in op_names
    assert "agg" in op_names
    assert "cache" in op_names
    assert "show" in op_names


# ============================================================================
# Variable Aliasing Tests
# ============================================================================


def test_simple_alias():
    """Test simple variable aliasing: alias = df"""
    code = """
    df = spark.read.parquet("data.parquet")
    alias = df
    filtered = alias.filter(col("x") > 10)
    """
    ops = parse_ops(code)

    # Filter should have df as parent (resolved from alias)
    filter_op = [op for op in ops if op.operation == "filter"][0]
    assert filter_op.parents == ["df"], f"Expected ['df'], got {filter_op.parents}"


def test_alias_chain():
    """Test chained variable aliasing: a = df; b = a; c = b"""
    code = """
    df = spark.read.parquet("data.parquet")
    backup = df
    other = backup
    result = other.filter(col("x") > 10)
    result.show()
    """
    ops = parse_ops(code)

    # Filter should resolve through the chain: other -> backup -> df
    filter_op = [op for op in ops if op.operation == "filter"][0]
    assert filter_op.parents == ["df"], f"Expected ['df'], got {filter_op.parents}"

    # Show is called on 'result', which is a new DataFrame (not an alias)
    # So it should have 'result' as its df_name and parent
    show_op = [op for op in ops if op.operation == "show"][0]
    assert show_op.df_name == "result", f"Expected 'result', got {show_op.df_name}"
    assert show_op.parents == ["result"], f"Expected ['result'], got {show_op.parents}"


def test_alias_with_action():
    """Test alias resolution with standalone actions like show()"""
    code = """
    df = spark.read.parquet("data.parquet")
    alias = df
    alias.show()
    """
    ops = parse_ops(code)

    # show() should have df as parent (resolved from alias)
    show_op = [op for op in ops if op.operation == "show"][0]
    assert show_op.parents == ["df"], f"Expected ['df'], got {show_op.parents}"
    assert show_op.df_name == "df", f"Expected 'df', got {show_op.df_name}"


def test_alias_with_write():
    """Test alias resolution with write patterns"""
    code = """
    df = spark.read.parquet("data.parquet")
    output_df = df
    output_df.write.parquet("output")
    """
    ops = parse_ops(code)

    # write should have df as parent (resolved from output_df)
    write_op = [op for op in ops if op.operation == "write"][0]
    assert write_op.parents == ["df"], f"Expected ['df'], got {write_op.parents}"
    assert write_op.df_name == "df", f"Expected 'df', got {write_op.df_name}"


def test_alias_with_join():
    """Test alias resolution with multi-parent operations (join)"""
    code = """
    df1 = spark.read.parquet("data1.parquet")
    df2 = spark.read.parquet("data2.parquet")
    alias1 = df1
    alias2 = df2
    result = alias1.join(alias2, on="id")
    """
    ops = parse_ops(code)

    # Join should have both df1 and df2 as parents (resolved from aliases)
    join_op = [op for op in ops if op.operation == "join"][0]
    assert set(join_op.parents) == {
        "df1",
        "df2",
    }, f"Expected {{'df1', 'df2'}}, got {set(join_op.parents)}"


def test_alias_partial_chain():
    """Test that non-aliased variables are not affected"""
    code = """
    df1 = spark.read.parquet("data.parquet")
    alias = df1
    df2 = alias.filter(col("x") > 10)
    result = df2.select("a")
    """
    ops = parse_ops(code)

    # filter should have df1 as parent (alias resolved)
    filter_op = [op for op in ops if op.operation == "filter"][0]
    assert filter_op.parents == ["df1"], f"Expected ['df1'], got {filter_op.parents}"

    # select should have df2 as parent (df2 is a real operation, not an alias)
    select_op = [op for op in ops if op.operation == "select"][0]
    assert select_op.parents == ["df2"], f"Expected ['df2'], got {select_op.parents}"


def test_alias_map_populated():
    """Test that alias_map is correctly populated"""
    code = """
    df = spark.read.parquet("data.parquet")
    alias1 = df
    alias2 = alias1
    """
    tree = ast.parse(textwrap.dedent(code))
    parser = PySparkASTParser()
    parser.visit(tree)

    assert "alias1" in parser.alias_map
    assert parser.alias_map["alias1"] == "df"
    assert "alias2" in parser.alias_map
    assert parser.alias_map["alias2"] == "alias1"


def test_resolve_alias_method():
    """Test the _resolve_alias method directly"""
    parser = PySparkASTParser()
    parser.alias_map = {
        "a": "df",
        "b": "a",
        "c": "b",
    }

    # Test resolution through the chain
    assert parser._resolve_alias("c") == "df"
    assert parser._resolve_alias("b") == "df"
    assert parser._resolve_alias("a") == "df"
    assert parser._resolve_alias("df") == "df"  # Not in alias_map, returns itself
    assert parser._resolve_alias("unknown") == "unknown"  # Unknown returns itself


def test_alias_cycle_detection():
    """Test that alias cycles don't cause infinite loops"""
    parser = PySparkASTParser()
    # Create a cycle: a -> b -> c -> a
    parser.alias_map = {
        "a": "b",
        "b": "c",
        "c": "a",
    }

    # Should not hang, should return some value
    result = parser._resolve_alias("a")
    assert result in {"a", "b", "c"}  # Returns one of the cycle members


# ============================================================================
# Nested Expression Tests
# ============================================================================


def test_join_with_nested_filter():
    """Test join with a nested filter operation: df1.join(df2.filter(...), on="id")"""
    code = """
    df1 = spark.read.parquet("orders.parquet")
    df2 = spark.read.parquet("customers.parquet")
    result = df1.join(df2.filter(col("active") == True), on="customer_id")
    """
    ops = parse_ops(code)

    # Should have: parquet, parquet, filter (nested), join
    op_names = [op.operation for op in ops]
    assert "parquet" in op_names
    assert "filter" in op_names
    assert "join" in op_names

    # The filter should be on an anonymous df (from the nested expression)
    filter_ops = [op for op in ops if op.operation == "filter"]
    assert len(filter_ops) == 1
    filter_op = filter_ops[0]
    # Filter should have df2 as parent
    assert "df2" in filter_op.parents

    # The join should have df1 and the anonymous df as parents
    join_ops = [op for op in ops if op.operation == "join"]
    assert len(join_ops) == 1
    join_op = join_ops[0]
    assert "df1" in join_op.parents
    # The second parent should be an anonymous df name
    assert any(p.startswith("_anon_df_") for p in join_op.parents)


def test_join_with_nested_chain():
    """Test join with a nested chain: df1.join(df2.filter(...).select(...), on="id")"""
    code = """
    df1 = spark.read.parquet("orders.parquet")
    df2 = spark.read.parquet("customers.parquet")
    result = df1.join(df2.filter(col("active") == True).select("id", "name"), on="customer_id")
    """
    ops = parse_ops(code)

    op_names = [op.operation for op in ops]
    assert "filter" in op_names
    assert "select" in op_names
    assert "join" in op_names

    # Both filter and select should be extracted from the nested expression
    filter_ops = [op for op in ops if op.operation == "filter"]
    select_ops = [op for op in ops if op.operation == "select"]
    assert len(filter_ops) >= 1
    assert len(select_ops) >= 1


def test_join_with_simple_df_argument():
    """Test that join with simple df argument still works"""
    code = """
    df3 = df1.join(df2, on="id")
    """
    ops = parse_ops(code)

    join_op = ops[0]
    assert join_op.operation == "join"
    assert set(join_op.parents) == {"df1", "df2"}


def test_union_with_nested_expression():
    """Test union with a nested filter: df1.union(df2.filter(...))"""
    code = """
    df1 = spark.read.parquet("data1.parquet")
    df2 = spark.read.parquet("data2.parquet")
    result = df1.union(df2.filter(col("type") == "active"))
    """
    ops = parse_ops(code)

    op_names = [op.operation for op in ops]
    assert "filter" in op_names
    assert "union" in op_names

    # The filter should be extracted
    filter_ops = [op for op in ops if op.operation == "filter"]
    assert len(filter_ops) == 1

    # The union should have df1 and the anonymous df as parents
    union_ops = [op for op in ops if op.operation == "union"]
    assert len(union_ops) == 1
    union_op = union_ops[0]
    assert "df1" in union_op.parents


def test_nested_expression_with_alias():
    """Test nested expression where the base df is an alias"""
    code = """
    df1 = spark.read.parquet("data1.parquet")
    df2 = spark.read.parquet("data2.parquet")
    alias = df2
    result = df1.join(alias.filter(col("x") > 10), on="id")
    """
    ops = parse_ops(code)

    # The filter should have df2 as parent (resolved from alias)
    filter_ops = [op for op in ops if op.operation == "filter"]
    assert len(filter_ops) == 1
    filter_op = filter_ops[0]
    assert "df2" in filter_op.parents


def test_deeply_nested_expressions():
    """Test deeply nested expressions: df1.join(df2.join(df3, on="a"), on="b")"""
    code = """
    result = df1.join(df2.join(df3, on="a"), on="b")
    """
    ops = parse_ops(code)

    # Should have two join operations
    join_ops = [op for op in ops if op.operation == "join"]
    assert len(join_ops) == 2

    # One join should have df2 and df3, the other should have df1 and an anonymous df
    parents_sets = [set(op.parents) for op in join_ops]

    # Find the inner join (df2.join(df3))
    inner_join_found = any(
        "df2" in parents and "df3" in parents for parents in parents_sets
    )
    assert inner_join_found, f"Expected inner join with df2, df3. Got: {parents_sets}"

    # Find the outer join (df1.join(...))
    outer_join_found = any("df1" in parents for parents in parents_sets)
    assert outer_join_found, f"Expected outer join with df1. Got: {parents_sets}"


def test_standalone_join_with_nested():
    """Test standalone expression with nested: df1.join(df2.filter(...)).show()"""
    code = """
    df1.join(df2.filter(col("x") > 10), on="id").show()
    """
    ops = parse_ops(code)

    op_names = [op.operation for op in ops]
    assert "filter" in op_names
    assert "join" in op_names
    assert "show" in op_names


def test_complete_nested_pipeline():
    """Test a complete pipeline with nested expressions"""
    code = """
    df1 = spark.read.parquet("orders.parquet")
    df2 = spark.read.parquet("customers.parquet")
    result = df1.join(df2.filter(col("active") == True), on="customer_id")
    result.show()
    """
    ops = parse_ops(code)

    # Verify all operations are captured
    op_names = [op.operation for op in ops]
    assert "parquet" in op_names
    assert "filter" in op_names
    assert "join" in op_names
    assert "show" in op_names

    # Verify the filter operation has the correct parent
    filter_op = [op for op in ops if op.operation == "filter"][0]
    assert "df2" in filter_op.parents
