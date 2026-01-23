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
