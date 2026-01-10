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