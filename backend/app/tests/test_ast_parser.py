from app.parsers.ast_parser import PySparkASTParser
import ast

def test_join_parsing():
    code = """
    df3 = df1.join(df2, on="id").select("a")
    """
    parser = PySparkASTParser()
    nodes = parser.parse(code)

    join_node = nodes[0]

    assert join_node.operation == "join"
    assert set(join_node.parents) == {"df1", "df2"}
