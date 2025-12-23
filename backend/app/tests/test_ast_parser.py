from app.parsers.ast_parser import PySparkASTParser
import ast

def test_unary_chain():
    code = """
    df2 = df.select("a").filter("b > 1")
    """
    parser = PySparkASTParser()
    nodes = parser.parse(code)

    select_node = nodes[0]
    filter_node = nodes[1]

    assert select_node.operation == "select"
    assert select_node.parents == ["df"]
    assert filter_node.operation == "filter"
    assert filter_node.parents == ["df2"]

def test_multiple_assignments():
    code = """
    df2 = df.select("a")
    df3 = df2.groupBy("a").count()
    """
    parser = PySparkASTParser()
    nodes = parser.parse(code)
    
    select_node = nodes[0]
    groupby_node = nodes[1]
    
    assert select_node.operation == "select"
    assert select_node.parents == ["df"]
    assert groupby_node.operation == "groupBy"
    assert groupby_node.parents == ["df2"]

def test_join_parsing():
    code = """
    df3 = df1.join(df2, on="id").select("a")
    """
    parser = PySparkASTParser()
    nodes = parser.parse(code)

    join_node = nodes[0]

    assert join_node.operation == "join"
    assert set(join_node.parents) == {"df1", "df2"}
