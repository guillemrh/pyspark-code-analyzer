# Testing Guide for Anti-Pattern Rules

## Test File Location

```
backend/app/tests/test_antipatterns.py
```

## Test Structure

```python
import pytest
from app.services.dag_pipeline import run_dag_pipeline
from app.graphs.antipatterns.rules.your_rule import YourNewRule

class TestYourNewRule:
    """Tests for YourNewRule anti-pattern detection."""
    
    @pytest.fixture
    def rule(self):
        return YourNewRule()
    
    # Positive tests (should detect)
    def test_detects_basic_case(self, rule):
        """Rule detects the anti-pattern in simple case."""
        pass
    
    def test_detects_complex_case(self, rule):
        """Rule detects anti-pattern in realistic code."""
        pass
    
    # Negative tests (should not detect)
    def test_no_false_positive_correct_code(self, rule):
        """Rule does not flag correct code."""
        pass
    
    def test_no_false_positive_edge_case(self, rule):
        """Rule handles edge case without false positive."""
        pass
    
    # Integration tests
    def test_integration_with_pipeline(self):
        """Rule works correctly in full pipeline."""
        pass
```

## Test Patterns

### Basic Detection Test

```python
def test_detects_multiple_actions(self):
    code = '''
df = spark.read.parquet("data.parquet")
df.count()
df.show()
'''
    result = run_dag_pipeline(code)
    antipatterns = result["antipatterns"]
    
    assert len(antipatterns) >= 1
    assert any(ap["rule"] == "MultipleActionsRule" for ap in antipatterns)
```

### False Positive Test

```python
def test_no_false_positive_with_cache(self):
    code = '''
df = spark.read.parquet("data.parquet")
df.cache()
df.count()
df.show()
'''
    result = run_dag_pipeline(code)
    antipatterns = result["antipatterns"]
    
    # Should NOT detect MultipleActionsRule because cache is used
    assert not any(ap["rule"] == "MultipleActionsRule" for ap in antipatterns)
```

### Unit Test (Rule Only)

```python
def test_rule_detect_method(self, rule):
    # Create mock DAG and lineage
    operation_dag = {
        "node_1": MockNode(df_name="df", operation="read", op_type=OpType.TRANSFORMATION),
        "node_2": MockNode(df_name="df", operation="count", op_type=OpType.ACTION),
        "node_3": MockNode(df_name="df", operation="show", op_type=OpType.ACTION),
    }
    lineage_graph = {"df": ["node_1"]}
    stage_assignments = {"node_1": 0, "node_2": 1, "node_3": 1}
    
    findings = rule.detect(operation_dag, lineage_graph, stage_assignments)
    
    assert len(findings) == 1
    assert findings[0]["rule"] == rule.name
```

## Edge Cases to Test

### 1. Empty/Minimal Input
```python
def test_empty_dag(self, rule):
    findings = rule.detect({}, {}, {})
    assert findings == []

def test_single_node(self, rule):
    dag = {"node_1": MockNode(...)}
    findings = rule.detect(dag, {}, {})
    assert findings == []
```

### 2. Chained Operations
```python
def test_chained_operations(self):
    code = '''
result = (spark.read.parquet("data.parquet")
    .filter(col("x") > 0)
    .select("x", "y")
    .groupBy("x")
    .count())
'''
    result = run_dag_pipeline(code)
    # Verify specific behavior with chains
```

### 3. Multiple DataFrames
```python
def test_multiple_independent_dataframes(self):
    code = '''
df1 = spark.read.parquet("data1.parquet")
df2 = spark.read.parquet("data2.parquet")
df1.show()
df2.show()
'''
    result = run_dag_pipeline(code)
    # Should NOT detect multiple actions (different DataFrames)
```

### 4. Diamond Dependencies
```python
def test_diamond_dependency(self):
    code = '''
base = spark.read.parquet("data.parquet")
left = base.filter(col("x") > 0)
right = base.filter(col("x") <= 0)
result = left.union(right)
result.show()
'''
    result = run_dag_pipeline(code)
    # Verify correct handling of diamond pattern
```

## Running Tests

```bash
# Run all anti-pattern tests
cd backend && pytest app/tests/test_antipatterns.py -v

# Run specific test class
cd backend && pytest app/tests/test_antipatterns.py::TestYourNewRule -v

# Run with coverage
cd backend && pytest app/tests/test_antipatterns.py --cov=app/graphs/antipatterns

# Run with print output
cd backend && pytest app/tests/test_antipatterns.py -v -s
```

## Mock Helpers

```python
from dataclasses import dataclass
from app.parsers.dag_nodes import OpType

@dataclass
class MockNode:
    df_name: str
    operation: str
    op_type: OpType
    parents: list = None
    args: dict = None
    line_number: int = 1
    
    def __post_init__(self):
        self.parents = self.parents or []
        self.args = self.args or {}
```