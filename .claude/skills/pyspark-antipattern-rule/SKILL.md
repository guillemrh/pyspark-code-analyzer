---
name: pyspark-antipattern-rule
description: Guide for creating new PySpark anti-pattern detection rules in the PySpark Intelligence Platform. Use when adding a new rule to detect performance issues, inefficient patterns, or bad practices in PySpark code. Covers rule implementation, registry registration, DAG integration, and test creation.
allowed-tools: Read, Grep, Glob, Write, Edit
---

# PySpark Anti-Pattern Rule Skill

Create new anti-pattern detection rules for the PySpark Intelligence Platform.

## Quick Start

1. Create rule class in `backend/app/graphs/antipatterns/rules/`
2. Register in `backend/app/graphs/antipatterns/registry.py`
3. Add tests in `backend/app/tests/test_antipatterns.py`

## Rule Implementation

### File Location
```
backend/app/graphs/antipatterns/rules/[rule_name]_rule.py
```

### Rule Template

```python
from typing import List, Dict, Any
from ..base_rule import BaseAntiPatternRule

class YourNewRule(BaseAntiPatternRule):
    """Detects [description of the anti-pattern]."""
    
    name = "YourNewRule"
    description = "Detects when [specific condition that indicates bad practice]"
    severity = "warning"  # "info" | "warning" | "error"
    
    def detect(
        self,
        operation_dag: Dict[str, Any],
        lineage_graph: Dict[str, Any],
        stage_assignments: Dict[str, int]
    ) -> List[Dict[str, Any]]:
        """
        Analyze the DAG and lineage to find anti-pattern instances.
        
        Args:
            operation_dag: DAG of SparkOperationNodes with parent/child relationships
            lineage_graph: DataFrame lineage tracking dependencies
            stage_assignments: Mapping of node_id -> stage_id
            
        Returns:
            List of detected anti-pattern instances with:
            - rule: str (rule name)
            - message: str (human-readable explanation)
            - severity: str
            - nodes: List[str] (affected node IDs)
            - suggestion: str (how to fix)
        """
        findings = []
        
        for node_id, node in operation_dag.items():
            if self._is_antipattern(node, operation_dag, lineage_graph):
                findings.append({
                    "rule": self.name,
                    "message": f"[Explanation of what's wrong at {node_id}]",
                    "severity": self.severity,
                    "nodes": [node_id],
                    "suggestion": "[How to fix this]"
                })
        
        return findings
    
    def _is_antipattern(self, node, dag, lineage) -> bool:
        """Check if a specific node exhibits the anti-pattern."""
        # Implementation logic here
        return False
```

### Registry Registration

Edit `backend/app/graphs/antipatterns/registry.py`:

```python
from .rules.your_new_rule import YourNewRule

ANTIPATTERN_RULES = [
    MultipleActionsRule(),
    EarlyShuffleRule(),
    ActionWithoutCacheRule(),
    RepartitionMisuseRule(),
    YourNewRule(),  # Add here
]
```

## Key Data Structures

### SparkOperationNode (from parsers/dag_nodes.py)
```python
@dataclass
class SparkOperationNode:
    df_name: str          # e.g., "result_df"
    operation: str        # e.g., "groupBy", "filter", "show"
    parents: List[str]    # Parent DataFrame names
    op_type: OpType       # TRANSFORMATION or ACTION
    args: Dict[str, Any]  # Operation arguments
    line_number: int
```

### OpType Enum
```python
class OpType(Enum):
    TRANSFORMATION = "transformation"  # filter, select, groupBy, join
    ACTION = "action"                  # show, count, collect, write
```

### Shuffle Operations
```python
SHUFFLE_OPS = {"groupBy", "join", "distinct", "repartition"}
```

## Common Detection Patterns

See [references/detection-patterns.md](references/detection-patterns.md) for:
- Multiple actions on same lineage
- Operations before necessary (early shuffle)
- Missing cache before reuse
- Inefficient operation ordering

## Testing

See [references/testing-guide.md](references/testing-guide.md) for:
- Test structure and fixtures
- Positive and negative test cases
- Edge case coverage