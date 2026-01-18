# Detection Patterns Reference

Common patterns for detecting PySpark anti-patterns.

## Pattern 1: Multiple Actions Detection

Detect when multiple actions are called on the same DataFrame lineage without caching.

```python
def _find_multiple_actions(self, dag, lineage):
    """Find DataFrames with multiple downstream actions."""
    action_counts = {}
    
    for node_id, node in dag.items():
        if node.op_type == OpType.ACTION:
            # Trace back through lineage
            source_df = self._get_root_dataframe(node, lineage)
            action_counts[source_df] = action_counts.get(source_df, 0) + 1
    
    return {df: count for df, count in action_counts.items() if count > 1}
```

## Pattern 2: Shuffle Before Filter Detection

Detect when shuffle operations occur before filtering reduces data size.

```python
def _is_early_shuffle(self, node, dag):
    """Check if shuffle happens before filters."""
    if node.operation not in SHUFFLE_OPS:
        return False
    
    # Check if any ancestor is a filter
    ancestors = self._get_ancestors(node, dag)
    filter_ancestors = [a for a in ancestors if a.operation == "filter"]
    
    # Check if filter could have been applied first
    return len(filter_ancestors) == 0 and self._has_filterable_columns(node)
```

## Pattern 3: Missing Cache Detection

Detect when a DataFrame is reused without caching.

```python
def _needs_cache(self, node, dag, lineage):
    """Check if DataFrame should be cached."""
    df_name = node.df_name
    
    # Count how many times this DataFrame is used
    usage_count = sum(
        1 for n in dag.values() 
        if df_name in n.parents
    )
    
    # Check if already cached
    is_cached = any(
        n.operation in {"cache", "persist"}
        for n in dag.values()
        if n.df_name == df_name
    )
    
    return usage_count > 1 and not is_cached
```

## Pattern 4: Stage Boundary Analysis

Use stage assignments to detect cross-stage inefficiencies.

```python
def _crosses_stage_boundary(self, node1_id, node2_id, stage_assignments):
    """Check if two nodes are in different stages."""
    return stage_assignments.get(node1_id) != stage_assignments.get(node2_id)

def _count_stage_crossings(self, dag, stage_assignments):
    """Count how many times data crosses stage boundaries."""
    crossings = 0
    for node_id, node in dag.items():
        for parent in node.parents:
            parent_id = self._find_node_by_df(parent, dag)
            if parent_id and self._crosses_stage_boundary(node_id, parent_id, stage_assignments):
                crossings += 1
    return crossings
```

## Pattern 5: Lineage Depth Analysis

Detect overly deep lineages that may cause memory issues.

```python
def _get_lineage_depth(self, node, lineage, depth=0):
    """Calculate the depth of a DataFrame's lineage."""
    if not node.parents:
        return depth
    
    max_parent_depth = 0
    for parent in node.parents:
        parent_node = lineage.get(parent)
        if parent_node:
            parent_depth = self._get_lineage_depth(parent_node, lineage, depth + 1)
            max_parent_depth = max(max_parent_depth, parent_depth)
    
    return max_parent_depth
```

## Utility Functions

### Traversing the DAG

```python
def _get_ancestors(self, node, dag):
    """Get all ancestor nodes."""
    ancestors = []
    to_visit = list(node.parents)
    visited = set()
    
    while to_visit:
        parent_name = to_visit.pop()
        if parent_name in visited:
            continue
        visited.add(parent_name)
        
        parent_node = self._find_node_by_df(parent_name, dag)
        if parent_node:
            ancestors.append(parent_node)
            to_visit.extend(parent_node.parents)
    
    return ancestors

def _get_descendants(self, node, dag):
    """Get all descendant nodes."""
    descendants = []
    for candidate in dag.values():
        if node.df_name in candidate.parents:
            descendants.append(candidate)
            descendants.extend(self._get_descendants(candidate, dag))
    return descendants
```

### Finding Nodes

```python
def _find_node_by_df(self, df_name, dag):
    """Find node that produces a DataFrame."""
    for node in dag.values():
        if node.df_name == df_name:
            return node
    return None

def _find_nodes_by_operation(self, operation, dag):
    """Find all nodes with a specific operation."""
    return [n for n in dag.values() if n.operation == operation]
```