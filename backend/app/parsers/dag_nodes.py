from dataclasses import dataclass
from typing import List, Optional
from app.parsers.spark_ops import OpType


@dataclass
class SparkOperationNode:
    id: str
    df_name: str
    operation: str
    parents: List[str]
    lineno: int
    op_type: Optional[OpType] = None
    causes_shuffle: bool = False 
