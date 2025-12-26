from abc import ABC, abstractmethod
from typing import List
from app.services.operation_dag_builder import OperationDAG
from dataclasses import dataclass


@dataclass
class AntiPatternFinding:
    rule_id: str
    severity: str
    message: str
    nodes: List[str]


class AntiPatternRule(ABC):
    rule_id: str
    severity: str

    @abstractmethod
    def detect(self, dag: OperationDAG) -> List[AntiPatternFinding]:
        pass
