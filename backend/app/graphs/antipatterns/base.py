from abc import ABC, abstractmethod
from typing import List, Optional
from app.graphs.operation.operation_graph_builder import OperationDAG
from dataclasses import dataclass


@dataclass
class AntiPatternFinding:
    rule_id: str
    severity: str
    message: str
    nodes: List[str]
    suggestion: str = None  # Optional autofix/recommendation with code example


class AntiPatternRule(ABC):
    rule_id: str
    severity: str

    @abstractmethod
    def detect(
        self, dag: OperationDAG, source_code: Optional[str] = None
    ) -> List[AntiPatternFinding]:
        """
        Detect anti-patterns in the operation DAG.

        Args:
            dag: The operation DAG to analyze
            source_code: Optional original source code for contextual suggestions

        Returns:
            List of AntiPatternFinding instances
        """
        pass

    def _get_source_line(
        self, source_code: Optional[str], lineno: int
    ) -> Optional[str]:
        """
        Extract a specific line from the source code.

        Args:
            source_code: The original source code
            lineno: Line number (1-indexed)

        Returns:
            The source line or None if unavailable
        """
        if source_code is None or lineno < 1:
            return None
        lines = source_code.splitlines()
        if lineno <= len(lines):
            return lines[lineno - 1].strip()
        return None
