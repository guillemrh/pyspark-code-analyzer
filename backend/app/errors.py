# backend/app/errors.py
"""Standardized error types and response formatting."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class AppError:
    """Standardized error response format."""

    type: str
    message: str
    details: Optional[dict] = None
    retryable: bool = False

    def to_dict(self) -> dict:
        result = {
            "type": self.type,
            "message": self.message,
            "retryable": self.retryable,
        }
        if self.details:
            result["details"] = self.details
        return result


class PySparkExplainerError(Exception):
    """Base exception for application errors."""

    def __init__(self, message: str, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable


class ValidationError(PySparkExplainerError):
    """Input validation failed."""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message, retryable=False)
        self.details = details


class LLMError(PySparkExplainerError):
    """LLM service error."""

    pass


class CacheError(PySparkExplainerError):
    """Cache service error."""

    def __init__(self, message: str):
        super().__init__(message, retryable=True)
