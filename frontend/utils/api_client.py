"""API client for communicating with the backend service."""

import os
import time
from dataclasses import dataclass
from typing import Callable

import requests

BACKEND_BASE = os.getenv("BACKEND_URL", "http://backend:8000")

# Polling configuration
MAX_POLL_TIME = 120  # 2 minutes max
MAX_ATTEMPTS = 60
BASE_DELAY = 1.0
MAX_DELAY = 5.0


@dataclass
class JobStatus:
    """Represents the current status of a job."""

    status: str  # pending, running, analysis_complete, finished, failed
    job_id: str
    result: dict | None = None
    error: dict | None = None
    job_duration_ms: float | None = None


@dataclass
class SubmitResponse:
    """Response from job submission."""

    success: bool
    job_id: str | None = None
    error: str | None = None


class APIClient:
    """Client for interacting with the PySpark backend API."""

    def __init__(self, base_url: str | None = None):
        self.base_url = base_url or BACKEND_BASE

    def submit_job(self, code: str) -> SubmitResponse:
        """Submit PySpark code for analysis."""
        try:
            response = requests.post(
                f"{self.base_url}/explain/pyspark",
                json={"code": code},
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                return SubmitResponse(success=True, job_id=data.get("job_id"))
            else:
                try:
                    error_data = response.json()
                    error_msg = str(error_data)
                except Exception:
                    error_msg = f"Status {response.status_code}: {response.text}"
                return SubmitResponse(success=False, error=error_msg)
        except requests.exceptions.ConnectionError:
            return SubmitResponse(
                success=False, error="Cannot connect to backend. Is it running?"
            )
        except requests.exceptions.Timeout:
            return SubmitResponse(success=False, error="Backend request timed out")
        except requests.exceptions.RequestException as e:
            return SubmitResponse(success=False, error=str(e))

    def get_status(self, job_id: str) -> JobStatus:
        """Get the current status of a job."""
        try:
            response = requests.get(
                f"{self.base_url}/status/{job_id}",
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            status = data.get("status", "unknown")
            result = data.get("result")
            error = None

            if status == "failed" and result:
                error = result.get("error", {})

            return JobStatus(
                status=status,
                job_id=job_id,
                result=result,
                error=error,
                job_duration_ms=data.get("job_duration_ms"),
            )
        except requests.exceptions.RequestException as e:
            return JobStatus(
                status="error",
                job_id=job_id,
                error={"type": "ConnectionError", "message": str(e)},
            )

    def poll_until_complete(
        self,
        job_id: str,
        on_status_change: Callable[[JobStatus], None] | None = None,
    ) -> JobStatus:
        """
        Poll job status until completion or timeout.

        Args:
            job_id: The job ID to poll
            on_status_change: Optional callback for status updates

        Returns:
            Final JobStatus
        """
        start_time = time.time()
        attempt = 0
        delay = BASE_DELAY
        last_status = None

        while attempt < MAX_ATTEMPTS:
            elapsed = time.time() - start_time
            if elapsed > MAX_POLL_TIME:
                return JobStatus(
                    status="timeout",
                    job_id=job_id,
                    error={
                        "type": "Timeout",
                        "message": f"Job timed out after {MAX_POLL_TIME} seconds",
                    },
                )

            status = self.get_status(job_id)

            # Notify callback if status changed
            if on_status_change and status.status != last_status:
                on_status_change(status)
                last_status = status.status

            if status.status in ("finished", "failed", "error"):
                return status

            if status.status in ("pending", "running", "analysis_complete"):
                attempt += 1
                time.sleep(delay)
                delay = BASE_DELAY  # Reset on successful poll
            else:
                # Unknown status
                return status

        return JobStatus(
            status="timeout",
            job_id=job_id,
            error={"type": "Timeout", "message": "Max polling attempts reached"},
        )

    def check_health(self) -> bool:
        """Check if the backend is healthy."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
