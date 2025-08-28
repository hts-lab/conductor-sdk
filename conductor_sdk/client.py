# conductor_sdk/client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Union
import os
import httpx

__all__ = ["ConductorClient", "Step", "submit_workflow"]

# ---- Types -------------------------------------------------------------------

@dataclass
class Step:
    operation: str
    method: str
    device: str

    def to_payload(self) -> Dict[str, str]:
        return {
            "operation": self.operation.strip(),
            "method": self.method.strip(),
            "device": self.device.strip(),
        }

def _steps_payload(steps: List[Union[Step, Dict[str, str]]]) -> List[Dict[str, str]]:
    if not steps:
        raise ValueError("At least one step is required")
    out: List[Dict[str, str]] = []
    for s in steps:
        if isinstance(s, Step):
            out.append(s.to_payload())
        else:
            out.append({
                "operation": str(s.get("operation","")).strip(),
                "method":    str(s.get("method","")).strip(),
                "device":    str(s.get("device","")).strip(),
            })
    # backend stores up to 5 step triplets
    return out[:5]

# ---- Client ------------------------------------------------------------------

class ConductorClient:
    """
    HTTP client for the Conductor Sync API (CSV-backed queue).
    Keep ctx-free: this is *not* tied to a running request context.
    """
    def __init__(
        self,
        base_url: str,
        bearer_token: Optional[str] = None,
        timeout: float = 30.0,
    ):
        if not base_url:
            raise ValueError("base_url is required (e.g. https://.../api)")
        self.base_url = base_url.rstrip("/")
        self.bearer_token = bearer_token or os.getenv("CONDUCTOR_BEARER_TOKEN") or None
        self.timeout = timeout

    @classmethod
    def from_env(cls) -> "ConductorClient":
        """
        Reads:
          - CONDUCTOR_API_BASE   e.g. https://...run.app
          - CONDUCTOR_BEARER_TOKEN (optional)
        """
        base = os.getenv("CONDUCTOR_API_BASE")
        if not base:
            raise RuntimeError("Set CONDUCTOR_API_BASE to your FastAPI base URL")
        return cls(base_url=base, bearer_token=os.getenv("CONDUCTOR_BEARER_TOKEN") or None)

    # --- Core call: submit a new workflow row --------------------------------
    def submit_workflow(
        self,
        *,
        project_id: str,
        experiment_id: str,
        created_by: str,
        source_plate_name: str,
        source_plate_format: str,
        steps: List[Union[Step, Dict[str, str]]],
    ) -> Dict[str, Any]:
        """
        POST /api/workflows/append-csv

        Returns: {"ok": True, "request_id": "...", "status": "NEW"}
        Raises httpx.HTTPStatusError on 4xx/5xx with server detail.
        """
        url = f"{self.base_url}/api/workflows/append-csv"
        payload = {
            "project_id": project_id.strip(),
            "experiment_id": experiment_id.strip(),
            "created_by": created_by.strip(),
            "source_plate_name": source_plate_name.strip(),
            "source_plate_format": source_plate_format.strip(),
            "steps": _steps_payload(steps),
        }

        headers = {"Content-Type": "application/json"}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"

        with httpx.Client(timeout=self.timeout) as client:
            r = client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            return r.json()

# ---- Convenience one-liner ---------------------------------------------------

def submit_workflow(
    *,
    project_id: str,
    experiment_id: str,
    created_by: str,
    source_plate_name: str,
    source_plate_format: str,
    steps: List[Union[Step, Dict[str, str]]],
    base_url: Optional[str] = None,
    bearer_token: Optional[str] = None,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    """
    Convenience wrapper; prefer ConductorClient for reuse.
    If base_url is omitted, uses CONDUCTOR_API_BASE from env.
    """
    client = (
        ConductorClient(base_url=base_url, bearer_token=bearer_token, timeout=timeout)
        if base_url else
        ConductorClient.from_env()
    )
    return client.submit_workflow(
        project_id=project_id,
        experiment_id=experiment_id,
        created_by=created_by,
        source_plate_name=source_plate_name,
        source_plate_format=source_plate_format,
        steps=steps,
    )
