# conductor_sdk/context.py
import os
import json
from pathlib import Path
from typing import Any, Dict, Optional, Union

from google.cloud import storage  # imported, but client is only constructed when online
from .publisher import ResultsPublisher


class _Ctx:
    """
    Runtime context injected via CONDUCTOR_CONTEXT (JSON), e.g.:
    {
      "bucket": "my-conductor-bucket",
      "mount_path": "/mnt/gcs",
      "request_root": "requests/REQ-1234",
      "paths": {
        "results":   "requests/REQ-1234/results",
        "figures":   "requests/REQ-1234/results/figures",
        "tables":    "requests/REQ-1234/results/tables",
        "artifacts": "requests/REQ-1234/results/artifacts"
      },
      "inputs": {...}   # optional catalog
    }

    OFFLINE mode (no GCP credentials required) is enabled when:
      - CONDUCTOR_DRY_RUN=1  OR  CONDUCTOR_LOCAL=1
    In offline mode, file discovery uses the local mounted filesystem only.
    """

    def __init__(self):
        raw = os.environ.get("CONDUCTOR_CONTEXT")
        if not raw:
            raise RuntimeError("Missing CONDUCTOR_CONTEXT")
        ctx = json.loads(raw)

        self.bucket_name: str = ctx["bucket"]
        self.mount: str = ctx.get("mount_path", "/mnt/gcs")
        self.request_root: str = ctx["request_root"].rstrip("/")
        self.paths: Dict[str, str] = ctx.get("paths", {})
        self.inputs: Dict[str, Any] = ctx.get("inputs", {})

        # Resolved standard locations (with fallbacks)
        self.results_root = self.paths.get("results", f"{self.request_root}/results")
        self.figures_root = self.paths.get("figures", f"{self.results_root}/figures")
        self.tables_root = self.paths.get("tables", f"{self.results_root}/tables")
        self.artifacts_root = self.paths.get("artifacts", f"{self.results_root}/artifacts")

        # Offline if dry-run or explicitly set
        self._offline = (
            os.environ.get("CONDUCTOR_DRY_RUN") == "1"
            or os.environ.get("CONDUCTOR_LOCAL") == "1"
        )

        if self._offline:
            self._gcs = None
            self._bucket = None
        else:
            self._gcs = storage.Client()
            self._bucket = self._gcs.bucket(self.bucket_name)

        # Atomic results.json updater (honors dry-run inside)
        self._publisher = ResultsPublisher(
            bucket=self.bucket_name, request_root=self.request_root
        )

    # ---------- Path helpers ----------
    def path(self, rel: Union[str, Path]) -> Path:
        """Absolute path under the mounted bucket root."""
        return Path(self.mount) / str(rel).lstrip("/")

    def request_path(self, rel: Union[str, Path]) -> Path:
        """Absolute path under this request root."""
        return self.path(f"{self.request_root}/{rel}")

    def data_path(self, rel: Union[str, Path]) -> Path:
        return self.request_path(f"data/{rel}")

    def results_path(self, rel: Union[str, Path]) -> Path:
        return self.request_path(f"results/{rel}")

    def figures_path(self, filename: str) -> Path:
        return self.path(f"{self.figures_root}/{filename}")

    def tables_path(self, filename: str) -> Path:
        return self.path(f"{self.tables_root}/{filename}")

    def artifacts_path(self, filename: str) -> Path:
        return self.path(f"{self.artifacts_root}/{filename}")

    # ---------- Basename resolver under data/** ----------
    def _resolve_under_data(self, rel: Union[str, Path]) -> Path:
        """
        If 'rel' exists as a direct path under request_root, use it.
        Otherwise, search request_root/data/** for the first lexicographic match
        of the basename and return it.

        Examples the user may pass:
          - "data/Operetta_objectresults.csv"
          - "data/some/deeper/tree/Operetta_objectresults.csv" (exact)
        """
        rel = Path(rel)

        # 1) Direct path (exact)
        direct = self.request_path(rel)
        if direct.exists():
            return direct

        # 2) Recursive search by filename under data/
        target = rel.name
        if self._offline:
            # Local filesystem search
            base = self.path(f"{self.request_root}/data")
            if not base.exists():
                raise FileNotFoundError(f"Data directory not found: {base}")
            # rglob with pattern 'target' finds files with that exact basename
            hits = sorted(
                [p for p in base.rglob(target) if p.is_file() and p.name == target],
                key=lambda p: str(p),
            )
            if not hits:
                raise FileNotFoundError(
                    f"Could not resolve '{rel}'. Looked under {base} recursively for '{target}'."
                )
            return hits[0]
        else:
            # GCS listing search (cheap suffix test for whole filename)
            prefix = f"{self.request_root}/data/"
            matches: list[str] = []
            for blob in self._gcs.list_blobs(self.bucket_name, prefix=prefix):
                # Ensure we match the terminal filename
                if blob.name.endswith("/" + target) or blob.name.endswith(target):
                    matches.append(blob.name)
            if not matches:
                raise FileNotFoundError(
                    f"Could not resolve '{rel}'. Tried '{direct}' and "
                    f"gs://{self.bucket_name}/{prefix}**/{target}"
                )
            matches.sort()  # deterministic first match
            return self.path(matches[0])

    # ---------- Readers ----------
    def read_csv(self, rel: Union[str, Path], **pandas_kwargs):
        import pandas as pd
        p = self._resolve_under_data(rel)
        return pd.read_csv(p, **pandas_kwargs)

    def read_parquet(self, rel: Union[str, Path], **pandas_kwargs):
        import pandas as pd
        p = self._resolve_under_data(rel)
        return pd.read_parquet(p, **pandas_kwargs)

    def open_bytes(self, rel: Union[str, Path]) -> bytes:
        p = self._resolve_under_data(rel)
        return p.read_bytes()

    def open_text(self, rel: Union[str, Path], encoding: str = "utf-8") -> str:
        p = self._resolve_under_data(rel)
        return p.read_text(encoding=encoding)

    # ---------- Publishers (auto-updates results.json atomically) ----------
    def publish_figure(
        self,
        fig_or_path: Any,
        filename: Optional[str] = None,
        title: str = "",
        description: str = "",
        **extra,
    ):
        """
        Publish a figure:
          - If given a matplotlib Figure (has .savefig), save it to results/figures/<filename>
          - If given a path, register that path (relative to request root if not absolute)
        Then upsert results.json with an auto-incremented id (atomic).
        """
        # If it's a matplotlib Figure (or fig-like), save it first
        if hasattr(fig_or_path, "savefig"):
            if not filename:
                raise ValueError("filename required when publishing a Figure")
            out = self.figures_path(filename)
            out.parent.mkdir(parents=True, exist_ok=True)
            fig_or_path.savefig(out, dpi=200, bbox_inches="tight")
            rel_for_json = f"results/figures/{filename}"
        else:
            p = Path(fig_or_path)
            if not p.is_absolute():
                p = self.request_path(p)
            # Derive request-relative portion after "<request_root>/"
            marker = self.request_root + "/"
            s = str(p)
            rel_for_json = s.split(marker, 1)[-1] if marker in s else s

        self._publisher.publish_figure(
            rel_for_json, title=title, description=description, **extra
        )

    def publish_table(
        self,
        rel: Union[str, Path],
        title: str = "",
        description: str = "",
        **extra,
    ):
        rel = f"results/tables/{Path(rel).name}" if not str(rel).startswith("results/") else str(rel)
        self._publisher.publish_table(rel, title=title, description=description, **extra)

    def publish_artifact(
        self,
        rel: Union[str, Path],
        title: str = "",
        description: str = "",
        **extra,
    ):
        rel = f"results/artifacts/{Path(rel).name}" if not str(rel).startswith("results/") else str(rel)
        self._publisher.publish_artifact(rel, title=title, description=description, **extra)


# Singleton users import:  from conductor_sdk import ctx
ctx = _Ctx()
