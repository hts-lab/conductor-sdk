import os, json
from pathlib import Path
from typing import Any, Dict, Optional, Union
from google.cloud import storage
from .publisher import ResultsPublisher

class _Ctx:
    """
    Runtime context injected via CONDUCTOR_CONTEXT (JSON), e.g.:
    {
      "bucket": "my-conductor-bucket",
      "mount_path": "/mnt/gcs",
      "request_root": "requests/REQ-1234",
      "paths": {
        "results":  "requests/REQ-1234/results",
        "figures":  "requests/REQ-1234/results/figures",
        "tables":   "requests/REQ-1234/results/tables",
        "artifacts":"requests/REQ-1234/results/artifacts"
      },
      "inputs": {...}   # optional
    }
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

        self.results_root = self.paths.get("results",   f"{self.request_root}/results")
        self.figures_root = self.paths.get("figures",   f"{self.results_root}/figures")
        self.tables_root  = self.paths.get("tables",    f"{self.results_root}/tables")
        self.artifacts_root = self.paths.get("artifacts", f"{self.results_root}/artifacts")
        self.inputs = ctx.get("inputs", {})

        # GCS client (not required for reading via mount, only for results.json updates)
        self._gcs = storage.Client()
        self._bucket = self._gcs.bucket(self.bucket_name)

        # Results publisher (atomic updates to results.json)
        self._publisher = ResultsPublisher(
            bucket=self.bucket_name,
            request_root=self.request_root
        )

    # ---------- Paths ----------
    def path(self, rel: Union[str, Path]) -> Path:
        return Path(self.mount) / str(rel).lstrip("/")

    def request_path(self, rel: Union[str, Path]) -> Path:
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

    # ---------- Resolver: auto-find by basename under data/** ----------
    def _resolve_under_data(self, rel: Union[str, Path]) -> Path:
        rel = Path(rel)
        # 1) try exact path under request root
        direct = self.request_path(rel)
        if direct.exists():
            return direct

        # 2) recursive search by filename inside request_root/data/**
        target = rel.name
        prefix = f"{self.request_root}/data/"
        matches: list[str] = []
        for blob in self._gcs.list_blobs(self.bucket_name, prefix=prefix):
            if blob.name.endswith("/" + target) or blob.name.endswith(target):
                matches.append(blob.name)
        if not matches:
            raise FileNotFoundError(
                f"Could not resolve '{rel}'. Tried '{direct}' and gs://{self.bucket_name}/{prefix}**/{target}"
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

    def open_text(self, rel: Union[str, Path], encoding="utf-8") -> str:
        p = self._resolve_under_data(rel)
        return p.read_text(encoding=encoding)

    # ---------- Publishers (auto-updates results.json atomically) ----------
    def publish_figure(self, fig_or_path: Any, filename: Optional[str] = None,
                       title: str = "", description: str = "", **extra):
        # Save if given a fig-like
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
            rel_for_json = str(p).split(self.request_root + "/", 1)[-1]

        self._publisher.publish_figure(rel_for_json, title=title, description=description, **extra)

    def publish_table(self, rel: Union[str, Path], title: str = "", description: str = "", **extra):
        rel = f"results/tables/{Path(rel).name}" if not str(rel).startswith("results/") else str(rel)
        self._publisher.publish_table(rel, title=title, description=description, **extra)

    def publish_artifact(self, rel: Union[str, Path], title: str = "", description: str = "", **extra):
        rel = f"results/artifacts/{Path(rel).name}" if not str(rel).startswith("results/") else str(rel)
        self._publisher.publish_artifact(rel, title=title, description=description, **extra)

# Singleton handle users import
ctx = _Ctx()
