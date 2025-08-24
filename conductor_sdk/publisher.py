# conductor_sdk/publisher.py
import os
import json
import datetime as dt
from typing import Dict, Any, Optional, Literal

Category = Literal["figures", "tables", "artifacts"]

def _is_offline() -> bool:
    # Treat either env as "no GCP needed"
    return os.environ.get("CONDUCTOR_DRY_RUN") == "1" or os.environ.get("CONDUCTOR_LOCAL") == "1"

class ResultsPublisher:
    """
    Writes/updates results.json atomically using GCS object-generation preconditions.
    In offline mode (CONDUCTOR_DRY_RUN=1 or CONDUCTOR_LOCAL=1), no GCP client is created;
    changes are printed to stdout instead.
    """
    def __init__(self, bucket: str, request_root: str):
        self.bucket_name = bucket
        self.request_root = request_root.rstrip("/")
        self._offline = _is_offline()

        if self._offline:
            # No GCS client in offline mode
            self.client = None
            self.bucket = None
            self.blob = None
        else:
            from google.cloud import storage  # import only when online
            self.client = storage.Client()
            self.bucket = self.client.bucket(self.bucket_name)
            self.blob = self.bucket.blob(f"{self.request_root}/results/results.json")

    def _load(self):
        if self._offline:
            return {"figures": [], "tables": [], "artifacts": []}, True
        # Import here to avoid requiring google-api-core in offline mode
        from google.api_core import exceptions as gexc
        try:
            data = self.blob.download_as_text()
            self.blob.reload()
            return json.loads(data), False
        except gexc.NotFound:
            return {"figures": [], "tables": [], "artifacts": []}, True

    def _save(self, doc: Dict[str, Any], creating: bool):
        payload = json.dumps(doc, indent=2)
        if self._offline:
            print("[DRY/OFFLINE] results.json would become:\n", payload)
            return
        if creating:
            self.blob.upload_from_string(payload, if_generation_match=0)
        else:
            self.blob.upload_from_string(payload, if_generation_match=self.blob.generation)

    def _next_id(self, doc):
        ids = [int(x.get("id", 0)) for k in ("figures","tables","artifacts") for x in doc.get(k, [])]
        return (max(ids) + 1) if ids else 1

    def _publish(self, cat: Category, rel: str, title="", description="", extra: Optional[Dict[str,Any]]=None):
        doc, creating = self._load()
        if not self._offline and not creating:
            # Refresh generation for safe conditional write
            self.blob.reload()
        item = {
            "id": self._next_id(doc),
            "path": f"{self.bucket_name}/{self.request_root}/{rel.lstrip('/')}",
            "title": title,
            "desc": description,
            "created_at": dt.datetime.utcnow().isoformat() + "Z",
        }
        if extra:
            item.update(extra)
        doc.setdefault(cat, []).append(item)
        self._save(doc, creating)
        return item

    def publish_figure(self, rel: str, title="", description="", **extra):
        return self._publish("figures", rel, title, description, extra or None)

    def publish_table(self, rel: str, title="", description="", **extra):
        return self._publish("tables", rel, title, description, extra or None)

    def publish_artifact(self, rel: str, title="", description="", **extra):
        return self._publish("artifacts", rel, title, description, extra or None)
