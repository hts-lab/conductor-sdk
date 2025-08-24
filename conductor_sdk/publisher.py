import os, json, datetime as dt
from typing import Dict, Any, Optional, Literal
from google.cloud import storage
from google.api_core import exceptions as gexc

Category = Literal["figures","tables","artifacts"]

class ResultsPublisher:
    """
    Writes/updates results.json atomically using GCS object generation preconditions.
    Set CONDUCTOR_DRY_RUN=1 to log the change instead of writing (handy for local tests).
    """
    def __init__(self, bucket: str, request_root: str):
        self.bucket_name = bucket
        self.request_root = request_root.rstrip("/")
        self._dry = os.environ.get("CONDUCTOR_DRY_RUN") == "1"
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)
        self.blob = self.bucket.blob(f"{self.request_root}/results/results.json")

    def _load(self):
        if self._dry:
            return {"figures": [], "tables": [], "artifacts": []}, True
        try:
            data = self.blob.download_as_text()
            self.blob.reload()
            return json.loads(data), False
        except gexc.NotFound:
            return {"figures": [], "tables": [], "artifacts": []}, True

    def _save(self, doc: Dict[str, Any], creating: bool):
        payload = json.dumps(doc, indent=2)
        if self._dry:
            print("[DRY RUN] results.json would become:\n", payload)
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
        if not creating and not self._dry:
            self.blob.reload()
        item = {
            "id": self._next_id(doc),
            "path": f"{self.bucket_name}/{self.request_root}/{rel.lstrip('/')}",
            "title": title, "desc": description,
            "created_at": dt.datetime.utcnow().isoformat() + "Z"
        }
        if extra: item.update(extra)
        doc.setdefault(cat, []).append(item)
        self._save(doc, creating)
        return item

    def publish_figure(self, rel: str, title="", description="", **extra):
        return self._publish("figures", rel, title, description, extra or None)

    def publish_table(self, rel: str, title="", description="", **extra):
        return self._publish("tables", rel, title, description, extra or None)

    def publish_artifact(self, rel: str, title="", description="", **extra):
        return self._publish("artifacts", rel, title, description, extra or None)
