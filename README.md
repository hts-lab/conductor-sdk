# conductor-sdk

A tiny Python SDK that gives analysis scripts a simple `ctx`:

- `ctx.read_csv("data/<basename>.csv")` → auto-resolves under `request_root/data/**`
- `ctx.publish_figure/table/artifact(...)` → writes outputs under `results/` and updates `results.json` atomically

## Install (dev)
```bash
pip install -e ".[analysis]"

```

## Local test
1. Create a sandbox tree and drop data:
```
_sandbox/requests/REQ-LOCAL/data/Operetta/1234/Operetta_objectresults.csv
```

2. Run the example:
```bash
python run_local.py --script examples/plate_heatmap.py --context examples/context.local.json --dry-run
```
- Set CONDUCTOR_DRY_RUN=1 (or --dry-run) to avoid needing GCP credentials.
- Without --dry-run, you’ll need Google Cloud auth and a bucket named in your context.




### Submitting a new workflow programmatically

```python
from conductor_sdk import submit_workflow, Step

resp = submit_workflow(
    project_id="demo",
    experiment_id="exp-001",
    created_by="user@mit.edu",
    source_plate_name="SrcPlate01",
    source_plate_format="96wEMPTY",
    steps=[
        Step("Dispense","LoadActiveWorklist","Tecan EVO"),
        Step("Read","Absorbance_350-750_1nm","Spark 1.1"),
    ],
    # or set CONDUCTOR_API_BASE / CONDUCTOR_BEARER_TOKEN in env
    base_url="https://conductor-sync-api-....run.app",
)
print(resp["request_id"])
```
