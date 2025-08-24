import os, json, argparse, subprocess, sys
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--script", required=True, help="Path to a user script (e.g., examples/plate_heatmap.py)")
    ap.add_argument("--context", required=True, help="Path to context JSON (e.g., examples/context.local.json)")
    ap.add_argument("--dry-run", action="store_true", help="Print results.json changes instead of writing")
    args = ap.parse_args()

    repo_root = str(Path(__file__).parent.resolve())  # repo containing conductor_sdk/
    ctx_json = Path(args.context).read_text(encoding="utf-8")

    env = os.environ.copy()
    env["CONDUCTOR_CONTEXT"] = ctx_json
    if args.dry_run:
        env["CONDUCTOR_DRY_RUN"] = "1"
        env["CONDUCTOR_LOCAL"] = "1"

    # Prepend repo to PYTHONPATH so child imports local conductor_sdk
    env["PYTHONPATH"] = repo_root + (os.pathsep + env.get("PYTHONPATH", ""))

    # Optional: ensure headless plotting works locally
    env.setdefault("MPLBACKEND", "Agg")

    # Run the user script as a child process
    proc = subprocess.run([sys.executable, args.script], env=env, text=True)
    sys.exit(proc.returncode)

if __name__ == "__main__":
    main()
