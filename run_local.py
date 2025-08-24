import os, json, argparse, subprocess, sys
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--script", required=True, help="Path to a user script (e.g., examples/plate_heatmap.py)")
    ap.add_argument("--context", required=True, help="Path to context JSON (e.g., examples/context.local.json)")
    ap.add_argument("--dry-run", action="store_true", help="Print results.json changes instead of writing")
    args = ap.parse_args()

    ctx = Path(args.context).read_text(encoding="utf-8")
    env = os.environ.copy()
    env["CONDUCTOR_CONTEXT"] = ctx
    if args.dry_run:
        env["CONDUCTOR_DRY_RUN"] = "1"

    # Ensure package import works if not installed
    sys.path.append(str(Path(__file__).parent.resolve()))

    proc = subprocess.run([sys.executable, args.script], env=env, text=True)
    sys.exit(proc.returncode)

if __name__ == "__main__":
    main()
