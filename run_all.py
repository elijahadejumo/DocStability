#!/usr/bin/env python3
"""
run_all.py

Runs contrib_concentration.py and doc_commit_ownership.py on every
subdirectory (git repo) inside a given directory.

Usage:
  python3 run_all.py \
    --repos_dir ./repos \
    --since 2020-06-30 \
    --until 2025-06-29 \
    --out_prefix 5yr
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def is_git_repo(path: Path) -> bool:
    return (path / ".git").exists()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repos_dir", required=True,
                    help="Directory containing all cloned repos as subdirectories")
    ap.add_argument("--since", required=True, help="YYYY-MM-DD")
    ap.add_argument("--until", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out_prefix", default="5yr")
    ap.add_argument("--dominant_threshold", default="0.5")
    ap.add_argument("--topk", nargs="+", default=["3", "5", "10"])
    args = ap.parse_args()

    repos_dir = Path(args.repos_dir).resolve()
    repos = sorted([p for p in repos_dir.iterdir()
                    if p.is_dir() and is_git_repo(p)])

    if not repos:
        print(f"No git repos found in {repos_dir}")
        sys.exit(1)

    print(f"Found {len(repos)} repos in {repos_dir}\n")

    failed = []

    for i, repo in enumerate(repos, 1):
        print(f"[{i}/{len(repos)}] {repo.name}")

        # ── contrib_concentration.py ──
        cmd_conc = [
            sys.executable, "contrib_concentration.py",
            "--repo",          str(repo),
            "--since",         args.since,
            "--until",         args.until,
            "--out_prefix",    args.out_prefix,
            "--topk",          *args.topk,
            "--exclude_bots",
            "--write_details",
            "--write_bots",
        ]
        result = subprocess.run(cmd_conc, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  ✗ contrib_concentration failed: {result.stderr.strip()}")
            failed.append((repo.name, "contrib_concentration"))
        else:
            print(f"  ✓ contrib_concentration done")

        # ── doc_commit_ownership.py ──
        cmd_own = [
            sys.executable, "doc_commit_ownership.py",
            "--repo",                str(repo),
            "--since",               args.since,
            "--until",               args.until,
            "--out_prefix",          args.out_prefix,
            "--dominant_threshold",  args.dominant_threshold,
        ]
        result = subprocess.run(cmd_own, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  ✗ doc_commit_ownership failed: {result.stderr.strip()}")
            failed.append((repo.name, "doc_commit_ownership"))
        else:
            print(f"  ✓ doc_commit_ownership done")

        print()

    # ── Summary ──
    print("=" * 50)
    print(f"Done. {len(repos) - len(failed)} / {len(repos)} repos completed cleanly.")
    if failed:
        print(f"\nFailed ({len(failed)}):")
        for repo_name, script in failed:
            print(f"  {repo_name} — {script}")


if __name__ == "__main__":
    main()