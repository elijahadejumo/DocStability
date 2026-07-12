#!/usr/bin/env python3
"""
Regenerate doc_commit_ownership.py output fresh for all 100 repos, using the
current script version. The blobless partial clones intermittently hit a git
promisor-remote object-fetch error on --name-only tree walks (transient --
observed repos succeed on a bare retry without recloning). Strategy: retry a
few times, then fall back to a full (non-blobless) reclone if still failing.
Resumable via a state file so a killed run doesn't lose completed work.
"""
import csv
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path("/Users/elijahadejumo/Documents/DocStability")
CLONES = ROOT / "_clones"
STATE_FILE = Path("/tmp/ownership_regen_done.txt")

def run(cmd, cwd=None):
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)

def try_ownership(repo):
    return run([
        "python3", "scripts/extraction/doc_commit_ownership.py",
        "--repo", f"_clones/{repo}",
        "--since", "2020-06-30", "--until", "2025-06-29",
        "--out_prefix", f"{repo}_2020_2025",
        "--dominant_threshold", "0.5",
    ], cwd=str(ROOT))

def repair_refetch(repo):
    # Cheaper than a full reclone: repairs just the objects the promisor
    # remote failed to lazily serve, on the EXISTING blobless clone -- no
    # need to re-download the whole repo.
    target = (CLONES / repo).resolve()
    print(f"  repairing {repo} via 'git fetch --refetch' (targeted, no full reclone)...", flush=True)
    r = run(["git", "-C", str(target), "fetch", "--refetch", "--filter=blob:none", "origin"])
    return r.returncode == 0

def refetch_full(repo):
    link = CLONES / repo
    target = link.resolve()
    owner_repo = target.name
    owner, gh_repo = owner_repo[:-4].split("__", 1)
    print(f"  full-reclone fallback for {repo} ({owner}/{gh_repo})...", flush=True)
    run(["rm", "-rf", str(target)])
    r = run(["git", "clone", "--bare", f"https://github.com/{owner}/{gh_repo}.git", str(target)])
    return r.returncode == 0

def load_done():
    if STATE_FILE.exists():
        return set(STATE_FILE.read_text().split())
    return set()

def mark_done(repo):
    with open(STATE_FILE, "a") as f:
        f.write(repo + "\n")

def main():
    with open(ROOT / "repos-names.csv", encoding="utf-8-sig") as f:
        repos = [row["repo"].strip() for row in csv.DictReader(f)]

    done = load_done()
    print(f"Resuming: {len(done)} already done", flush=True)

    failed = []
    for i, repo in enumerate(repos, 1):
        if repo in done:
            continue
        print(f"[{i}/{len(repos)}] {repo}", flush=True)

        ok = False
        for attempt in range(3):
            r = try_ownership(repo)
            if r.returncode == 0:
                ok = True
                break
            time.sleep(2)

        if not ok and repair_refetch(repo):
            r = try_ownership(repo)
            ok = r.returncode == 0

        if not ok:
            if refetch_full(repo):
                r = try_ownership(repo)
                ok = r.returncode == 0

        if ok:
            print(f"  ok", flush=True)
            mark_done(repo)
        else:
            print(f"  !! FAILED after retries+reclone: {repo}: {r.stderr[-400:]}", flush=True)
            failed.append(repo)

    print(f"\nDone. {len(repos)-len(failed)}/{len(repos)} succeeded (cumulative).")
    if failed:
        print(f"FAILED: {failed}")

if __name__ == "__main__":
    sys.exit(main())
