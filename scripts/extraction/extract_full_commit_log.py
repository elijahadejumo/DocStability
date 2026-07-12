#!/usr/bin/env python3
"""
extract_full_commit_log.py

Run this LOCALLY (not in the sandboxed agent session) -- it needs real git clone access.

For each repo in repos-names.csv, does a blobless bare clone (fast, small -- no file
contents downloaded, just commit metadata) and extracts the FULL commit history
(no date bound) with author identity, normalized the same way the original
ownership scripts do (GitHub noreply-email collapsing).

This single extraction feeds two things:
  1. RQ4 retention / newcomer-to-core analysis (needs each contributor's true
     first-ever commit date, not just their first commit inside the 2020-2025
     window -- otherwise a contributor active since 2015 who also commits in
     2021 looks like a "newcomer").
  2. A bot-filter join key for RQ1's rhythm computation: Doc_rhythm.py /
     doc_entropy.py never filter bots (only the ownership scripts do), so a few
     archetype labels may be contaminated by bot-driven AUTHORS/CONTRIBUTORS
     churn (see react-router's contributors.yml: 563 commits, django's AUTHORS:
     157 commits -- almost certainly an all-contributors-style bot). This
     extraction lets us look up the author of every doc-touching commit and
     re-run RQ1 with bots excluded.

Output: one CSV per repo at <out_dir>/<repo>_full_commit_log.csv with columns:
  repo, commit_sha, author_name, author_email, author_id, commit_date, is_bot

author_id follows the SAME normalization as your existing pipeline:
  GitHub noreply addresses {N}+{username}@users.noreply.github.com are
  collapsed to {username}@users.noreply.github.com so web-editor and local-
  client commits by the same person merge.

is_bot uses the same heuristic patterns as contrib_concentration.py
(name/email containing 'bot', '[bot]', 'dependabot', 'github-actions', etc.)
-- adjust BOT_PATTERNS below if your original script's list differs; check
looks_like_bot() in contrib_concentration.py and paste the exact patterns in
if you want byte-identical bot classification.

Usage:
  python3 extract_full_commit_log.py \
      --repos-csv /path/to/repos-names.csv \
      --clone-dir ./_clones \
      --out-dir ./full_commit_logs \
      [--resume] [--include-merges]

Then zip/send back the out-dir (or just the concatenated CSV -- see
combine step at the bottom) to continue the analysis.

Estimated cost: blobless clones are typically 5-15% the size of a full clone.
Expect this to take a while for the largest repos (FFmpeg, WordPress, go,
aspnetcore, linux-scale projects) -- run with --resume so it's safe to
interrupt and restart; already-extracted repos are skipped.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# EXACT same list as contrib_concentration.py's BOT_PATTERNS -- kept byte-identical
# so bot classification here matches your existing ownership/bus-factor pipeline.
BOT_PATTERNS = [
    r'\bbot\b', r'\bbots\b', r'github-actions', r'dependabot',
    r'renovate', r'greenkeeper', r'codecov', r'coveralls',
    r'travis-ci', r'circleci', r'jenkins', r'azure-pipelines',
    r'\bbors\b', r'homu', r'mergify', r'kodiak', r'auto-merge',
    r'rust-timer', r'rustbot', r'rust-highfive',
    r'kubernetes-', r'k8s-ci-robot', r'automation',
    r'\[bot\]', r'\(bot\)', r'service account',
    r'noreply@github\.com$', r'actions@github\.com$',
    r'@github\.com$',
    r'automated',
    r'version-bump', r'release-bot', r'changelog-bot',
    r'homebrew-', r'allcontributors',
]
BOT_PATTERNS_COMPILED = [(p, re.compile(p, re.IGNORECASE)) for p in BOT_PATTERNS]


def looks_like_bot(name: str, email: str) -> bool:
    s = f"{name} {email}".strip()
    return any(rx.search(s) for (_, rx) in BOT_PATTERNS_COMPILED)


def normalize_author_id(name: str, email: str) -> str:
    email = (email or "").strip().lower()
    m = re.match(r"^\d+\+([^@]+)@users\.noreply\.github\.com$", email)
    if m:
        return f"{m.group(1)}@users.noreply.github.com"
    if email:
        return email
    return (name or "unknown").strip().lower()


def run(cmd: list[str], cwd: str | None = None) -> subprocess.CompletedProcess:
    # errors="replace": real-world commit messages/author names are not always
    # valid UTF-8 (mixed-locale history, old commits, etc). A strict decode
    # crashes the whole batch on a single bad byte -- replace instead of raise.
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                           encoding="utf-8", errors="replace")


def clone_repo(owner: str, repo: str, clone_dir: Path, github_repo: str | None = None) -> Path | None:
    # 'repo' is the LOCAL name (matches existing per_repo/<repo>/ folders and output
    # filenames); 'github_repo' overrides just the URL path when the repo was renamed
    # on GitHub (e.g. local name "Python" / actual repo "cpython"). Target dir is keyed
    # on the local name so it lines up with the rest of the replication package.
    target = clone_dir / f"{owner}__{repo}.git"
    if target.exists():
        print(f"  [skip clone] {target} already exists")
        return target
    url_repo = github_repo or repo
    url = f"https://github.com/{owner}/{url_repo}.git"
    print(f"  cloning {url} (blobless, bare) ...")
    r = run(["git", "clone", "--bare", "--filter=blob:none", url, str(target)])
    if r.returncode != 0:
        print(f"  !! clone failed for {owner}/{url_repo}: {r.stderr[-500:]}")
        return None
    return target


def extract_log(repo_path: Path, include_merges: bool) -> list[tuple[str, str, str, int, int, str]]:
    # %at = author time (matches contrib_concentration.py's ownership/bus-factor convention)
    # %ct = committer time (matches Doc_rhythm.py/doc_entropy.py's rhythm convention)
    # %s  = subject line -- needed to re-validate the external-coordination-linkage
    #       heuristic (commit_message_external_links.py) against a proper random
    #       sample, since no raw commit message is currently persisted anywhere
    #       in the replication package (only a handful of truncated examples).
    # both timestamps are captured so downstream analysis can match whichever
    # pipeline (rhythm vs ownership) it's extending
    fmt = "--pretty=format:%H%x1f%an%x1f%ae%x1f%at%x1f%ct%x1f%s"
    cmd = ["git", "-C", str(repo_path), "log", fmt]
    if not include_merges:
        cmd.insert(4, "--no-merges")
    r = run(cmd)
    if r.returncode != 0:
        print(f"  !! log failed: {r.stderr[-500:]}")
        return []
    rows = []
    for line in r.stdout.splitlines():
        parts = line.split("\x1f", 5)
        if len(parts) != 6:
            continue
        sha, an, ae, at, ct, subject = parts
        try:
            author_epoch = int(at)
            committer_epoch = int(ct)
        except ValueError:
            continue
        rows.append((sha, an, ae, author_epoch, committer_epoch, subject))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repos-csv", required=True)
    ap.add_argument("--clone-dir", default="./_clones")
    ap.add_argument("--out-dir", default="./full_commit_logs")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--include-merges", action="store_true")
    args = ap.parse_args()

    clone_dir = Path(args.clone_dir)
    out_dir = Path(args.out_dir)
    clone_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(args.repos_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        repo_rows = list(reader)

    print(f"Loaded {len(repo_rows)} repos from {args.repos_csv}")

    failures: list[tuple[str, str, str]] = []

    for i, row in enumerate(repo_rows, 1):
        repo = row["repo"].strip()
        owner = row["owner"].strip()
        github_repo = (row.get("github_repo") or "").strip() or None
        out_csv = out_dir / f"{repo}_full_commit_log.csv"
        if args.resume and out_csv.exists():
            print(f"[{i}/{len(repo_rows)}] {repo}: already extracted, skipping")
            continue

        print(f"[{i}/{len(repo_rows)}] {owner}/{github_repo or repo}", flush=True)
        try:
            repo_path = clone_repo(owner, repo, clone_dir, github_repo=github_repo)
            if repo_path is None:
                failures.append((repo, owner, "clone failed (see log above -- check owner is still correct)"))
                continue

            rows = extract_log(repo_path, args.include_merges)
            print(f"  extracted {len(rows)} commits", flush=True)

            with open(out_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["repo", "commit_sha", "author_name", "author_email", "author_id",
                            "author_date", "committer_date", "is_bot", "subject"])
                for sha, an, ae, at_epoch, ct_epoch, subject in rows:
                    author_dt = datetime.fromtimestamp(at_epoch, tz=timezone.utc).date().isoformat()
                    committer_dt = datetime.fromtimestamp(ct_epoch, tz=timezone.utc).date().isoformat()
                    author_id = normalize_author_id(an, ae)
                    is_bot = looks_like_bot(an, ae)
                    w.writerow([repo, sha, an, ae, author_id, author_dt, committer_dt, is_bot, subject])
        except Exception as e:
            # one bad repo (encoding oddity, truncated clone, etc.) must not
            # take down the other 99 -- log it and keep going.
            print(f"  !! unexpected failure on {owner}/{repo}: {e!r}", flush=True)
            failures.append((repo, owner, repr(e)))
            if out_csv.exists():
                out_csv.unlink()  # don't leave a partial/corrupt file that --resume would treat as done
            continue

    print("\nDone. Combine everything into one file with:")
    print(f"  python3 -c \"import pandas as pd, glob; "
          f"pd.concat([pd.read_csv(f) for f in glob.glob('{out_dir}/*_full_commit_log.csv')])"
          f".to_csv('{out_dir}/combined_full_commit_log.csv', index=False)\"")
    print(f"\nThen send back: {out_dir}/combined_full_commit_log.csv")

    if failures:
        print(f"\n{len(failures)} repo(s) failed and were skipped:")
        for repo, owner, reason in failures:
            print(f"  - {owner}/{repo}: {reason}")
        print("Re-run with --resume after fixing (e.g. correcting a stale owner in repos-names.csv);"
              " already-succeeded repos will be skipped automatically.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
