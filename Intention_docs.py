#!/usr/bin/env python3
"""
health_docs_intention_summary_rhythm_consistent.py

Intention summary that matches doc_touch_rhythm.py EXACTLY for doc-touch counting:
- Same git log format: __COMMIT__%H\t%ct + --name-only  (COMMITTER time)
- Same include/exclude logic: get_health_files() (exclude first, then match)
- Same merge handling: default excludes merges unless --include_merges
- NO bot filtering by default (to match rhythm)

Classification is done on the SAME filtered universe as rhythm:
- We ignore excluded files when deciding "only" vs "mixed"
  (because rhythm ignores excluded files entirely).

Outputs:
  outputs/<repo_name>/<out_prefix>_health_docs_intention_summary.csv
  outputs/<repo_name>/<out_prefix>_summary.json
Optional audit:
  outputs/<repo_name>/<out_prefix>_health_docs_touch_shas.txt
  outputs/<repo_name>/<out_prefix>_health_docs_commit_classification.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterator, List, Optional, Sequence, Tuple


# ----------------------------
# SAME RULES AS RHYTHM SCRIPT
# ----------------------------

ROOT_HEALTH_FILES = [
    r"^README(?:\..+)?$",
    r"^CONTRIBUTING(?:\..+)?$",
    r"^CONTRIBUTORS(?:\..+)?$",
    r"^COMMIT_CONVENTIONS(?:\..+)?$",
    r"^PULL_REQUEST_TEMPLATE(?:\..+)?$",
    r"^ISSUE_TEMPLATE(?:\..+)?$",
    r"^BUILDING(?:\..+)?$",

    r"^CHANGELOG(?:\..+)?$",
    r"^HISTORY(?:\..+)?$",
    r"^RELEASES?(?:\..+)?$",

    r"^CODE_OF_CONDUCT(?:\..+)?$",
    r"^GOVERNANCE(?:\..+)?$",
    r"^SUPPORT(?:\..+)?$",
    r"^MAINTAINERS(?:\..+)?$",

    r"^SECURITY(?:\..+)?$",
    r"^LICENSE(?:\..+)?$",
    r"^NOTICE(?:\..+)?$",
    r"^COPYING(?:\..+)?$",

    r"^AUTHORS(?:\..+)?$",
    r"^CREDITS(?:\..+)?$",
    r"^THANKS(?:\..+)?$",

    r"^ROADMAP(?:\..+)?$",
    r"^VISION(?:\..+)?$",

    r"^\.github/SECURITY(?:\..+)?$",
    r"^\.github/CONTRIBUTING(?:\..+)?$",
    r"^\.github/CODE_OF_CONDUCT(?:\..+)?$",
    r"^\.github/SUPPORT(?:\..+)?$",
    r"^\.github/COMMIT_CONVENTIONS(?:\..+)?$",
    r"^\.github/PULL_REQUEST_TEMPLATE(?:\..+)?$",
    r"^\.github/ISSUE_TEMPLATE(?:\..+)?$",
    r"^\.github/BUILDING(?:\..+)?$",

    r"^\.gitlab/CONTRIBUTING(?:\..+)?$",
    r"^\.gitlab/CODE_OF_CONDUCT(?:\..+)?$",
    r"^\.gitlab/COMMIT_CONVENTIONS(?:\..+)?$",
    r"^\.gitlab/PULL_REQUEST_TEMPLATE(?:\..+)?$",
    r"^\.gitlab/ISSUE_TEMPLATE(?:\..+)?$",
    r"^\.gitlab/BUILDING(?:\..+)?$",
]

EXCLUDE_PATTERNS = [
    r"^[^/]+/[^/]+/",

    r"^libs/",
    r"^modules/",
    r"^x-pack/",
    r"^plugins?/",
    r"^packages?/",
    r"^distribution/",
    r"^build-tools",
    r"^qa/",
    r"^test/",
    r"^benchmarks?/",

    r"^src/",
    r"/src/",

    r"(^|/)node_modules/",
    r"(^|/)vendor/",
    r"(^|/)dist/",
    r"(^|/)build/",
    r"(^|/)site/",
    r"(^|/)out/",
    r"(^|/)target/",
    r"(^|/)\.git/",

    r"(^|/)docs?/",
    r"(^|/)documentation/",
    r"(^|/)wiki/",
    r"(^|/)guides?/",
    r"(^|/)tutorials?/",
    r"(^|/)examples?/",
    r"(^|/)man/",
    r"(^|/)api/",
    r"(^|/)reference/",

    r"\.(png|jpg|jpeg|gif|svg|ico|pdf)$",
    r"\.(zip|tar|gz|bz2|7z|rar)$",
    r"\.(exe|dll|so|dylib|bin)$",

    r"\.(java|py|js|ts|go|rs|cpp|c|h|hpp)$",
    r"\.(scala|kt|swift|rb|php|cs|fs)$",

    r"(^|/)test/",
    r"(^|/)tests/",
    r"/resources/",
    r"\.(cef|json|xml|yaml|yml)\.txt$",

    r"(^|/)conf\.py$",
    r"(^|/)_config\.yml$",
    r"(^|/)mkdocs\.yml$",
    r"(^|/)Doxyfile$",
    r"output\.txt$",

    r"/translations?/",
    r"/i18n/",
    r"/locales?/",
    r"\.(zh|ja|ko|fr|de|es|it|pt|ru)\.md$",
]


# ----------------------------
# SAME GIT PARSER AS RHYTHM
# ----------------------------

@dataclass(frozen=True)
class CommitRecord:
    sha: str
    commit_dt: datetime
    files: Tuple[str, ...]


def run_git(repo: str, args: List[str]) -> str:
    cmd = ["git", "-C", repo] + args
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    return out.decode("utf-8", errors="replace")


def iter_commits_with_files(repo: str, since: date, until: date, include_merges: bool) -> Iterator[CommitRecord]:
    log_args = [
        "log",
        "--no-color",
        f"--since={since.isoformat()} 00:00:00",
        f"--until={until.isoformat()} 23:59:59",
        "--pretty=format:__COMMIT__%H\t%ct",
        "--name-only",
    ]
    if not include_merges:
        log_args.insert(1, "--no-merges")

    raw = run_git(repo, log_args)

    sha = None
    epoch = None
    files: List[str] = []

    for line in raw.splitlines():
        if line.startswith("__COMMIT__"):
            if sha is not None and epoch is not None:
                dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
                yield CommitRecord(sha=sha, commit_dt=dt, files=tuple(f for f in files if f))

            files = []
            header = line[len("__COMMIT__"):]
            parts = header.split("\t")
            if len(parts) != 2:
                sha, epoch = None, None
                continue
            sha = parts[0].strip()
            try:
                epoch = int(parts[1].strip())
            except ValueError:
                sha, epoch = None, None
        else:
            if sha is not None:
                s = line.strip()
                if s:
                    files.append(s)

    if sha is not None and epoch is not None:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        yield CommitRecord(sha=sha, commit_dt=dt, files=tuple(f for f in files if f))


# ----------------------------
# SAME HEALTH FILE MATCHING AS RHYTHM
# ----------------------------

def compile_health_patterns() -> Tuple[List[re.Pattern], List[re.Pattern]]:
    health_rx = [re.compile(p, re.IGNORECASE) for p in ROOT_HEALTH_FILES]
    excl_rx = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_PATTERNS]
    return health_rx, excl_rx


def is_excluded(path: str, excl_rx: Sequence[re.Pattern]) -> bool:
    p = path.replace("\\", "/")
    return any(rx.search(p) for rx in excl_rx)


def is_health_file(path: str, health_rx: Sequence[re.Pattern]) -> bool:
    p = path.replace("\\", "/")
    return any(rx.match(p) for rx in health_rx)


def get_health_files(files: Sequence[str], health_rx: Sequence[re.Pattern], excl_rx: Sequence[re.Pattern]) -> List[str]:
    health_files: List[str] = []
    for f in files:
        if is_excluded(f, excl_rx):
            continue
        if is_health_file(f, health_rx):
            health_files.append(f)
    return health_files


# ----------------------------
# INTENTION (CONSISTENT UNIVERSE)
# ----------------------------

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    ap = argparse.ArgumentParser(description="Health-doc intention summary (rhythm-consistent)")
    ap.add_argument("--repo", required=True, help="Path to local git repo")
    ap.add_argument("--since", required=True, type=parse_date, help="Start date YYYY-MM-DD")
    ap.add_argument("--until", required=True, type=parse_date, help="End date YYYY-MM-DD")
    ap.add_argument("--include_merges", action="store_true", help="Include merge commits (default: exclude)")
    ap.add_argument("--dominant_threshold", type=float, default=0.5,
                    help="Dominant threshold by file-count share (default 0.5)")
    ap.add_argument("--out_prefix", default="intention", help="Output file prefix")
    ap.add_argument("--write_touch_shas", action="store_true", help="Write SHAs of doc-touch commits")
    ap.add_argument("--write_commit_classification", action="store_true",
                    help="Write per-commit classification CSV for auditing")
    args = ap.parse_args()

    repo = os.path.abspath(args.repo)
    repo_name = os.path.basename(repo)
    out_dir = os.path.join("outputs", repo_name)
    os.makedirs(out_dir, exist_ok=True)

    health_rx, excl_rx = compile_health_patterns()

    total_commits_in_range = 0
    health_docs_touch_commits = 0

    health_docs_only_commits = 0
    health_docs_dominant_mixed_commits = 0
    health_docs_mixed_non_dominant_commits = 0

    touch_shas: List[str] = []
    audit_rows: List[dict] = []

    for cr in iter_commits_with_files(repo, args.since, args.until, include_merges=args.include_merges):
        total_commits_in_range += 1

        health_files = get_health_files(cr.files, health_rx, excl_rx)
        if not health_files:
            continue

        # IMPORTANT: match rhythm’s worldview — ignore excluded files entirely
        other_files = []
        for f in cr.files:
            if is_excluded(f, excl_rx):
                continue
            if is_health_file(f, health_rx):
                continue
            other_files.append(f)

        health_docs_touch_commits += 1
        touch_shas.append(cr.sha)

        if len(other_files) == 0:
            category = "health_docs_only"
            health_docs_only_commits += 1
        else:
            share = len(health_files) / (len(health_files) + len(other_files))
            if share >= args.dominant_threshold:
                category = "health_docs_dominant_mixed"
                health_docs_dominant_mixed_commits += 1
            else:
                category = "health_docs_mixed_non_dominant"
                health_docs_mixed_non_dominant_commits += 1

        if args.write_commit_classification:
            audit_rows.append({
                "repo": repo_name,
                "commit_sha": cr.sha,
                "commit_date_utc": cr.commit_dt.date().isoformat(),
                "category": category,
                "health_files_count": len(health_files),
                "other_files_count": len(other_files),
                "health_files": ";".join(health_files),
            })

    partition_sum = (
        health_docs_only_commits
        + health_docs_dominant_mixed_commits
        + health_docs_mixed_non_dominant_commits
    )

    def rate(num: int, den: int) -> str:
        return f"{(num / den):.6f}" if den > 0 else ""

    row = {
        "repo": repo_name,
        "since": args.since.isoformat(),
        "until": args.until.isoformat(),
        "include_merges": "yes" if args.include_merges else "no",
        "dominant_threshold": f"{args.dominant_threshold:.3f}",

        "total_commits_in_range": str(total_commits_in_range),
        "health_docs_touch_commits": str(health_docs_touch_commits),

        "health_docs_only_commits": str(health_docs_only_commits),
        "health_docs_dominant_mixed_commits": str(health_docs_dominant_mixed_commits),
        "health_docs_mixed_non_dominant_commits": str(health_docs_mixed_non_dominant_commits),

        "health_docs_only_rate": rate(health_docs_only_commits, health_docs_touch_commits),
        "health_docs_dominant_mixed_rate": rate(health_docs_dominant_mixed_commits, health_docs_touch_commits),
        "health_docs_mixed_non_dominant_rate": rate(health_docs_mixed_non_dominant_commits, health_docs_touch_commits),

        "partition_check_sum": str(partition_sum),
    }

    csv_path = os.path.join(out_dir, f"{args.out_prefix}_health_docs_intention_summary.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        w.writeheader()
        w.writerow(row)

    sha_path = None
    if args.write_touch_shas:
        sha_path = os.path.join(out_dir, f"{args.out_prefix}_health_docs_touch_shas.txt")
        with open(sha_path, "w") as f:
            for sha in touch_shas:
                f.write(sha + "\n")

    audit_path = None
    if args.write_commit_classification:
        audit_path = os.path.join(out_dir, f"{args.out_prefix}_health_docs_commit_classification.csv")
        with open(audit_path, "w", newline="") as f:
            fn = ["repo", "commit_sha", "commit_date_utc", "category",
                  "health_files_count", "other_files_count", "health_files"]
            w = csv.DictWriter(f, fieldnames=fn)
            w.writeheader()
            w.writerows(audit_rows)

    summary = {
        "repo": repo_name,
        "since": args.since.isoformat(),
        "until": args.until.isoformat(),
        "include_merges": bool(args.include_merges),
        "dominant_threshold": args.dominant_threshold,
        "output_csv": csv_path,
        "touch_shas_txt": sha_path,
        "audit_csv": audit_path,
        "notes": [
            "Commit enumeration uses COMMITTER time (%ct) and --name-only, matching doc_touch_rhythm.py.",
            "health_docs detection uses the same include/exclude logic (exclude first, then match).",
            "For intention classification, excluded paths are ignored (same worldview as rhythm).",
            "partition_check_sum should equal health_docs_touch_commits.",
        ],
    }

    summary_path = os.path.join(out_dir, f"{args.out_prefix}_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print("Saved:")
    print(f"  ✓ {csv_path}")
    if sha_path:
        print(f"  ✓ {sha_path}")
    if audit_path:
        print(f"  ✓ {audit_path}")
    print(f"  ✓ {summary_path}")

    print(f"\nDoc-touch commits (should match rhythm count): {health_docs_touch_commits}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
