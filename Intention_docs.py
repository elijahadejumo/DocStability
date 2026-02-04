#!/usr/bin/env python3
"""
health_docs_intention_summary.py

Repo-level "intention" summary for STRICT health documentation files
over a date range (e.g., 2020-01-01 to 2025-12-31).

Counts three mutually-exclusive categories among commits that touch health_docs:

1) health_docs_only_commits
   - touches >=1 health_docs file
   - touches ZERO other files

2) health_docs_dominant_mixed_commits
   - touches >=1 health_docs file AND >=1 other file
   - health_docs files are >= dominant_threshold of changed files (by count)

3) health_docs_mixed_non_dominant_commits
   - touches >=1 health_docs file AND >=1 other file
   - health_docs file share < dominant_threshold

Outputs:
  outputs/<repo_name>/<out_prefix>_health_docs_intention_summary.csv
  outputs/<repo_name>/<out_prefix>_summary.json

Usage:
  python3 health_docs_intention_summary.py \
    --repo ./airflow \
    --since 2020-01-01 \
    --until 2025-12-31 \
    --out_prefix airflow_2020_2025 \
    --dominant_threshold 0.5
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
from typing import Iterator, List, Optional, Tuple


# ---------------------------------------------------------------------------
# YOUR STRICT HEALTH_DOCS INCLUDE/EXCLUDE RULES (as provided)
# ---------------------------------------------------------------------------

ROOT_HEALTH_FILES = [
    # Essential project documentation (root only)
    r"^README(?:\..+)?$",
    r"^CONTRIBUTING(?:\..+)?$",
    r"^CONTRIBUTORS(?:\..+)?$",
    r"^COMMIT_CONVENTIONS(?:\..+)?$",
    r"^PULL_REQUEST_TEMPLATE(?:\..+)?$",
    r"^ISSUE_TEMPLATE(?:\..+)?$",
    r"^BUILDING(?:\..+)?$",

    # Version & change documentation (root only)
    r"^CHANGELOG(?:\..+)?$",
    r"^HISTORY(?:\..+)?$",
    r"^RELEASES?(?:\..+)?$",

    # Community & governance (root only)
    r"^CODE_OF_CONDUCT(?:\..+)?$",
    r"^GOVERNANCE(?:\..+)?$",
    r"^SUPPORT(?:\..+)?$",
    r"^MAINTAINERS(?:\..+)?$",

    # Security & legal (root only)
    r"^SECURITY(?:\..+)?$",
    r"^LICENSE(?:\..+)?$",
    r"^NOTICE(?:\..+)?$",
    r"^COPYING(?:\..+)?$",

    # Credit & attribution (root only)
    r"^AUTHORS(?:\..+)?$",
    r"^CREDITS(?:\..+)?$",
    r"^THANKS(?:\..+)?$",

    # Project roadmap & vision (root only)
    r"^ROADMAP(?:\..+)?$",
    r"^VISION(?:\..+)?$",

    # GitHub-specific health files (.github/ directory)
    r"^\.github/SECURITY(?:\..+)?$",
    r"^\.github/CONTRIBUTING(?:\..+)?$",
    r"^\.github/CODE_OF_CONDUCT(?:\..+)?$",
    r"^\.github/SUPPORT(?:\..+)?$",
    r"^\.github/COMMIT_CONVENTIONS(?:\..+)?$",
    r"^\.github/PULL_REQUEST_TEMPLATE(?:\..+)?$",
    r"^\.github/ISSUE_TEMPLATE(?:\..+)?$",
    r"^\.github/BUILDING(?:\..+)?$",

    # GitLab-specific health files (.gitlab/ directory)
    r"^\.gitlab/CONTRIBUTING(?:\..+)?$",
    r"^\.gitlab/CODE_OF_CONDUCT(?:\..+)?$",
    r"^\.gitlab/COMMIT_CONVENTIONS(?:\..+)?$",
    r"^\.gitlab/PULL_REQUEST_TEMPLATE(?:\..+)?$",
    r"^\.gitlab/ISSUE_TEMPLATE(?:\..+)?$",
    r"^\.gitlab/BUILDING(?:\..+)?$",
]

EXCLUDE_PATTERNS = [
    # ANY nested directory structure (libs/, modules/, x-pack/, etc.)
    r"^[^/]+/[^/]+/",

    # Specific component directories
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

    # Source code directories
    r"^src/",
    r"/src/",

    # Build artifacts & dependencies
    r"(^|/)node_modules/",
    r"(^|/)vendor/",
    r"(^|/)dist/",
    r"(^|/)build/",
    r"(^|/)site/",
    r"(^|/)out/",
    r"(^|/)target/",
    r"(^|/)\.git/",

    # Technical documentation directories
    r"(^|/)docs?/",
    r"(^|/)documentation/",
    r"(^|/)wiki/",
    r"(^|/)guides?/",
    r"(^|/)tutorials?/",
    r"(^|/)examples?/",
    r"(^|/)man/",
    r"(^|/)api/",
    r"(^|/)reference/",

    # Binary/media files
    r"\.(png|jpg|jpeg|gif|svg|ico|pdf)$",
    r"\.(zip|tar|gz|bz2|7z|rar)$",
    r"\.(exe|dll|so|dylib|bin)$",

    # Source code files
    r"\.(java|py|js|ts|go|rs|cpp|c|h|hpp)$",
    r"\.(scala|kt|swift|rb|php|cs|fs)$",

    # Test/resource files
    r"(^|/)test/",
    r"(^|/)tests/",
    r"/resources/",
    r"\.(cef|json|xml|yaml|yml)\.txt$",

    # Build/config files
    r"(^|/)conf\.py$",
    r"(^|/)_config\.yml$",
    r"(^|/)mkdocs\.yml$",
    r"(^|/)Doxyfile$",
    r"output\.txt$",

    # Translation files
    r"/translations?/",
    r"/i18n/",
    r"/locales?/",
    r"\.(zh|ja|ko|fr|de|es|it|pt|ru)\.md$",
]

HEALTH_INCLUDE_RX = [re.compile(p, re.IGNORECASE) for p in ROOT_HEALTH_FILES]
EXCLUDE_RX = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_PATTERNS]


def normalize_path(p: str) -> str:
    """Normalize and handle basic rename syntax in --numstat output."""
    p = (p or "").strip().replace("\\", "/")
    if "=>" in p:
        p = p.split("=>")[-1].strip()
        p = p.strip("{} ").strip()
    return p.lstrip("./")


def is_excluded(path: str) -> bool:
    return any(rx.search(path) for rx in EXCLUDE_RX)


def is_health_docs(path: str) -> bool:
    """
    Strict: must match inclusion AND must NOT match any exclusion.
    """
    p = normalize_path(path)
    if is_excluded(p):
        return False
    return any(rx.match(p) for rx in HEALTH_INCLUDE_RX)


# ---------------------------------------------------------------------------
# Bot detection (optional, default exclude)
# ---------------------------------------------------------------------------

BOT_PATTERNS = [
    r"\bbot\b", r"\bbots\b", r"\[bot\]", r"\(bot\)",
    r"github-actions", r"dependabot", r"renovate", r"greenkeeper",
    r"codecov", r"coveralls", r"travis-ci", r"circleci", r"jenkins",
    r"azure-pipelines", r"bors", r"homu", r"mergify", r"kodiak",
    r"auto-merge", r"automation", r"automated", r"service account",
    r"noreply@", r"github\.com",
]
BOT_RX = [re.compile(p, re.IGNORECASE) for p in BOT_PATTERNS]


def looks_like_bot(name: str, email: str) -> bool:
    s = f"{(name or '').strip()} <{(email or '').strip()}>".strip()
    return any(rx.search(s) for rx in BOT_RX)


# ---------------------------------------------------------------------------
# Git parsing: one pass over git log --numstat
# ---------------------------------------------------------------------------

def run_git(repo: str, args: List[str]) -> str:
    cmd = ["git", "-C", repo] + args
    return subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8", errors="replace")


def safe_int(s: str) -> Optional[int]:
    try:
        return int(s)
    except Exception:
        return None


@dataclass
class CommitRec:
    dt: datetime
    sha: str
    name: str
    email: str
    subject: str
    files: List[Tuple[Optional[int], Optional[int], str]]  # (add, del, path)


def iter_commits_with_numstat(repo: str, since: date, until: date) -> Iterator[CommitRec]:
    marker = "__C__"
    fmt = f"{marker}%H\t%at\t%an\t%ae\t%s"
    raw = run_git(repo, [
        "log", "--no-color",
        f"--since={since.isoformat()} 00:00:00",
        f"--until={until.isoformat()} 23:59:59",
        f"--pretty=format:{fmt}",
        "--numstat",
    ])

    cur: Optional[CommitRec] = None

    for line in raw.splitlines():
        if line.startswith(marker):
            if cur is not None:
                yield cur

            parts = line[len(marker):].split("\t", 4)
            if len(parts) != 5:
                cur = None
                continue

            sha, epoch_s, name, email, subject = parts
            try:
                epoch = int(epoch_s)
            except ValueError:
                cur = None
                continue

            dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
            cur = CommitRec(dt=dt, sha=sha, name=name, email=email, subject=subject, files=[])
        else:
            if cur is None or not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            add_s, del_s, path = parts[0], parts[1], "\t".join(parts[2:]).strip()
            add = None if add_s == "-" else safe_int(add_s)
            dele = None if del_s == "-" else safe_int(del_s)
            cur.files.append((add, dele, path))

    if cur is not None:
        yield cur


# ---------------------------------------------------------------------------
# Core: intention summary
# ---------------------------------------------------------------------------

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    ap = argparse.ArgumentParser(description="Strict health_docs intention summary (no windows)")
    ap.add_argument("--repo", required=True, help="Path to local git repo")
    ap.add_argument("--since", required=True, type=parse_date, help="Start date YYYY-MM-DD")
    ap.add_argument("--until", required=True, type=parse_date, help="End date YYYY-MM-DD")
    ap.add_argument("--dominant_threshold", type=float, default=0.5,
                    help="health_docs dominant threshold by file-share (default 0.5)")
    ap.add_argument("--include_bots", action="store_true", help="Include bots (default: exclude)")
    ap.add_argument("--out_prefix", default="intention", help="Output file prefix")
    args = ap.parse_args()

    repo = os.path.abspath(args.repo)
    repo_name = os.path.basename(repo)
    out_dir = os.path.join("outputs", repo_name)
    os.makedirs(out_dir, exist_ok=True)

    commits = list(iter_commits_with_numstat(repo, args.since, args.until))

    total_commits = 0
    health_docs_touch_commits = 0

    health_docs_only_commits = 0
    health_docs_dominant_mixed_commits = 0
    health_docs_mixed_non_dominant_commits = 0

    for c in commits:
        if (not args.include_bots) and looks_like_bot(c.name, c.email):
            continue

        total_commits += 1

        health_files = 0
        other_files = 0

        # classify changed files in this commit
        for _add, _del, p in c.files:
            p2 = normalize_path(p)
            if is_health_docs(p2):
                health_files += 1
            else:
                other_files += 1

        if health_files == 0:
            continue  # not a health_docs commit

        health_docs_touch_commits += 1

        if other_files == 0:
            health_docs_only_commits += 1
        else:
            total_files = health_files + other_files
            share = (health_files / total_files) if total_files > 0 else 0.0
            if share >= args.dominant_threshold:
                health_docs_dominant_mixed_commits += 1
            else:
                health_docs_mixed_non_dominant_commits += 1

    def rate(num: int, den: int) -> str:
        return f"{(num / den):.6f}" if den > 0 else ""

    # sanity: these should partition health_docs_touch_commits
    partition_sum = (
        health_docs_only_commits
        + health_docs_dominant_mixed_commits
        + health_docs_mixed_non_dominant_commits
    )

    row = {
        "repo": repo_name,
        "since": args.since.isoformat(),
        "until": args.until.isoformat(),
        "dominant_threshold": f"{args.dominant_threshold:.3f}",
        "bots_included": "yes" if args.include_bots else "no",

        "total_commits_in_range": str(total_commits),
        "health_docs_touch_commits": str(health_docs_touch_commits),

        "health_docs_only_commits": str(health_docs_only_commits),
        "health_docs_dominant_mixed_commits": str(health_docs_dominant_mixed_commits),
        "health_docs_mixed_non_dominant_commits": str(health_docs_mixed_non_dominant_commits),

        "health_docs_only_rate": rate(health_docs_only_commits, health_docs_touch_commits),
        "health_docs_dominant_mixed_rate": rate(health_docs_dominant_mixed_commits, health_docs_touch_commits),
        "health_docs_mixed_non_dominant_rate": rate(health_docs_mixed_non_dominant_commits, health_docs_touch_commits),

        "health_docs_partition_check_sum": str(partition_sum),
    }

    csv_path = os.path.join(out_dir, f"{args.out_prefix}_health_docs_intention_summary.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        w.writeheader()
        w.writerow(row)

    summary = {
        "repo": repo_name,
        "since": args.since.isoformat(),
        "until": args.until.isoformat(),
        "dominant_threshold": args.dominant_threshold,
        "bots_included": bool(args.include_bots),
        "output_csv": csv_path,
        "notes": [
            "Counts are over commits in [since, until] by author date.",
            "health_docs are matched strictly by your include patterns AND not excluded.",
            "Dominant mixed uses file-count share (health_files / total_files).",
            "Partition check should equal health_docs_touch_commits.",
        ],
    }
    summary_path = os.path.join(out_dir, f"{args.out_prefix}_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print("Saved:")
    print(f"  ✓ {csv_path}")
    print(f"  ✓ {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
