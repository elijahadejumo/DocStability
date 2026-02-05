#!/usr/bin/env python3
"""
health_docs_ownership_summary.py

Repo-level ownership/concentration metrics for STRICT health_docs over a date range.

We classify commits into four categories (mutually exclusive within doc_touch):
  - doc_touch:        commit touches >=1 health_docs file
  - doc_only:         doc_touch AND touches no other files
  - doc_dominant:     doc_touch AND touches other files AND doc_share >= dominant_threshold
  - doc_non_dominant: doc_touch AND touches other files AND doc_share <  dominant_threshold

Then for each category, compute:
  - commit count
  - unique contributors
  - Top-K shares: Top1, Top3, Top5, Top10 (by commit counts)
  - Bus50 / Bus80: smallest number of contributors accounting for >=50% / >=80% of commits

Bots are excluded by default (recommended for ownership metrics).

Outputs:
  outputs/<repo_name>/<out_prefix>_health_docs_ownership_summary.csv
  outputs/<repo_name>/<out_prefix>_summary.json

Usage:
  python3 health_docs_ownership_summary.py \
    --repo ./airflow \
    --since 2020-06-30 \
    --until 2025-06-29 \
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
from typing import Dict, Iterator, List, Optional, Tuple


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

    # Specific component directories that contain LICENSE/README
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

    # Source code directories (should NEVER match)
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

    # Source code files (CRITICAL - should NEVER be docs)
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
    """
    Normalize and handle simple rename syntax in numstat output:
      "path/{old => new}/file.md" or "a => b"
    Keep the right side of '=>' when present.
    """
    p = (p or "").strip().replace("\\", "/")
    if "=>" in p:
        p = p.split("=>")[-1].strip()
        p = p.strip("{} ").strip()
    return p.lstrip("./")


def is_excluded(path: str) -> bool:
    return any(rx.search(path) for rx in EXCLUDE_RX)


def is_health_docs(path: str) -> bool:
    p = normalize_path(path)
    if is_excluded(p):
        return False
    return any(rx.match(p) for rx in HEALTH_INCLUDE_RX)


# ---------------------------------------------------------------------------
# Identity normalization (consistent with your prior scripts)
# ---------------------------------------------------------------------------

def normalize_email(email: str) -> str:
    e = (email or "").strip().lower()
    m = re.match(r"^\d+\+(.+@users\.noreply\.github\.com)$", e)
    return m.group(1) if m else e


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip()).lower()


def author_id(name: str, email: str) -> str:
    e = normalize_email(email)
    n = normalize_name(name)
    return e if e else f"name:{n}"


# ---------------------------------------------------------------------------
# Bot detection (default exclude)
# ---------------------------------------------------------------------------

BOT_PATTERNS = [
     r'\bbot\b', r'\bbots\b', r'github-actions', r'dependabot',
    r'renovate', r'greenkeeper', r'codecov', r'coveralls',
    r'travis-ci', r'circleci', r'jenkins', r'azure-pipelines',
    r'\bbors\b', r'homu', r'mergify', r'kodiak', r'auto-merge',
    r'rust-timer', r'rustbot', r'rust-highfive',
    r'kubernetes-', r'k8s-ci-robot', r'automation',
    r'\[bot\]', r'\(bot\)', r'service account',
    r'noreply@', r'github\.com', r'automated',
    r'version-bump', r'release-bot', r'changelog-bot',
    r'homebrew-', r'allcontributors',
]
BOT_RX = [re.compile(p, re.IGNORECASE) for p in BOT_PATTERNS]


def looks_like_bot(name: str, email: str) -> bool:
    s = f"{(name or '').strip()} <{(email or '').strip()}>".strip()
    return any(rx.search(s) for rx in BOT_RX)


# ---------------------------------------------------------------------------
# Git parsing: git log --numstat
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
# Ownership metrics: Top-K share and Bus-X
# ---------------------------------------------------------------------------

def top_k_share(counts: List[int], k: int) -> float:
    if not counts:
        return 0.0
    total = sum(counts)
    if total <= 0:
        return 0.0
    counts_sorted = sorted(counts, reverse=True)
    return sum(counts_sorted[:k]) / total


def bus_x(counts: List[int], x: float) -> int:
    """
    Smallest number of contributors needed to cover >= x fraction of commits.
    x in (0,1], e.g. 0.5 or 0.8
    """
    if not counts:
        return 0
    total = sum(counts)
    if total <= 0:
        return 0
    target = x * total
    counts_sorted = sorted(counts, reverse=True)

    cum = 0
    k = 0
    for c in counts_sorted:
        cum += c
        k += 1
        if cum >= target - 1e-12:
            return k
    return k


def format_float(x: float) -> str:
    return f"{x:.6f}"


# ---------------------------------------------------------------------------
# CLI + main
# ---------------------------------------------------------------------------

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    ap = argparse.ArgumentParser(description="Health_docs ownership summary (Top-K shares and Bus-X) for intention categories")
    ap.add_argument("--repo", required=True, help="Path to local git repo")
    ap.add_argument("--since", required=True, type=parse_date, help="Start date YYYY-MM-DD")
    ap.add_argument("--until", required=True, type=parse_date, help="End date YYYY-MM-DD")
    ap.add_argument("--dominant_threshold", type=float, default=0.5,
                    help="Doc-dominant threshold by file-share (default 0.5)")
    ap.add_argument("--include_bots", action="store_true", help="Include bots (default: exclude)")
    ap.add_argument("--out_prefix", default="ownership", help="Output file prefix")
    args = ap.parse_args()

    repo = os.path.abspath(args.repo)
    repo_name = os.path.basename(repo)
    out_dir = os.path.join("outputs", repo_name)
    os.makedirs(out_dir, exist_ok=True)

    commits = list(iter_commits_with_numstat(repo, args.since, args.until))

    # contributor commit counts per category
    counts_touch: Dict[str, int] = {}
    counts_only: Dict[str, int] = {}
    counts_dom: Dict[str, int] = {}
    counts_nondom: Dict[str, int] = {}

    total_commits_in_range = 0

    for c in commits:
        if (not args.include_bots) and looks_like_bot(c.name, c.email):
            continue

        total_commits_in_range += 1

        # classify changed files in this commit
        health_files = 0
        other_files = 0
        for _add, _del, p in c.files:
            p2 = normalize_path(p)
            if is_health_docs(p2):
                health_files += 1
            else:
                other_files += 1

        if health_files == 0:
            continue  # not doc_touch

        aid = author_id(c.name, c.email)

        # doc_touch
        counts_touch[aid] = counts_touch.get(aid, 0) + 1

        # intention sub-categories
        if other_files == 0:
            counts_only[aid] = counts_only.get(aid, 0) + 1
        else:
            total_files = health_files + other_files
            share = (health_files / total_files) if total_files > 0 else 0.0
            if share >= args.dominant_threshold:
                counts_dom[aid] = counts_dom.get(aid, 0) + 1
            else:
                counts_nondom[aid] = counts_nondom.get(aid, 0) + 1

    def summarize(prefix: str, d: Dict[str, int]) -> Dict[str, str]:
        counts = list(d.values())
        n_commits = sum(counts)
        n_contrib = len(d)

        top1 = top_k_share(counts, 1)
        top3 = top_k_share(counts, 3)
        top5 = top_k_share(counts, 5)
        top10 = top_k_share(counts, 10)

        b50 = bus_x(counts, 0.5)
        b80 = bus_x(counts, 0.8)

        return {
            f"{prefix}_commits": str(n_commits),
            f"{prefix}_contributors": str(n_contrib),
            f"{prefix}_top1_share": format_float(top1) if n_commits > 0 else "",
            f"{prefix}_top3_share": format_float(top3) if n_commits > 0 else "",
            f"{prefix}_top5_share": format_float(top5) if n_commits > 0 else "",
            f"{prefix}_top10_share": format_float(top10) if n_commits > 0 else "",
            f"{prefix}_bus50": str(b50) if n_commits > 0 else "",
            f"{prefix}_bus80": str(b80) if n_commits > 0 else "",
        }

    row: Dict[str, str] = {
        "repo": repo_name,
        "since": args.since.isoformat(),
        "until": args.until.isoformat(),
        "dominant_threshold": f"{args.dominant_threshold:.3f}",
        "bots_included": "yes" if args.include_bots else "no",
        "total_commits_in_range": str(total_commits_in_range),
    }

    # Add summaries for each category
    row.update(summarize("health_docs_touch", counts_touch))
    row.update(summarize("health_docs_only", counts_only))
    row.update(summarize("health_docs_dominant", counts_dom))
    row.update(summarize("health_docs_non_dominant", counts_nondom))

    # sanity: partition check (touch = only + dominant + non_dominant)
    touch_commits = sum(counts_touch.values())
    only_commits = sum(counts_only.values())
    dom_commits = sum(counts_dom.values())
    nondom_commits = sum(counts_nondom.values())
    row["health_docs_partition_check"] = str(touch_commits == (only_commits + dom_commits + nondom_commits))
    row["health_docs_partition_touch"] = str(touch_commits)
    row["health_docs_partition_sum_parts"] = str(only_commits + dom_commits + nondom_commits)

    csv_path = os.path.join(out_dir, f"{args.out_prefix}_health_docs_ownership_summary.csv")
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
            "Ownership metrics computed over commit counts (not lines changed).",
            "Top-K shares and Bus50/Bus80 are computed per category: touch/only/dominant/non_dominant.",
            "Commit categories are based on STRICT health_docs include/exclude rules you provided.",
            "Partition check verifies: touch = only + dominant + non_dominant (commit-level).",
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
