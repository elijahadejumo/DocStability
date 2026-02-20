#!/usr/bin/env python3
"""
health_docs_ownership_summary.py

Repo-level ownership/concentration metrics for STRICT health_docs over a date range.

CONSISTENCY GOAL (with your rhythm script):
- Uses git log with:
  - committer time (%ct) like rhythm
  - --name-only like rhythm
  - --no-merges by default (unless --include_merges is passed) like rhythm
- Uses the SAME include/exclude logic as rhythm:
  - excluded paths are ignored entirely (not counted as "other")
  - health docs are matched only after exclusions
- Path handling is IDENTICAL to rhythm (no lstrip, no extra normalization)
- Bot filtering applies ONLY to ownership attribution metrics, NOT to commit counts
  (so health_docs_touch_commits matches rhythm's health_file_commits exactly)

We classify commits into four categories (mutually exclusive within doc_touch):
  - doc_touch:        commit touches >=1 health_docs file
  - doc_only:         doc_touch AND touches no other (non-excluded) files
  - doc_dominant:     doc_touch AND touches other (non-excluded) files AND doc_share >= dominant_threshold
  - doc_non_dominant: doc_touch AND touches other (non-excluded) files AND doc_share <  dominant_threshold

Then for each category, compute:
  - commit count  (ALL commits, including bots, to match rhythm)
  - unique contributors (bots excluded unless --include_bots)
  - Top-K shares: Top1, Top3, Top5, Top10 (by commit counts, bots excluded unless --include_bots)
  - Bus50 / Bus80: smallest number of contributors accounting for >=50% / >=80% of commits

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
from typing import Dict, Iterator, List, Optional, Sequence, Tuple


# ---------------------------------------------------------------------------
# STRICT HEALTH_DOCS INCLUDE/EXCLUDE RULES (MATCH RHYTHM SCRIPT EXACTLY)
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


# ---------------------------------------------------------------------------
# Bot detection
# ---------------------------------------------------------------------------

BOT_PATTERNS = [
    r"\bbot\b", r"\bbots\b", r"github-actions", r"dependabot",
    r"renovate", r"greenkeeper", r"codecov", r"coveralls",
    r"travis-ci", r"circleci", r"jenkins", r"azure-pipelines",
    r"\bbors\b", r"homu", r"mergify", r"kodiak", r"auto-merge",
    r"rust-timer", r"rustbot", r"rust-highfive",
    r"kubernetes-", r"k8s-ci-robot", r"automation",
    r"\[bot\]", r"\(bot\)", r"service account",
    r"noreply@", r"github\.com", r"automated",
    r"version-bump", r"release-bot", r"changelog-bot",
    r"homebrew-", r"allcontributors",
]
BOT_RX = [re.compile(p, re.IGNORECASE) for p in BOT_PATTERNS]


def looks_like_bot(name: str, email: str) -> bool:
    s = f"{(name or '').strip()} <{(email or '').strip()}>".strip()
    return any(rx.search(s) for rx in BOT_RX)


# ---------------------------------------------------------------------------
# Identity normalization
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
# Git parsing — RHYTHM-IDENTICAL path handling
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CommitRecord:
    sha: str
    commit_dt: datetime
    author_name: str
    author_email: str
    files: Tuple[str, ...]


def run_git(repo: str, args: List[str]) -> str:
    cmd = ["git", "-C", repo] + args
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    return out.decode("utf-8", errors="replace")


def iter_commits_with_files(
    repo: str, since: date, until: date, include_merges: bool
) -> Iterator[CommitRecord]:
    """
    Parses git log in a way that is byte-for-byte consistent with the rhythm
    script, with author name/email added for ownership attribution.

    KEY CONSISTENCY POINTS vs. original ownership script:
      - Files are appended as `s` (stripped of whitespace only), NOT lstripped
        of "./" — this matches rhythm exactly and prevents path corruption.
      - %ct (committer time) is used, matching rhythm.
      - --no-merges is the default, matching rhythm.
    """
    log_args = [
        "log",
        "--no-color",
        f"--since={since.isoformat()} 00:00:00",
        f"--until={until.isoformat()} 23:59:59",
        "--pretty=format:__COMMIT__%H\t%ct\t%an\t%ae",
        "--name-only",
    ]
    if not include_merges:
        log_args.insert(1, "--no-merges")

    raw = run_git(repo, log_args)

    sha: Optional[str] = None
    epoch: Optional[int] = None
    an: str = ""
    ae: str = ""
    files: List[str] = []

    for line in raw.splitlines():
        if line.startswith("__COMMIT__"):
            # flush previous commit
            if sha is not None and epoch is not None:
                dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
                yield CommitRecord(
                    sha=sha,
                    commit_dt=dt,
                    author_name=an,
                    author_email=ae,
                    files=tuple(f for f in files if f),
                )

            files = []
            header = line[len("__COMMIT__"):]
            parts = header.split("\t")
            if len(parts) != 4:
                sha, epoch = None, None
                an, ae = "", ""
                continue

            sha = parts[0].strip()
            try:
                epoch = int(parts[1].strip())
            except ValueError:
                sha, epoch = None, None
                an, ae = "", ""
                continue
            an = parts[2].strip()
            ae = parts[3].strip()
        else:
            if sha is not None:
                # *** RHYTHM-IDENTICAL: strip whitespace only, no lstrip("./") ***
                s = line.strip()
                if s:
                    files.append(s)

    # flush last commit
    if sha is not None and epoch is not None:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        yield CommitRecord(
            sha=sha,
            commit_dt=dt,
            author_name=an,
            author_email=ae,
            files=tuple(f for f in files if f),
        )


# ---------------------------------------------------------------------------
# Health file detection — RHYTHM-IDENTICAL
# ---------------------------------------------------------------------------

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


def get_health_files(
    files: Sequence[str],
    health_rx: Sequence[re.Pattern],
    excl_rx: Sequence[re.Pattern],
) -> List[str]:
    health_files: List[str] = []
    for f in files:
        if is_excluded(f, excl_rx):
            continue
        if is_health_file(f, health_rx):
            health_files.append(f)
    return health_files


# ---------------------------------------------------------------------------
# Ownership metrics
# ---------------------------------------------------------------------------

def top_k_share(counts: List[int], k: int) -> float:
    if not counts:
        return 0.0
    total = sum(counts)
    if total <= 0:
        return 0.0
    return sum(sorted(counts, reverse=True)[:k]) / total


def bus_x(counts: List[int], x: float) -> int:
    """Smallest number of contributors needed to cover >= x fraction of commits."""
    if not counts:
        return 0
    total = sum(counts)
    if total <= 0:
        return 0
    target = x * total
    cum = 0
    for k, c in enumerate(sorted(counts, reverse=True), start=1):
        cum += c
        if cum >= target - 1e-12:
            return k
    return len(counts)


def format_float(x: float) -> str:
    return f"{x:.6f}"


# ---------------------------------------------------------------------------
# CLI + main
# ---------------------------------------------------------------------------

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Health_docs ownership summary (Top-K shares and Bus-X) — "
            "commit counts match rhythm script exactly; "
            "bot filtering applies to ownership attribution only."
        )
    )
    ap.add_argument("--repo", required=True, help="Path to local git repo")
    ap.add_argument("--since", required=True, type=parse_date, help="Start date YYYY-MM-DD")
    ap.add_argument("--until", required=True, type=parse_date, help="End date YYYY-MM-DD")
    ap.add_argument(
        "--dominant_threshold", type=float, default=0.5,
        help="Doc-dominant threshold by file-share (default 0.5)",
    )
    ap.add_argument(
        "--include_bots", action="store_true",
        help="Include bots in ownership attribution (default: exclude bots from attribution)",
    )
    ap.add_argument(
        "--include_merges", action="store_true",
        help="Include merge commits (default: exclude, matching rhythm default)",
    )
    ap.add_argument("--out_prefix", default="ownership", help="Output file prefix")
    args = ap.parse_args()

    repo = os.path.abspath(args.repo)
    repo_name = os.path.basename(repo)
    out_dir = os.path.join("outputs", repo_name)
    os.makedirs(out_dir, exist_ok=True)

    health_rx, excl_rx = compile_health_patterns()

    # -----------------------------------------------------------------------
    # Two parallel sets of counters:
    #   _all  → includes every commit (bots included) → matches rhythm counts
    #   _attr → bots excluded unless --include_bots   → used for ownership metrics
    # -----------------------------------------------------------------------
    counts_touch_all: Dict[str, int] = {}   # aid → commit count (all authors)
    counts_only_all: Dict[str, int] = {}
    counts_dom_all: Dict[str, int] = {}
    counts_nondom_all: Dict[str, int] = {}

    counts_touch_attr: Dict[str, int] = {}  # aid → commit count (humans only)
    counts_only_attr: Dict[str, int] = {}
    counts_dom_attr: Dict[str, int] = {}
    counts_nondom_attr: Dict[str, int] = {}

    total_commits_in_range = 0

    for c in iter_commits_with_files(repo, args.since, args.until, args.include_merges):
        total_commits_in_range += 1

        is_bot = looks_like_bot(c.author_name, c.author_email)

        # Classify files — IDENTICAL logic to rhythm
        health_files_list = get_health_files(c.files, health_rx, excl_rx)
        n_health = len(health_files_list)

        if n_health == 0:
            continue  # not a health-doc-touching commit

        # Count other (non-excluded, non-health) files
        n_other = sum(
            1 for f in c.files
            if not is_excluded(f, excl_rx) and not is_health_file(f, health_rx)
        )

        aid = author_id(c.author_name, c.author_email)

        # Determine sub-category
        if n_other == 0:
            category = "only"
        else:
            total_files = n_health + n_other
            share = n_health / total_files
            category = "dom" if share >= args.dominant_threshold else "nondom"

        # --- Always count for rhythm-matching totals (_all) ---
        counts_touch_all[aid] = counts_touch_all.get(aid, 0) + 1
        if category == "only":
            counts_only_all[aid] = counts_only_all.get(aid, 0) + 1
        elif category == "dom":
            counts_dom_all[aid] = counts_dom_all.get(aid, 0) + 1
        else:
            counts_nondom_all[aid] = counts_nondom_all.get(aid, 0) + 1

        # --- Count for ownership attribution (_attr), skipping bots unless flagged ---
        if args.include_bots or not is_bot:
            counts_touch_attr[aid] = counts_touch_attr.get(aid, 0) + 1
            if category == "only":
                counts_only_attr[aid] = counts_only_attr.get(aid, 0) + 1
            elif category == "dom":
                counts_dom_attr[aid] = counts_dom_attr.get(aid, 0) + 1
            else:
                counts_nondom_attr[aid] = counts_nondom_attr.get(aid, 0) + 1

    def summarize_counts(prefix: str, all_d: Dict[str, int], attr_d: Dict[str, int]) -> Dict[str, str]:
        """
        Commit counts come from all_d (matches rhythm).
        Contributor counts and concentration metrics come from attr_d (bots excluded).
        """
        all_counts = list(all_d.values())
        attr_counts = list(attr_d.values())

        n_commits = sum(all_counts)       # ← matches rhythm's health_file_commits
        n_contrib = len(attr_d)           # ← human contributors only

        top1  = top_k_share(attr_counts, 1)
        top3  = top_k_share(attr_counts, 3)
        top5  = top_k_share(attr_counts, 5)
        top10 = top_k_share(attr_counts, 10)

        b50 = bus_x(attr_counts, 0.5)
        b80 = bus_x(attr_counts, 0.8)

        return {
            f"{prefix}_commits":      str(n_commits),
            f"{prefix}_contributors": str(n_contrib),
            f"{prefix}_top1_share":   format_float(top1)  if n_commits > 0 else "",
            f"{prefix}_top3_share":   format_float(top3)  if n_commits > 0 else "",
            f"{prefix}_top5_share":   format_float(top5)  if n_commits > 0 else "",
            f"{prefix}_top10_share":  format_float(top10) if n_commits > 0 else "",
            f"{prefix}_bus50":        str(b50) if n_commits > 0 else "",
            f"{prefix}_bus80":        str(b80) if n_commits > 0 else "",
        }

    row: Dict[str, str] = {
        "repo":                    repo_name,
        "since":                   args.since.isoformat(),
        "until":                   args.until.isoformat(),
        "dominant_threshold":      f"{args.dominant_threshold:.3f}",
        "bots_included_in_attr":   "yes" if args.include_bots else "no",
        "merges_included":         "yes" if args.include_merges else "no",
        "total_commits_in_range":  str(total_commits_in_range),
    }

    row.update(summarize_counts("health_docs_touch",       counts_touch_all,  counts_touch_attr))
    row.update(summarize_counts("health_docs_only",        counts_only_all,   counts_only_attr))
    row.update(summarize_counts("health_docs_dominant",    counts_dom_all,    counts_dom_attr))
    row.update(summarize_counts("health_docs_non_dominant",counts_nondom_all, counts_nondom_attr))

    # Partition check (using all-inclusive counts, i.e. rhythm-aligned)
    touch_all  = sum(counts_touch_all.values())
    only_all   = sum(counts_only_all.values())
    dom_all    = sum(counts_dom_all.values())
    nondom_all = sum(counts_nondom_all.values())
    row["health_docs_partition_check"]    = str(touch_all == (only_all + dom_all + nondom_all))
    row["health_docs_partition_touch"]    = str(touch_all)
    row["health_docs_partition_sum_parts"]= str(only_all + dom_all + nondom_all)

    csv_path = os.path.join(out_dir, f"{args.out_prefix}_health_docs_ownership_summary.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        w.writeheader()
        w.writerow(row)

    summary = {
        "repo":               repo_name,
        "since":              args.since.isoformat(),
        "until":              args.until.isoformat(),
        "dominant_threshold": args.dominant_threshold,
        "bots_included_in_attr": bool(args.include_bots),
        "merges_included":    bool(args.include_merges),
        "output_csv":         csv_path,
        "notes": [
            "Commit counts (_commits columns) include ALL authors (bots + humans) to match rhythm script.",
            "Contributor counts and concentration metrics (top-K, bus-X) exclude bots unless --include_bots.",
            "Path handling is rhythm-identical: files are appended with strip() only, no lstrip('./').",
            "Parsing uses %ct (committer time) + --name-only, matching rhythm.",
            "Partition check uses all-inclusive counts: touch = only + dominant + non_dominant.",
        ],
    }
    summary_path = os.path.join(out_dir, f"{args.out_prefix}_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print("Saved:")
    print(f"  ✓ {csv_path}")
    print(f"  ✓ {summary_path}")

    # Print a quick alignment summary for verification
    print(f"\nRhythm alignment check:")
    print(f"  health_docs_touch_commits (all authors) = {touch_all}")
    print(f"  (This should equal health_file_commits from the rhythm script)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())