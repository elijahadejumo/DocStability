#!/usr/bin/env python3
"""
commit_message_reactive.py

Approach 1 — Issue/PR Reference Detection for Documentation-Touching Commits.

For each repository, extracts commit messages for all doc-touch commits
and detects whether each commit references an external trigger (issue number,
PR number, or explicit fix+reference language).

This avoids semantic keyword ambiguity entirely — we detect structural signals
(issue/PR references) that objectively indicate reactive maintenance.

USAGE (if you have raw git repos):
  python commit_message_reactive.py \
    --mode git \
    --repos_dir /path/to/cloned/repos \
    --since 2020-06-30 \
    --until 2025-06-29 \
    --output reactive_analysis.csv

USAGE (if you have SHA files from --write_touch_shas):
  python commit_message_reactive.py \
    --mode shas \
    --repos_dir /path/to/cloned/repos \
    --shas_dir /path/to/sha/txt/files \
    --output reactive_analysis.csv

OUTPUT:
  One row per repository with:
    - total doc-touch commits
    - reactive count (has issue/PR reference)
    - proactive count (no reference)
    - reactive rate
    - top trigger patterns found
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
from collections import Counter
from datetime import datetime, date
from pathlib import Path
from typing import List, Tuple, Dict


# ─────────────────────────────────────────────
# REACTIVE DETECTION PATTERNS
# Objective signals only — no semantic guessing
# ─────────────────────────────────────────────

REACTIVE_PATTERNS = [
    # Issue/PR number references — strongest signal
    (r'fix(?:es|ed)?\s+#\d+',           'fixes_issue'),
    (r'close[sd]?\s+#\d+',              'closes_issue'),
    (r'resolve[sd]?\s+#\d+',            'resolves_issue'),
    (r'(?:see|ref(?:erences?)?)\s+#\d+','references_issue'),
    (r'#\d{2,}',                         'issue_number'),   # bare #123

    # URL references to issues/PRs
    (r'github\.com/[^/]+/[^/]+/issues/\d+',  'github_issue_url'),
    (r'github\.com/[^/]+/[^/]+/pull/\d+',    'github_pr_url'),

    # High-confidence reactive fix language
    # Only when paired with clear error terms — avoids "fix formatting" ambiguity
    (r'\bfix(?:es|ed|ing)?\b.{0,40}\b(?:broken|wrong|incorrect|outdated|'
     r'stale|missing|typo|error|mistake|invalid|dead\s+link|broken\s+link)\b',
     'fix_with_error_term'),

    # Revert commits touching docs
    (r'^revert\b',                       'revert'),
]

COMPILED = [(re.compile(p, re.IGNORECASE), label)
            for p, label in REACTIVE_PATTERNS]


def is_reactive(message: str) -> Tuple[bool, List[str]]:
    """
    Returns (is_reactive, [matched_pattern_labels])
    A commit is reactive if ANY pattern matches.
    """
    matched = []
    for rx, label in COMPILED:
        if rx.search(message):
            matched.append(label)
    return (len(matched) > 0, matched)


# ─────────────────────────────────────────────
# GIT HELPERS
# ─────────────────────────────────────────────

def run_git(repo: str, args: List[str]) -> str:
    cmd = ["git", "-C", repo] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout


def get_doc_touch_shas_from_git(repo: str, since: str, until: str) -> List[str]:
    """
    Re-run the same git log logic as the intention script to get doc-touch SHAs.
    Uses --name-only to find commits touching health doc files.
    """
    # Exact health file patterns — must match intention script exactly
    HEALTH_PATTERNS = [
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
    # Exact exclude patterns — must match intention script exactly
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
    health_rx = [re.compile(p, re.IGNORECASE) for p in HEALTH_PATTERNS]
    excl_rx   = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_PATTERNS]

    raw = run_git(repo, [
        "log", "--no-merges", "--no-color",
        f"--since={since} 00:00:00",
        f"--until={until} 23:59:59",
        "--pretty=format:__COMMIT__%H",
        "--name-only",
    ])

    shas = []
    current_sha = None
    files = []

    for line in raw.splitlines():
        if line.startswith("__COMMIT__"):
            if current_sha and files:
                # check if any file is health
                health = [f for f in files
                          if not any(r.search(f) for r in excl_rx)
                          and any(r.match(f) for r in health_rx)]
                if health:
                    shas.append(current_sha)
            current_sha = line[len("__COMMIT__"):]
            files = []
        else:
            s = line.strip()
            if s:
                files.append(s)

    # last commit
    if current_sha and files:
        health = [f for f in files
                  if not any(r.search(f) for r in excl_rx)
                  and any(r.match(f) for r in health_rx)]
        if health:
            shas.append(current_sha)

    return shas


def get_commit_message(repo: str, sha: str) -> str:
    """Get full commit message for a given SHA."""
    return run_git(repo, ["log", "-1", "--pretty=format:%B", sha]).strip()


def get_messages_batch(repo: str, shas: List[str]) -> Dict[str, str]:
    """Get commit messages for a list of SHAs efficiently."""
    if not shas:
        return {}

    # Write SHAs to temp file for batch processing
    sha_args = []
    messages = {}

    # Process in batches of 100 to avoid arg limit
    batch_size = 100
    for i in range(0, len(shas), batch_size):
        batch = shas[i:i+batch_size]
        for sha in batch:
            msg = get_commit_message(repo, sha)
            messages[sha] = msg

    return messages


def load_shas_from_file(sha_file: Path) -> List[str]:
    """Load SHAs from a --write_touch_shas output file."""
    with open(sha_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]


# ─────────────────────────────────────────────
# MAIN ANALYSIS
# ─────────────────────────────────────────────

def analyze_repo(repo_name: str, repo_path: str,
                 shas: List[str]) -> Dict:
    """Analyze reactive vs proactive for one repo."""
    messages = get_messages_batch(repo_path, shas)

    reactive_count    = 0
    proactive_count   = 0
    pattern_counter   = Counter()
    reactive_examples = []

    for sha, msg in messages.items():
        reactive, patterns = is_reactive(msg)
        if reactive:
            reactive_count += 1
            for p in patterns:
                pattern_counter[p] += 1
            if len(reactive_examples) < 3:
                reactive_examples.append(msg[:120].replace('\n', ' '))
        else:
            proactive_count += 1

    total = len(shas)
    reactive_rate  = reactive_count / total if total > 0 else 0
    proactive_rate = proactive_count / total if total > 0 else 0

    top_patterns = ', '.join(
        f"{k}({v})" for k, v in pattern_counter.most_common(3)
    )

    return {
        'repo':             repo_name,
        'doc_touch_total':  total,
        'reactive_count':   reactive_count,
        'proactive_count':  proactive_count,
        'reactive_rate':    round(reactive_rate, 4),
        'proactive_rate':   round(proactive_rate, 4),
        'top_patterns':     top_patterns,
        'reactive_examples': ' | '.join(reactive_examples),
    }


def main():
    ap = argparse.ArgumentParser(
        description="Detect reactive documentation commits via issue/PR references"
    )
    ap.add_argument('--mode', choices=['git', 'shas'], default='git',
                    help="'git' = extract SHAs fresh; 'shas' = load from SHA files")
    ap.add_argument('--repos_dir', required=True,
                    help="Directory containing cloned git repositories")
    ap.add_argument('--shas_dir', default=None,
                    help="[shas mode] Directory with per-repo SHA .txt files")
    ap.add_argument('--since', default='2020-06-30',
                    help="Start date YYYY-MM-DD")
    ap.add_argument('--until', default='2025-06-29',
                    help="End date YYYY-MM-DD")
    ap.add_argument('--output', default='reactive_analysis.csv',
                    help="Output CSV path")
    args = ap.parse_args()

    repos_dir = Path(args.repos_dir)
    results   = []

    repo_dirs = sorted([d for d in repos_dir.iterdir() if d.is_dir()])
    print(f"Found {len(repo_dirs)} repositories in {repos_dir}\n")

    for repo_dir in repo_dirs:
        repo_name = repo_dir.name
        repo_path = str(repo_dir)

        print(f"Processing: {repo_name} ... ", end='', flush=True)

        try:
            # Get SHAs
            if args.mode == 'shas' and args.shas_dir:
                sha_file = Path(args.shas_dir) / repo_name / \
                           "intention_health_docs_touch_shas.txt"
                if not sha_file.exists():
                    # try alternate naming
                    candidates = list(
                        Path(args.shas_dir).glob(
                            f"{repo_name}/*touch_shas.txt"))
                    sha_file = candidates[0] if candidates else None

                if sha_file and sha_file.exists():
                    shas = load_shas_from_file(sha_file)
                else:
                    print(f"SHA file not found, falling back to git extraction")
                    shas = get_doc_touch_shas_from_git(
                        repo_path, args.since, args.until)
            else:
                shas = get_doc_touch_shas_from_git(
                    repo_path, args.since, args.until)

            if not shas:
                print(f"no doc-touch commits found")
                continue

            result = analyze_repo(repo_name, repo_path, shas)
            results.append(result)
            print(f"done — {result['doc_touch_total']} commits, "
                  f"{result['reactive_count']} reactive "
                  f"({result['reactive_rate']*100:.1f}%)")

        except Exception as e:
            print(f"ERROR: {e}")
            continue

    if not results:
        print("No results to save.")
        return

    # Write output
    fieldnames = ['repo', 'doc_touch_total', 'reactive_count',
                  'proactive_count', 'reactive_rate', 'proactive_rate',
                  'top_patterns', 'reactive_examples']

    with open(args.output, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    import pandas as pd
    df = pd.DataFrame(results)
    print(f"  Repos analyzed:        {len(df)}")
    print(f"  Total doc-touch:       {df['doc_touch_total'].sum():,}")
    print(f"  Total reactive:        {df['reactive_count'].sum():,} "
          f"({df['reactive_count'].sum()/df['doc_touch_total'].sum()*100:.1f}%)")
    print(f"  Median reactive rate:  {df['reactive_rate'].median()*100:.1f}%")
    print(f"  Mean reactive rate:    {df['reactive_rate'].mean()*100:.1f}%")
    print(f"\n  Results saved to: {args.output}")


if __name__ == "__main__":
    main()