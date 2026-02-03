#!/usr/bin/env python3
"""
contrib_concentration.py

Compute contributor concentration over a time window using a LOCAL git repo:
- Top-3 / Top-5 / Top-10 commit share
- Gini coefficient of commit counts across contributors
- Bot filtering using regex patterns (name and email)

NEW:
- Optional bot report CSV with matched bot patterns per contributor_id.

Example:
  python3 contrib_concentration.py \
    --repo ./react \
    --since 2020-06-30 \
    --until 2025-06-29 \
    --topk 3 5 10 \
    --exclude_bots \
    --write_bots \
    --out_prefix react_5yr \
    --write_details
"""

from __future__ import annotations
import argparse
import csv
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import Dict, List, Tuple, Set


# ----------------------------
# Bot detection (pattern strings + compiled)
# ----------------------------
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
BOT_PATTERNS_COMPILED = [(p, re.compile(p, re.IGNORECASE)) for p in BOT_PATTERNS]


def looks_like_bot(author_name: str, author_email: str) -> Tuple[bool, List[str]]:
    """
    Returns (is_bot, matched_patterns).
    We test BOTH author name and email together.
    """
    s = f"{author_name} {author_email}".strip()
    matched = [pat for (pat, rx) in BOT_PATTERNS_COMPILED if rx.search(s)]
    return (len(matched) > 0), matched


# ----------------------------
# Git helpers
# ----------------------------
def run_git(repo: str, args: List[str]) -> str:
    cmd = ["git", "-C", repo] + args
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    return out.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class CommitRow:
    epoch: int
    author_name: str
    author_email: str


def iter_commits(repo: str, since: date, until: date, include_merges: bool) -> List[CommitRow]:
    """
    Returns commits with (epoch, author name, author email) in date range.
    Uses AUTHOR date (%at).
    """
    log_args = [
        "log",
        "--no-color",
        f"--since={since.isoformat()} 00:00:00",
        f"--until={until.isoformat()} 23:59:59",
        "--pretty=format:%H\t%at\t%an\t%ae",
    ]
    if not include_merges:
        log_args.insert(1, "--no-merges")

    raw = run_git(repo, log_args)

    rows: List[CommitRow] = []
    for line in raw.splitlines():
        parts = line.split("\t")
        if len(parts) != 4:
            continue
        _, epoch_s, an, ae = parts
        try:
            epoch = int(epoch_s)
        except ValueError:
            continue
        rows.append(CommitRow(epoch=epoch, author_name=an.strip(), author_email=ae.strip()))
    return rows


# ----------------------------
# Metrics
# ----------------------------
def gini(values: List[int]) -> float:
    if not values:
        return 0.0
    vals = [v for v in values if v >= 0]
    n = len(vals)
    total = sum(vals)
    if n < 2 or total == 0:
        return 0.0

    vals.sort()
    weighted_sum = 0
    for i, x in enumerate(vals, start=1):
        weighted_sum += i * x
    g = (2 * weighted_sum) / (n * total) - (n + 1) / n
    return max(0.0, min(1.0, g))


def topk_share(counts_desc: List[Tuple[str, int]], k: int) -> float:
    total = sum(c for _, c in counts_desc)
    if total == 0:
        return 0.0
    return sum(c for _, c in counts_desc[:k]) / total


def contributor_id(author_name: str, author_email: str) -> str:
    # Prefer email as stable key
    if author_email:
        return author_email.lower()
    return author_name.strip().lower()


# ----------------------------
# Main
# ----------------------------
def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="Path to local git repo")
    ap.add_argument("--since", required=True, type=parse_date, help="YYYY-MM-DD")
    ap.add_argument("--until", required=True, type=parse_date, help="YYYY-MM-DD")
    ap.add_argument("--topk", nargs="+", type=int, default=[3, 5, 10], help="e.g., 3 5 10")
    ap.add_argument("--exclude_bots", action="store_true", help="Filter bot/automation authors from HUMAN metrics")
    ap.add_argument("--include_merges", action="store_true")
    ap.add_argument("--out_prefix", default="contributors_5yr")
    ap.add_argument("--write_details", action="store_true", help="Write per-contributor counts CSV (humans)")
    ap.add_argument("--write_bots", action="store_true", help="Write bot-classified contributor IDs + patterns")
    ap.add_argument("--bot_samples", type=int, default=3, help="How many sample name/email strings to store per bot id")
    args = ap.parse_args()

    repo = os.path.abspath(args.repo)
    repo_name = os.path.basename(repo)

    commits = iter_commits(repo, args.since, args.until, args.include_merges)

    # Human counts (after optional exclusion)
    human_counts: Dict[str, int] = {}

    # Bot tracking (always tracked, even if you don't exclude)
    bot_counts: Dict[str, int] = {}
    bot_patterns: Dict[str, Set[str]] = {}
    bot_samples: Dict[str, List[str]] = {}

    total_commits = 0
    bot_commits = 0

    for c in commits:
        total_commits += 1
        cid = contributor_id(c.author_name, c.author_email)

        is_bot, matched = looks_like_bot(c.author_name, c.author_email)
        if is_bot:
            bot_commits += 1
            bot_counts[cid] = bot_counts.get(cid, 0) + 1
            if cid not in bot_patterns:
                bot_patterns[cid] = set()
            bot_patterns[cid].update(matched)

            # keep a few sample strings for inspection
            if cid not in bot_samples:
                bot_samples[cid] = []
            if len(bot_samples[cid]) < args.bot_samples:
                bot_samples[cid].append(f"name='{c.author_name}' email='{c.author_email}'")

        # For HUMAN metrics, optionally exclude bots
        if args.exclude_bots and is_bot:
            continue

        human_counts[cid] = human_counts.get(cid, 0) + 1

    # Sort humans desc
    human_desc = sorted(human_counts.items(), key=lambda kv: kv[1], reverse=True)
    human_total = sum(v for _, v in human_desc)

    # Compute shares + gini
    topk_sorted = sorted(set(args.topk))
    shares = {k: topk_share(human_desc, k) for k in topk_sorted}
    g = gini([v for _, v in human_desc])

    # Print summary
    print("=" * 70)
    print(f"Repo: {repo_name}")
    print(f"Window: {args.since.isoformat()} → {args.until.isoformat()}")
    print(f"Commits (all): {total_commits}")
    print(f"Bot-classified commits (by patterns): {bot_commits}")
    print(f"Unique bot contributor_ids: {len(bot_counts)}")
    if args.exclude_bots:
        print(f"Commits counted (human, bots excluded): {human_total}")
        print(f"Unique contributors (human): {len(human_desc)}")
    else:
        print(f"Commits counted (all contributors): {human_total}")
        print(f"Unique contributors: {len(human_desc)}")
    print("-" * 70)
    for k in topk_sorted:
        print(f"Top-{k:>2} share: {shares[k]*100:6.2f}%")
    print(f"Gini (commit inequality): {g:.4f}")
    print("=" * 70)

    # Output files
    out_dir = os.path.join("outputs", repo_name)
    os.makedirs(out_dir, exist_ok=True)

    summary_path = os.path.join(out_dir, f"{args.out_prefix}_summary.csv")
    with open(summary_path, "w", newline="") as f:
        fieldnames = [
            "repo", "since", "until",
            "total_commits_all",
            "bot_commits_classified",
            "unique_bot_contributor_ids",
            "commits_counted_for_metrics",
            "unique_contributors_for_metrics",
            "gini",
        ] + [f"top{k}_share" for k in topk_sorted]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        row = {
            "repo": repo_name,
            "since": args.since.isoformat(),
            "until": args.until.isoformat(),
            "total_commits_all": total_commits,
            "bot_commits_classified": bot_commits,
            "unique_bot_contributor_ids": len(bot_counts),
            "commits_counted_for_metrics": human_total,
            "unique_contributors_for_metrics": len(human_desc),
            "gini": g,
        }
        for k in topk_sorted:
            row[f"top{k}_share"] = shares[k]
        w.writerow(row)

    if args.write_details:
        details_path = os.path.join(out_dir, f"{args.out_prefix}_contributors.csv")
        with open(details_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["repo", "contributor_id", "commits"])
            w.writeheader()
            for cid, n in human_desc:
                w.writerow({"repo": repo_name, "contributor_id": cid, "commits": n})
        print(f"✓ Wrote human details: {details_path}")

    if args.write_bots:
        bots_path = os.path.join(out_dir, f"{args.out_prefix}_bots.csv")
        bots_desc = sorted(bot_counts.items(), key=lambda kv: kv[1], reverse=True)
        with open(bots_path, "w", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["repo", "contributor_id", "bot_commits", "matched_patterns", "samples"]
            )
            w.writeheader()
            for cid, n in bots_desc:
                w.writerow({
                    "repo": repo_name,
                    "contributor_id": cid,
                    "bot_commits": n,
                    "matched_patterns": ";".join(sorted(bot_patterns.get(cid, set()))),
                    "samples": " | ".join(bot_samples.get(cid, [])),
                })
        print(f"✓ Wrote bot report: {bots_path}")

    print(f"✓ Wrote summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
