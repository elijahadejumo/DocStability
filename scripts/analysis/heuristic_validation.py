#!/usr/bin/env python3
"""
Validate the external-coordination-linkage heuristic (commit_message_external_links.py)
against actual commit subjects, now available via the full commit-log extraction.
No raw commit message was previously persisted anywhere in the replication
package, so this is the first time the heuristic can be checked against real
text rather than the handful of truncated examples in combined_reactive_analysis.csv.
"""
import glob
import os
import re

import pandas as pd

ROOT = "/Users/elijahadejumo/Documents/DocStability"
LOGS_DIR = os.path.join(ROOT, "full_commit_logs")
OUT = os.path.join(ROOT, "analysis_outputs")

# byte-identical to commit_message_external_links.py's REACTIVE_PATTERNS
REACTIVE_PATTERNS = [
    (r'fix(?:es|ed)?\s+#\d+',           'fixes_issue'),
    (r'close[sd]?\s+#\d+',              'closes_issue'),
    (r'resolve[sd]?\s+#\d+',            'resolves_issue'),
    (r'(?:see|ref(?:erences?)?)\s+#\d+','references_issue'),
    (r'#\d{2,}',                         'issue_number'),
    (r'github\.com/[^/]+/[^/]+/issues/\d+',  'github_issue_url'),
    (r'github\.com/[^/]+/[^/]+/pull/\d+',    'github_pr_url'),
    (r'\bfix(?:es|ed|ing)?\b.{0,40}\b(?:broken|wrong|incorrect|outdated|'
     r'stale|missing|typo|error|mistake|invalid|dead\s+link|broken\s+link)\b',
     'fix_with_error_term'),
    (r'^revert\b',                       'revert'),
]
COMPILED = [(re.compile(p, re.IGNORECASE), label) for p, label in REACTIVE_PATTERNS]


def is_reactive(message):
    matched = [label for rx, label in COMPILED if rx.search(str(message))]
    return (len(matched) > 0, matched)


def main():
    fd_files = glob.glob(os.path.join(ROOT, "per_repo", "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True)
    fd = fd.drop_duplicates(["repo", "commit_sha"])
    print(f"Doc-touching commits: {len(fd)}")

    rows = []
    missing = 0
    for repo, group in fd.groupby("repo"):
        log_path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
        if not os.path.exists(log_path):
            missing += len(group)
            continue
        log = pd.read_csv(log_path, usecols=["commit_sha", "subject"]).drop_duplicates("commit_sha")
        merged = group.merge(log, on="commit_sha", how="left")
        rows.append(merged)

    all_df = pd.concat(rows, ignore_index=True)
    unmatched = all_df["subject"].isna().sum()
    print(f"Unmatched (no subject found): {unmatched} / {len(all_df)}")
    all_df = all_df.dropna(subset=["subject"])

    all_df[["is_reactive", "matched_patterns"]] = all_df["subject"].apply(
        lambda m: pd.Series(is_reactive(m))
    )

    print(f"\nReactive rate (recomputed from real subjects): {all_df['is_reactive'].mean():.4f}")
    print(f"  (paper reports 49.0% ecosystem-level)")

    # criterion-level breakdown
    from collections import Counter
    pattern_counts = Counter()
    for pats in all_df.loc[all_df["is_reactive"], "matched_patterns"]:
        for p in pats:
            pattern_counts[p] += 1
    print("\nPattern frequency among reactive commits:")
    for p, c in pattern_counts.most_common():
        print(f"  {p}: {c}")

    # stratified random sample for manual spot-check: 75 reactive + 75 proactive
    reactive_sample = all_df[all_df["is_reactive"]].sample(n=min(75, all_df["is_reactive"].sum()), random_state=42)
    proactive_sample = all_df[~all_df["is_reactive"]].sample(n=min(75, (~all_df["is_reactive"]).sum()), random_state=42)
    sample = pd.concat([reactive_sample, proactive_sample])[["repo", "commit_sha", "subject", "is_reactive", "matched_patterns"]]
    sample.to_csv(os.path.join(OUT, "heuristic_validation_sample.csv"), index=False)
    print(f"\nSaved stratified sample (75 reactive + 75 proactive) to: {OUT}/heuristic_validation_sample.csv")

    all_df.to_csv(os.path.join(OUT, "heuristic_validation_full.csv"), index=False)
    print(f"Saved full recomputed classification to: {OUT}/heuristic_validation_full.csv")


if __name__ == "__main__":
    main()
