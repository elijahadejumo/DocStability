#!/usr/bin/env python3
"""
Bus factor robustness check, addressing reviewer C: "Bus factor methodology
follows the older baseline by Cosentino et al. rather than the newer,
more well-rounded method by Jabrayilzade et al."

The paper's current Bus-50/Bus-80 (via contrib_concentration.py /
doc_commit_ownership.py) uses raw commit-count top-K: rank contributors by
total commit count, find the smallest K whose combined commits reach 50%/80%.

Jabrayilzade et al.'s core methodological insight is that ownership should
be FILE-CENTRIC, not just aggregate-commit-centric: a contributor's true
"criticality" comes from being the primary owner of specific files, not
just having a high raw commit count across everything. A full Degree-of-
Authorship (DOA) model (Fritz et al. 2010, as adapted by Jabrayilzade et al.)
needs line-level blame data we don't have; this implements the more
tractable file-centric proxy: for each health-doc file, identify its
top-commit-count owner, then compute bus factor as the minimum number of
distinct "top-owners" needed to collectively be the top owner of >=50%/80%
of touched files. This is a genuinely different operationalization (file
primary-ownership vs. raw commit share) and provides a real robustness
check, not a reproduction of the existing metric under a new name.
"""
import glob
import os

import numpy as np
import pandas as pd
from scipy import stats

ROOT = "/Users/elijahadejumo/Documents/DocStability"
OUT = os.path.join(ROOT, "analysis_outputs")
LOGS_DIR = os.path.join(ROOT, "full_commit_logs")


def bus_factor_from_owner_counts(owner_of_file_counts, threshold):
    """Given a Counter of {owner: n_files_owned}, find min contributors to reach threshold share."""
    total = sum(owner_of_file_counts.values())
    if total == 0:
        return None
    counts_desc = sorted(owner_of_file_counts.values(), reverse=True)
    cum = 0
    for i, c in enumerate(counts_desc, 1):
        cum += c
        if cum / total >= threshold:
            return i
    return len(counts_desc)


def main():
    fd_files = glob.glob(os.path.join(ROOT, "per_repo", "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True)

    rows = []
    for repo, group in fd.groupby("repo"):
        log_path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
        if not os.path.exists(log_path):
            continue
        log = pd.read_csv(log_path, usecols=["commit_sha", "author_id", "is_bot"]).drop_duplicates("commit_sha")
        log["is_bot"] = log["is_bot"].astype(str).str.lower() == "true"
        log = log[~log["is_bot"]]

        merged = group.merge(log, on="commit_sha", how="inner")
        if merged.empty:
            continue

        # per-file: who has the most commits touching that file? (file-centric primary ownership)
        file_owner_counts = (
            merged.groupby(["health_file", "author_id"]).size().reset_index(name="n")
        )
        top_owner_per_file = file_owner_counts.sort_values("n", ascending=False).drop_duplicates("health_file")
        owner_file_counts = top_owner_per_file["author_id"].value_counts().to_dict()

        bus50_file = bus_factor_from_owner_counts(owner_file_counts, 0.5)
        bus80_file = bus_factor_from_owner_counts(owner_file_counts, 0.8)

        rows.append({
            "repo": repo,
            "n_health_files_touched": merged["health_file"].nunique(),
            "n_distinct_file_owners": len(owner_file_counts),
            "bus50_file_centric": bus50_file,
            "bus80_file_centric": bus80_file,
        })

    df = pd.DataFrame(rows)
    print(f"Computed file-centric bus factor for {len(df)} repos")
    print(df[["bus50_file_centric", "bus80_file_centric"]].describe())

    # compare against existing commit-count-based Bus-50/80
    existing = pd.read_csv(os.path.join(ROOT, "combined", "combined_doc_contributors.csv"))[
        ["repo", "health_docs_touch_bus50", "health_docs_touch_bus80"]
    ]
    cmp_df = df.merge(existing, on="repo", how="inner")

    rho50, p50 = stats.spearmanr(cmp_df["bus50_file_centric"], cmp_df["health_docs_touch_bus50"])
    rho80, p80 = stats.spearmanr(cmp_df["bus80_file_centric"], cmp_df["health_docs_touch_bus80"])
    print(f"\nSpearman corr, Bus-50 (file-centric vs commit-count): rho={rho50:.3f}, p={p50:.4g}")
    print(f"Spearman corr, Bus-80 (file-centric vs commit-count): rho={rho80:.3f}, p={p80:.4g}")

    n_bus50_eq1_file = (cmp_df["bus50_file_centric"] == 1).sum()
    n_bus50_eq1_commit = (cmp_df["health_docs_touch_bus50"] == 1).sum()
    print(f"\nRepos with Bus-50=1: file-centric={n_bus50_eq1_file}/{len(cmp_df)} ({n_bus50_eq1_file/len(cmp_df):.1%}), "
          f"commit-count={n_bus50_eq1_commit}/{len(cmp_df)} ({n_bus50_eq1_commit/len(cmp_df):.1%})")

    cmp_df.to_csv(os.path.join(OUT, "bus_factor_file_centric_comparison.csv"), index=False)
    print(f"\nSaved: {OUT}/bus_factor_file_centric_comparison.csv")


if __name__ == "__main__":
    main()
