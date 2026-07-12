#!/usr/bin/env python3
"""
Artifact-type stratification analysis.

Splits health documentation into two strata:
  - living/process docs: expected to evolve with the project (README, CONTRIBUTING,
    CHANGELOG, HISTORY, RELEASE(S), GOVERNANCE, SECURITY, SUPPORT, MAINTAINERS,
    CODE_OF_CONDUCT, ROADMAP, VISION, templates, BUILDING, COMMIT_CONVENTIONS)
  - static/attribution docs: write-once or rarely-touched (LICENSE, NOTICE, COPYING,
    AUTHORS, CREDITS, THANKS, CONTRIBUTORS)

Recomputes normalized Shannon entropy + Active Window Rate per stratum per repo,
using the SAME windowing logic as doc_entropy.py (61 months, 2020-06-30..2025-06-29),
and compares against the existing combined (all-types) archetype assignments to test
whether rhythm archetypes are being driven by a small subset of dominant file types.
"""
import glob
import math
import os
from collections import defaultdict
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
PROJ = str(REPO_ROOT / "per_repo")
COMBINED = str(REPO_ROOT / "combined")
OUT = str(REPO_ROOT / "analysis_outputs")

SINCE = (2020, 6)
UNTIL = (2025, 6)

LIVING_STEMS = {
    "readme", "contributing", "changelog", "history", "release", "releases",
    "pull_request_template", "issue_template", "commit_conventions", "building",
    "code_of_conduct", "governance", "support", "maintainers", "security",
    "roadmap", "vision",
}
STATIC_STEMS = {
    "license", "notice", "copying", "authors", "credits", "thanks", "contributors",
}


def classify(path: str) -> str:
    p = path.replace("\\", "/")
    basename = p.split("/")[-1].lower()
    stem = basename.split(".")[0]
    if stem in LIVING_STEMS:
        return "living"
    if stem in STATIC_STEMS:
        return "static"
    return "unmapped"


def iter_month_keys(since_y, since_m, until_y, until_m):
    y, m = since_y, since_m
    out = []
    while (y < until_y) or (y == until_y and m <= until_m):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


MONTHS = iter_month_keys(*SINCE, *UNTIL)
M = len(MONTHS)


def entropy_norm(counts):
    total = sum(counts)
    if total <= 0:
        return None
    if len(counts) <= 1:
        return 0.0
    H = 0.0
    for c in counts:
        if c <= 0:
            continue
        p = c / total
        H -= p * math.log(p)
    return max(0.0, min(1.0, H / math.log(len(counts))))


def active_window_rate(counts):
    return sum(1 for c in counts if c > 0) / len(counts)


def main():
    files = glob.glob(os.path.join(PROJ, "*/*file_details.csv"))
    print(f"Found {len(files)} file_details files")

    dfs = [pd.read_csv(f) for f in files]
    all_df = pd.concat(dfs, ignore_index=True)
    all_df["commit_date"] = pd.to_datetime(all_df["commit_date"])
    all_df["month"] = all_df["commit_date"].dt.strftime("%Y-%m")
    all_df["category"] = all_df["health_file"].apply(classify)

    unmapped = all_df[all_df["category"] == "unmapped"]
    print(f"\nTotal (repo,file) rows: {len(all_df)}")
    print(f"Unmapped rows: {len(unmapped)} ({len(unmapped)/len(all_df):.2%})")
    if len(unmapped):
        print("Unmapped health_file values:")
        print(unmapped["health_file"].value_counts())

    # ---- ecosystem-level artifact type imbalance ----
    commit_level = all_df.drop_duplicates(["repo", "commit_sha", "category"])
    cat_counts = commit_level["category"].value_counts()
    print("\nEcosystem-level commit-category counts (a commit can count in both):")
    print(cat_counts)

    # commits that touch ONLY static, ONLY living, or BOTH
    commit_cats = all_df.groupby(["repo", "commit_sha"])["category"].apply(set)
    only_living = (commit_cats.apply(lambda s: s == {"living"})).sum()
    only_static = (commit_cats.apply(lambda s: s == {"static"})).sum()
    mixed = (commit_cats.apply(lambda s: len(s) > 1)).sum()
    total_commits = len(commit_cats)
    print(f"\nTotal unique documentation-touching commits: {total_commits}")
    print(f"  living-only commits: {only_living} ({only_living/total_commits:.1%})")
    print(f"  static-only commits: {only_static} ({only_static/total_commits:.1%})")
    print(f"  mixed/other commits: {mixed} ({mixed/total_commits:.1%})")

    # ---- per-repo, per-category monthly counts -> entropy/AWR ----
    rows = []
    for repo, g in all_df.groupby("repo"):
        for category in ["living", "static"]:
            sub = g[g["category"] == category]
            # unique commit per month (a commit touching 2 living files counts once)
            month_counts = defaultdict(int)
            for (sha, month), _ in sub.groupby(["commit_sha", "month"]):
                month_counts[month] += 1
            counts = [month_counts.get(mk, 0) for mk in MONTHS]
            n_commits = sub.drop_duplicates("commit_sha").shape[0]
            rows.append({
                "repo": repo,
                "category": category,
                "n_commits": n_commits,
                "entropy_norm": entropy_norm(counts),
                "active_window_rate": active_window_rate(counts),
            })
    strat_df = pd.DataFrame(rows)
    strat_df.to_csv(os.path.join(OUT, "artifact_stratified_rhythm.csv"), index=False)

    # pivot for comparison
    piv_h = strat_df.pivot(index="repo", columns="category", values="entropy_norm")
    piv_a = strat_df.pivot(index="repo", columns="category", values="active_window_rate")
    piv_n = strat_df.pivot(index="repo", columns="category", values="n_commits")

    # ---- merge with existing combined (all-types) archetype metrics ----
    combined = pd.read_csv(os.path.join(COMBINED, "archetype_assignments.csv"))
    combined = combined.set_index("repo")

    cmp_df = pd.DataFrame({
        "entropy_all": combined["entropy_norm"],
        "awr_all": combined["active_window_rate"],
        "archetype_all": combined["archetype"],
        "entropy_living": piv_h["living"],
        "awr_living": piv_a["living"],
        "n_living": piv_n["living"],
        "entropy_static": piv_h.get("static"),
        "awr_static": piv_a.get("static"),
        "n_static": piv_n.get("static"),
    })
    cmp_df.to_csv(os.path.join(OUT, "artifact_stratified_vs_combined.csv"))

    valid = cmp_df.dropna(subset=["entropy_all", "entropy_living"])
    corr_h = valid["entropy_all"].corr(valid["entropy_living"], method="spearman")
    corr_a = valid[["awr_all", "awr_living"]].dropna()
    corr_a_val = corr_a["awr_all"].corr(corr_a["awr_living"], method="spearman")

    print(f"\nSpearman corr(entropy_all, entropy_living): {corr_h:.3f}  (n={len(valid)})")
    print(f"Spearman corr(awr_all, awr_living): {corr_a_val:.3f}  (n={len(corr_a)})")

    # how many repos have zero static-doc activity in window (bus-factor-irrelevant docs)
    zero_static = (piv_n.get("static", pd.Series(dtype=float)).fillna(0) == 0).sum()
    print(f"\nRepos with ZERO static-doc commits in window: {zero_static} / {len(piv_n)}")

    # distribution of static-doc share of total doc commits, per repo
    static_share = (piv_n.get("static", 0).fillna(0) / (piv_n["living"].fillna(0) + piv_n.get("static", 0).fillna(0)))
    print("\nStatic-doc share of doc commits per repo (describe):")
    print(static_share.describe())

    print("\nSaved:")
    print(f"  {OUT}/artifact_stratified_rhythm.csv")
    print(f"  {OUT}/artifact_stratified_vs_combined.csv")


if __name__ == "__main__":
    main()
