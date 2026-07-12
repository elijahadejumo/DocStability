#!/usr/bin/env python3
"""
Finer-grained artifact-type taxonomy (4 categories instead of the earlier
living/static binary split):
  1. Onboarding/Process: README, CONTRIBUTING, BUILDING, COMMIT_CONVENTIONS,
     PULL_REQUEST_TEMPLATE, ISSUE_TEMPLATE
  2. Governance/Policy: GOVERNANCE, CODE_OF_CONDUCT, SECURITY, SUPPORT,
     MAINTAINERS, ROADMAP, VISION
  3. Change-tracking: CHANGELOG, HISTORY, RELEASE(S)
  4. Legal/Attribution: LICENSE, NOTICE, COPYING, AUTHORS, CREDITS, THANKS,
     CONTRIBUTORS

Reports (a) ecosystem-level and per-repo descriptive distribution across
categories, and (b) tests each category's entropy/AWR/participation against
the two validated outcomes (documentation staleness, documentation
contributor loss) using the same robustness battery as the rest of the
session.
"""
import glob
import math
import os
from collections import defaultdict

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy.stats import rankdata

ROOT = "/Users/elijahadejumo/Documents/DocStability"
LOGS_DIR = os.path.join(ROOT, "full_commit_logs")
OUT = os.path.join(ROOT, "analysis_outputs")

SINCE = (2020, 6)
UNTIL_YM = (2025, 6)
UNTIL_TS = pd.Timestamp("2025-06-29")

CATEGORIES = {
    "onboarding_process": {"readme", "contributing", "building", "commit_conventions",
                            "pull_request_template", "issue_template"},
    "governance_policy": {"governance", "code_of_conduct", "security", "support",
                           "maintainers", "roadmap", "vision"},
    "change_tracking": {"changelog", "history", "release", "releases"},
    "legal_attribution": {"license", "notice", "copying", "authors", "credits",
                           "thanks", "contributors"},
}
STEM_TO_CATEGORY = {stem: cat for cat, stems in CATEGORIES.items() for stem in stems}


def classify(path):
    basename = path.replace("\\", "/").split("/")[-1].lower()
    stem = basename.split(".")[0]
    return STEM_TO_CATEGORY.get(stem, "unmapped")


def iter_month_keys(since, until):
    y, m = since
    out = []
    while (y < until[0]) or (y == until[0] and m <= until[1]):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m, y = 1, y + 1
    return out


MONTHS = iter_month_keys(SINCE, UNTIL_YM)


def entropy_norm(counts):
    total = sum(counts)
    if total <= 0:
        return None
    if len(counts) <= 1:
        return 0.0
    H = 0.0
    for c in counts:
        if c > 0:
            p = c / total
            H -= p * math.log(p)
    return max(0.0, min(1.0, H / math.log(len(counts))))


def main():
    fd_files = glob.glob(os.path.join(ROOT, "per_repo", "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True)
    fd["commit_date"] = pd.to_datetime(fd["commit_date"])
    fd["month"] = fd["commit_date"].dt.strftime("%Y-%m")
    fd["category"] = fd["health_file"].apply(classify)

    unmapped = fd[fd["category"] == "unmapped"]
    print(f"Unmapped rows: {len(unmapped)} / {len(fd)}")
    if len(unmapped):
        print(unmapped["health_file"].value_counts())

    # ---- Descriptive: ecosystem-level commit share per category ----
    print("\n=== Ecosystem-level: unique-commit share per category ===")
    print("(a commit touching multiple categories counts once per category)")
    commit_cat = fd.groupby(["repo", "commit_sha", "category"]).size().reset_index()
    cat_commit_counts = commit_cat.groupby("category")["commit_sha"].nunique()
    total_commits = fd.drop_duplicates(["repo", "commit_sha"]).shape[0]
    for cat, n in cat_commit_counts.items():
        print(f"  {cat:22s}: {n:6d} commits ({n/total_commits:.1%} of {total_commits} total)")

    # ---- Per-repo category-level entropy/AWR/participation ----
    rows = []
    for repo, repo_group in fd.groupby("repo"):
        log_path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
        author_map = {}
        if os.path.exists(log_path):
            log = pd.read_csv(log_path, usecols=["commit_sha", "author_id", "is_bot"]).drop_duplicates("commit_sha")
            log["is_bot"] = log["is_bot"].astype(str).str.lower() == "true"
            author_map = log.set_index("commit_sha")[["author_id", "is_bot"]].to_dict("index")

        for cat in CATEGORIES:
            cat_group = repo_group[repo_group["category"] == cat]
            commits = cat_group.drop_duplicates("commit_sha").copy()
            if commits.empty:
                rows.append({"repo": repo, "category": cat, "n_commits": 0, "entropy_norm": None,
                             "awr": None, "n_contributors": 0})
                continue
            commits["is_bot"] = commits["commit_sha"].map(lambda s: author_map.get(s, {}).get("is_bot", False))
            commits["author_id"] = commits["commit_sha"].map(lambda s: author_map.get(s, {}).get("author_id"))
            human = commits[~commits["is_bot"]]

            month_counts = defaultdict(int)
            for sha, month in human[["commit_sha", "month"]].drop_duplicates().values:
                month_counts[month] += 1
            counts = [month_counts.get(mk, 0) for mk in MONTHS]
            awr = sum(1 for c in counts if c > 0) / len(counts)
            ent = entropy_norm(counts)

            rows.append({
                "repo": repo, "category": cat, "n_commits": len(human),
                "entropy_norm": ent, "awr": awr,
                "n_contributors": human["author_id"].nunique(),
            })

    cat_df = pd.DataFrame(rows)
    cat_df.to_csv(os.path.join(OUT, "category_breakdown_dataset.csv"), index=False)

    # per-repo share (descriptive)
    print("\n=== Per-repo median category share of documentation commits ===")
    pivot = cat_df.pivot(index="repo", columns="category", values="n_commits").fillna(0)
    shares = pivot.div(pivot.sum(axis=1), axis=0)
    print(shares.median().sort_values(ascending=False))
    print("\nRepos with ZERO commits in each category:")
    print((pivot == 0).sum())

    # ---- Merge with outcomes and test ----
    staleness = pd.read_csv(os.path.join(OUT, "final_battery_dataset.csv"))[["repo", "log_staleness"]]
    loss = pd.read_csv(os.path.join(OUT, "doc_contributor_loss_dataset.csv"))[
        ["repo", "doc_contributor_loss_rate_all", "doc_contributor_loss_rate_top", "log_contributors"]
    ]
    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[["repo", "unique_contributors_for_metrics"]]

    def partial_spearman(x, y, z):
        rx, ry, rz = rankdata(x), rankdata(y), rankdata(z)
        rxz = np.corrcoef(rx, rz)[0, 1]; ryz = np.corrcoef(ry, rz)[0, 1]; rxy = np.corrcoef(rx, ry)[0, 1]
        return (rxy - rxz * ryz) / np.sqrt((1 - rxz ** 2) * (1 - ryz ** 2))

    def battery(outcome, pred, sub, vol_col):
        sub = sub.dropna(subset=[outcome, pred, "log_contributors", vol_col])
        if len(sub) < 20:
            print(f"    {pred:14s} insufficient data (n={len(sub)})"); return
        base = smf.ols(f"{outcome} ~ log_contributors", data=sub).fit()
        m = smf.ols(f"{outcome} ~ log_contributors + {pred}", data=sub).fit()
        ft = m.compare_f_test(base)
        base_v = smf.ols(f"{outcome} ~ log_contributors + {vol_col}", data=sub).fit()
        m_v = smf.ols(f"{outcome} ~ log_contributors + {vol_col} + {pred}", data=sub).fit()
        ft_v = m_v.compare_f_test(base_v)
        m_hc3 = smf.ols(f"{outcome} ~ log_contributors + {pred}", data=sub).fit(cov_type="HC3")
        infl = m.get_influence()
        cooks_d = infl.cooks_distance[0]
        drop_idx = pd.Series(cooks_d, index=sub.index).sort_values(ascending=False).head(3).index
        sub_r = sub.drop(drop_idx)
        m_r = smf.ols(f"{outcome} ~ log_contributors + {pred}", data=sub_r).fit()
        pr = partial_spearman(sub[pred], sub[outcome], sub["log_contributors"])
        flag = " <---" if (ft[1] < 0.05 and ft_v[1] < 0.05 and m_hc3.pvalues[pred] < 0.05 and m_r.pvalues[pred] < 0.05) else ""
        print(f"    {pred:14s} n={len(sub):3d} p(size)={ft[1]:.4f} p(vol)={ft_v[1]:.4f} p(HC3)={m_hc3.pvalues[pred]:.4f} "
              f"p(no-outlier)={m_r.pvalues[pred]:.4f} rho={pr:.3f}{flag}")

    print("\n\n=== IMPACT: each category's entropy/AWR/participation vs. staleness (all-docs outcome) ===")
    for cat in CATEGORIES:
        print(f"  --- {cat} ---")
        sub = cat_df[cat_df["category"] == cat].merge(staleness, on="repo").merge(contrib, on="repo")
        sub["log_contributors"] = np.log1p(sub["unique_contributors_for_metrics"])
        sub["participation_rate"] = sub["n_contributors"] / sub["unique_contributors_for_metrics"]
        sub["log_commits"] = np.log1p(sub["n_commits"])
        for pred in ["entropy_norm", "awr", "participation_rate"]:
            battery("log_staleness", pred, sub.copy(), vol_col="log_commits")

    print("\n\n=== IMPACT: each category's entropy/AWR/participation vs. doc contributor loss (top-quartile) ===")
    for cat in CATEGORIES:
        print(f"  --- {cat} ---")
        sub = cat_df[cat_df["category"] == cat].merge(loss[["repo", "doc_contributor_loss_rate_top"]], on="repo").merge(contrib, on="repo")
        sub["log_contributors"] = np.log1p(sub["unique_contributors_for_metrics"])
        sub["participation_rate"] = sub["n_contributors"] / sub["unique_contributors_for_metrics"]
        sub["log_commits"] = np.log1p(sub["n_commits"])
        for pred in ["entropy_norm", "awr", "participation_rate"]:
            battery("doc_contributor_loss_rate_top", pred, sub.copy(), vol_col="log_commits")


if __name__ == "__main__":
    main()
