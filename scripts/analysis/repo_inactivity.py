#!/usr/bin/env python3
"""
Does DOCUMENTATION rhythm/participation/bus-factor predict REPOSITORY-WIDE
(all commits, not just docs) inactivity? Unlike the doc-vs-doc dormancy gap
test (which was circular -- both computed from the same monthly doc-commit
vector), documentation predictors and overall-repo activity are genuinely
different data sources, so this isn't mechanically guaranteed either way.

Three outcomes:
1. Overall longest dormancy gap (longest consecutive run of months with
   zero commits of ANY type) -- contemporaneous, full window.
2. Days since last commit (any type) as of window end -- a recency/currency
   proxy for "is this project still alive right now."
3. LAGGED: first-half DOC rhythm -> second-half OVERALL commit decline --
   construct-different AND properly time-ordered.
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
SINCE_TS = pd.Timestamp("2020-06-30")
UNTIL_TS = pd.Timestamp("2025-06-29")


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
MID_IDX = len(MONTHS) // 2
MONTHS_H1 = set(MONTHS[:MID_IDX])
MONTHS_H2 = set(MONTHS[MID_IDX:])


def longest_zero_run(counts):
    best = cur = 0
    for c in counts:
        if c == 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


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


def repo_outcomes(repo):
    path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, usecols=["author_date", "is_bot"])
    df["author_date"] = pd.to_datetime(df["author_date"], errors="coerce")
    df = df.dropna(subset=["author_date"])
    win = df[(df["author_date"] >= SINCE_TS) & (df["author_date"] <= UNTIL_TS)]
    if win.empty:
        return None

    win = win.copy()
    win["month"] = win["author_date"].dt.strftime("%Y-%m")
    month_counts = win.groupby("month").size().to_dict()
    counts_full = [month_counts.get(mk, 0) for mk in MONTHS]
    gap = longest_zero_run(counts_full)

    last_commit = df["author_date"].max()
    days_since_last = (UNTIL_TS - last_commit).days

    h1_commits = sum(month_counts.get(mk, 0) for mk in MONTHS_H1)
    h2_commits = sum(month_counts.get(mk, 0) for mk in MONTHS_H2)
    decline_ratio = (h2_commits - h1_commits) / (h1_commits + 1)

    return {
        "repo": repo,
        "overall_longest_gap_months": gap,
        "days_since_last_commit": max(days_since_last, 0),
        "overall_h2_decline_ratio": decline_ratio,
    }


def main():
    logs = glob.glob(os.path.join(LOGS_DIR, "*_full_commit_log.csv"))
    repos = sorted(os.path.basename(f).replace("_full_commit_log.csv", "") for f in logs)
    repos = [r for r in repos if r != "combined"]

    rows = [repo_outcomes(r) for r in repos]
    rows = [r for r in rows if r is not None]
    df = pd.DataFrame(rows)

    # doc-specific first-half rhythm (for the lagged test)
    fd_files = glob.glob(os.path.join(ROOT, "per_repo", "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True)
    fd["month"] = pd.to_datetime(fd["commit_date"]).dt.strftime("%Y-%m")
    h1_rows = []
    for repo, group in fd.groupby("repo"):
        log_path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
        if not os.path.exists(log_path):
            continue
        log = pd.read_csv(log_path, usecols=["commit_sha", "is_bot"]).drop_duplicates("commit_sha")
        log["is_bot"] = log["is_bot"].astype(str).str.lower() == "true"
        bot_shas = set(log[log["is_bot"]]["commit_sha"])
        human = group.drop_duplicates("commit_sha")
        human = human[~human["commit_sha"].isin(bot_shas)]
        month_counts = defaultdict(int)
        for m in human["month"]:
            if m in MONTHS_H1:
                month_counts[m] += 1
        counts_h1 = [month_counts.get(mk, 0) for mk in sorted(MONTHS_H1)]
        h1_rows.append({
            "repo": repo,
            "doc_h1_entropy": entropy_norm(counts_h1),
            "doc_h1_awr": sum(1 for c in counts_h1 if c > 0) / len(counts_h1),
        })
    h1_df = pd.DataFrame(h1_rows)
    df = df.merge(h1_df, on="repo", how="left")

    arch = pd.read_csv(os.path.join(ROOT, "combined", "archetype_assignments.csv"))[["repo", "entropy_norm", "active_window_rate"]]
    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[["repo", "unique_contributors_for_metrics"]]
    owner = pd.read_csv(os.path.join(ROOT, "combined", "combined_doc_contributors.csv"))[["repo", "health_docs_touch_contributors", "health_docs_touch_bus50"]]
    rhythm = pd.read_csv(os.path.join(ROOT, "combined", "combined_doc_stability_metrics.csv"))[["repo", "health_file_commits"]]

    df = df.merge(arch, on="repo").merge(contrib, on="repo").merge(owner, on="repo").merge(rhythm, on="repo")
    df["log_contributors"] = np.log1p(df["unique_contributors_for_metrics"])
    df["log_doc_commits"] = np.log1p(df["health_file_commits"])
    df["participation_rate"] = df["health_docs_touch_contributors"] / df["unique_contributors_for_metrics"]
    df["log_bus50"] = np.log1p(df["health_docs_touch_bus50"])
    df["log_days_since_last"] = np.log1p(df["days_since_last_commit"])
    df.to_csv(os.path.join(OUT, "repo_inactivity_dataset.csv"), index=False)
    print(f"n={len(df)}")
    print(df[["overall_longest_gap_months", "days_since_last_commit"]].describe())

    def partial_spearman(x, y, z):
        rx, ry, rz = rankdata(x), rankdata(y), rankdata(z)
        rxz = np.corrcoef(rx, rz)[0, 1]; ryz = np.corrcoef(ry, rz)[0, 1]; rxy = np.corrcoef(rx, ry)[0, 1]
        return (rxy - rxz * ryz) / np.sqrt((1 - rxz ** 2) * (1 - ryz ** 2))

    def battery(outcome, pred, sub, vol_col="log_doc_commits"):
        sub = sub.dropna(subset=[outcome, pred, "log_contributors", vol_col])
        if len(sub) < 20:
            print(f"{pred:22s} insufficient data")
            return
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
        print(f"{pred:22s} p(size)={ft[1]:.4f} p(vol)={ft_v[1]:.4f} p(HC3)={m_hc3.pvalues[pred]:.4f} "
              f"p(no-outlier)={m_r.pvalues[pred]:.4f} rho={pr:.3f}{flag}")

    predictors = ["entropy_norm", "active_window_rate", "participation_rate", "log_bus50"]

    print("\n=== 1. Overall repo-wide longest dormancy gap (contemporaneous) ===")
    for pred in predictors:
        battery("overall_longest_gap_months", pred, df.copy())

    print("\n=== 2. Days since last commit (recency, as of window end) ===")
    for pred in predictors:
        battery("log_days_since_last", pred, df.copy())

    print("\n=== 3. LAGGED: doc h1 rhythm -> overall repo h2 decline ===")
    for pred in ["doc_h1_entropy", "doc_h1_awr"]:
        sub = df.copy()
        sub["log_h1_doc"] = np.log1p(sub.groupby("repo")["log_doc_commits"].transform("first"))  # placeholder vol control
        battery("overall_h2_decline_ratio", pred, sub, vol_col="log_doc_commits")


if __name__ == "__main__":
    main()
