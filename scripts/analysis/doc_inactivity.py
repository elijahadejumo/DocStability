#!/usr/bin/env python3
"""
Documentation-specific inactivity tests, construct-matched (doc predictors
-> doc outcomes, following what worked for the newcomer-retention test):

1. Longest dormancy gap: longest consecutive run of months with zero doc
   commits, anywhere in the 61-month window. Tested contemporaneously
   (same-window) against entropy/AWR/participation/bus50.

2. Lagged decline: rhythm computed on the FIRST HALF only, predicting
   whether documentation activity declines or goes fully dormant in the
   SECOND HALF -- proper temporal ordering, addresses reverse-causality
   concerns present in every contemporaneous test so far.
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
MID_TS = SINCE_TS + (UNTIL_TS - SINCE_TS) / 2


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
MONTHS_H1 = MONTHS[:MID_IDX]
MONTHS_H2 = MONTHS[MID_IDX:]


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


def longest_zero_run(counts):
    best = cur = 0
    for c in counts:
        if c == 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def get_human_doc_commits(repo, fd_group):
    log_path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
    if not os.path.exists(log_path):
        return None
    log = pd.read_csv(log_path, usecols=["commit_sha", "is_bot"]).drop_duplicates("commit_sha")
    log["is_bot"] = log["is_bot"].astype(str).str.lower() == "true"
    bot_shas = set(log[log["is_bot"]]["commit_sha"])
    commits = fd_group.drop_duplicates("commit_sha")
    return commits[~commits["commit_sha"].isin(bot_shas)]


def main():
    fd_files = glob.glob(os.path.join(ROOT, "per_repo", "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True)
    fd["commit_date"] = pd.to_datetime(fd["commit_date"])
    fd["month"] = fd["commit_date"].dt.strftime("%Y-%m")

    rows = []
    for repo, group in fd.groupby("repo"):
        human = get_human_doc_commits(repo, group)
        if human is None or human.empty:
            continue

        month_counts = defaultdict(int)
        for m in human["month"]:
            if m in MONTHS:
                month_counts[m] += 1
        counts_full = [month_counts.get(mk, 0) for mk in MONTHS]
        counts_h1 = [month_counts.get(mk, 0) for mk in MONTHS_H1]
        counts_h2 = [month_counts.get(mk, 0) for mk in MONTHS_H2]

        gap = longest_zero_run(counts_full)
        h1_commits = sum(counts_h1)
        h2_commits = sum(counts_h2)
        h1_ent = entropy_norm(counts_h1)
        h1_awr = sum(1 for c in counts_h1 if c > 0) / len(counts_h1)
        went_dormant_h2 = int(h2_commits == 0)
        decline_ratio = (h2_commits - h1_commits) / (h1_commits + 1)  # +1 smoothing

        rows.append({
            "repo": repo,
            "longest_dormancy_gap_months": gap,
            "h1_doc_commits": h1_commits,
            "h2_doc_commits": h2_commits,
            "h1_entropy": h1_ent,
            "h1_awr": h1_awr,
            "went_dormant_h2": went_dormant_h2,
            "h2_decline_ratio": decline_ratio,
        })

    df = pd.DataFrame(rows)
    arch = pd.read_csv(os.path.join(ROOT, "combined", "archetype_assignments.csv"))[["repo", "entropy_norm", "active_window_rate"]]
    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[["repo", "unique_contributors_for_metrics"]]
    owner = pd.read_csv(os.path.join(ROOT, "combined", "combined_doc_contributors.csv"))[["repo", "health_docs_touch_contributors", "health_docs_touch_bus50"]]
    rhythm = pd.read_csv(os.path.join(ROOT, "combined", "combined_doc_stability_metrics.csv"))[["repo", "health_file_commits"]]

    df = df.merge(arch, on="repo").merge(contrib, on="repo").merge(owner, on="repo").merge(rhythm, on="repo")
    df["log_contributors"] = np.log1p(df["unique_contributors_for_metrics"])
    df["log_doc_commits"] = np.log1p(df["health_file_commits"])
    df["participation_rate"] = df["health_docs_touch_contributors"] / df["unique_contributors_for_metrics"]
    df["log_bus50"] = np.log1p(df["health_docs_touch_bus50"])
    df.to_csv(os.path.join(OUT, "doc_inactivity_dataset.csv"), index=False)
    print(f"n={len(df)}")
    print(f"went_dormant_h2 rate: {df['went_dormant_h2'].mean():.2%}")
    print(df["longest_dormancy_gap_months"].describe())

    def partial_spearman(x, y, z):
        rx, ry, rz = rankdata(x), rankdata(y), rankdata(z)
        rxz = np.corrcoef(rx, rz)[0, 1]; ryz = np.corrcoef(ry, rz)[0, 1]; rxy = np.corrcoef(rx, ry)[0, 1]
        return (rxy - rxz * ryz) / np.sqrt((1 - rxz ** 2) * (1 - ryz ** 2))

    def battery(outcome, pred, sub, vol_col="log_doc_commits"):
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

    print("\n=== 1. Longest dormancy gap (contemporaneous, full window) ===")
    for pred in predictors:
        sub = df.dropna(subset=["longest_dormancy_gap_months", pred, "log_contributors", "log_doc_commits"])
        battery("longest_dormancy_gap_months", pred, sub)

    print("\n=== 2. LAGGED: h1 rhythm -> h2 decline ratio ===")
    for pred in ["h1_entropy", "h1_awr"]:
        sub = df.dropna(subset=["h2_decline_ratio", pred, "log_contributors"])
        sub = sub.copy()
        sub["log_h1_commits"] = np.log1p(sub["h1_doc_commits"])
        battery("h2_decline_ratio", pred, sub, vol_col="log_h1_commits")

    print("\n=== 3. LAGGED: h1 rhythm -> went fully dormant in h2 (logistic) ===")
    import statsmodels.api as sm
    for pred in ["h1_entropy", "h1_awr"]:
        sub = df.dropna(subset=["went_dormant_h2", pred, "log_contributors"])
        X = sm.add_constant(sub[["log_contributors", pred]])
        try:
            m = sm.Logit(sub["went_dormant_h2"], X).fit(disp=0)
            print(f"{pred:22s} coef={m.params[pred]:.4f} p={m.pvalues[pred]:.4f}")
        except Exception as e:
            print(f"{pred}: {e}")


if __name__ == "__main__":
    main()
