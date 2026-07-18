#!/usr/bin/env python3
"""
Does onboarding/process documentation specifically (README, CONTRIBUTING,
BUILDING, COMMIT_CONVENTIONS, PULL_REQUEST_TEMPLATE, ISSUE_TEMPLATE -- the
broader 4-category "onboarding_process" bucket, not just README+CONTRIBUTING)
link to newcomer arrival and first-commit behavior?

Two genuinely new tests:
1. onboarding_process entropy/AWR/participation/bus-factor vs. raw newcomer
   COUNT (volume of first-time contributors), not yet tested at this scope.
2. onboarding_process bus-factor vs. doc-specific newcomer RETENTION -- the
   exact construct where all-docs bus factor was validated. Testing whether
   narrowing to just onboarding files strengthens, weakens, or reproduces it.
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

ONBOARDING_STEMS = {"readme", "contributing", "building", "commit_conventions",
                     "pull_request_template", "issue_template"}


def classify(path):
    basename = path.replace("\\", "/").split("/")[-1].lower()
    stem = basename.split(".")[0]
    return stem in ONBOARDING_STEMS


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


def bus_factor(counts_by_owner, threshold=0.5):
    total = sum(counts_by_owner.values())
    if total == 0:
        return None
    counts_desc = sorted(counts_by_owner.values(), reverse=True)
    cum = 0
    for i, c in enumerate(counts_desc, 1):
        cum += c
        if cum / total >= threshold:
            return i
    return len(counts_desc)


def main():
    fd_files = glob.glob(os.path.join(ROOT, "per_repo", "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True)
    fd["commit_date"] = pd.to_datetime(fd["commit_date"])
    fd["month"] = fd["commit_date"].dt.strftime("%Y-%m")
    fd["is_onboarding"] = fd["health_file"].apply(classify)
    onb = fd[fd["is_onboarding"]].copy()
    print(f"Onboarding rows: {len(onb)} ({len(onb)/len(fd):.1%} of all doc-touch rows)")
    print(f"Repos with onboarding-doc activity: {onb['repo'].nunique()}")

    rows = []
    for repo, group in onb.groupby("repo"):
        log_path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
        if not os.path.exists(log_path):
            continue
        log = pd.read_csv(log_path, usecols=["commit_sha", "author_id", "is_bot"]).drop_duplicates("commit_sha")
        log["is_bot"] = log["is_bot"].astype(str).str.lower() == "true"
        author_map = log.set_index("commit_sha")[["author_id", "is_bot"]].to_dict("index")

        commits = group.drop_duplicates("commit_sha").copy()
        commits["author_id"] = commits["commit_sha"].map(lambda s: author_map.get(s, {}).get("author_id"))
        commits["is_bot"] = commits["commit_sha"].map(lambda s: author_map.get(s, {}).get("is_bot", False))
        human = commits[~commits["is_bot"]].dropna(subset=["author_id"])
        if human.empty:
            continue

        # rhythm
        month_counts = defaultdict(int)
        for sha, month in human[["commit_sha", "month"]].drop_duplicates().values:
            month_counts[month] += 1
        counts = [month_counts.get(mk, 0) for mk in MONTHS]
        awr = sum(1 for c in counts if c > 0) / len(counts)
        ent = entropy_norm(counts)

        # bus factor (contemporaneous, full window)
        owner_counts = human.groupby("author_id").size().to_dict()
        b50 = bus_factor(owner_counts)

        # newcomers: first-ever onboarding-doc touch inside the window
        first_touch = human.groupby("author_id")["commit_date"].min()
        newcomers = first_touch[(first_touch >= SINCE_TS) & (first_touch <= UNTIL_TS)].index
        n_newcomers = len(newcomers)

        # doc-specific newcomer retention (>=2 distinct months)
        retention_rate = None
        if n_newcomers >= 3:
            newcomer_df = human[human["author_id"].isin(newcomers)]
            months_touched = newcomer_df.groupby("author_id")["month"].nunique()
            retention_rate = (months_touched >= 2).mean()

        rows.append({
            "repo": repo, "onb_n_commits": len(human), "onb_entropy": ent, "onb_awr": awr,
            "onb_bus50": b50, "onb_n_contributors": human["author_id"].nunique(),
            "n_newcomers": n_newcomers, "onb_newcomer_retention_rate": retention_rate,
        })

    df = pd.DataFrame(rows)
    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[["repo", "unique_contributors_for_metrics"]]
    df = df.merge(contrib, on="repo", how="left")
    df["log_contributors"] = np.log1p(df["unique_contributors_for_metrics"])
    df["log_onb_commits"] = np.log1p(df["onb_n_commits"])
    df["log_onb_bus50"] = np.log1p(df["onb_bus50"])
    df["onb_participation_rate"] = df["onb_n_contributors"] / df["unique_contributors_for_metrics"]
    df["log_newcomers"] = np.log1p(df["n_newcomers"])
    df.to_csv(os.path.join(OUT, "onboarding_newcomers_dataset.csv"), index=False)
    print(f"\nn={len(df)}")
    print(df[["n_newcomers", "onb_newcomer_retention_rate"]].describe())

    def partial_spearman(x, y, z):
        rx, ry, rz = rankdata(x), rankdata(y), rankdata(z)
        rxz = np.corrcoef(rx, rz)[0, 1]; ryz = np.corrcoef(ry, rz)[0, 1]; rxy = np.corrcoef(rx, ry)[0, 1]
        denom = np.sqrt((1 - rxz ** 2) * (1 - ryz ** 2))
        return (rxy - rxz * ryz) / denom if denom > 0 else np.nan

    def battery(outcome, pred, sub, vol_col="log_onb_commits"):
        sub = sub.dropna(subset=[outcome, pred, "log_contributors", vol_col])
        if len(sub) < 20:
            print(f"    {pred:20s} insufficient data (n={len(sub)})"); return
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
        print(f"    {pred:20s} n={len(sub):3d} p(size)={ft[1]:.4f} p(vol)={ft_v[1]:.4f} p(HC3)={m_hc3.pvalues[pred]:.4f} "
              f"p(no-outlier)={m_r.pvalues[pred]:.4f} rho={pr:.3f}{flag}")

    print("\n=== 1. Onboarding-doc rhythm/participation/bus-factor vs NEWCOMER COUNT ===")
    for pred in ["onb_entropy", "onb_awr", "onb_participation_rate", "log_onb_bus50"]:
        battery("log_newcomers", pred, df.copy())

    print("\n=== 2. Onboarding-doc rhythm/participation/bus-factor vs DOC-SPECIFIC NEWCOMER RETENTION ===")
    for pred in ["onb_entropy", "onb_awr", "onb_participation_rate", "log_onb_bus50"]:
        battery("onb_newcomer_retention_rate", pred, df.copy())

    print("\n=== 3. Same, restricted to repos with >=10 onboarding-doc newcomers (avoid small-n noise) ===")
    sub10 = df[df["n_newcomers"] >= 10]
    for pred in ["onb_entropy", "onb_awr", "onb_participation_rate", "log_onb_bus50"]:
        battery("onb_newcomer_retention_rate", pred, sub10.copy())


if __name__ == "__main__":
    main()
