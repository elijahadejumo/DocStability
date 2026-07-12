#!/usr/bin/env python3
"""
Sharpest possible test: restrict to README + CONTRIBUTING ONLY (the two
artifacts the onboarding literature -- Steinmacher et al., Gaughan et al.,
both already cited in the paper -- specifically identifies as onboarding-
critical, as opposed to governance/security/roadmap docs which serve a
different purpose and may dilute the "living" bucket's signal).

Tests entropy/AWR/participation of README+CONTRIBUTING activity specifically
against contributor-count outcomes: retention rate, contributor growth,
raw newcomer count.
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


def classify(path):
    basename = path.replace("\\", "/").split("/")[-1].lower()
    stem = basename.split(".")[0]
    return stem in {"readme", "contributing"}


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
    fd["is_rc"] = fd["health_file"].apply(classify)
    rc = fd[fd["is_rc"]].copy()

    print(f"README/CONTRIBUTING rows: {len(rc)} ({len(rc)/len(fd):.1%} of all doc-touch rows)")
    print(f"Repos with README/CONTRIBUTING activity: {rc['repo'].nunique()}")

    rows = []
    for repo, group in rc.groupby("repo"):
        log_path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
        author_map = {}
        if os.path.exists(log_path):
            log = pd.read_csv(log_path, usecols=["commit_sha", "author_id", "is_bot"]).drop_duplicates("commit_sha")
            log["is_bot"] = log["is_bot"].astype(str).str.lower() == "true"
            author_map = log.set_index("commit_sha")[["author_id", "is_bot"]].to_dict("index")

        commits = group.drop_duplicates("commit_sha").copy()
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
            "repo": repo,
            "rc_n_commits_human": len(human),
            "rc_entropy_norm": ent,
            "rc_awr": awr,
            "rc_n_contributors": human["author_id"].nunique(),
        })

    df = pd.DataFrame(rows)
    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[
        ["repo", "unique_contributors_for_metrics"]
    ]
    df = df.merge(contrib, on="repo", how="left")
    df["rc_participation_rate"] = df["rc_n_contributors"] / df["unique_contributors_for_metrics"]
    df["log_contributors"] = np.log1p(df["unique_contributors_for_metrics"])
    df["log_rc_commits"] = np.log1p(df["rc_n_commits_human"])

    # merge outcomes: retention, growth, raw newcomer count
    rq4 = pd.read_csv(os.path.join(OUT, "rq4_model_dataset.csv"))[["repo", "retention_rate", "n_newcomers"]]
    battery = pd.read_csv(os.path.join(OUT, "final_battery_dataset.csv"))[["repo", "contributor_growth_y1_y5", "y1_contributors", "y5_contributors"]]
    df = df.merge(rq4, on="repo", how="left").merge(battery, on="repo", how="left")
    df["log_newcomers"] = np.log1p(df["n_newcomers"])

    df.to_csv(os.path.join(OUT, "readme_contributing_dataset.csv"), index=False)
    print(f"\nSaved: n={len(df)}")

    def partial_spearman(x, y, z):
        rx, ry, rz = rankdata(x), rankdata(y), rankdata(z)
        rxz = np.corrcoef(rx, rz)[0, 1]; ryz = np.corrcoef(ry, rz)[0, 1]; rxy = np.corrcoef(rx, ry)[0, 1]
        return (rxy - rxz * ryz) / np.sqrt((1 - rxz ** 2) * (1 - ryz ** 2))

    predictors = ["rc_entropy_norm", "rc_awr", "rc_participation_rate"]
    outcomes = ["retention_rate", "contributor_growth_y1_y5", "log_newcomers"]

    print(f"\n{'outcome':28s} {'predictor':22s} {'p(size)':>9s} {'p(vol)':>9s} {'p(HC3)':>9s} {'p(no-outlier)':>13s} {'partial_rho':>11s}")
    for outcome in outcomes:
        for pred in predictors:
            sub = df.dropna(subset=[outcome, pred, "log_contributors", "log_rc_commits"]).copy()
            if len(sub) < 20:
                continue
            base = smf.ols(f"{outcome} ~ log_contributors", data=sub).fit()
            m = smf.ols(f"{outcome} ~ log_contributors + {pred}", data=sub).fit()
            ft = m.compare_f_test(base)

            base_v = smf.ols(f"{outcome} ~ log_contributors + log_rc_commits", data=sub).fit()
            m_v = smf.ols(f"{outcome} ~ log_contributors + log_rc_commits + {pred}", data=sub).fit()
            ft_v = m_v.compare_f_test(base_v)

            m_hc3 = smf.ols(f"{outcome} ~ log_contributors + {pred}", data=sub).fit(cov_type="HC3")

            infl = m.get_influence()
            cooks_d = infl.cooks_distance[0]
            drop_idx = pd.Series(cooks_d, index=sub.index).sort_values(ascending=False).head(3).index
            sub_r = sub.drop(drop_idx)
            m_r = smf.ols(f"{outcome} ~ log_contributors + {pred}", data=sub_r).fit()

            pr = partial_spearman(sub[pred], sub[outcome], sub["log_contributors"])

            flag = " <---" if (ft[1] < 0.05 and ft_v[1] < 0.05 and m_hc3.pvalues[pred] < 0.05 and m_r.pvalues[pred] < 0.05) else ""
            print(f"{outcome:28s} {pred:22s} {ft[1]:9.4f} {ft_v[1]:9.4f} {m_hc3.pvalues[pred]:9.4f} {m_r.pvalues[pred]:13.4f} {pr:11.3f}{flag}")
        print()


if __name__ == "__main__":
    main()
