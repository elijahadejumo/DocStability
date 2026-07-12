#!/usr/bin/env python3
"""
Compute documentation bus factor (Bus-50) restricted to FIRST-HALF commits
only, to test whether the bus50 -> doc-contributor-loss relationship
survives proper temporal ordering (predictor computed before the outcome
period, breaking the circularity that killed the entropy/AWR version of
this same test).
"""
import glob
import os

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy.stats import rankdata

ROOT = "/Users/elijahadejumo/Documents/DocStability"
LOGS_DIR = os.path.join(ROOT, "full_commit_logs")
OUT = os.path.join(ROOT, "analysis_outputs")

SINCE = pd.Timestamp("2020-06-30")
UNTIL = pd.Timestamp("2025-06-29")
MID = SINCE + (UNTIL - SINCE) / 2


def bus_factor(counts_by_owner, threshold):
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

    rows = []
    for repo, group in fd.groupby("repo"):
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
        h1 = human[human["commit_date"] < MID]
        if h1.empty:
            continue

        owner_counts = h1.groupby("author_id").size().to_dict()
        b50 = bus_factor(owner_counts, 0.5)

        rows.append({"repo": repo, "h1_bus50": b50, "h1_n_doc_contributors": len(owner_counts)})

    df = pd.DataFrame(rows)
    print(f"n={len(df)}")
    print(df["h1_bus50"].describe())

    loss = pd.read_csv(os.path.join(OUT, "doc_contributor_loss_dataset.csv"))
    inact = pd.read_csv(os.path.join(OUT, "doc_inactivity_dataset.csv"))[["repo", "h1_doc_commits"]]
    merged = loss.merge(df, on="repo").merge(inact, on="repo")
    merged["log_h1_bus50"] = np.log1p(merged["h1_bus50"])
    merged["log_h1_commits"] = np.log1p(merged["h1_doc_commits"])
    merged.to_csv(os.path.join(OUT, "h1_bus_factor_dataset.csv"), index=False)

    def partial_spearman(x, y, z):
        rx, ry, rz = rankdata(x), rankdata(y), rankdata(z)
        rxz = np.corrcoef(rx, rz)[0, 1]; ryz = np.corrcoef(ry, rz)[0, 1]; rxy = np.corrcoef(rx, ry)[0, 1]
        return (rxy - rxz * ryz) / np.sqrt((1 - rxz ** 2) * (1 - ryz ** 2))

    def battery(outcome, pred, sub, vol_col="log_h1_commits"):
        sub = sub.dropna(subset=[outcome, pred, "log_contributors", vol_col])
        if len(sub) < 20:
            print(f"{pred:22s} insufficient data"); return
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

    print("\n=== H1-only bus50 -> H2 loss (properly time-ordered) ===")
    print("--- ALL established doc contributors ---")
    battery("doc_contributor_loss_rate_all", "log_h1_bus50", merged.copy())
    print("--- TOP-QUARTILE established (veterans) ---")
    battery("doc_contributor_loss_rate_top", "log_h1_bus50", merged.copy())


if __name__ == "__main__":
    main()
