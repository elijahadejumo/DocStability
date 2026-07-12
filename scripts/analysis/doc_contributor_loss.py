#!/usr/bin/env python3
"""
Construct-matched contributor-LOSS test (complement to the newcomer-
retention finding): among established documentation contributors (first
doc-touch predates the 5yr window), what fraction go dormant (no doc
commits in the final 12 months)? Tested against entropy, AWR, participation
rate, and bus factor -- doc-specific throughout, unlike the earlier general
"maintainer turnover" test (any commit type) which was null.

Two variants: all established doc contributors, and top-quartile-by-volume
established doc contributors (the "veteran" definition used earlier).
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
LAST12_START = UNTIL - pd.Timedelta(days=365)
# file_details.csv only contains commits INSIDE the window (no pre-window doc
# history available), so "established" is redefined as: first doc-touch in
# the window's FIRST HALF, "departed" = no doc commits in the SECOND HALF.
MID = SINCE + (UNTIL - SINCE) / 2


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
        if human.empty:
            continue

        first_doc_touch = human.groupby("author_id")["commit_date"].min()
        last_doc_touch = human.groupby("author_id")["commit_date"].max()
        doc_commit_count = human.groupby("author_id").size()

        # "established" = first doc touch in the first half of the window
        established = first_doc_touch[first_doc_touch < MID].index
        if len(established) < 3:
            continue

        # variant 1: all established doc contributors -- departed = no doc
        # commit in the second half of the window at all
        departed_all = (last_doc_touch[established] < MID).mean()

        # variant 2: top-quartile-by-volume established doc contributors ("veterans")
        est_counts = doc_commit_count[established]
        top_est = est_counts[est_counts >= est_counts.quantile(0.75)].index
        departed_top = (last_doc_touch[top_est] < MID).mean() if len(top_est) >= 3 else None

        rows.append({
            "repo": repo,
            "n_established_doc_contributors": len(established),
            "doc_contributor_loss_rate_all": departed_all,
            "n_top_established": len(top_est),
            "doc_contributor_loss_rate_top": departed_top,
        })

    df = pd.DataFrame(rows)
    print(f"n = {len(df)} repos with established doc contributors")
    print(df[["doc_contributor_loss_rate_all", "doc_contributor_loss_rate_top"]].describe())

    arch = pd.read_csv(os.path.join(ROOT, "combined", "archetype_assignments.csv"))[["repo", "entropy_norm", "active_window_rate"]]
    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[["repo", "unique_contributors_for_metrics"]]
    owner = pd.read_csv(os.path.join(ROOT, "combined", "combined_doc_contributors.csv"))[["repo", "health_docs_touch_contributors", "health_docs_touch_bus50"]]
    rhythm = pd.read_csv(os.path.join(ROOT, "combined", "combined_doc_stability_metrics.csv"))[["repo", "health_file_commits"]]

    df = df.merge(arch, on="repo").merge(contrib, on="repo").merge(owner, on="repo").merge(rhythm, on="repo")
    df["log_contributors"] = np.log1p(df["unique_contributors_for_metrics"])
    df["log_doc_commits"] = np.log1p(df["health_file_commits"])
    df["participation_rate"] = df["health_docs_touch_contributors"] / df["unique_contributors_for_metrics"]
    df["log_bus50"] = np.log1p(df["health_docs_touch_bus50"])
    df.to_csv(os.path.join(OUT, "doc_contributor_loss_dataset.csv"), index=False)

    def partial_spearman(x, y, z):
        rx, ry, rz = rankdata(x), rankdata(y), rankdata(z)
        rxz = np.corrcoef(rx, rz)[0, 1]; ryz = np.corrcoef(ry, rz)[0, 1]; rxy = np.corrcoef(rx, ry)[0, 1]
        return (rxy - rxz * ryz) / np.sqrt((1 - rxz ** 2) * (1 - ryz ** 2))

    def battery(outcome, pred, sub, vol_col="log_doc_commits"):
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

    predictors = ["entropy_norm", "active_window_rate", "participation_rate", "log_bus50"]

    print("\n=== ALL established doc contributors: loss rate ===")
    for pred in predictors:
        battery("doc_contributor_loss_rate_all", pred, df.copy())

    print("\n=== TOP-QUARTILE established doc contributors ('veterans'): loss rate ===")
    for pred in predictors:
        battery("doc_contributor_loss_rate_top", pred, df.copy())


if __name__ == "__main__":
    main()
