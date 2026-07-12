#!/usr/bin/env python3
"""
Construct-matched retention test: instead of predicting GENERAL commit
retention (any file type) from DOCUMENTATION-specific rhythm (a domain
mismatch that every prior test suffered from), predict DOCUMENTATION-
newcomer retention -- do first-time documentation contributors return to
touch documentation again -- from documentation rhythm/participation. Same
domain on both sides of the test.
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


def main():
    fd_files = glob.glob(os.path.join(ROOT, "per_repo", "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True)
    fd["commit_date"] = pd.to_datetime(fd["commit_date"])
    fd["month"] = fd["commit_date"].dt.to_period("M")

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

        # first-ever DOC touch per author (within this repo's doc history, not
        # bounded to the 5yr window here since we need to know if their FIRST
        # doc touch overall was inside the window)
        first_doc_touch = human.groupby("author_id")["commit_date"].min()
        doc_newcomers = first_doc_touch[(first_doc_touch >= SINCE) & (first_doc_touch <= UNTIL)].index
        if len(doc_newcomers) < 3:
            continue

        newcomer_activity = human[human["author_id"].isin(doc_newcomers)]
        months_touched = newcomer_activity.groupby("author_id")["month"].nunique()
        retained = (months_touched >= 2).sum()

        rows.append({
            "repo": repo,
            "n_doc_newcomers": len(doc_newcomers),
            "n_doc_newcomers_retained": int(retained),
            "doc_newcomer_retention_rate": retained / len(doc_newcomers),
        })

    df = pd.DataFrame(rows)
    print(f"n = {len(df)} repos with doc newcomers")
    print(df["doc_newcomer_retention_rate"].describe())

    arch = pd.read_csv(os.path.join(ROOT, "combined", "archetype_assignments.csv"))[["repo", "entropy_norm", "active_window_rate"]]
    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[["repo", "unique_contributors_for_metrics"]]
    owner = pd.read_csv(os.path.join(ROOT, "combined", "combined_doc_contributors.csv"))[["repo", "health_docs_touch_contributors", "health_docs_touch_bus50"]]
    rhythm = pd.read_csv(os.path.join(ROOT, "combined", "combined_doc_stability_metrics.csv"))[["repo", "health_file_commits"]]

    model_df = df.merge(arch, on="repo").merge(contrib, on="repo").merge(owner, on="repo").merge(rhythm, on="repo")
    model_df["log_contributors"] = np.log1p(model_df["unique_contributors_for_metrics"])
    model_df["log_doc_commits"] = np.log1p(model_df["health_file_commits"])
    model_df["participation_rate"] = model_df["health_docs_touch_contributors"] / model_df["unique_contributors_for_metrics"]
    model_df["log_bus50"] = np.log1p(model_df["health_docs_touch_bus50"])
    model_df.to_csv(os.path.join(OUT, "doc_newcomer_retention_dataset.csv"), index=False)

    def partial_spearman(x, y, z):
        rx, ry, rz = rankdata(x), rankdata(y), rankdata(z)
        rxz = np.corrcoef(rx, rz)[0, 1]; ryz = np.corrcoef(ry, rz)[0, 1]; rxy = np.corrcoef(rx, ry)[0, 1]
        return (rxy - rxz * ryz) / np.sqrt((1 - rxz ** 2) * (1 - ryz ** 2))

    predictors = ["entropy_norm", "active_window_rate", "participation_rate", "log_bus50"]
    outcome = "doc_newcomer_retention_rate"

    print(f"\n{'predictor':22s} {'p(size)':>9s} {'p(vol)':>9s} {'p(HC3)':>9s} {'p(no-outlier)':>13s} {'partial_rho':>11s}")
    for pred in predictors:
        sub = model_df.dropna(subset=[outcome, pred, "log_contributors", "log_doc_commits"]).copy()
        base = smf.ols(f"{outcome} ~ log_contributors", data=sub).fit()
        m = smf.ols(f"{outcome} ~ log_contributors + {pred}", data=sub).fit()
        ft = m.compare_f_test(base)

        base_v = smf.ols(f"{outcome} ~ log_contributors + log_doc_commits", data=sub).fit()
        m_v = smf.ols(f"{outcome} ~ log_contributors + log_doc_commits + {pred}", data=sub).fit()
        ft_v = m_v.compare_f_test(base_v)

        m_hc3 = smf.ols(f"{outcome} ~ log_contributors + {pred}", data=sub).fit(cov_type="HC3")

        infl = m.get_influence()
        cooks_d = infl.cooks_distance[0]
        drop_idx = pd.Series(cooks_d, index=sub.index).sort_values(ascending=False).head(3).index
        sub_r = sub.drop(drop_idx)
        m_r = smf.ols(f"{outcome} ~ log_contributors + {pred}", data=sub_r).fit()

        pr = partial_spearman(sub[pred], sub[outcome], sub["log_contributors"])

        flag = " <---" if (ft[1] < 0.05 and ft_v[1] < 0.05 and m_hc3.pvalues[pred] < 0.05 and m_r.pvalues[pred] < 0.05) else ""
        print(f"{pred:22s} {ft[1]:9.4f} {ft_v[1]:9.4f} {m_hc3.pvalues[pred]:9.4f} {m_r.pvalues[pred]:13.4f} {pr:11.3f}{flag}")


if __name__ == "__main__":
    main()
