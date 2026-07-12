#!/usr/bin/env python3
"""
Test stricter "became core" outcome definitions instead of the loose
"returned once" bar, to see whether documentation rhythm shows a real
effect on sustained/substantial newcomer contribution rather than just
any return visit.
"""
import glob
import os

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

ROOT = "/Users/elijahadejumo/Documents/DocStability"
LOGS_DIR = os.path.join(ROOT, "full_commit_logs")

SINCE = pd.Timestamp("2020-06-30")
UNTIL = pd.Timestamp("2025-06-29")


def classify_repo(repo):
    path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, usecols=["author_id", "author_date", "is_bot"])
    df["is_bot"] = df["is_bot"].astype(str).str.lower() == "true"
    df = df[~df["is_bot"]]
    if df.empty:
        return None
    df["author_date"] = pd.to_datetime(df["author_date"], errors="coerce")
    df = df.dropna(subset=["author_date"])
    df["month"] = df["author_date"].dt.to_period("M")

    first_commit = df.groupby("author_id")["author_date"].min()
    newcomers = first_commit[(first_commit >= SINCE) & (first_commit <= UNTIL)].index
    if len(newcomers) == 0:
        return None

    newcomer_df = df[df["author_id"].isin(newcomers)]
    per_author = newcomer_df.groupby("author_id").agg(
        n_commits=("author_date", "size"),
        n_months=("month", "nunique"),
        first=("author_date", "min"),
        last=("author_date", "max"),
    )
    per_author["span_days"] = (per_author["last"] - per_author["first"]).dt.days

    n = len(newcomers)
    return {
        "repo": repo,
        "n_newcomers": n,
        "rate_2plus_months": (per_author["n_months"] >= 2).mean(),
        "rate_3plus_months": (per_author["n_months"] >= 3).mean(),
        "rate_5plus_commits": (per_author["n_commits"] >= 5).mean(),
        "rate_10plus_commits": (per_author["n_commits"] >= 10).mean(),
        "rate_90plus_days_span": (per_author["span_days"] >= 90).mean(),
        "rate_180plus_days_span": (per_author["span_days"] >= 180).mean(),
    }


def main():
    logs = glob.glob(os.path.join(LOGS_DIR, "*_full_commit_log.csv"))
    repos = sorted(os.path.basename(f).replace("_full_commit_log.csv", "") for f in logs)
    repos = [r for r in repos if r != "combined"]

    rows = []
    for repo in repos:
        r = classify_repo(repo)
        if r is not None:
            rows.append(r)
    df = pd.DataFrame(rows)
    print(f"Processed {len(df)} repos with newcomers")

    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[
        ["repo", "unique_contributors_for_metrics"]
    ]
    arch = pd.read_csv(os.path.join(ROOT, "combined", "archetype_assignments.csv"))[
        ["repo", "entropy_norm", "active_window_rate"]
    ]
    model_df = df.merge(contrib, on="repo").merge(arch, on="repo")
    model_df["log_contributors"] = np.log1p(model_df["unique_contributors_for_metrics"])

    outcomes = ["rate_2plus_months", "rate_3plus_months", "rate_5plus_commits",
                "rate_10plus_commits", "rate_90plus_days_span", "rate_180plus_days_span"]

    print(f"\n{'outcome':25s} {'baseline_R2':>12s} {'+entropy_p':>12s} {'+AWR_p':>10s} {'n':>5s}")
    for outcome in outcomes:
        sub = model_df.dropna(subset=[outcome])
        base = smf.ols(f"{outcome} ~ log_contributors", data=sub).fit()
        m_ent = smf.ols(f"{outcome} ~ log_contributors + entropy_norm", data=sub).fit()
        m_awr = smf.ols(f"{outcome} ~ log_contributors + active_window_rate", data=sub).fit()
        print(f"{outcome:25s} {base.rsquared:12.3f} {m_ent.pvalues['entropy_norm']:12.4f} "
              f"{m_awr.pvalues['active_window_rate']:10.4f} {len(sub):5d}")

    model_df.to_csv(os.path.join(ROOT, "analysis_outputs", "rq4_core_variants.csv"), index=False)


if __name__ == "__main__":
    main()
