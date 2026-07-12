#!/usr/bin/env python3
"""
Four "outside the box" tests for whether documentation rhythm (entropy/AWR)
carries independent value, beyond the cross-sectional "does it predict
outcome X" template already exhausted:

1. Veteran/maintainer retention (not newcomer): do EXISTING top contributors
   (active before the window) stay active longer in high-rhythm repos?
2. Contributor-count STABILITY (variance), not level: does rhythm predict a
   steadier monthly active-contributor count, controlling for its mean?
3. Lagged design: does rhythm in the FIRST half of the window predict
   contributor-count CHANGE in the second half, controlling for first-half
   size? (addresses reverse-causality: size->rhythm is the null-consistent
   story; this tests rhythm->future change instead)
4. Human/bot substitution: does poor human doc rhythm correlate with a
   HIGHER bot share of documentation commits (compensating automation)?
"""
import glob
import os

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats

ROOT = "/Users/elijahadejumo/Documents/DocStability"
LOGS_DIR = os.path.join(ROOT, "full_commit_logs")

SINCE = pd.Timestamp("2020-06-30")
UNTIL = pd.Timestamp("2025-06-29")
MID = SINCE + (UNTIL - SINCE) / 2


def load_all():
    logs = glob.glob(os.path.join(LOGS_DIR, "*_full_commit_log.csv"))
    repos = sorted(os.path.basename(f).replace("_full_commit_log.csv", "") for f in logs)
    return [r for r in repos if r != "combined"]


def veteran_retention(repo):
    path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
    df = pd.read_csv(path, usecols=["author_id", "author_date", "is_bot"])
    df["is_bot"] = df["is_bot"].astype(str).str.lower() == "true"
    df = df[~df["is_bot"]]
    df["author_date"] = pd.to_datetime(df["author_date"], errors="coerce")
    df = df.dropna(subset=["author_date"])

    first_commit = df.groupby("author_id")["author_date"].min()
    last_commit = df.groupby("author_id")["author_date"].max()
    commit_count = df.groupby("author_id").size()

    # veterans: active before the window, AND substantial (top quartile by commits)
    veterans = first_commit[first_commit < SINCE].index
    if len(veterans) < 5:
        return None
    vet_commits = commit_count[veterans]
    top_vets = vet_commits[vet_commits >= vet_commits.quantile(0.75)].index
    if len(top_vets) < 3:
        return None

    # "still active" = has a commit in the last 6 months of the window
    cutoff = UNTIL - pd.Timedelta(days=180)
    still_active = (last_commit[top_vets] >= cutoff).mean()
    return {"repo": repo, "n_top_veterans": len(top_vets), "veteran_still_active_rate": still_active}


def contributor_stability(repo):
    path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
    df = pd.read_csv(path, usecols=["author_id", "author_date", "is_bot"])
    df["is_bot"] = df["is_bot"].astype(str).str.lower() == "true"
    df = df[~df["is_bot"]]
    df["author_date"] = pd.to_datetime(df["author_date"], errors="coerce")
    df = df.dropna(subset=["author_date"])
    df = df[(df["author_date"] >= SINCE) & (df["author_date"] <= UNTIL)]
    if df.empty:
        return None
    df["month"] = df["author_date"].dt.to_period("M")
    monthly_active = df.groupby("month")["author_id"].nunique()
    if monthly_active.mean() == 0:
        return None
    cv = monthly_active.std() / monthly_active.mean()
    return {"repo": repo, "monthly_contributor_mean": monthly_active.mean(), "monthly_contributor_cv": cv}


def lagged_design(repo):
    path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
    df = pd.read_csv(path, usecols=["author_id", "author_date", "is_bot"])
    df["is_bot"] = df["is_bot"].astype(str).str.lower() == "true"
    df = df[~df["is_bot"]]
    df["author_date"] = pd.to_datetime(df["author_date"], errors="coerce")
    df = df.dropna(subset=["author_date"])
    df = df[(df["author_date"] >= SINCE) & (df["author_date"] <= UNTIL)]
    if df.empty:
        return None
    h1 = df[df["author_date"] < MID]
    h2 = df[df["author_date"] >= MID]
    h1_contributors = h1["author_id"].nunique()
    h2_contributors = h2["author_id"].nunique()
    if h1_contributors == 0:
        return None
    growth = (h2_contributors - h1_contributors) / h1_contributors
    return {"repo": repo, "h1_contributors": h1_contributors, "h2_contributors": h2_contributors,
            "contributor_growth_h2": growth}


def human_bot_substitution():
    fd_files = glob.glob(os.path.join(ROOT, "per_repo", "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True).drop_duplicates(["repo", "commit_sha"])
    rows = []
    for repo, group in fd.groupby("repo"):
        log_path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
        if not os.path.exists(log_path):
            continue
        log = pd.read_csv(log_path, usecols=["commit_sha", "is_bot"]).drop_duplicates("commit_sha")
        log["is_bot"] = log["is_bot"].astype(str).str.lower() == "true"
        merged = group.merge(log, on="commit_sha", how="left")
        merged["is_bot"] = merged["is_bot"].fillna(False)
        rows.append({"repo": repo, "bot_doc_share": merged["is_bot"].mean()})
    return pd.DataFrame(rows)


def main():
    repos = load_all()
    print(f"{len(repos)} repos\n")

    arch = pd.read_csv(os.path.join(ROOT, "combined", "archetype_assignments.csv"))[
        ["repo", "entropy_norm", "active_window_rate"]
    ]
    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[
        ["repo", "unique_contributors_for_metrics"]
    ]

    # ---- 1. Veteran retention ----
    print("=" * 70)
    print("1. VETERAN/MAINTAINER RETENTION")
    print("=" * 70)
    vet_rows = [r for r in (veteran_retention(r) for r in repos) if r is not None]
    vet_df = pd.DataFrame(vet_rows).merge(arch, on="repo").merge(contrib, on="repo")
    vet_df["log_contributors"] = np.log1p(vet_df["unique_contributors_for_metrics"])
    print(f"n={len(vet_df)}")
    base = smf.ols("veteran_still_active_rate ~ log_contributors", data=vet_df).fit()
    print(f"baseline R2={base.rsquared:.3f}")
    for var in ["entropy_norm", "active_window_rate"]:
        m = smf.ols(f"veteran_still_active_rate ~ log_contributors + {var}", data=vet_df).fit()
        ft = m.compare_f_test(base)
        print(f"  + {var}: coef={m.params[var]:.4f}, p={m.pvalues[var]:.4f}, F-test p={ft[1]:.4f}")
    vet_df.to_csv(os.path.join(ROOT, "analysis_outputs", "outside_box_veteran_retention.csv"), index=False)

    # ---- 2. Contributor stability ----
    print()
    print("=" * 70)
    print("2. CONTRIBUTOR-COUNT STABILITY (lower CV = steadier)")
    print("=" * 70)
    stab_rows = [r for r in (contributor_stability(r) for r in repos) if r is not None]
    stab_df = pd.DataFrame(stab_rows).merge(arch, on="repo").merge(contrib, on="repo")
    stab_df["log_contributors"] = np.log1p(stab_df["unique_contributors_for_metrics"])
    print(f"n={len(stab_df)}")
    base = smf.ols("monthly_contributor_cv ~ log_contributors", data=stab_df).fit()
    print(f"baseline R2={base.rsquared:.3f}")
    for var in ["entropy_norm", "active_window_rate"]:
        m = smf.ols(f"monthly_contributor_cv ~ log_contributors + {var}", data=stab_df).fit()
        ft = m.compare_f_test(base)
        print(f"  + {var}: coef={m.params[var]:.4f}, p={m.pvalues[var]:.4f}, F-test p={ft[1]:.4f}")
    stab_df.to_csv(os.path.join(ROOT, "analysis_outputs", "outside_box_contributor_stability.csv"), index=False)

    # ---- 3. Lagged design ----
    print()
    print("=" * 70)
    print("3. LAGGED: first-half rhythm -> second-half contributor growth")
    print("=" * 70)
    lag_rows = [r for r in (lagged_design(r) for r in repos) if r is not None]
    lag_df = pd.DataFrame(lag_rows).merge(arch, on="repo").merge(contrib, on="repo")
    lag_df["log_h1_contributors"] = np.log1p(lag_df["h1_contributors"])
    print(f"n={len(lag_df)}")
    base = smf.ols("contributor_growth_h2 ~ log_h1_contributors", data=lag_df).fit()
    print(f"baseline R2={base.rsquared:.3f}")
    for var in ["entropy_norm", "active_window_rate"]:
        m = smf.ols(f"contributor_growth_h2 ~ log_h1_contributors + {var}", data=lag_df).fit()
        ft = m.compare_f_test(base)
        print(f"  + {var}: coef={m.params[var]:.4f}, p={m.pvalues[var]:.4f}, F-test p={ft[1]:.4f}")
    lag_df.to_csv(os.path.join(ROOT, "analysis_outputs", "outside_box_lagged_growth.csv"), index=False)

    # ---- 4. Human/bot substitution ----
    print()
    print("=" * 70)
    print("4. HUMAN/BOT SUBSTITUTION: does low rhythm correlate with more bot doc activity?")
    print("=" * 70)
    bot_df = human_bot_substitution().merge(arch, on="repo").merge(contrib, on="repo")
    bot_df["log_contributors"] = np.log1p(bot_df["unique_contributors_for_metrics"])
    print(f"n={len(bot_df)}")
    base = smf.ols("bot_doc_share ~ log_contributors", data=bot_df).fit()
    print(f"baseline R2={base.rsquared:.3f}")
    for var in ["entropy_norm", "active_window_rate"]:
        m = smf.ols(f"bot_doc_share ~ log_contributors + {var}", data=bot_df).fit()
        ft = m.compare_f_test(base)
        print(f"  + {var}: coef={m.params[var]:.4f}, p={m.pvalues[var]:.4f}, F-test p={ft[1]:.4f}")
    bot_df.to_csv(os.path.join(ROOT, "analysis_outputs", "outside_box_bot_substitution.csv"), index=False)


if __name__ == "__main__":
    main()
