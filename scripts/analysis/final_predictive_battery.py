#!/usr/bin/env python3
"""
Final predictive-validity battery: entropy_norm, active_window_rate,
log(bus50), and participation_rate (4 continuous predictors, archetype
broken back into its raw components) against 4 outcomes computable from
git history:
  A. Year-over-year contributor growth (year1 -> year5)
  B. Maintainer/veteran turnover (established contributors going inactive
     in the final 12 months of the window)
  C. Documentation staleness (median days-since-last-touch across a repo's
     health files, as of window end)
  D. Repository activity trend (slope of monthly total-commit count,
     normalized by mean level -- an inactivity-risk proxy)

Issue time-to-first-response (outcome E) is handled separately via the
GitHub API (fetch_first_response.py) since it needs new API data.
"""
import glob
import os

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

ROOT = "/Users/elijahadejumo/Documents/DocStability"
LOGS_DIR = os.path.join(ROOT, "full_commit_logs")
OUT = os.path.join(ROOT, "analysis_outputs")

SINCE = pd.Timestamp("2020-06-30")
UNTIL = pd.Timestamp("2025-06-29")
YEAR1_END = SINCE + pd.Timedelta(days=365)
YEAR5_START = UNTIL - pd.Timedelta(days=365)
LAST12_START = UNTIL - pd.Timedelta(days=365)


def load_repos():
    logs = glob.glob(os.path.join(LOGS_DIR, "*_full_commit_log.csv"))
    repos = sorted(os.path.basename(f).replace("_full_commit_log.csv", "") for f in logs)
    return [r for r in repos if r != "combined"]


def outcomes_from_commits(repo):
    path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
    df = pd.read_csv(path, usecols=["author_id", "author_date", "is_bot"])
    df["is_bot"] = df["is_bot"].astype(str).str.lower() == "true"
    df = df[~df["is_bot"]]
    df["author_date"] = pd.to_datetime(df["author_date"], errors="coerce")
    df = df.dropna(subset=["author_date"])
    win = df[(df["author_date"] >= SINCE) & (df["author_date"] <= UNTIL)]
    if win.empty:
        return None

    # A. contributor growth: year1 vs year5 unique active contributors
    y1 = win[win["author_date"] < YEAR1_END]["author_id"].nunique()
    y5 = win[win["author_date"] >= YEAR5_START]["author_id"].nunique()
    growth = (y5 - y1) / y1 if y1 > 0 else None

    # B. maintainer turnover: veterans (first commit before window) in top
    # quartile by commit count who have NO commit in the final 12 months
    first_commit = df.groupby("author_id")["author_date"].min()
    veterans = first_commit[first_commit < SINCE].index
    commit_count = df.groupby("author_id").size()
    turnover = None
    if len(veterans) >= 5:
        vet_counts = commit_count[veterans]
        top_vets = vet_counts[vet_counts >= vet_counts.quantile(0.75)].index
        if len(top_vets) >= 3:
            last_commit = df.groupby("author_id")["author_date"].max()
            departed = (last_commit[top_vets] < LAST12_START).mean()
            turnover = departed

    # D. activity trend: slope of monthly total commit count (all authors incl. bots here,
    # since "inactivity risk" is about overall project activity, not just human governance)
    full = pd.read_csv(path, usecols=["author_date"])
    full["author_date"] = pd.to_datetime(full["author_date"], errors="coerce")
    full = full.dropna(subset=["author_date"])
    fw = full[(full["author_date"] >= SINCE) & (full["author_date"] <= UNTIL)]
    monthly = fw.groupby(fw["author_date"].dt.to_period("M")).size()
    months = pd.period_range(SINCE.to_period("M"), UNTIL.to_period("M"), freq="M")
    monthly = monthly.reindex(months, fill_value=0)
    x = np.arange(len(monthly))
    y = monthly.values
    if y.mean() > 0 and len(x) > 1:
        slope = np.polyfit(x, y, 1)[0]
        trend = slope / y.mean()  # normalized: relative monthly growth rate
    else:
        trend = None

    return {
        "repo": repo,
        "y1_contributors": y1,
        "y5_contributors": y5,
        "contributor_growth_y1_y5": growth,
        "n_veterans": len(veterans),
        "maintainer_turnover_rate": turnover,
        "activity_trend": trend,
    }


def documentation_staleness():
    fd_files = glob.glob(os.path.join(ROOT, "per_repo", "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True)
    fd["commit_date"] = pd.to_datetime(fd["commit_date"])
    rows = []
    for repo, group in fd.groupby("repo"):
        last_touch = group.groupby("health_file")["commit_date"].max()
        staleness_days = (UNTIL - last_touch).dt.days
        rows.append({
            "repo": repo,
            "n_files_touched": len(last_touch),
            "median_staleness_days": staleness_days.median(),
            "mean_staleness_days": staleness_days.mean(),
        })
    return pd.DataFrame(rows)


def main():
    repos = load_repos()
    rows = [outcomes_from_commits(r) for r in repos]
    rows = [r for r in rows if r is not None]
    commit_outcomes = pd.DataFrame(rows)

    staleness = documentation_staleness()

    arch = pd.read_csv(os.path.join(ROOT, "combined", "archetype_assignments.csv"))[
        ["repo", "entropy_norm", "active_window_rate"]
    ]
    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[
        ["repo", "unique_contributors_for_metrics"]
    ]
    owner = pd.read_csv(os.path.join(ROOT, "combined", "combined_doc_contributors.csv"))[
        ["repo", "health_docs_touch_bus50", "health_docs_touch_contributors"]
    ]

    df = commit_outcomes.merge(staleness, on="repo").merge(arch, on="repo").merge(contrib, on="repo").merge(owner, on="repo")
    df["log_contributors"] = np.log1p(df["unique_contributors_for_metrics"])
    df["log_bus50"] = np.log1p(df["health_docs_touch_bus50"])
    df["participation_rate"] = df["health_docs_touch_contributors"] / df["unique_contributors_for_metrics"]
    df["log_staleness"] = np.log1p(df["median_staleness_days"])

    df.to_csv(os.path.join(OUT, "final_battery_dataset.csv"), index=False)
    print(f"n = {len(df)}")

    predictors = ["entropy_norm", "active_window_rate", "log_bus50", "participation_rate"]
    outcomes = ["contributor_growth_y1_y5", "maintainer_turnover_rate", "log_staleness", "activity_trend"]

    print(f"\n{'outcome':30s} {'predictor':20s} {'coef':>10s} {'p':>8s} {'n':>5s}")
    print("-" * 80)
    results = []
    for outcome in outcomes:
        sub = df.dropna(subset=[outcome] + predictors)
        base = smf.ols(f"{outcome} ~ log_contributors", data=sub).fit()
        for pred in predictors:
            m = smf.ols(f"{outcome} ~ log_contributors + {pred}", data=sub).fit()
            ft = m.compare_f_test(base)
            flag = " <---" if ft[1] < 0.05 else ""
            print(f"{outcome:30s} {pred:20s} {m.params[pred]:10.4f} {ft[1]:8.4f} {len(sub):5d}{flag}")
            results.append({"outcome": outcome, "predictor": pred, "coef": m.params[pred],
                             "p_value": ft[1], "n": len(sub)})

    pd.DataFrame(results).to_csv(os.path.join(OUT, "final_battery_results.csv"), index=False)
    print(f"\nSaved: {OUT}/final_battery_results.csv")


if __name__ == "__main__":
    main()
