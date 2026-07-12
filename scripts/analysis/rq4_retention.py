#!/usr/bin/env python3
"""
RQ4: Does documentation governance predict OSS coordination outcomes,
beyond what contributor count alone predicts?

Outcome: newcomer retention rate. For each repo, using FULL commit history
(not just the 5-year window), classify each human (non-bot) contributor as:
  - newcomer: first-ever commit (any file type) falls inside the 5-year
    observation window (2020-06-30 to 2025-06-29) -- i.e. no prior history
  - retained: a newcomer who committed in >=2 distinct calendar months
    (returned at least once after their first month), consistent with the
    monthly-granularity convention already used for RQ1's rhythm metrics

retention_rate = retained_newcomers / total_newcomers, per repo.

Model: retention_rate ~ log(contributor_count) + rhythm archetype +
bus_factor_50 + external_linkage_rate, nested F-test for incremental R^2
of documentation predictors over the contributor-count-only baseline --
same structure as confounding_controls.py for RQ2/RQ3.
"""
import glob
import os

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats

ROOT = "/Users/elijahadejumo/Documents/DocStability"
LOGS_DIR = os.path.join(ROOT, "full_commit_logs")
OUT = os.path.join(ROOT, "analysis_outputs")

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
        return {"repo": repo, "n_newcomers": 0, "n_retained": 0, "retention_rate": np.nan}

    newcomer_df = df[df["author_id"].isin(newcomers)]
    months_active = newcomer_df.groupby("author_id")["month"].nunique()
    retained = (months_active >= 2).sum()

    return {
        "repo": repo,
        "n_newcomers": len(newcomers),
        "n_retained": int(retained),
        "retention_rate": retained / len(newcomers),
    }


def main():
    logs = glob.glob(os.path.join(LOGS_DIR, "*_full_commit_log.csv"))
    repos = sorted(os.path.basename(f).replace("_full_commit_log.csv", "") for f in logs)
    repos = [r for r in repos if r != "combined"]  # exclude the consolidated file, which matches the glob
    print(f"Found {len(repos)} full commit logs")

    rows = []
    for i, repo in enumerate(repos, 1):
        r = classify_repo(repo)
        if r is not None:
            rows.append(r)
        if i % 20 == 0:
            print(f"  {i}/{len(repos)} processed")

    df = pd.DataFrame(rows)
    print(f"\nProcessed {len(df)} repos")
    print(df["retention_rate"].describe())
    df.to_csv(os.path.join(OUT, "rq4_retention_by_repo.csv"), index=False)
    print(f"Saved: {OUT}/rq4_retention_by_repo.csv")

    # ---- merge with governance metrics for the regression ----
    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[
        ["repo", "unique_contributors_for_metrics"]
    ]
    arch = pd.read_csv(os.path.join(ROOT, "combined", "archetype_assignments.csv"))[
        ["repo", "archetype", "entropy_norm", "active_window_rate"]
    ]
    owner = pd.read_csv(os.path.join(ROOT, "combined", "combined_doc_contributors.csv"))[
        ["repo", "health_docs_touch_bus50"]
    ]
    reactive = pd.read_csv(os.path.join(ROOT, "combined", "combined_reactive_analysis.csv"))[
        ["repo", "reactive_rate"]
    ]

    model_df = df.merge(contrib, on="repo").merge(arch, on="repo").merge(owner, on="repo").merge(reactive, on="repo")
    model_df = model_df.dropna(subset=["retention_rate"])
    model_df["log_contributors"] = np.log1p(model_df["unique_contributors_for_metrics"])
    model_df["log_bus50"] = np.log1p(model_df["health_docs_touch_bus50"])
    model_df["archetype"] = pd.Categorical(model_df["archetype"], categories=["Sparse", "Occasional", "Consistent"])

    print(f"\nModel dataset: n={len(model_df)}")

    base = smf.ols("retention_rate ~ log_contributors", data=model_df).fit()
    print(f"\nBaseline: retention_rate ~ log_contributors")
    print(f"  R^2={base.rsquared:.3f}, coef={base.params['log_contributors']:.4f}, p={base.pvalues['log_contributors']:.4f}")

    full = smf.ols(
        "retention_rate ~ log_contributors + C(archetype) + log_bus50 + reactive_rate",
        data=model_df,
    ).fit()
    print(f"\nFull model: retention_rate ~ log_contributors + archetype + log_bus50 + reactive_rate")
    print(full.summary().tables[1])
    print(f"  R^2={full.rsquared:.3f} (delta over baseline = {full.rsquared - base.rsquared:.3f})")

    ftest = full.compare_f_test(base)
    print(f"\nF-test (do documentation governance metrics add explanatory power beyond contributor count?):")
    print(f"  F={ftest[0]:.3f}, p={ftest[1]:.4f}")

    # individual predictors, avoiding collinearity by testing separately
    for var in ["log_bus50", "reactive_rate"]:
        m = smf.ols(f"retention_rate ~ log_contributors + {var}", data=model_df).fit()
        ft = m.compare_f_test(base)
        print(f"\n  retention_rate ~ log_contributors + {var}: coef={m.params[var]:.4f}, p={m.pvalues[var]:.4f}, "
              f"F-test vs baseline: F={ft[0]:.3f}, p={ft[1]:.4f}")

    model_df.to_csv(os.path.join(OUT, "rq4_model_dataset.csv"), index=False)
    print(f"\nSaved: {OUT}/rq4_model_dataset.csv")


if __name__ == "__main__":
    main()
