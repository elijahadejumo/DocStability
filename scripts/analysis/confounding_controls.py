#!/usr/bin/env python3
"""
Confounding-control analysis: do rhythm archetypes, external-linkage rate, and
bus factor findings survive controlling for project size / contributor count?

Directly answers reviewer A ("rhythm may reflect project size/release frequency
rather than governance culture... lacks multivariate controls... consider
regression") and reviewer C ("confounding likely in RQ2: external linking
compared across archetypes that correlate with contributor count... control for
contributor count").
"""
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
from scipy import stats

REPO_ROOT = Path(__file__).resolve().parents[2]
PROJ = str(REPO_ROOT / "combined")
OUT = str(REPO_ROOT / "analysis_outputs")

contrib = pd.read_csv(f"{PROJ}/combined_contributors.csv")[
    ["repo", "total_commits_all", "unique_contributors_for_metrics", "gini", "top3_share"]
].rename(columns={"top3_share": "code_top3_share"})

reactive = pd.read_csv(f"{PROJ}/combined_reactive_analysis.csv").drop_duplicates("repo")[
    ["repo", "reactive_rate", "doc_touch_total"]
]

arch = pd.read_csv(f"{PROJ}/archetype_assignments.csv")[
    ["repo", "entropy_norm", "active_window_rate", "archetype"]
]

owner = pd.read_csv(f"{PROJ}/combined_doc_contributors.csv")[
    ["repo", "health_docs_touch_bus50", "health_docs_touch_bus80",
     "health_docs_touch_top3_share", "health_docs_touch_contributors"]
]

df = arch.merge(contrib, on="repo").merge(reactive, on="repo").merge(owner, on="repo")
print(f"Merged N = {len(df)} repos")

df["log_contributors"] = np.log1p(df["unique_contributors_for_metrics"])
df["log_total_commits"] = np.log1p(df["total_commits_all"])
df["log_bus50"] = np.log1p(df["health_docs_touch_bus50"])

# ---------------------------------------------------------------------
# 1. Raw correlations: size/contributor count vs rhythm metrics
# ---------------------------------------------------------------------
print("\n=== 1. Raw correlations: size proxies vs rhythm metrics ===")
for x in ["log_contributors", "log_total_commits"]:
    for y in ["entropy_norm", "active_window_rate"]:
        rho, p = stats.spearmanr(df[x], df[y])
        print(f"  {x} vs {y}: rho={rho:.3f}, p={p:.4f}")

# ---------------------------------------------------------------------
# 2. Does archetype predict external linkage rate BEYOND contributor count?
#    (reviewer C's specific ask)
# ---------------------------------------------------------------------
print("\n=== 2. External linkage rate ~ archetype, controlling for contributor count ===")
df["archetype"] = pd.Categorical(df["archetype"], categories=["Sparse", "Occasional", "Consistent"])

m1 = smf.ols("reactive_rate ~ log_contributors", data=df).fit()
print("\nModel A: reactive_rate ~ log_contributors (no archetype)")
print(f"  R^2 = {m1.rsquared:.3f}, coef(log_contributors) = {m1.params['log_contributors']:.4f} (p={m1.pvalues['log_contributors']:.4f})")

m2 = smf.ols("reactive_rate ~ log_contributors + C(archetype)", data=df).fit()
print("\nModel B: reactive_rate ~ log_contributors + archetype")
print(m2.summary().tables[1])
print(f"  R^2 = {m2.rsquared:.3f}  (delta R^2 over Model A = {m2.rsquared - m1.rsquared:.3f})")

f_test = m2.compare_f_test(m1)
print(f"  F-test (does archetype add explanatory power beyond contributor count?): F={f_test[0]:.3f}, p={f_test[1]:.4f}")

# ---------------------------------------------------------------------
# 3. Bus-50 ~ entropy + AWR, controlling for contributor count
#    (paper currently reports bivariate rho only -- this is the actual
#    multivariate version reviewers asked for)
# ---------------------------------------------------------------------
print("\n=== 3. Documentation bus factor (log Bus-50) ~ rhythm, controlling for size ===")
m3 = smf.ols("log_bus50 ~ log_contributors", data=df).fit()
m4 = smf.ols("log_bus50 ~ log_contributors + entropy_norm + active_window_rate", data=df).fit()
print("\nModel C: log_bus50 ~ log_contributors")
print(f"  R^2 = {m3.rsquared:.3f}, coef = {m3.params['log_contributors']:.4f} (p={m3.pvalues['log_contributors']:.4f})")
print("\nModel D: log_bus50 ~ log_contributors + entropy_norm + active_window_rate")
print(m4.summary().tables[1])
print(f"  R^2 = {m4.rsquared:.3f} (delta R^2 over Model C = {m4.rsquared - m3.rsquared:.3f})")
f_test2 = m4.compare_f_test(m3)
print(f"  F-test (do rhythm metrics add explanatory power beyond contributor count?): F={f_test2[0]:.3f}, p={f_test2[1]:.4f}")

# ---------------------------------------------------------------------
# 4. Ownership concentration: doc top3 vs code top3, controlling for size
#    (paper already computes the raw excess concentration; check if it
#    holds after controlling for size too)
# ---------------------------------------------------------------------
print("\n=== 4. Excess doc-ownership concentration (doc_top3 - code_top3) vs size ===")
df["excess_top3"] = df["health_docs_touch_top3_share"] - df["code_top3_share"]
rho, p = stats.spearmanr(df["log_contributors"], df["excess_top3"])
print(f"  corr(log_contributors, excess_top3): rho={rho:.3f}, p={p:.4f}")
print(f"  mean excess_top3 = {df['excess_top3'].mean():.3f}, repos with excess_top3>0: {(df['excess_top3']>0).sum()}/100")

df.to_csv(f"{OUT}/confounding_controls_merged.csv", index=False)
print(f"\nSaved merged dataset to {OUT}/confounding_controls_merged.csv")
