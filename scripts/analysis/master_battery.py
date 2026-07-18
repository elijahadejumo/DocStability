#!/usr/bin/env python3
"""
Master consolidation: every predictor x outcome combination tested this
session, run through the SAME standardized battery, compiled into one
table so near-misses (3/4 checks) are as visible as clean nulls (0-1/4)
and the two validated findings (4/4, marked VALIDATED).
"""
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy.stats import rankdata

OUT = "/Users/elijahadejumo/Documents/DocStability/analysis_outputs"


def partial_spearman(x, y, z):
    rx, ry, rz = rankdata(x), rankdata(y), rankdata(z)
    rxz = np.corrcoef(rx, rz)[0, 1]; ryz = np.corrcoef(ry, rz)[0, 1]; rxy = np.corrcoef(rx, ry)[0, 1]
    denom = np.sqrt((1 - rxz ** 2) * (1 - ryz ** 2))
    return (rxy - rxz * ryz) / denom if denom > 0 else np.nan


def battery(df, outcome, pred, size_col="log_contributors", vol_col=None, label=""):
    cols = [outcome, pred, size_col] + ([vol_col] if vol_col else [])
    sub = df.dropna(subset=cols).copy()
    n = len(sub)
    if n < 15 or sub[pred].std() == 0 or sub[outcome].std() == 0:
        return {"scope": label, "predictor": pred, "outcome": outcome, "n": n,
                "p_size": np.nan, "p_vol": np.nan, "p_hc3": np.nan, "p_outlier": np.nan,
                "rho": np.nan, "checks_passed": np.nan, "note": "insufficient data"}
    try:
        base = smf.ols(f"{outcome} ~ {size_col}", data=sub).fit()
        m = smf.ols(f"{outcome} ~ {size_col} + {pred}", data=sub).fit()
        ft = m.compare_f_test(base)
        p_size = ft[1]

        if vol_col:
            base_v = smf.ols(f"{outcome} ~ {size_col} + {vol_col}", data=sub).fit()
            m_v = smf.ols(f"{outcome} ~ {size_col} + {vol_col} + {pred}", data=sub).fit()
            ft_v = m_v.compare_f_test(base_v)
            p_vol = ft_v[1]
        else:
            p_vol = np.nan

        m_hc3 = smf.ols(f"{outcome} ~ {size_col} + {pred}", data=sub).fit(cov_type="HC3")
        p_hc3 = m_hc3.pvalues[pred]

        infl = m.get_influence()
        cooks_d = infl.cooks_distance[0]
        drop_idx = pd.Series(cooks_d, index=sub.index).sort_values(ascending=False).head(3).index
        sub_r = sub.drop(drop_idx)
        m_r = smf.ols(f"{outcome} ~ {size_col} + {pred}", data=sub_r).fit()
        p_outlier = m_r.pvalues[pred]

        rho = partial_spearman(sub[pred], sub[outcome], sub[size_col])

        checks = [p_size < 0.05, p_hc3 < 0.05, p_outlier < 0.05] + ([p_vol < 0.05] if vol_col else [])
        checks_passed = sum(checks)
        max_checks = len(checks)

        return {"scope": label, "predictor": pred, "outcome": outcome, "n": n,
                "p_size": round(p_size, 4), "p_vol": round(p_vol, 4) if vol_col else None,
                "p_hc3": round(p_hc3, 4), "p_outlier": round(p_outlier, 4),
                "rho": round(rho, 3), "checks_passed": f"{checks_passed}/{max_checks}", "note": ""}
    except Exception as e:
        return {"scope": label, "predictor": pred, "outcome": outcome, "n": n,
                "p_size": np.nan, "p_vol": np.nan, "p_hc3": np.nan, "p_outlier": np.nan,
                "rho": np.nan, "checks_passed": np.nan, "note": f"error: {e}"}


results = []

# ---- 1. final_battery: entropy/AWR/bus50/participation vs growth/turnover/staleness/trend ----
df = pd.read_csv(f"{OUT}/final_battery_dataset.csv")
rhythm_vol = pd.read_csv("/Users/elijahadejumo/Documents/DocStability/combined/combined_doc_stability_metrics.csv")[["repo", "health_file_commits"]]
df = df.merge(rhythm_vol, on="repo", how="left")
df["log_doc_commits"] = np.log1p(df["health_file_commits"])
for outcome in ["contributor_growth_y1_y5", "maintainer_turnover_rate", "log_staleness", "activity_trend"]:
    for pred in ["entropy_norm", "active_window_rate", "log_bus50", "participation_rate"]:
        results.append(battery(df, outcome, pred, vol_col="log_doc_commits", label="all-docs, contemporaneous"))

# ---- 2. RQ4 newcomer retention (general, any-commit-type) ----
df = pd.read_csv(f"{OUT}/rq4_model_dataset.csv")
df["log_bus50"] = np.log1p(df["health_docs_touch_bus50"])
for pred in ["entropy_norm", "active_window_rate", "log_bus50", "reactive_rate"]:
    results.append(battery(df, "retention_rate", pred, label="all-docs, general retention (any commit type)"))

# ---- 3. rq4_core_variants (6 stricter "core" definitions) ----
df = pd.read_csv(f"{OUT}/rq4_core_variants.csv")
for outcome in ["rate_2plus_months", "rate_3plus_months", "rate_5plus_commits",
                "rate_10plus_commits", "rate_90plus_days_span", "rate_180plus_days_span"]:
    for pred in ["entropy_norm", "active_window_rate"]:
        results.append(battery(df.dropna(subset=[outcome]), outcome, pred, label="general retention, stricter core definitions"))

# ---- 4. outside-the-box: veteran retention, contributor stability, lagged growth, bot substitution ----
df = pd.read_csv(f"{OUT}/outside_box_veteran_retention.csv")
for pred in ["entropy_norm", "active_window_rate"]:
    results.append(battery(df, "veteran_still_active_rate", pred, label="general veteran retention (any commit type)"))

df = pd.read_csv(f"{OUT}/outside_box_contributor_stability.csv")
for pred in ["entropy_norm", "active_window_rate"]:
    results.append(battery(df, "monthly_contributor_cv", pred, label="contributor-count stability (CV)"))

df = pd.read_csv(f"{OUT}/outside_box_lagged_growth.csv")
df["log_h1_contributors"] = np.log1p(df["h1_contributors"])
for pred in ["entropy_norm", "active_window_rate"]:
    results.append(battery(df, "contributor_growth_h2", pred, size_col="log_h1_contributors", label="lagged: h1 rhythm -> h2 general growth"))

df = pd.read_csv(f"{OUT}/outside_box_bot_substitution.csv")
for pred in ["entropy_norm", "active_window_rate"]:
    results.append(battery(df, "bot_doc_share", pred, label="human/bot substitution"))

# ---- 5. issue/PR resolution latency (all-docs) ----
df = pd.read_csv(f"{OUT}/issue_pr_model_dataset.csv")
for outcome in ["log_issue_close", "log_pr_merge"]:
    for pred in ["entropy_norm", "active_window_rate"]:
        results.append(battery(df, outcome, pred, label="all-docs, issue/PR resolution time"))

# ---- 6. first response time / never-responded ----
df = pd.read_csv(f"{OUT}/first_response_model_dataset.csv")
for outcome in ["log_response_hours", "never_responded_rate"]:
    for pred in ["entropy_norm", "active_window_rate", "participation_rate", "log_bus50"]:
        results.append(battery(df, outcome, pred, label="all-docs, issue first-response"))

# ---- 7. process-only (living docs) ----
df = pd.read_csv(f"{OUT}/process_docs_only_dataset.csv")
for pred in ["living_entropy_norm", "living_awr", "living_participation_rate"]:
    results.append(battery(df, "log_living_staleness", pred, vol_col="log_living_commits", label="process-only, staleness"))

resp = pd.read_csv(f"{OUT}/first_response_computed.csv")
resp["log_response_hours"] = np.log1p(resp["median_response_hours"])
proc = pd.read_csv(f"{OUT}/process_docs_only_dataset.csv")
raw_vol = pd.read_csv(f"{OUT}/issue_pr_resolution.csv")[["repo", "issues_total_closed"]]
df2 = resp.merge(proc, on="repo").merge(raw_vol, on="repo")
for outcome in ["log_response_hours"]:
    for pred in ["living_entropy_norm", "living_awr", "living_participation_rate"]:
        results.append(battery(df2, outcome, pred, vol_col="log_living_commits", label="process-only, issue first-response"))

# ---- 8. README+CONTRIBUTING only ----
df = pd.read_csv(f"{OUT}/readme_contributing_dataset.csv")
for outcome in ["retention_rate", "contributor_growth_y1_y5", "log_newcomers"]:
    for pred in ["rc_entropy_norm", "rc_awr", "rc_participation_rate"]:
        results.append(battery(df, outcome, pred, vol_col="log_rc_commits", label="README+CONTRIBUTING-only"))

# ---- 9. doc-specific newcomer retention (construct-matched) -- VALIDATED bus50 ----
df = pd.read_csv(f"{OUT}/doc_newcomer_retention_dataset.csv")
for pred in ["entropy_norm", "active_window_rate", "participation_rate", "log_bus50"]:
    results.append(battery(df, "doc_newcomer_retention_rate", pred, vol_col="log_doc_commits", label="doc-specific newcomer retention (construct-matched)"))
sub10 = df[df["n_doc_newcomers"] >= 10]
results.append(battery(sub10, "doc_newcomer_retention_rate", "log_bus50", vol_col="log_doc_commits", label="doc-specific newcomer retention, n>=10 newcomers [VALIDATED]"))

# ---- 10. doc contributor loss (contemporaneous -- entropy/AWR CIRCULAR, excluded from ranking) ----
df = pd.read_csv(f"{OUT}/doc_contributor_loss_dataset.csv")
for outcome in ["doc_contributor_loss_rate_all", "doc_contributor_loss_rate_top"]:
    for pred in ["entropy_norm", "active_window_rate", "participation_rate", "log_bus50"]:
        r = battery(df, outcome, pred, vol_col="log_doc_commits", label="doc contributor loss, contemporaneous")
        if pred in ("entropy_norm", "active_window_rate"):
            r["note"] = "CIRCULAR (contemporaneous full-window rhythm partly caused by same departure event) -- see H1-only version"
        results.append(r)

# ---- 11. H1-only lagged versions (properly time-ordered) ----
inact = pd.read_csv(f"{OUT}/doc_inactivity_dataset.csv")[["repo", "h1_entropy", "h1_awr", "h1_doc_commits"]]
loss = pd.read_csv(f"{OUT}/doc_contributor_loss_dataset.csv")
dfL = loss.merge(inact, on="repo")
dfL["log_h1_commits"] = np.log1p(dfL["h1_doc_commits"])
for outcome in ["doc_contributor_loss_rate_all", "doc_contributor_loss_rate_top"]:
    for pred in ["h1_entropy", "h1_awr"]:
        results.append(battery(dfL, outcome, pred, vol_col="log_h1_commits", label="doc contributor loss, LAGGED (h1->h2, properly time-ordered)"))

h1bus = pd.read_csv(f"{OUT}/h1_bus_factor_dataset.csv")
for outcome in ["doc_contributor_loss_rate_all", "doc_contributor_loss_rate_top"]:
    r = battery(h1bus, outcome, "log_h1_bus50", vol_col="log_h1_commits", label="doc contributor loss, LAGGED bus50 [VALIDATED]")
    results.append(r)

# ---- 12. doc dormancy gap (contemporaneous -- CIRCULAR, excluded) ----
df = pd.read_csv(f"{OUT}/doc_inactivity_dataset.csv")
for pred in ["entropy_norm", "active_window_rate", "participation_rate", "log_bus50"]:
    r = battery(df, "longest_dormancy_gap_months", pred, vol_col="log_doc_commits", label="doc dormancy gap, contemporaneous")
    if pred in ("entropy_norm", "active_window_rate"):
        r["note"] = "CIRCULAR (same monthly-commit-vector determines both predictor and outcome)"
    results.append(r)
for pred in ["h1_entropy", "h1_awr"]:
    results.append(battery(df, "h2_decline_ratio", pred, vol_col="log_doc_commits", label="doc activity, LAGGED (h1->h2 decline, properly time-ordered)"))

# ---- 13. repo-wide inactivity (overall commits, not doc-specific) ----
df = pd.read_csv(f"{OUT}/repo_inactivity_dataset.csv")
for pred in ["entropy_norm", "active_window_rate", "participation_rate", "log_bus50"]:
    r = battery(df, "overall_longest_gap_months", pred, vol_col="log_doc_commits", label="repo-wide dormancy gap, contemporaneous")
    if pred == "entropy_norm":
        r["note"] = "SIZE-THRESHOLD ARTIFACT (disappears when restricted to >=20 contributors, p=0.99)"
    results.append(r)
    results.append(battery(df, "log_days_since_last", pred, vol_col="log_doc_commits", label="repo-wide recency (days since last commit)"))
for pred in ["doc_h1_entropy", "doc_h1_awr"]:
    results.append(battery(df, "overall_h2_decline_ratio", pred, vol_col="log_doc_commits", label="LAGGED: doc h1 rhythm -> repo-wide h2 decline"))

# ---- 14. category breakdown (4 categories x staleness/loss) ----
cat_df = pd.read_csv(f"{OUT}/category_breakdown_dataset.csv")
staleness = pd.read_csv(f"{OUT}/final_battery_dataset.csv")[["repo", "log_staleness"]]
loss2 = pd.read_csv(f"{OUT}/doc_contributor_loss_dataset.csv")[["repo", "doc_contributor_loss_rate_top", "log_contributors"]]
contrib = pd.read_csv("/Users/elijahadejumo/Documents/DocStability/combined/combined_contributors.csv")[["repo", "unique_contributors_for_metrics"]]
for cat in ["onboarding_process", "governance_policy", "change_tracking", "legal_attribution"]:
    sub = cat_df[cat_df["category"] == cat].merge(staleness, on="repo").merge(contrib, on="repo")
    sub["log_contributors"] = np.log1p(sub["unique_contributors_for_metrics"])
    sub["participation_rate"] = sub["n_contributors"] / sub["unique_contributors_for_metrics"]
    sub["log_commits"] = np.log1p(sub["n_commits"])
    for pred in ["entropy_norm", "awr", "participation_rate"]:
        results.append(battery(sub, "log_staleness", pred, vol_col="log_commits", label=f"category={cat}, staleness"))

    sub2 = cat_df[cat_df["category"] == cat].merge(loss2, on="repo").merge(contrib, on="repo")
    sub2["log_contributors"] = np.log1p(sub2["unique_contributors_for_metrics"])
    sub2["participation_rate"] = sub2["n_contributors"] / sub2["unique_contributors_for_metrics"]
    sub2["log_commits"] = np.log1p(sub2["n_commits"])
    for pred in ["entropy_norm", "awr", "participation_rate"]:
        results.append(battery(sub2, "doc_contributor_loss_rate_top", pred, vol_col="log_commits", label=f"category={cat}, contributor loss"))

# ---- 15. excess ownership concentration (the very first false lead) ----
df = pd.read_csv(f"{OUT}/confounding_controls_merged.csv")
if "excess_top3" not in df.columns:
    df["excess_top3"] = df["health_docs_touch_top3_share"] - df["code_top3_share"]
for pred in ["entropy_norm", "active_window_rate"]:
    r = battery(df, "excess_top3", pred, label="excess ownership concentration (RQ3)")
    if pred == "entropy_norm":
        r["note"] = "FRAGILE (p=0.023 plain OLS -> p=0.396 after dropping 3 outliers)"
    results.append(r)

# ---- assemble ----
out_df = pd.DataFrame(results)
out_df.to_csv(f"{OUT}/MASTER_BATTERY_RESULTS.csv", index=False)
print(f"Total tests compiled: {len(out_df)}")
print(out_df["checks_passed"].value_counts())
