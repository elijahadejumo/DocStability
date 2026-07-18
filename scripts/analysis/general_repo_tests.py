#!/usr/bin/env python3
"""
Two tests requested directly:
1. General repository retention (newcomer = first-ever commit of ANY file
   type; retained = returns in a 2nd distinct month) against ALL FOUR
   documentation predictors, including participation_rate which was
   missing from the original rq4_model_dataset.csv test.
2. General repository newcomer COUNT/arrival (not retention, not doc-
   specific) -- does documentation rhythm/participation/bus-factor predict
   how many new people show up to the repo at all -- genuinely untested
   until now.
"""
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy.stats import rankdata

ROOT = "/Users/elijahadejumo/Documents/DocStability"
OUT = f"{ROOT}/analysis_outputs"


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
        print(f"    {pred:20s} insufficient data (n={n})")
        return None
    base = smf.ols(f"{outcome} ~ {size_col}", data=sub).fit()
    m = smf.ols(f"{outcome} ~ {size_col} + {pred}", data=sub).fit()
    ft = m.compare_f_test(base)
    p_size = ft[1]

    if vol_col:
        base_v = smf.ols(f"{outcome} ~ {size_col} + {vol_col}", data=sub).fit()
        m_v = smf.ols(f"{outcome} ~ {size_col} + {vol_col} + {pred}", data=sub).fit()
        p_vol = m_v.compare_f_test(base_v)[1]
    else:
        p_vol = None

    m_hc3 = smf.ols(f"{outcome} ~ {size_col} + {pred}", data=sub).fit(cov_type="HC3")
    p_hc3 = m_hc3.pvalues[pred]

    infl = m.get_influence()
    cooks_d = infl.cooks_distance[0]
    drop_idx = pd.Series(cooks_d, index=sub.index).sort_values(ascending=False).head(3).index
    m_r = smf.ols(f"{outcome} ~ {size_col} + {pred}", data=sub.drop(drop_idx)).fit()
    p_outlier = m_r.pvalues[pred]

    rho = partial_spearman(sub[pred], sub[outcome], sub[size_col])
    checks = [p_size < 0.05, p_hc3 < 0.05, p_outlier < 0.05] + ([p_vol < 0.05] if vol_col else [])
    flag = " <---" if all(checks) else ""
    print(f"    {pred:20s} n={n:3d} p(size)={p_size:.4f} "
          f"p(vol)={'—' if p_vol is None else f'{p_vol:.4f}'} p(HC3)={p_hc3:.4f} "
          f"p(no-outlier)={p_outlier:.4f} rho={rho:.3f}{flag}")
    return {"predictor": pred, "outcome": outcome, "n": n, "p_size": round(p_size, 4),
            "p_vol": round(p_vol, 4) if p_vol is not None else None,
            "p_hc3": round(p_hc3, 4), "p_outlier": round(p_outlier, 4), "rho": round(rho, 3),
            "checks_passed": f"{sum(checks)}/{len(checks)}"}


def main():
    df = pd.read_csv(f"{OUT}/rq4_model_dataset.csv")
    owner = pd.read_csv(f"{ROOT}/combined/combined_doc_contributors.csv")[["repo", "health_docs_touch_contributors"]]
    df = df.merge(owner, on="repo", how="left")
    df["participation_rate"] = df["health_docs_touch_contributors"] / df["unique_contributors_for_metrics"]
    df["log_newcomers"] = np.log1p(df["n_newcomers"])

    rhythm = pd.read_csv(f"{ROOT}/combined/combined_doc_stability_metrics.csv")[["repo", "health_file_commits"]]
    df = df.merge(rhythm, on="repo", how="left")
    df["log_doc_commits"] = np.log1p(df["health_file_commits"])

    df.to_csv(f"{OUT}/general_repo_tests_dataset.csv", index=False)

    predictors = ["entropy_norm", "active_window_rate", "participation_rate", "log_bus50"]
    results = []

    print("=== 1. GENERAL retention (any commit type) -- all 4 predictors, incl. participation_rate ===")
    for pred in predictors:
        r = battery(df, "retention_rate", pred, vol_col="log_doc_commits", label="general retention")
        if r:
            r["scope"] = "general repo retention (any commit type)"
            results.append(r)

    print("\n=== 2. GENERAL newcomer COUNT/arrival (not retention, not doc-specific) ===")
    for pred in predictors:
        r = battery(df, "log_newcomers", pred, vol_col="log_doc_commits", label="general newcomer count")
        if r:
            r["scope"] = "general repo newcomer count/arrival"
            results.append(r)

    out_df = pd.DataFrame(results)
    out_df.to_csv(f"{OUT}/general_repo_tests_results.csv", index=False)

    master = pd.read_csv(f"{OUT}/MASTER_BATTERY_RESULTS.csv")
    out_df["note"] = ""
    combined = pd.concat([master, out_df[["scope", "predictor", "outcome", "n", "p_size", "p_vol",
                                           "p_hc3", "p_outlier", "rho", "checks_passed", "note"]]], ignore_index=True)
    combined.to_csv(f"{OUT}/MASTER_BATTERY_RESULTS.csv", index=False)
    print(f"\nAppended {len(out_df)} rows to master battery, new total: {len(combined)}")


if __name__ == "__main__":
    main()
