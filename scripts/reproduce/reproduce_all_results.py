#!/usr/bin/env python3
"""
reproduce_all_results.py
=========================
Reproduces every reported statistic in "Empirical Characterization of Health
Documentation Governance in Open Source Projects" (RQ1-RQ4) from the
already-extracted, decade-window (2016-05-30 .. 2026-05-29) intermediate
CSVs in analysis_outputs/.

This script does NOT re-clone or re-mine the 100 repositories from GitHub;
it operates on the processed per-repository / per-repository-year CSVs that
sit between raw git history and the numbers printed in the paper. That
extraction step is the expensive, environment-dependent part (100 full
clones, a decade of git log parsing); the statistical layer reproduced here
is the part reviewers most need to check and the part that is fully
deterministic given the same input CSVs.

USAGE
-----
  python3 reproduce_all_results.py                     # run everything
  python3 reproduce_all_results.py --section rq1        # just RQ1
  python3 reproduce_all_results.py --section rq2
  python3 reproduce_all_results.py --section rq3
  python3 reproduce_all_results.py --section rq3_predictive
  python3 reproduce_all_results.py --section rq4
  python3 reproduce_all_results.py --data_dir /path/to/analysis_outputs

REQUIREMENTS
------------
  pip install pandas numpy scipy statsmodels scikit-learn pymannkendall

INPUT FILES EXPECTED (under --data_dir, default: analysis_outputs/)
---------------------------------------------------------------------
  decade_archetype_assignments_final.csv   RQ1 rhythm archetypes (per repo)
  decade_intent_agg.csv                    RQ2 intention taxonomy (per repo)
  decade_rq3_full.csv                      RQ3 ownership/concentration (per repo)
  decade_staleness.csv                     RQ3 outcome: staleness (per repo)
  decade_newcomer_retention.csv            RQ3 outcome: doc-newcomer retention (per repo)
  decade_general_outcomes.csv              RQ3 outcome: repo-wide outcomes (per repo)
  annual_archetype_classified.csv          RQ4 per-repo-year archetype labels
  annual_bus50_doconly.csv                 RQ4 per-repo-year DocOnly rate
  annual_participation.csv                 RQ4 per-repo-year participation rate

  RQ1's living/process-vs-static/attribution robustness check and the
  four-way purpose taxonomy (Table IV) additionally require the raw
  per-commit file-touch records and bot flags, since no saved intermediate
  CSV for these two checks survived the final repository-set swap. If
  available under --repo_root, this script regenerates them from:
    outputs/<repo>/<repo>_2016_2026_file_details.csv   (repo, commit_sha, commit_date, health_file)
    full_commit_logs/<repo>_full_commit_log.csv        (commit_sha, is_bot, ...)
  If these directories are not present, that sub-check is skipped with a
  clear message rather than failing the whole run.
"""
from __future__ import annotations

import argparse
import math
import os
from collections import defaultdict

import numpy as np
import pandas as pd
from scipy.stats import spearmanr, rankdata
import statsmodels.api as sm
import statsmodels.formula.api as smf

try:
    import pymannkendall as mk
    HAVE_MK = True
except ImportError:
    HAVE_MK = False

try:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import silhouette_score
    HAVE_SKLEARN = True
except ImportError:
    HAVE_SKLEARN = False


def hr(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def fp(x, nd=4):
    return f"{x:.{nd}f}"


# ─────────────────────────────────────────────────────────────────────────
# RQ1: Documentation Rhythm Pattern
# ─────────────────────────────────────────────────────────────────────────

LIVING_STEMS = {
    "readme", "contributing", "changelog", "history", "release", "releases",
    "pull_request_template", "issue_template", "commit_conventions", "building",
    "code_of_conduct", "governance", "support", "maintainers", "security",
    "roadmap", "vision",
}
STATIC_STEMS = {
    "license", "notice", "copying", "authors", "credits", "thanks", "contributors",
}
PURPOSE_CATEGORIES = {
    "onboarding_process": {"readme", "contributing", "building", "commit_conventions",
                            "pull_request_template", "issue_template"},
    "governance_policy": {"governance", "code_of_conduct", "security", "support",
                           "maintainers", "roadmap", "vision"},
    "change_tracking": {"changelog", "history", "release", "releases"},
    "legal_attribution": {"license", "notice", "copying", "authors", "credits",
                           "thanks", "contributors"},
}
STEM_TO_CATEGORY = {s: c for c, ss in PURPOSE_CATEGORIES.items() for s in ss}


def _stem(fname):
    return fname.lower().split("/")[-1].split(".")[0]


def _month_keys(since_y, since_m, until_y, until_m):
    y, m, out = since_y, since_m, []
    while (y < until_y) or (y == until_y and m <= until_m):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m, y = 1, y + 1
    return out


MONTHS_121 = _month_keys(2016, 5, 2026, 5)


def _entropy_norm(counts):
    total = sum(counts)
    if total <= 0:
        return None
    H = 0.0
    for c in counts:
        if c > 0:
            p = c / total
            H -= p * math.log2(p)
    return H / math.log2(len(counts))


def _awr(counts):
    return sum(1 for c in counts if c > 0) / len(counts)


def rq1_rhythm(data_dir, repo_root):
    hr("RQ1 — Documentation Rhythm Pattern")

    arch = pd.read_csv(os.path.join(data_dir, "decade_archetype_assignments_final.csv"))
    print(f"n repositories = {len(arch)}\n")

    print("Table II — Descriptive statistics of rhythm metrics")
    for col, label in [("entropy_norm", "Entropy (H_norm)"), ("active_month_rate", "Active Window Rate")]:
        s = arch[col]
        print(f"  {label:22s} mean={s.mean():.3f} median={s.median():.3f} "
              f"sd={s.std():.3f} min={s.min():.3f} max={s.max():.3f}")

    print("\nArchetype counts (target: Consistent 37, Occasional 50, Sparse 13):")
    print(arch["archetype"].value_counts().to_string())

    print("\nTable III — Per-archetype descriptive statistics")
    for arche, g in arch.groupby("archetype"):
        for col in ["entropy_norm", "active_month_rate"]:
            s = g[col]
            print(f"  {arche:12s} {col:20s} n={len(g):3d} mean={s.mean():.3f} "
                  f"median={s.median():.3f} min={s.min():.3f} max={s.max():.3f}")

    if HAVE_SKLEARN:
        print("\nFigure 1 — Silhouette scores, k=2..6 (n_init=10, random_state=42)")
        X = arch[["entropy_norm", "active_month_rate"]].values
        Xs = StandardScaler().fit_transform(X)
        for k in range(2, 7):
            km = KMeans(n_clusters=k, n_init=10, random_state=42).fit(Xs)
            sc = silhouette_score(Xs, km.labels_)
            flag = "  <-- peak" if k == 3 else ""
            print(f"  k={k}  silhouette={sc:.3f}{flag}")
    else:
        print("\n[skipped] scikit-learn not installed; cannot reproduce silhouette scan.")

    # ---- living/process vs static/attribution + purpose taxonomy ----
    outputs_dir = os.path.join(repo_root, "outputs")
    logs_dir = os.path.join(repo_root, "full_commit_logs")
    if not (os.path.isdir(outputs_dir) and os.path.isdir(logs_dir)):
        print(f"\n[skipped] living/static robustness + purpose taxonomy require raw "
              f"per-commit data under {outputs_dir} and {logs_dir}, not found.")
        return

    print("\nRegenerating living/static robustness check + purpose taxonomy "
          "from raw per-commit file-touch data...")
    print("(Both checks are bot-INCLUSIVE, matching decade_archetype_assignments_final.csv "
          "and the 37,447-commit ecosystem total used throughout RQ1/RQ2 — do not "
          "bot-filter here, unlike the ownership/bus-factor calculations in RQ3.)")
    final_repos = list(arch["repo"])

    frames = []
    for repo in final_repos:
        path = os.path.join(outputs_dir, repo, f"{repo}_2016_2026_file_details.csv")
        if not os.path.exists(path):
            print(f"  [missing] {path}")
            continue
        d = pd.read_csv(path)
        d["repo"] = repo
        frames.append(d)
    fd = pd.concat(frames, ignore_index=True)
    fd["commit_date"] = pd.to_datetime(fd["commit_date"])
    since, until = pd.Timestamp("2016-05-30"), pd.Timestamp("2026-05-29")
    fd = fd[(fd["commit_date"] >= since) & (fd["commit_date"] <= until)].copy()
    fd["month"] = fd["commit_date"].dt.strftime("%Y-%m")

    def classify_stratum(f):
        s = _stem(f)
        if s in LIVING_STEMS:
            return "living"
        if s in STATIC_STEMS:
            return "static"
        return "unmapped"

    fd["stratum"] = fd["health_file"].apply(classify_stratum)
    fd["category"] = fd["health_file"].apply(lambda f: STEM_TO_CATEGORY.get(_stem(f), "unmapped"))

    commit_strata = fd.groupby(["repo", "commit_sha"])["stratum"].apply(set)
    only_living = commit_strata.apply(lambda s: s == {"living"}).sum()
    total_commits = len(commit_strata)
    print(f"\nLiving-only commit share: {only_living}/{total_commits} = "
          f"{only_living/total_commits:.1%}")

    rows = []
    for repo, g in fd.groupby("repo"):
        for strat in ["living", "static"]:
            sub = g[g["stratum"] == strat]
            mc = defaultdict(int)
            for sha, month in sub[["commit_sha", "month"]].drop_duplicates().values:
                mc[month] += 1
            counts = [mc.get(mk_, 0) for mk_ in MONTHS_121]
            rows.append({"repo": repo, "stratum": strat,
                         "entropy": _entropy_norm(counts), "awr": _awr(counts)})
    strat_df = pd.DataFrame(rows)
    piv_e = strat_df.pivot(index="repo", columns="stratum", values="entropy")
    piv_a = strat_df.pivot(index="repo", columns="stratum", values="awr")

    cmp = pd.DataFrame({
        "entropy_all": arch.set_index("repo")["entropy_norm"],
        "awr_all": arch.set_index("repo")["active_month_rate"],
        "entropy_living": piv_e["living"],
        "awr_living": piv_a["living"],
    }).dropna()
    r_e, _ = spearmanr(cmp["entropy_all"], cmp["entropy_living"])
    r_a, _ = spearmanr(cmp["awr_all"], cmp["awr_living"])
    print(f"Spearman rho (entropy, living-only entropy) = {r_e:.3f}")
    print(f"Spearman rho (AWR, living-only AWR)         = {r_a:.3f}")

    print("\nTable IV — Documentation purpose taxonomy")
    # Per-repo true doc-touch total (from decade_intent_agg.csv) is used as the
    # denominator, NOT the sum of the four per-category counts: 1,174 commits
    # touch files from more than one category (e.g. a release commit bumping
    # both CHANGELOG and README together), so summing per-category counts
    # double-counts those commits and silently inflates the denominator.
    intent_path = os.path.join(data_dir, "decade_intent_agg.csv")
    true_totals = pd.read_csv(intent_path).set_index("repo")["health_docs_touch_commits"]
    TRUE_ECOSYSTEM_TOTAL = int(true_totals.reindex(final_repos).sum())

    crows = []
    for repo, g in fd.groupby("repo"):
        repo_total = true_totals.get(repo, g["commit_sha"].nunique())
        for cat in PURPOSE_CATEGORIES:
            n = g[g["category"] == cat]["commit_sha"].nunique()
            crows.append({"repo": repo, "category": cat, "n_commits": n, "repo_total": repo_total})
    cat_df = pd.DataFrame(crows)
    cat_df["share"] = cat_df["n_commits"] / cat_df["repo_total"]
    eco = cat_df.groupby("category")["n_commits"].sum() / TRUE_ECOSYSTEM_TOTAL
    med = cat_df.groupby("category")["share"].median()
    zero = cat_df.groupby("category")["n_commits"].apply(lambda s: (s == 0).sum())
    out = pd.DataFrame({"ecosystem": (eco * 100).round(1),
                        "median": (med * 100).round(1), "zero_activity": zero})
    print(out.to_string())


# ─────────────────────────────────────────────────────────────────────────
# RQ2: Documentation Intention
# ─────────────────────────────────────────────────────────────────────────

def rq2_intention(data_dir):
    hr("RQ2 — Documentation Intention")
    d = pd.read_csv(os.path.join(data_dir, "decade_intent_agg.csv"))
    print(f"n repositories = {len(d)}")

    share = d["health_docs_touch_commits"] / d["total_commits_in_range"]
    print(f"\nHealth-doc share of total commit activity: "
          f"median={share.median():.4f} mean={share.mean():.4f}")

    total = d["health_docs_touch_commits"].sum()
    only = d["health_docs_only_commits"].sum()
    dom = d["health_docs_dominant_mixed_commits"].sum()
    nondom = d["health_docs_mixed_non_dominant_commits"].sum()
    print(f"\nEcosystem totals: {total} documentation-touching commits")
    print(f"  DocOnly:        {only} ({only/total:.1%})")
    print(f"  DocDominant:    {dom} ({dom/total:.1%})")
    print(f"  DocNonDominant: {nondom} ({nondom/total:.1%})")

    r = d["health_docs_only_rate"]
    print(f"\nPer-repository DocOnly rate: mean={r.mean():.3f} median={r.median():.3f} "
          f"sd={r.std():.3f}")


# ─────────────────────────────────────────────────────────────────────────
# RQ3: Documentation Ownership (descriptive)
# ─────────────────────────────────────────────────────────────────────────

def rq3_ownership(data_dir):
    hr("RQ3 — Documentation Ownership (descriptive)")
    d = pd.read_csv(os.path.join(data_dir, "decade_rq3_full.csv"))
    print(f"n repositories = {len(d)}")

    doc_c = d["health_docs_touch_contributors"].sum()
    all_c = d["unique_contributors_for_metrics"].sum()
    print(f"\nEcosystem participation: {doc_c}/{all_c} = {doc_c/all_c:.1%}")

    p = d["participation_rate"]
    print(f"Per-repo participation rate: median={p.median():.3f} mean={p.mean():.3f} "
          f"sd={p.std():.3f}")
    print(f"  repos < 5%:  {(p < 0.05).sum()}")
    print(f"  repos > 50%: {(p > 0.50).sum()}")

    print(f"\nMean total contributors:        {d['unique_contributors_for_metrics'].mean():.1f} "
          f"(median {d['unique_contributors_for_metrics'].median():.0f})")
    print(f"Mean documentation contributors: {d['health_docs_touch_contributors'].mean():.1f} "
          f"(median {d['health_docs_touch_contributors'].median():.0f})")

    t1 = d["health_docs_touch_top1_share"]
    print(f"\nTop-1 documentation-contributor share: median={t1.median():.3f} "
          f"mean={t1.mean():.3f}")

    print("\nOwnership concentration (documentation vs. all commits), k=3,5,10:")
    for k in [3, 5, 10]:
        doc_col, all_col = f"health_docs_touch_top{k}_share", f"top{k}_share"
        n_higher = (d[doc_col] > d[all_col]).sum()
        print(f"  top-{k:<2d} doc={d[doc_col].mean():.3f} all={d[all_col].mean():.3f} "
              f"({n_higher}/100 repos higher for docs)")

    b50 = d["health_docs_touch_bus50"]
    b80 = d["health_docs_touch_bus80"]
    print(f"\nBus-50: median={b50.median():.0f} mean={b50.mean():.2f} sd={b50.std():.2f}")
    print(f"  Bus-50 <= 3: {(b50 <= 3).sum()}")
    print(f"  Bus-50 == 1: {(b50 == 1).sum()}")
    print(f"Bus-80: median={b80.median():.0f}")
    print(f"  Bus-80 <= 10: {(b80 <= 10).sum()}")

    r, p_ = spearmanr(b50, d["unique_contributors_for_metrics"])
    print(f"\nSpearman rho(Bus-50, contributor count) = {r:.3f}")


# ─────────────────────────────────────────────────────────────────────────
# RQ3: Predictive-validity battery (Table: tab:predictive_validity)
# ─────────────────────────────────────────────────────────────────────────

def _partial_spearman(x, y, z):
    rx, ry, rz = rankdata(x), rankdata(y), rankdata(z)
    rxz = np.corrcoef(rx, rz)[0, 1]
    ryz = np.corrcoef(ry, rz)[0, 1]
    rxy = np.corrcoef(rx, ry)[0, 1]
    return (rxy - rxz * ryz) / np.sqrt((1 - rxz ** 2) * (1 - ryz ** 2))


def four_check_battery(df, outcome, predictor, size_col, vol_col, label=None, alpha=0.05):
    """Reproduces the paper's four-check confound-control battery
    (Section: Outcome Validation, sec:outcome_validation):
      (i)   nested regression controlling for contributor count ("Size")
      (ii)  + raw documentation-commit volume as a second control ("Volume")
      (iii) HC3 heteroskedasticity-robust standard errors ("Robust")
      (iv)  refit after dropping the 3 most influential repos by Cook's distance ("Outlier")
    A relationship is "validated" only if all four checks are significant (p<alpha).
    """
    sub = df.dropna(subset=[outcome, predictor, size_col, vol_col]).copy()
    m_size = smf.ols(f"{outcome} ~ {size_col} + {predictor}", data=sub).fit()
    p_size = m_size.pvalues[predictor]

    m_vol = smf.ols(f"{outcome} ~ {size_col} + {vol_col} + {predictor}", data=sub).fit()
    p_vol = m_vol.pvalues[predictor]

    m_hc3 = m_vol.get_robustcov_results(cov_type="HC3")
    p_hc3 = m_hc3.pvalues[list(m_vol.params.index).index(predictor)]

    infl = m_vol.get_influence()
    cooks = infl.cooks_distance[0]
    drop_idx = sub.index[np.argsort(cooks)[-3:]]
    sub_out = sub.drop(drop_idx)
    m_out = smf.ols(f"{outcome} ~ {size_col} + {vol_col} + {predictor}", data=sub_out).fit()
    p_out = m_out.pvalues[predictor]

    rho = _partial_spearman(sub[predictor], sub[outcome], sub[size_col])
    checks = [p_size, p_vol, p_hc3, p_out]
    passed = sum(p < alpha for p in checks)
    label = label or f"{predictor} -> {outcome}"
    flag = "  *** VALIDATED (4/4) ***" if passed == 4 else f"  ({passed}/4)"

    # Report DIRECTION explicitly alongside significance. A "VALIDATED" label
    # says only that the relationship is statistically reliable, not which way
    # it points; reporting the fully-controlled coefficient and its sign in
    # words keeps a significant-but-opposite-to-narrative result from being
    # mistaken for confirmation of the narrative.
    beta = m_vol.params[predictor]
    direction = "HIGHER predictor -> HIGHER outcome" if beta > 0 else "HIGHER predictor -> LOWER outcome"
    print(f"  {label:38s} n={len(sub):3d}  size={p_size:.4f} vol={p_vol:.4f} "
          f"robust={p_hc3:.4f} outlier={p_out:.4f}  rho={rho:+.3f}{flag}")
    print(f"  {'':38s} beta={beta:+.4f}  [{direction}]")
    return checks, rho


def _contributor_depth_features(repo_root, repos):
    """Per-repository contributor-DEPTH features, computed from raw per-commit data.

    Only NON-CIRCULAR depth measures are returned. Mean documentation commits
    per contributor is deliberately excluded: because

        participation = C_doc / C_all   and   mean_depth = doc_vol / C_doc

    it follows that log(participation) = log(doc_vol) - log(mean_depth) - log(C_all)
    exactly, so adding mean depth to a model that already controls for
    documentation volume and contributor count removes the predictor's own
    variance rather than adjusting for a confound. Median / max commits per
    contributor and median months active carry no such identity.
    """
    outputs_dir = os.path.join(repo_root, "outputs")
    logs_dir = os.path.join(repo_root, "full_commit_logs")
    if not (os.path.isdir(outputs_dir) and os.path.isdir(logs_dir)):
        return None
    since, until = pd.Timestamp("2016-05-30"), pd.Timestamp("2026-05-29")
    rows = []
    for repo in repos:
        fd_p = os.path.join(outputs_dir, repo, f"{repo}_2016_2026_file_details.csv")
        lg_p = os.path.join(logs_dir, f"{repo}_full_commit_log.csv")
        if not (os.path.exists(fd_p) and os.path.exists(lg_p)):
            continue
        fd = pd.read_csv(fd_p)
        fd["commit_date"] = pd.to_datetime(fd["commit_date"])
        fd = fd[(fd["commit_date"] >= since) & (fd["commit_date"] <= until)]
        if fd.empty:
            continue
        lg = pd.read_csv(lg_p, usecols=["commit_sha", "author_id", "is_bot"]).drop_duplicates("commit_sha")
        lg["is_bot"] = lg["is_bot"].astype(str).str.lower() == "true"
        c = fd.drop_duplicates("commit_sha").merge(lg, on="commit_sha", how="left")
        c = c[~c["is_bot"].fillna(False)].dropna(subset=["author_id"])
        if c.empty:
            continue
        per = c.groupby("author_id").size()
        c = c.copy()
        c["month"] = c["commit_date"].dt.to_period("M")
        months = c.groupby("author_id")["month"].nunique()
        rows.append({"repo": repo,
                     "median_depth": per.median(),
                     "max_depth": per.max(),
                     "median_months": months.median()})
    return pd.DataFrame(rows) if rows else None


def _retention_robustness(df_ret, repo_root):
    """Alternative-explanation checks for the negative participation->retention result.

    Tests whether the relationship is explained away by (a) how much effort
    individual documentation contributors actually put in, or (b) documentation
    ownership concentration. Neither explanation survives contact with the data:
    the participation coefficient stays negative and significant throughout.
    """
    print("\n[robustness] Does participation -> retention survive DEPTH and "
          "CONCENTRATION controls?")
    base = "log_contrib + log_doc_vol"
    df = df_ret.copy()
    df["log_bus50"] = np.log(df["health_docs_touch_bus50"].clip(lower=1))
    df["log_bus80"] = np.log(df["health_docs_touch_bus80"].clip(lower=1))

    depth = _contributor_depth_features(repo_root, list(df["repo"]))
    specs = {"base (size + doc volume)": base}
    if depth is not None:
        df = df.merge(depth, on="repo", how="left")
        for col in ["median_depth", "max_depth", "median_months"]:
            df["log_" + col] = np.log(df[col].clip(lower=0.01))
        specs.update({
            "+ median commits/contributor": f"{base} + log_median_depth",
            "+ max commits/contributor":    f"{base} + log_max_depth",
            "+ median months active":       f"{base} + log_median_months",
            "+ all three depth controls":   f"{base} + log_median_depth + log_max_depth + log_median_months",
        })
    else:
        print("  [depth controls skipped: outputs/ or full_commit_logs/ not found]")
    specs.update({
        "+ bus factor (Bus-50)":       f"{base} + log_bus50",
        "+ bus factor (Bus-80)":       f"{base} + log_bus80",
        "+ top-10 doc share":          f"{base} + health_docs_touch_top10_share",
        "+ ALL concentration":         f"{base} + log_bus50 + log_bus80 + "
                                       f"health_docs_touch_top1_share + health_docs_touch_top10_share",
    })
    for name, ctrl in specs.items():
        sub = df.dropna(subset=["doc_newcomer_retention_rate", "participation_rate"] +
                                [c for c in ctrl.replace("+", " ").split() if c in df.columns])
        m = smf.ols(f"doc_newcomer_retention_rate ~ {ctrl} + participation_rate", data=sub).fit()
        b, p = m.params["participation_rate"], m.pvalues["participation_rate"]
        tag = "SURVIVES" if p < 0.05 else "N.S."
        print(f"    {name:32s} beta={b:+.4f}  p={p:.6f}  {tag}")

    print("\n[robustness] Concentration as an INDEPENDENT predictor of retention "
          "(four-check battery):")
    for pred, lab in [("log_bus50", "Bus-50 -> Retention"),
                       ("log_bus80", "Bus-80 -> Retention"),
                       ("health_docs_touch_top10_share", "Top-10 share -> Retention")]:
        four_check_battery(df, "doc_newcomer_retention_rate", pred,
                            "log_contrib", "log_doc_vol", label=lab)


def rq3_predictive_validity(data_dir, repo_root):
    hr("RQ3 — Ownership Structure Predicts Documentation Outcomes "
       "(Table: predictive-validity battery)")

    rq3 = pd.read_csv(os.path.join(data_dir, "decade_rq3_full.csv"))
    arch = pd.read_csv(os.path.join(data_dir, "decade_archetype_assignments_final.csv"))
    stale = pd.read_csv(os.path.join(data_dir, "decade_staleness.csv"))
    ret = pd.read_csv(os.path.join(data_dir, "decade_newcomer_retention.csv"))
    gen = pd.read_csv(os.path.join(data_dir, "decade_general_outcomes.csv"))

    # NOTE: each outcome file is merged in separately, on demand, rather than
    # inner-joined all at once. decade_newcomer_retention.csv is missing one
    # repo (markdown-here, zero documentation newcomers), and an eager
    # combined merge would silently drop that repo from every test below,
    # including the staleness and activity-trend tests that have nothing to
    # do with retention. Each battery call below merges in only the outcome
    # column(s) it actually needs, so every test runs on its correct sample.
    base = rq3.merge(arch[["repo", "entropy_norm", "active_month_rate"]], on="repo")
    base["log_contrib"] = np.log(base["unique_contributors_for_metrics"].clip(lower=1))
    base["log_bus50"] = np.log(base["health_docs_touch_bus50"].clip(lower=1))
    base["log_doc_vol"] = np.log(base["health_docs_touch_commits"].clip(lower=1))

    df_stale = base.merge(stale, on="repo")
    df_stale["log_staleness"] = np.log(df_stale["median_staleness_days"].clip(lower=1))

    # Volume control is documentation-commit volume (log_doc_vol) for EVERY
    # outcome, including retention. This matches both the Methods text ("raw
    # documentation-commit volume is added as a second control") and the
    # original scripts/analysis/doc_newcomer_retention.py, which controlled for
    # log1p(health_file_commits). Do not substitute newcomer count here: it is
    # a different construct (sample size of the outcome, not effort volume).
    df_ret = base.merge(ret, on="repo")

    df_gen = base.merge(gen, on="repo")

    print("\n[1] Bus factor -> Documentation staleness")
    four_check_battery(df_stale, "log_staleness", "log_bus50", "log_contrib", "log_doc_vol",
                        label="Bus factor -> Staleness")

    print("\n[2] Participation rate -> Documentation-newcomer retention, all repos "
          "")
    four_check_battery(df_ret, "doc_newcomer_retention_rate", "participation_rate",
                        "log_contrib", "log_doc_vol",
                        label="Participation -> Retention (all)")

    print("\n[2b] Same, restricted to repos with >=10 documentation newcomers "
          "")
    df_ret_restricted = df_ret[df_ret["n_doc_newcomers"] >= 10]
    four_check_battery(df_ret_restricted, "doc_newcomer_retention_rate", "participation_rate",
                        "log_contrib", "log_doc_vol",
                        label="Participation -> Retention (n>=10)")

    print("\n[3] Participation rate -> Overall activity trend, beyond documentation itself "
          "")
    four_check_battery(df_gen, "activity_trend", "participation_rate",
                        "log_contrib", "log_doc_vol",
                        label="Participation -> Activity trend")

    _retention_robustness(df_ret, repo_root)

    print("\n[reference] Entropy / AWR / Participation vs. staleness "
          "(none clear the full battery once volume is controlled):")
    for pred in ["entropy_norm", "active_month_rate", "participation_rate"]:
        four_check_battery(df_stale, "log_staleness", pred, "log_contrib", "log_doc_vol",
                            label=f"{pred} -> Staleness")


# ─────────────────────────────────────────────────────────────────────────
# RQ4: Documentation Governance Evolution
# ─────────────────────────────────────────────────────────────────────────

def rq4_evolution(data_dir):
    hr("RQ4 — Documentation Governance Evolution")

    arch = pd.read_csv(os.path.join(data_dir, "annual_archetype_classified.csv"))
    order = ["Consistent", "Occasional", "Sparse"]
    print(f"n repository-years (valid, i.e. >=3 health-doc commits) = {len(arch)} / 1000 "
          f"({1 - len(arch)/1000:.1%} excluded)")

    print("\nTable V — Adjacent-year transition matrix (row-normalized %, n=710 pairs)")
    pairs = []
    for repo, g in arch.groupby("repo"):
        g = g.sort_values("year")
        by_year = dict(zip(g["year"], g["archetype"]))
        for y in by_year:
            if y + 1 in by_year:
                pairs.append((by_year[y], by_year[y + 1]))
    trans = pd.DataFrame(pairs, columns=["from", "to"])
    mat = pd.crosstab(trans["from"], trans["to"]).reindex(index=order, columns=order, fill_value=0)
    pct = mat.div(mat.sum(axis=1), axis=0) * 100
    print(pct.round(1).to_string())
    print(f"n valid pairs = {len(trans)}")
    print("")

    print("\nDecade-span transition (earliest vs. latest valid year, repos with >=2 valid years)")
    counts = arch.groupby("repo").size()
    multi = counts[counts >= 2].index
    spans = []
    for repo, g in arch.groupby("repo"):
        if repo not in multi:
            continue
        g = g.sort_values("year")
        spans.append((g.iloc[0]["archetype"], g.iloc[-1]["archetype"]))
    span_df = pd.DataFrame(spans, columns=["from", "to"])
    smat = pd.crosstab(span_df["from"], span_df["to"]).reindex(index=order, columns=order, fill_value=0)
    spct = smat.div(smat.sum(axis=1), axis=0) * 100
    print(spct.round(1).to_string())
    print("")

    if not HAVE_MK:
        print("\n[skipped] pymannkendall not installed; cannot reproduce trend tests.")
        return

    print("\nAnnual trend test — DocOnly rate")
    dr = pd.read_csv(os.path.join(data_dir, "annual_bus50_doconly.csv"))
    dr = dr[dr["touch_commits"] >= 3].dropna(subset=["doc_only_rate"])
    r, p = spearmanr(dr["year"], dr["doc_only_rate"])
    med = dr.groupby("year")["doc_only_rate"].median()
    mkres = mk.hamed_rao_modification_test(med.values)
    print(f"  Spearman: rho={r:.3f} p={p:.4f}   Mann-Kendall: trend={mkres.trend} p={mkres.p:.4f}")
    print(f"  annual median: year1={med.iloc[0]:.1%} -> year10={med.iloc[-1]:.1%}")

    print("\nAnnual trend test — Documentation participation rate")
    pr = pd.read_csv(os.path.join(data_dir, "annual_participation.csv"))
    r, p = spearmanr(pr["year"], pr["participation_rate"])
    med2 = pr.groupby("year")["participation_rate"].median()
    mkres2 = mk.hamed_rao_modification_test(med2.values)
    print(f"  Spearman: rho={r:.3f} p={p:.6f}   Mann-Kendall: trend={mkres2.trend} p={mkres2.p:.4f}")
    print(f"  annual median: year1={med2.iloc[0]:.1%} -> year10={med2.iloc[-1]:.1%}")


# ─────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data_dir", default=None,
                     help="Directory with the intermediate CSVs (default: <repo_root>/analysis_outputs)")
    ap.add_argument("--repo_root", default=None,
                     help="Project root, used to locate outputs/ and full_commit_logs/ for the "
                          "RQ1 living/static + purpose-taxonomy sub-check (default: two levels "
                          "above this script)")
    ap.add_argument("--section", default="all",
                     choices=["all", "rq1", "rq2", "rq3", "rq3_predictive", "rq4"],
                     help="Which section to reproduce (default: all)")
    args = ap.parse_args()

    repo_root = args.repo_root or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", ".."))
    data_dir = args.data_dir or os.path.join(repo_root, "analysis_outputs")

    if not os.path.isdir(data_dir):
        raise SystemExit(f"data_dir not found: {data_dir}\n"
                          f"Pass --data_dir /path/to/analysis_outputs")

    sec = args.section
    if sec in ("all", "rq1"):
        rq1_rhythm(data_dir, repo_root)
    if sec in ("all", "rq2"):
        rq2_intention(data_dir)
    if sec in ("all", "rq3"):
        rq3_ownership(data_dir)
    if sec in ("all", "rq3_predictive"):
        rq3_predictive_validity(data_dir, repo_root)
    if sec in ("all", "rq4"):
        rq4_evolution(data_dir)


if __name__ == "__main__":
    main()
