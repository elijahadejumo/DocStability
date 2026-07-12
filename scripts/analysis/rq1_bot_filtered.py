#!/usr/bin/env python3
"""
Rerun RQ1 rhythm archetypes with bot commits excluded from the doc-touch
commit set, using the full per-author commit log (which has bot
classification) joined against file_details.csv (doc-touching commits).

Doc_rhythm.py / doc_entropy.py never filter bots (unlike the ownership
pipeline) -- this recomputes entropy_norm / active_window_rate per repo
with bot-authored doc commits excluded, and re-clusters archetypes to see
how many of the artifact-stratification-flagged discordant repos resolve.
"""
import glob
import math
import os
from collections import defaultdict

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score

REPO_ROOT = "/Users/elijahadejumo/Documents/DocStability"
PER_REPO = os.path.join(REPO_ROOT, "per_repo")
LOGS_DIR = os.path.join(REPO_ROOT, "full_commit_logs")
OUT = os.path.join(REPO_ROOT, "analysis_outputs")

SINCE = (2020, 6)
UNTIL = (2025, 6)


def iter_month_keys(since_y, since_m, until_y, until_m):
    y, m = since_y, since_m
    out = []
    while (y < until_y) or (y == until_y and m <= until_m):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


MONTHS = iter_month_keys(*SINCE, *UNTIL)


def entropy_norm(counts):
    total = sum(counts)
    if total <= 0:
        return None
    if len(counts) <= 1:
        return 0.0
    H = 0.0
    for c in counts:
        if c <= 0:
            continue
        p = c / total
        H -= p * math.log(p)
    return max(0.0, min(1.0, H / math.log(len(counts))))


def active_window_rate(counts):
    return sum(1 for c in counts if c > 0) / len(counts)


def main():
    fd_files = glob.glob(os.path.join(PER_REPO, "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True)
    fd["commit_date"] = pd.to_datetime(fd["commit_date"])
    fd["month"] = fd["commit_date"].dt.strftime("%Y-%m")

    rows = []
    bot_commit_total = 0
    doc_commit_total = 0

    skipped = []
    for repo in sorted(fd["repo"].unique()):
        try:
            log_path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
            if not os.path.exists(log_path):
                print(f"WARNING: no full commit log for {repo}, skipping bot-filter")
                skipped.append(repo)
                continue
            log = pd.read_csv(log_path, usecols=["commit_sha", "is_bot"])
            log = log.drop_duplicates("commit_sha")
            log["is_bot"] = log["is_bot"].astype(str).str.lower() == "true"

            sub = fd[fd["repo"] == repo][["commit_sha", "commit_date", "month"]].drop_duplicates("commit_sha").reset_index(drop=True)
            merged = sub.merge(log, on="commit_sha", how="left")
            merged = merged.reset_index(drop=True)
            is_bot_col = merged["is_bot"]
            if isinstance(is_bot_col, pd.DataFrame):
                is_bot_col = is_bot_col.iloc[:, 0]
            is_bot_col = is_bot_col.fillna(False).astype(bool)
            merged["is_bot"] = is_bot_col
            unmatched = int(is_bot_col.isna().sum())

            doc_commit_total += len(merged)
            bot_commit_total += int(merged["is_bot"].sum())

            human = merged.loc[~merged["is_bot"].values]
            month_counts = defaultdict(int)
            for m in human["month"]:
                month_counts[m] += 1
            counts = [month_counts.get(mk, 0) for mk in MONTHS]
        except Exception as e:
            print(f"ERROR processing {repo}: {e!r}")
            skipped.append(repo)
            continue

        rows.append({
            "repo": repo,
            "doc_commits_total": len(merged),
            "doc_commits_bot": int(merged["is_bot"].sum()),
            "doc_commits_human": len(human),
            "unmatched_sha": int(unmatched),
            "entropy_norm_botfiltered": entropy_norm(counts),
            "awr_botfiltered": active_window_rate(counts),
        })

    df = pd.DataFrame(rows)
    print(f"Skipped repos: {skipped}")
    print(f"Total doc commits: {doc_commit_total}, bot-authored: {bot_commit_total} ({bot_commit_total/doc_commit_total:.2%})")
    print(f"Total unmatched SHAs (not found in full commit log): {df['unmatched_sha'].sum()}")

    combined = pd.read_csv(os.path.join(REPO_ROOT, "combined", "archetype_assignments.csv"))
    cmp_df = combined.merge(df, on="repo", how="left")

    valid = cmp_df.dropna(subset=["entropy_norm_botfiltered"])
    X = valid[["entropy_norm_botfiltered", "awr_botfiltered"]].values
    km = KMeans(n_clusters=3, n_init=10, random_state=42)
    labels = km.fit_predict(X)
    valid = valid.copy()
    valid["cluster_bf"] = labels
    order = valid.groupby("cluster_bf")["entropy_norm_botfiltered"].mean().sort_values(ascending=False).index.tolist()
    name_map = {order[0]: "Consistent", order[1]: "Occasional", order[2]: "Sparse"}
    valid["archetype_botfiltered"] = valid["cluster_bf"].map(name_map)

    print("\nBot-filtered archetype distribution:")
    print(valid["archetype_botfiltered"].value_counts())
    print("\nOriginal (non-bot-filtered) archetype distribution:")
    print(valid["archetype"].value_counts())

    ari = adjusted_rand_score(valid["archetype"], valid["archetype_botfiltered"])
    agree = (valid["archetype"] == valid["archetype_botfiltered"]).mean()
    print(f"\nAdjusted Rand Index (original vs bot-filtered): {ari:.3f}")
    print(f"Raw agreement: {agree:.1%}")

    changed = valid[valid["archetype"] != valid["archetype_botfiltered"]]
    print(f"\n{len(changed)} repos changed archetype after bot filtering:")
    print(changed[["repo", "doc_commits_total", "doc_commits_bot", "archetype", "archetype_botfiltered"]].to_string(index=False))

    valid.to_csv(os.path.join(OUT, "rq1_bot_filtered_archetypes.csv"), index=False)
    print(f"\nSaved: {OUT}/rq1_bot_filtered_archetypes.csv")


if __name__ == "__main__":
    main()
