#!/usr/bin/env python3
"""
Rebuild the entropy/AWR/participation/staleness predictors restricted to
LIVING/PROCESS documentation only (README, CONTRIBUTING, CHANGELOG, HISTORY,
RELEASE(S), GOVERNANCE, SECURITY, SUPPORT, MAINTAINERS, CODE_OF_CONDUCT,
ROADMAP, VISION, BUILDING, COMMIT_CONVENTIONS, templates) -- excluding
static/attribution docs (LICENSE, NOTICE, COPYING, AUTHORS, CREDITS, THANKS,
CONTRIBUTORS) which are write-once by nature and may dilute the signal.

Fresh build on the CURRENT canonical dataset (post data-quality-audit,
bot-filtered, includes vscode/excludes invalid data-owid), unlike the
artifact_stratified_rhythm.csv from earlier in the session which predates
those fixes.
"""
import glob
import math
import os
from collections import defaultdict

import numpy as np
import pandas as pd

ROOT = "/Users/elijahadejumo/Documents/DocStability"
LOGS_DIR = os.path.join(ROOT, "full_commit_logs")
OUT = os.path.join(ROOT, "analysis_outputs")

SINCE = (2020, 6)
UNTIL_YM = (2025, 6)
UNTIL_TS = pd.Timestamp("2025-06-29")

LIVING_STEMS = {
    "readme", "contributing", "changelog", "history", "release", "releases",
    "pull_request_template", "issue_template", "commit_conventions", "building",
    "code_of_conduct", "governance", "support", "maintainers", "security",
    "roadmap", "vision",
}


def classify(path):
    basename = path.replace("\\", "/").split("/")[-1].lower()
    stem = basename.split(".")[0]
    return "living" if stem in LIVING_STEMS else "other"


def iter_month_keys(since, until):
    y, m = since
    out = []
    while (y < until[0]) or (y == until[0] and m <= until[1]):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m, y = 1, y + 1
    return out


MONTHS = iter_month_keys(SINCE, UNTIL_YM)


def entropy_norm(counts):
    total = sum(counts)
    if total <= 0:
        return None
    if len(counts) <= 1:
        return 0.0
    H = 0.0
    for c in counts:
        if c > 0:
            p = c / total
            H -= p * math.log(p)
    return max(0.0, min(1.0, H / math.log(len(counts))))


def main():
    fd_files = glob.glob(os.path.join(ROOT, "per_repo", "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True)
    fd["commit_date"] = pd.to_datetime(fd["commit_date"])
    fd["month"] = fd["commit_date"].dt.strftime("%Y-%m")
    fd["category"] = fd["health_file"].apply(classify)
    living = fd[fd["category"] == "living"].copy()

    print(f"Total rows: {len(fd)}, living rows: {len(living)} ({len(living)/len(fd):.1%})")
    print(f"Repos with living-doc activity: {living['repo'].nunique()}")

    rows = []
    for repo, group in living.groupby("repo"):
        log_path = os.path.join(LOGS_DIR, f"{repo}_full_commit_log.csv")
        author_map = {}
        if os.path.exists(log_path):
            log = pd.read_csv(log_path, usecols=["commit_sha", "author_id", "is_bot"]).drop_duplicates("commit_sha")
            log["is_bot"] = log["is_bot"].astype(str).str.lower() == "true"
            author_map = log.set_index("commit_sha")[["author_id", "is_bot"]].to_dict("index")

        commits = group.drop_duplicates("commit_sha").copy()
        commits["is_bot"] = commits["commit_sha"].map(lambda s: author_map.get(s, {}).get("is_bot", False))
        commits["author_id"] = commits["commit_sha"].map(lambda s: author_map.get(s, {}).get("author_id"))
        human_commits = commits[~commits["is_bot"]]

        month_counts = defaultdict(int)
        for sha, month in human_commits[["commit_sha", "month"]].drop_duplicates().values:
            month_counts[month] += 1
        counts = [month_counts.get(mk, 0) for mk in MONTHS]
        awr = sum(1 for c in counts if c > 0) / len(counts)
        ent = entropy_norm(counts)

        n_living_contributors = human_commits["author_id"].nunique()

        last_touch = group.groupby("health_file")["commit_date"].max()
        staleness_days = (UNTIL_TS - last_touch).dt.days

        rows.append({
            "repo": repo,
            "living_n_commits_human": len(human_commits),
            "living_entropy_norm": ent,
            "living_awr": awr,
            "living_n_contributors": n_living_contributors,
            "living_median_staleness_days": staleness_days.median(),
            "living_n_files_touched": len(last_touch),
        })

    df = pd.DataFrame(rows)
    contrib = pd.read_csv(os.path.join(ROOT, "combined", "combined_contributors.csv"))[
        ["repo", "unique_contributors_for_metrics"]
    ]
    df = df.merge(contrib, on="repo", how="left")
    df["living_participation_rate"] = df["living_n_contributors"] / df["unique_contributors_for_metrics"]
    df["log_contributors"] = np.log1p(df["unique_contributors_for_metrics"])
    df["log_living_commits"] = np.log1p(df["living_n_commits_human"])
    df["log_living_staleness"] = np.log1p(df["living_median_staleness_days"])

    df.to_csv(os.path.join(OUT, "process_docs_only_dataset.csv"), index=False)
    print(f"\nSaved: {OUT}/process_docs_only_dataset.csv, n={len(df)}")
    print(df[["living_entropy_norm", "living_awr", "living_participation_rate", "living_median_staleness_days"]].describe())


if __name__ == "__main__":
    main()
