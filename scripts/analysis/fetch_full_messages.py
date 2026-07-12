#!/usr/bin/env python3
"""
Fetch FULL commit messages (%B: subject + body) for just the doc-touching
commits (14,949 total, not the full 4.2M history) using the already-cloned
repos in _clones/. One batched `git show` call per repo.
"""
import glob
import os
import subprocess

import pandas as pd

ROOT = "/Users/elijahadejumo/Documents/DocStability"
CLONES = os.path.join(ROOT, "_clones")
OUT = os.path.join(ROOT, "analysis_outputs")

SEP1 = "\x1e"
SEP2 = "\x1d"


def fetch_repo_messages(repo, shas):
    clone_path = os.path.join(CLONES, repo)
    if not os.path.exists(clone_path):
        return {}
    fmt = f"--pretty=format:__C__%H{SEP1}%B{SEP2}"
    out = {}
    batch_size = 300
    for i in range(0, len(shas), batch_size):
        batch = shas[i:i + batch_size]
        cmd = ["git", "-C", clone_path, "show", "-s", fmt] + batch
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        if r.returncode != 0:
            continue
        for chunk in r.stdout.split("__C__"):
            chunk = chunk.strip(SEP2 + "\n")
            if not chunk or SEP1 not in chunk:
                continue
            sha, msg = chunk.split(SEP1, 1)
            out[sha.strip()] = msg.strip()
    return out


def main():
    fd_files = glob.glob(os.path.join(ROOT, "per_repo", "*", "*file_details.csv"))
    fd = pd.concat([pd.read_csv(f) for f in fd_files], ignore_index=True)
    fd = fd.drop_duplicates(["repo", "commit_sha"])

    rows = []
    for i, (repo, group) in enumerate(fd.groupby("repo"), 1):
        shas = group["commit_sha"].tolist()
        messages = fetch_repo_messages(repo, shas)
        for sha in shas:
            rows.append({"repo": repo, "commit_sha": sha, "full_message": messages.get(sha)})
        if i % 20 == 0:
            print(f"  {i}/100 repos processed")

    out_df = pd.DataFrame(rows)
    missing = out_df["full_message"].isna().sum()
    print(f"\nTotal: {len(out_df)}, missing: {missing}")
    out_df.to_csv(os.path.join(OUT, "doc_touch_full_messages.csv"), index=False)
    print(f"Saved: {OUT}/doc_touch_full_messages.csv")


if __name__ == "__main__":
    main()
