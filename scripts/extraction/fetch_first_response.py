#!/usr/bin/env python3
"""
Time-to-FIRST-RESPONSE (not close time, which was already tested and null):
for the same sample of recent closed issues per repo, fetch the first
comment's timestamp and compute (first_comment_createdAt - issue_createdAt).
A distinct construct from resolution latency -- measures whether someone
shows up to acknowledge an issue, not how long the full fix takes.
"""
import json
import os
import time

import requests

ROOT = "/Users/elijahadejumo/Documents/DocStability"
OUT = os.path.join(ROOT, "analysis_outputs")

with open(os.path.join(ROOT, ".env")) as f:
    for line in f:
        if line.startswith("GITHUB_TOKEN"):
            TOKEN = line.strip().split("=", 1)[1]

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
API_URL = "https://api.github.com/graphql"

QUERY = """
query($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    issues(states: CLOSED, first: 100, orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        createdAt
        authorAssociation
        comments(first: 1) {
          totalCount
          nodes { createdAt }
        }
      }
    }
  }
}
"""


def run_query(owner, name, retries=5):
    for attempt in range(retries):
        r = requests.post(API_URL, headers=HEADERS, json={"query": QUERY, "variables": {"owner": owner, "name": name}})
        if r.status_code == 200:
            data = r.json()
            if "errors" in data and not data.get("data", {}).get("repository"):
                return None, data["errors"]
            return data.get("data", {}).get("repository"), None
        elif r.status_code == 403 or "secondary rate limit" in r.text.lower():
            wait = 60 * (attempt + 1)
            print(f"  secondary rate limit hit, waiting {wait}s...", flush=True)
            time.sleep(wait)
            continue
        elif r.status_code in (502, 503):
            time.sleep(2 * (attempt + 1))
            continue
        else:
            return None, r.text
    return None, "max retries"


def main():
    import csv
    with open(os.path.join(ROOT, "repos-names.csv"), encoding="utf-8-sig", newline="") as f:
        repo_rows = list(csv.DictReader(f))

    out_path = os.path.join(OUT, "first_response_raw.json")
    results = []
    done_repos = set()
    if os.path.exists(out_path):
        results = json.load(open(out_path))
        done_repos = {r["repo"] for r in results}
        print(f"Resuming: {len(done_repos)} already done")

    errors = []
    for i, row in enumerate(repo_rows, 1):
        repo = row["repo"].strip()
        if repo in done_repos:
            continue
        owner = row["owner"].strip()
        github_repo = (row.get("github_repo") or "").strip() or repo

        data, err = run_query(owner, github_repo)
        if err:
            print(f"[{i}/100] {owner}/{github_repo}: ERROR {str(err)[:150]}")
            errors.append({"repo": repo, "error": str(err)})
            time.sleep(3)
            continue

        results.append({"repo": repo, "issues": data["issues"]["nodes"]})
        with open(out_path, "w") as f:
            json.dump(results, f)
        if i % 10 == 0:
            print(f"[{i}/100] processed", flush=True)
        time.sleep(1.5)  # stay well under secondary rate limit thresholds

    print(f"\nSaved {len(results)} repos total")
    if errors:
        print(f"{len(errors)} errors this run: {[e['repo'] for e in errors]}")

    r = requests.get("https://api.github.com/rate_limit", headers=HEADERS)
    gql = r.json()["resources"]["graphql"]
    print(f"GraphQL rate limit remaining: {gql['remaining']}/{gql['limit']}")


if __name__ == "__main__":
    main()
