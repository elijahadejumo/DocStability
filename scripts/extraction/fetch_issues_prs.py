#!/usr/bin/env python3
"""
Pull issue/PR resolution-latency data via GitHub GraphQL API for all 100
repos. New outcome type, not derivable from git history: does documentation
rhythm predict faster issue/PR resolution (a coordination-quality signal),
controlling for contributor count?

Scope: last 100 closed issues + last 100 merged PRs per repo (recency-
ordered, not exhaustive across the full 5-year window -- a representative
sample of current coordination latency, not a census). Budget: ~2-3 GraphQL
points per repo per type, well within the 5000/hour limit for 100 repos.
"""
import csv
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
      totalCount
      nodes { createdAt closedAt }
    }
    pullRequests(states: MERGED, first: 100, orderBy: {field: UPDATED_AT, direction: DESC}) {
      totalCount
      nodes { createdAt mergedAt }
    }
  }
}
"""


def run_query(owner, name, retries=3):
    for attempt in range(retries):
        r = requests.post(API_URL, headers=HEADERS, json={"query": QUERY, "variables": {"owner": owner, "name": name}})
        if r.status_code == 200:
            data = r.json()
            if "errors" in data and not data.get("data", {}).get("repository"):
                return None, data["errors"]
            return data.get("data", {}).get("repository"), None
        elif r.status_code in (502, 503):
            time.sleep(2 * (attempt + 1))
            continue
        else:
            return None, r.text
    return None, "max retries exceeded"


def main():
    with open(os.path.join(ROOT, "repos-names.csv"), encoding="utf-8-sig", newline="") as f:
        repo_rows = list(csv.DictReader(f))

    results = []
    errors = []
    for i, row in enumerate(repo_rows, 1):
        repo = row["repo"].strip()
        owner = row["owner"].strip()
        github_repo = (row.get("github_repo") or "").strip() or repo

        data, err = run_query(owner, github_repo)
        if err:
            print(f"[{i}/100] {owner}/{github_repo}: ERROR {err}")
            errors.append({"repo": repo, "error": str(err)})
            continue

        results.append({
            "repo": repo,
            "issues_total_closed": data["issues"]["totalCount"],
            "issues_sample": data["issues"]["nodes"],
            "prs_total_merged": data["pullRequests"]["totalCount"],
            "prs_sample": data["pullRequests"]["nodes"],
        })
        if i % 20 == 0:
            print(f"[{i}/100] processed")

    with open(os.path.join(OUT, "issues_prs_raw.json"), "w") as f:
        json.dump(results, f)
    print(f"\nSaved {len(results)} repos to issues_prs_raw.json")
    if errors:
        print(f"{len(errors)} errors: {[e['repo'] for e in errors]}")

    # check remaining rate limit
    r = requests.get("https://api.github.com/rate_limit", headers=HEADERS)
    gql = r.json()["resources"]["graphql"]
    print(f"GraphQL rate limit remaining: {gql['remaining']}/{gql['limit']}")


if __name__ == "__main__":
    main()
