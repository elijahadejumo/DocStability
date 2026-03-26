#!/usr/bin/env python3
"""
reactive_github_api.py
──────────────────────
GitHub API-grounded reactive signal detection for health
documentation commits across OSS repositories.

For each commit in the health-doc-touching set T, classifies
it as reactive or non-reactive using three evidence tiers:

  Tier 1 (strongest): Commit is part of a merged PR whose
          closingIssuesReferences (GraphQL) is non-empty.
  Tier 2: Commit is part of a merged PR whose body contains
          GitHub-recognised closing keywords.
  Tier 3: Commit message itself contains closing keywords
          (direct push, no PR).

  Non-reactive: none of the above.

Usage:
    python3 reactive_github_api.py \
        --repos_csv  repos-names.csv \
        --repos_dir  /path/to/your/Repositories \
        --token      YOUR_GITHUB_TOKEN \
        --since      2020-06-30 \
        --until      2025-06-29 \
        --out_dir    ./reactive_api_outputs \
        --resume

repos_csv must have 'repo' and 'owner' columns.

Outputs per repo:
    {out_dir}/{repo}_commits.csv     — commit-level classification
    {out_dir}/{repo}_summary.csv     — single-row summary

Final aggregate:
    {out_dir}/combined_reactive_api.csv
"""

import os
import re
import time
import argparse
import subprocess
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd

# ── Logging ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────
GITHUB_REST    = 'https://api.github.com'
GITHUB_GRAPHQL = 'https://api.github.com/graphql'
RATE_LIMIT_PAUSE = 1.0
GRAPHQL_PAUSE    = 0.5
RETRY_AFTER      = 60

# GitHub's official closing keywords
CLOSING_PATTERN = re.compile(
    r'\b(close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s+'
    r'([\w\-\.]+/[\w\-\.]+)?#\d+',
    re.IGNORECASE,
)

# ── Health file patterns — identical to intention/rhythm scripts ──
# Apply EXCLUDE first, then match. This ensures the commit universe
# T is identical to RQ1 and RQ2.

ROOT_HEALTH_FILES = [
    r"^README(?:\..+)?$",
    r"^CONTRIBUTING(?:\..+)?$",
    r"^CONTRIBUTORS(?:\..+)?$",
    r"^COMMIT_CONVENTIONS(?:\..+)?$",
    r"^PULL_REQUEST_TEMPLATE(?:\..+)?$",
    r"^ISSUE_TEMPLATE(?:\..+)?$",
    r"^BUILDING(?:\..+)?$",
    r"^CHANGELOG(?:\..+)?$",
    r"^HISTORY(?:\..+)?$",
    r"^RELEASES?(?:\..+)?$",
    r"^CODE_OF_CONDUCT(?:\..+)?$",
    r"^GOVERNANCE(?:\..+)?$",
    r"^SUPPORT(?:\..+)?$",
    r"^MAINTAINERS(?:\..+)?$",
    r"^SECURITY(?:\..+)?$",
    r"^LICENSE(?:\..+)?$",
    r"^NOTICE(?:\..+)?$",
    r"^COPYING(?:\..+)?$",
    r"^AUTHORS(?:\..+)?$",
    r"^CREDITS(?:\..+)?$",
    r"^THANKS(?:\..+)?$",
    r"^ROADMAP(?:\..+)?$",
    r"^VISION(?:\..+)?$",
    r"^\.github/SECURITY(?:\..+)?$",
    r"^\.github/CONTRIBUTING(?:\..+)?$",
    r"^\.github/CODE_OF_CONDUCT(?:\..+)?$",
    r"^\.github/SUPPORT(?:\..+)?$",
    r"^\.github/COMMIT_CONVENTIONS(?:\..+)?$",
    r"^\.github/PULL_REQUEST_TEMPLATE(?:\..+)?$",
    r"^\.github/ISSUE_TEMPLATE(?:\..+)?$",
    r"^\.github/BUILDING(?:\..+)?$",
    r"^\.gitlab/CONTRIBUTING(?:\..+)?$",
    r"^\.gitlab/CODE_OF_CONDUCT(?:\..+)?$",
    r"^\.gitlab/COMMIT_CONVENTIONS(?:\..+)?$",
    r"^\.gitlab/PULL_REQUEST_TEMPLATE(?:\..+)?$",
    r"^\.gitlab/ISSUE_TEMPLATE(?:\..+)?$",
    r"^\.gitlab/BUILDING(?:\..+)?$",
]

EXCLUDE_PATTERNS = [
    r"^[^/]+/[^/]+/",
    r"^libs/",
    r"^modules/",
    r"^x-pack/",
    r"^plugins?/",
    r"^packages?/",
    r"^distribution/",
    r"^build-tools",
    r"^qa/",
    r"^test/",
    r"^benchmarks?/",
    r"^src/",
    r"/src/",
    r"(^|/)node_modules/",
    r"(^|/)vendor/",
    r"(^|/)dist/",
    r"(^|/)build/",
    r"(^|/)site/",
    r"(^|/)out/",
    r"(^|/)target/",
    r"(^|/)\.git/",
    r"(^|/)docs?/",
    r"(^|/)documentation/",
    r"(^|/)wiki/",
    r"(^|/)guides?/",
    r"(^|/)tutorials?/",
    r"(^|/)examples?/",
    r"(^|/)man/",
    r"(^|/)api/",
    r"(^|/)reference/",
    r"\.(png|jpg|jpeg|gif|svg|ico|pdf)$",
    r"\.(zip|tar|gz|bz2|7z|rar)$",
    r"\.(exe|dll|so|dylib|bin)$",
    r"\.(java|py|js|ts|go|rs|cpp|c|h|hpp)$",
    r"\.(scala|kt|swift|rb|php|cs|fs)$",
    r"(^|/)test/",
    r"(^|/)tests/",
    r"/resources/",
    r"\.(cef|json|xml|yaml|yml)\.txt$",
    r"(^|/)conf\.py$",
    r"(^|/)_config\.yml$",
    r"(^|/)mkdocs\.yml$",
    r"(^|/)Doxyfile$",
    r"output\.txt$",
    r"/translations?/",
    r"/i18n/",
    r"/locales?/",
    r"\.(zh|ja|ko|fr|de|es|it|pt|ru)\.md$",
]

HEALTH_RX  = [re.compile(p, re.IGNORECASE) for p in ROOT_HEALTH_FILES]
EXCLUDE_RX = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_PATTERNS]


# ══════════════════════════════════════════════════════════════
# GitHub API helpers
# ══════════════════════════════════════════════════════════════

def _headers(token):
    return {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json',
    }


def _rest_get(url, token, params=None):
    for attempt in range(4):
        try:
            r = requests.get(url, headers=_headers(token),
                             params=params, timeout=20)
            if r.status_code == 200:
                time.sleep(RATE_LIMIT_PAUSE)
                return r.json()
            elif r.status_code in (403, 429):
                wait = int(r.headers.get('Retry-After', RETRY_AFTER))
                log.warning(f'Rate limited — sleeping {wait}s')
                time.sleep(wait)
            elif r.status_code in (404, 422):
                return None
            else:
                log.warning(f'HTTP {r.status_code} for {url}')
                time.sleep(5 * (attempt + 1))
        except requests.RequestException as e:
            log.warning(f'Request error: {e}, retry {attempt + 1}')
            time.sleep(10)
    return None


def _graphql(query, variables, token):
    for attempt in range(4):
        try:
            r = requests.post(
                GITHUB_GRAPHQL,
                headers={**_headers(token),
                         'Content-Type': 'application/json'},
                json={'query': query, 'variables': variables},
                timeout=20,
            )
            if r.status_code == 200:
                data = r.json()
                time.sleep(GRAPHQL_PAUSE)
                if 'errors' in data:
                    log.warning(f'GraphQL errors: {data["errors"]}')
                    return None
                return data.get('data')
            elif r.status_code in (403, 429):
                wait = int(r.headers.get('Retry-After', RETRY_AFTER))
                log.warning(f'GraphQL rate limited — sleeping {wait}s')
                time.sleep(wait)
            else:
                time.sleep(5 * (attempt + 1))
        except requests.RequestException as e:
            log.warning(f'GraphQL error: {e}, retry {attempt + 1}')
            time.sleep(10)
    return None


PR_CLOSING_ISSUES_QUERY = """
query($owner: String!, $repo: String!, $pr: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $pr) {
      merged
      body
      closingIssuesReferences(first: 10) {
        totalCount
      }
    }
  }
}
"""


def get_prs_for_commit(owner, repo, sha, token):
    url = f'{GITHUB_REST}/repos/{owner}/{repo}/commits/{sha}/pulls'
    result = _rest_get(url, token, params={'per_page': 100, 'state': 'closed'})
    return result if isinstance(result, list) else []


def pr_closes_issue(owner, repo, pr_number, token, cache):
    key = f'{owner}/{repo}#{pr_number}'
    if key in cache:
        return cache[key]

    data = _graphql(PR_CLOSING_ISSUES_QUERY,
                    {'owner': owner, 'repo': repo, 'pr': pr_number},
                    token)

    tier1, tier2 = False, False
    if data:
        pr = (data.get('repository') or {}).get('pullRequest') or {}
        if pr.get('merged'):
            tier1 = (pr.get('closingIssuesReferences') or {}).get('totalCount', 0) > 0
            tier2 = bool(CLOSING_PATTERN.search(pr.get('body') or ''))

    cache[key] = (tier1, tier2)
    return tier1, tier2


# ══════════════════════════════════════════════════════════════
# Git helpers
# ══════════════════════════════════════════════════════════════

def is_health_doc_file(filepath):
    """
    Exclude-first then match — identical logic to the intention
    and rhythm scripts so the commit universe T is consistent
    across all three RQs.
    """
    p = filepath.replace('\\', '/')
    if any(rx.search(p) for rx in EXCLUDE_RX):
        return False
    return any(rx.match(p) for rx in HEALTH_RX)


def get_health_doc_commits(repo_path, since, until):
    """
    Extract commits touching health doc files using git log.

    git log --name-only --format=COMMIT|... produces this structure:

        COMMIT|sha|email|date|subject
        <blank line>            <- git always inserts this after format line
        file1.txt
        file2.txt
        <blank line>            <- separates commits

    The blank line immediately after COMMIT| must be absorbed without
    resetting state, otherwise all file names are missed.
    """
    cmd = [
        'git', '-C', repo_path, 'log',
        '--no-merges',
        f'--since={since} 00:00:00',
        f'--until={until} 23:59:59',
        '--name-only',
        '--format=COMMIT|%H|%ae|%ct|%s',
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300
        )
    except subprocess.TimeoutExpired:
        log.error(f'git log timed out for {repo_path}')
        return []

    if result.returncode != 0:
        log.warning(f'git log error: {result.stderr[:200]}')
        return []

    commits    = []
    current    = None
    skip_blank = False  # absorb the blank line git puts after COMMIT line

    for line in result.stdout.splitlines():
        stripped = line.strip()

        # ── New commit header ──────────────────────────────────
        if stripped.startswith('COMMIT|'):
            if current and current['health_doc']:
                commits.append(current)
            parts = stripped.split('|', 4)
            # parts[3] is %ct (unix committer timestamp) — convert to ISO
            try:
                dt = datetime.fromtimestamp(int(parts[3]), tz=timezone.utc)
                date_str = dt.date().isoformat()
            except (ValueError, IndexError):
                date_str = parts[3] if len(parts) > 3 else ''
            current = {
                'sha':        parts[1],
                'email':      parts[2],
                'date':       date_str,
                'message':    parts[4] if len(parts) > 4 else '',
                'health_doc': False,
            }
            skip_blank = True   # next blank is formatting, not a separator
            continue

        # ── Blank line ─────────────────────────────────────────
        if not stripped:
            if skip_blank:
                skip_blank = False  # absorb and keep current
                continue
            # Real inter-commit separator
            if current and current['health_doc']:
                commits.append(current)
            current = None
            continue

        # ── File name line ─────────────────────────────────────
        skip_blank = False
        if current is not None and is_health_doc_file(stripped):
            current['health_doc'] = True

    # Catch final commit if output has no trailing blank line
    if current and current['health_doc']:
        commits.append(current)

    return commits


# ══════════════════════════════════════════════════════════════
# Per-commit classification
# ══════════════════════════════════════════════════════════════

def classify_commit(owner, repo, commit, token, pr_cache):
    sha     = commit['sha']
    message = commit['message']

    reactive      = False
    reactive_tier = None
    pr_number     = None

    # ── Check PRs containing this commit ──────────────────────
    prs = get_prs_for_commit(owner, repo, sha, token)
    for pr in prs:
        if pr.get('merged_at') is None:
            continue
        pr_num    = pr.get('number')
        pr_number = pr_num
        t1, t2    = pr_closes_issue(owner, repo, pr_num, token, pr_cache)
        if t1:
            reactive, reactive_tier = True, 'tier1_closing_issues_api'
            break
        elif t2 and not reactive:
            reactive, reactive_tier = True, 'tier2_pr_body_keyword'

    # ── Fallback: closing keyword in commit message ────────────
    if not reactive and CLOSING_PATTERN.search(message):
        reactive      = True
        reactive_tier = 'tier3_commit_message_keyword'

    return {
        **commit,
        'reactive':      reactive,
        'reactive_tier': reactive_tier or 'non_reactive',
        'pr_number':     pr_number,
    }


# ══════════════════════════════════════════════════════════════
# Per-repo processing
# ══════════════════════════════════════════════════════════════

def process_repo(owner, repo, repo_path, since, until,
                 token, out_dir, resume):
    commits_file = out_dir / f'{repo}_commits.csv'
    summary_file = out_dir / f'{repo}_summary.csv'

    if resume and summary_file.exists():
        log.info(f'[{repo}] Already processed — skipping.')
        df = pd.read_csv(summary_file)
        return df.iloc[0].to_dict() if len(df) else None

    log.info(f'[{repo}] Extracting health doc commits …')
    commits = get_health_doc_commits(repo_path, since, until)
    log.info(f'[{repo}] Found {len(commits)} health doc commits.')

    if not commits:
        summary = {
            'repo': repo, 'owner': owner,
            'health_doc_commits': 0,
            'reactive_total': 0, 'reactive_rate': None,
            'tier1_count': 0, 'tier2_count': 0,
            'tier3_count': 0, 'non_reactive_count': 0,
        }
        pd.DataFrame([summary]).to_csv(summary_file, index=False)
        return summary

    pr_cache   = {}
    classified = []
    for i, commit in enumerate(commits, 1):
        log.info(
            f'[{repo}] Classifying {i}/{len(commits)}: '
            f'{commit["sha"][:10]}'
        )
        result = classify_commit(owner, repo, commit, token, pr_cache)
        classified.append(result)

    pd.DataFrame(classified).to_csv(commits_file, index=False)

    total    = len(classified)
    reactive = sum(1 for c in classified if c['reactive'])
    t1 = sum(1 for c in classified
             if c['reactive_tier'] == 'tier1_closing_issues_api')
    t2 = sum(1 for c in classified
             if c['reactive_tier'] == 'tier2_pr_body_keyword')
    t3 = sum(1 for c in classified
             if c['reactive_tier'] == 'tier3_commit_message_keyword')

    summary = {
        'repo': repo, 'owner': owner,
        'health_doc_commits': total,
        'reactive_total': reactive,
        'reactive_rate': round(reactive / total, 4) if total else None,
        'tier1_count': t1, 'tier2_count': t2,
        'tier3_count': t3, 'non_reactive_count': total - reactive,
    }
    pd.DataFrame([summary]).to_csv(summary_file, index=False)
    log.info(
        f'[{repo}] Done — reactive={reactive}/{total} '
        f'(T1={t1} T2={t2} T3={t3})'
    )
    return summary


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='GitHub API reactive signal detection'
    )
    parser.add_argument('--repos_csv', required=True,
                        help='CSV with repo and owner columns')
    parser.add_argument('--repos_dir', required=True,
                        help='Directory containing cloned repos')
    parser.add_argument('--token',     required=True,
                        help='GitHub personal access token')
    parser.add_argument('--since',     default='2020-06-30',
                        help='Start date YYYY-MM-DD (default: 2020-06-30)')
    parser.add_argument('--until',     default='2025-06-29',
                        help='End date YYYY-MM-DD (default: 2025-06-29)')
    parser.add_argument('--out_dir',   default='./reactive_api_outputs',
                        help='Output directory')
    parser.add_argument('--resume',    action='store_true',
                        help='Skip repos already processed')
    parser.add_argument('--repos',     nargs='+', default=None,
                        help='Process only these repos (optional)')
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Load repos and owners from single CSV ──────────────────
    df = pd.read_csv(args.repos_csv)
    if 'repo' not in df.columns or 'owner' not in df.columns:
        raise ValueError(
            f"CSV must have 'repo' and 'owner' columns. "
            f"Found: {df.columns.tolist()}"
        )
    repo_names = df['repo'].str.strip().tolist()
    owners     = dict(zip(df['repo'].str.strip(), df['owner'].str.strip()))
    log.info(f'Loaded {len(repo_names)} repos from {args.repos_csv}')

    if args.repos:
        repo_names = [r for r in repo_names if r in args.repos]
        log.info(f'Filtered to {len(repo_names)} repos.')

    summaries, failures = [], []

    for repo in repo_names:
        owner     = owners.get(repo)
        repo_path = os.path.join(args.repos_dir, repo)

        if not os.path.isdir(os.path.join(repo_path, '.git')):
            log.warning(f'[{repo}] Not a git repo at {repo_path}')
            failures.append((repo, 'not_found'))
            continue

        try:
            summary = process_repo(
                owner=owner, repo=repo, repo_path=repo_path,
                since=args.since, until=args.until,
                token=args.token, out_dir=out_dir, resume=args.resume,
            )
            if summary:
                summaries.append(summary)
        except Exception as e:
            log.error(f'[{repo}] Failed: {e}')
            failures.append((repo, str(e)))

    # ── Write combined output ──────────────────────────────────
    if summaries:
        df_out   = pd.DataFrame(summaries)
        out_path = out_dir / 'combined_reactive_api.csv'
        df_out.to_csv(out_path, index=False)

        total_commits  = df_out['health_doc_commits'].sum()
        total_reactive = df_out['reactive_total'].sum()

        log.info('=' * 55)
        log.info(f'Repos processed  : {len(summaries)}')
        log.info(f'Repos failed     : {len(failures)}')
        log.info(f'Total doc commits: {total_commits:,}')
        if total_commits > 0:
            log.info(
                f'Reactive (API)   : {total_reactive:,} '
                f'({total_reactive / total_commits * 100:.1f}%)'
            )
            log.info(f'  Tier 1 (closing issues API): {df_out["tier1_count"].sum():,}')
            log.info(f'  Tier 2 (PR body keyword)   : {df_out["tier2_count"].sum():,}')
            log.info(f'  Tier 3 (commit msg keyword): {df_out["tier3_count"].sum():,}')
        log.info(f'Output           : {out_path}')
        log.info('=' * 55)

    if failures:
        log.warning('Failed repos:')
        for repo, reason in failures:
            log.warning(f'  {repo}: {reason}')


if __name__ == '__main__':
    main()