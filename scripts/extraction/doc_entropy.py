#!/usr/bin/env python3
"""
health_docs_entropy_aligned.py

Entropy + concentration metrics for PROJECT HEALTH FILES, aligned with
doc_touch_rhythm.py counting semantics:

- Uses `git log --name-only` (same as CV script)
- Same health file include/exclude rules
- Same merge handling flag (--include_merges)
- Monthly distribution across [since..until] months

Outputs:
  outputs/<repo_name>/<out_prefix>_health_docs_monthly_distribution.csv
  outputs/<repo_name>/<out_prefix>_health_docs_entropy_summary.csv
  outputs/<repo_name>/<out_prefix>_summary.json
Optional:
  outputs/<repo_name>/<out_prefix>_health_docs_touch_shas.txt

Run:
  python3 health_docs_entropy_aligned.py \
    --repo ./airflow \
    --since 2020-06-30 \
    --until 2025-06-29 \
    --out_prefix airflow_2020_2025 \
    --write_sha_list
"""

from __future__ import annotations
import argparse, csv, json, math, os, re, subprocess
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Sequence, Tuple
from collections import defaultdict
from pathlib import Path

# ----------------------------
# SAME HEALTH FILE RULES (COPY-PASTE FROM YOUR CV SCRIPT)
# ----------------------------

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


@dataclass(frozen=True)
class CommitRecord:
    sha: str
    commit_dt: datetime
    files: Tuple[str, ...]


# ----------------------------
# Git helpers (SAME STYLE AS YOUR CV SCRIPT)
# ----------------------------

def run_git(repo: str, args: List[str]) -> str:
    cmd = ["git", "-C", repo] + args
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    return out.decode("utf-8", errors="replace")


def iter_commits_with_files(repo: str, since: date, until: date, include_merges: bool) -> Iterator[CommitRecord]:
    log_args = [
        "log",
        "--no-color",
        f"--since={since.isoformat()} 00:00:00",
        f"--until={until.isoformat()} 23:59:59",
        "--pretty=format:__COMMIT__%H\t%ct",
        "--name-only",
    ]
    if not include_merges:
        log_args.insert(1, "--no-merges")

    raw = run_git(repo, log_args)

    sha = None
    epoch = None
    files: List[str] = []

    for line in raw.splitlines():
        if line.startswith("__COMMIT__"):
            if sha is not None and epoch is not None:
                dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
                yield CommitRecord(sha=sha, commit_dt=dt, files=tuple(f for f in files if f))

            files = []
            header = line[len("__COMMIT__"):]
            parts = header.split("\t")
            if len(parts) != 2:
                sha, epoch = None, None
                continue
            sha = parts[0].strip()
            try:
                epoch = int(parts[1].strip())
            except ValueError:
                sha, epoch = None, None
        else:
            if sha is not None:
                s = line.strip()
                if s:
                    files.append(s)

    if sha is not None and epoch is not None:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        yield CommitRecord(sha=sha, commit_dt=dt, files=tuple(f for f in files if f))


# ----------------------------
# Health file detection (MATCH YOUR CV SCRIPT)
# ----------------------------

def compile_health_patterns() -> Tuple[List[re.Pattern], List[re.Pattern]]:
    health_rx = [re.compile(p, re.IGNORECASE) for p in ROOT_HEALTH_FILES]
    excl_rx = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_PATTERNS]
    return health_rx, excl_rx


def is_excluded(path: str, excl_rx: Sequence[re.Pattern]) -> bool:
    p = path.replace("\\", "/")
    return any(rx.search(p) for rx in excl_rx)


def is_health_file(path: str, health_rx: Sequence[re.Pattern]) -> bool:
    p = path.replace("\\", "/")
    return any(rx.match(p) for rx in health_rx)


def get_health_files(files: Sequence[str], health_rx: Sequence[re.Pattern], excl_rx: Sequence[re.Pattern]) -> List[str]:
    out = []
    for f in files:
        if is_excluded(f, excl_rx):
            continue
        if is_health_file(f, health_rx):
            out.append(f)
    return out


# ----------------------------
# Monthly skeleton + entropy/concentration
# ----------------------------

def iter_month_keys(since: date, until: date) -> List[str]:
    y, m = since.year, since.month
    ey, em = until.year, until.month
    out: List[str] = []
    while (y < ey) or (y == ey and m <= em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def entropy_norm(counts: List[int]) -> Optional[float]:
    total = sum(counts)
    if total <= 0:
        return None
    M = len(counts)
    if M <= 1:
        return 0.0
    H = 0.0
    for c in counts:
        if c <= 0:
            continue
        p = c / total
        H -= p * math.log(p)
    return max(0.0, min(1.0, H / math.log(M)))


def top_k_share(counts: List[int], k: int) -> Optional[float]:
    total = sum(counts)
    if total <= 0:
        return None
    return sum(sorted(counts, reverse=True)[:k]) / total


def gini_from_counts(counts: List[int]) -> Optional[float]:
    total = sum(counts)
    if total <= 0:
        return None
    xs = sorted(counts)
    n = len(xs)
    if n == 0:
        return None
    cum = 0
    for i, x in enumerate(xs, start=1):
        cum += i * x
    g = (2 * cum) / (n * total) - (n + 1) / n
    return float(g)


# ----------------------------
# CLI + main
# ----------------------------

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    ap = argparse.ArgumentParser(description="Entropy/concentration for health docs (aligned with doc_touch_rhythm.py)")
    ap.add_argument("--repo", required=True, help="Path to local git repo")
    ap.add_argument("--since", required=True, type=parse_date, help="Start date YYYY-MM-DD")
    ap.add_argument("--until", required=True, type=parse_date, help="End date YYYY-MM-DD")
    ap.add_argument("--include_merges", action="store_true", help="Include merge commits (default: exclude)")
    ap.add_argument("--out_prefix", default="health_docs_entropy", help="Output file prefix")
    ap.add_argument("--write_sha_list", action="store_true", help="Write list of health-doc-touch commit SHAs")
    ap.add_argument("--write_probabilities", action="store_true", help="Also write month probability distribution column")
    args = ap.parse_args()

    repo = os.path.abspath(args.repo)
    repo_name = os.path.basename(repo)
    out_dir = Path("outputs") / repo_name
    out_dir.mkdir(parents=True, exist_ok=True)

    health_rx, excl_rx = compile_health_patterns()

    months = iter_month_keys(args.since, args.until)
    month_counts: Dict[str, int] = {mk: 0 for mk in months}

    total_commits_in_range = 0
    health_touch_commits = 0
    touch_shas: List[str] = []

    for cr in iter_commits_with_files(repo, args.since, args.until, include_merges=args.include_merges):
        total_commits_in_range += 1
        health_files = get_health_files(cr.files, health_rx, excl_rx)
        if not health_files:
            continue
        health_touch_commits += 1
        touch_shas.append(cr.sha)

        mk = f"{cr.commit_dt.year:04d}-{cr.commit_dt.month:02d}"
        if mk in month_counts:
            month_counts[mk] += 1

    counts_list = [month_counts[m] for m in months]
    total_months = len(months)
    active_months = sum(1 for c in counts_list if c > 0)
    active_month_rate = active_months / total_months if total_months else 0.0

    Hn = entropy_norm(counts_list)
    top1 = top_k_share(counts_list, 1)
    top3 = top_k_share(counts_list, 3)
    top6 = top_k_share(counts_list, 6)
    gini = gini_from_counts(counts_list)

    # ---- monthly distribution ----
    dist_path = out_dir / f"{args.out_prefix}_monthly_distribution.csv"
    with dist_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if args.write_probabilities:
            w.writerow(["repo", "month", "health_file_commit_count", "p_month"])
            denom = sum(counts_list)
            for mk in months:
                c = month_counts[mk]
                p = (c / denom) if denom > 0 else ""
                w.writerow([repo_name, mk, c, f"{p:.10f}" if denom > 0 else ""])
        else:
            w.writerow(["repo", "month", "health_file_commit_count"])
            for mk in months:
                w.writerow([repo_name, mk, month_counts[mk]])

    # ---- summary CSV ----
    summ_path = out_dir / f"{args.out_prefix}_entropy_summary.csv"
    with summ_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "repo","since","until","months",
            "include_merges",
            "total_commits_in_range",
            "health_file_commits",
            "active_months","active_month_rate",
            "entropy_norm",
            "top1_month_share","top3_month_share","top6_month_share",
            "gini_month_concentration",
        ])
        w.writerow([
            repo_name, args.since.isoformat(), args.until.isoformat(), total_months,
            "yes" if args.include_merges else "no",
            total_commits_in_range,
            health_touch_commits,
            active_months, f"{active_month_rate:.6f}",
            "" if Hn is None else f"{Hn:.6f}",
            "" if top1 is None else f"{top1:.6f}",
            "" if top3 is None else f"{top3:.6f}",
            "" if top6 is None else f"{top6:.6f}",
            "" if gini is None else f"{gini:.6f}",
        ])

    # ---- optional SHA list ----
    sha_path = None
    if args.write_sha_list:
        sha_path = out_dir / f"{args.out_prefix}_health_docs_touch_shas.txt"
        sha_path.write_text("\n".join(touch_shas) + ("\n" if touch_shas else ""), encoding="utf-8")

    # ---- summary JSON ----
    summary = {
        "repo": repo_name,
        "since": args.since.isoformat(),
        "until": args.until.isoformat(),
        "include_merges": bool(args.include_merges),
        "months": total_months,
        "total_commits_in_range": total_commits_in_range,
        "health_file_commits": health_touch_commits,
        "active_months": active_months,
        "active_month_rate": active_month_rate,
        "entropy_norm": Hn,
        "top1_month_share": top1,
        "top3_month_share": top3,
        "top6_month_share": top6,
        "gini_month_concentration": gini,
        "outputs": {
            "monthly_distribution_csv": str(dist_path),
            "entropy_summary_csv": str(summ_path),
            "sha_list": str(sha_path) if sha_path else None,
        },
        "notes": [
            "Aligned with doc_touch_rhythm.py: git log --name-only, same include/exclude patterns.",
            "entropy_norm is normalized Shannon entropy over monthly health-file commit distribution (0..1).",
            "Lower entropy => activity concentrated into fewer months; higher entropy => spread across months.",
        ],
    }
    json_path = out_dir / f"{args.out_prefix}_summary.json"
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Saved:")
    print(f"  ✓ {dist_path}")
    print(f"  ✓ {summ_path}")
    if sha_path:
        print(f"  ✓ {sha_path}")
    print(f"  ✓ {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
