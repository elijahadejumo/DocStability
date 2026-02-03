#!/usr/bin/env python3
"""
doc_touch_rhythm.py

Compute documentation rhythm based on *project health files* only:
- Project health file = community/governance docs (README, CONTRIBUTING, etc.)
- Excludes: technical docs, API docs, tutorials, build configs, internal artifacts

Outputs per granularity (week/month):
- total health-file commits
- mu, sigma, CV, phi_c
- active-window-rate
- label: inactive / sparse / stable / unstable
- detailed list of touched files (optional)
"""

from __future__ import annotations
import argparse, csv, json, math, os, re, subprocess, sys
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Sequence, Tuple
from collections import defaultdict


# ----------------------------
# PROJECT HEALTH FILE RULES (ROOT LEVEL ONLY)
# ----------------------------

# STRICT: Only root-level health files
# Pattern explanation:
# ^README\.md$ = matches EXACTLY "README.md" at root (no subdirectories)
# ^\.github/SECURITY\.md$ = matches EXACTLY ".github/SECURITY.md"

ROOT_HEALTH_FILES = [
    # Essential project documentation (root only)
    r"^README(?:\..+)?$",                    # README, README.md, README.txt
    r"^CONTRIBUTING(?:\..+)?$",              # CONTRIBUTING.md
    r"^CONTRIBUTORS(?:\..+)?$",              # CONTRIBUTORS.md
    r"^COMMIT_CONVENTIONS(?:\..+)?$",        #COMMIT CONVENTIONS
    r"^PULL_REQUEST_TEMPLATE(?:\..+)?$",     #PULL_REQUEST_TEMPLATE
    r"^ISSUE_TEMPLATE(?:\..+)?$",            #ISSUE_TEMPLATE
    r"^BUILDING(?:\..+)?$",                #BUILDING_TEMPLATE
    
    # Version & change documentation (root only)
    r"^CHANGELOG(?:\..+)?$",                 # CHANGELOG.md
    r"^HISTORY(?:\..+)?$",                   # HISTORY.md
    r"^RELEASES?(?:\..+)?$",                 # RELEASE.md, RELEASES.md
    
    # Community & governance (root only)
    r"^CODE_OF_CONDUCT(?:\..+)?$",           # CODE_OF_CONDUCT.md
    r"^GOVERNANCE(?:\..+)?$",                # GOVERNANCE.md
    r"^SUPPORT(?:\..+)?$",                   # SUPPORT.md
    r"^MAINTAINERS(?:\..+)?$",               # MAINTAINERS.md
    
    # Security & legal (root only)
    r"^SECURITY(?:\..+)?$",                  # SECURITY.md
    r"^LICENSE(?:\..+)?$",                   # LICENSE, LICENSE.txt
    r"^NOTICE(?:\..+)?$",                    # NOTICE, NOTICE.txt
    r"^COPYING(?:\..+)?$",                   # COPYING
    
    # Credit & attribution (root only)
    r"^AUTHORS(?:\..+)?$",                   # AUTHORS, AUTHORS.md
    r"^CREDITS(?:\..+)?$",                   # CREDITS.md
    r"^THANKS(?:\..+)?$",                    # THANKS.md
    
    # Project roadmap & vision (root only)
    r"^ROADMAP(?:\..+)?$",                   # ROADMAP.md
    r"^VISION(?:\..+)?$",                    # VISION.md
    
    # GitHub-specific health files (.github/ directory)
    r"^\.github/SECURITY(?:\..+)?$",         # .github/SECURITY.md
    r"^\.github/CONTRIBUTING(?:\..+)?$",     # .github/CONTRIBUTING.md
    r"^\.github/CODE_OF_CONDUCT(?:\..+)?$",  # .github/CODE_OF_CONDUCT.md
    r"^\.github/SUPPORT(?:\..+)?$",          # .github/SUPPORT.md
     r"^\.github/COMMIT_CONVENTIONS(?:\..+)?$",  # .github/COMMIT_CONVENTIONS.md
    r"^\.github/PULL_REQUEST_TEMPLATE(?:\..+)?$",  # .github/PULL_REQUEST_TEMPLATE.md
    r"^\.github/ISSUE_TEMPLATE(?:\..+)?$",  # .github/ISSUE_TEMPLATE.md
    r"^\.github/BUILDING(?:\..+)?$",        # .githubb/BUILDING.md

    
    # GitLab-specific health files (.gitlab/ directory)
    r"^\.gitlab/CONTRIBUTING(?:\..+)?$",     # .gitlab/CONTRIBUTING.md
    r"^\.gitlab/CODE_OF_CONDUCT(?:\..+)?$",  # .gitlab/CODE_OF_CONDUCT.md
    r"^\.gitlab/COMMIT_CONVENTIONS(?:\..+)?$",  # .gitlab/COMMIT_CONVENTIONS.md
    r"^\.gitlab/PULL_REQUEST_TEMPLATE(?:\..+)?$",  # .gitlab/PULL_REQUEST_TEMPLATE.md
    r"^\.gitlab/ISSUE_TEMPLATE(?:\..+)?$",  # .gitlab/ISSUE_TEMPLATE.md
    r"^\.gitlab/BUILDING(?:\..+)?$",        # .gitlab/BUILDING.md
]

# ----------------------------
# EXCLUSION RULES (ULTRA-STRICT)
# ----------------------------

# Exclude EVERYTHING except root-level and .github/.gitlab health files
EXCLUDE_PATTERNS = [
    # ANY nested directory structure (libs/, modules/, x-pack/, etc.)
    r"^[^/]+/[^/]+/",              # Matches anything 2+ directories deep
    
    # Specific component directories that contain LICENSE/README
    r"^libs/",                     # libs/gpu-codec/README, etc.
    r"^modules/",                  # modules/apm/NAMING.md, etc.
    r"^x-pack/",                   # x-pack/plugin/*/README
    r"^plugins?/",                 # plugins/*/README
    r"^packages?/",                # packages/*/README
    r"^distribution/",             # distribution/tools/*/README
    r"^build-tools",               # build-tools/*/README
    r"^qa/",                       # qa/*/README
    r"^test/",                     # test/*/README
    r"^benchmarks?/",              # benchmarks/*/README
    
    # Source code directories (should NEVER match)
    r"^src/",                      # Source code
    r"/src/",                      # Any src/ subdirectory
    
    # Build artifacts & dependencies
    r"(^|/)node_modules/",
    r"(^|/)vendor/",
    r"(^|/)dist/",
    r"(^|/)build/",
    r"(^|/)site/",
    r"(^|/)out/",
    r"(^|/)target/",
    r"(^|/)\.git/",
    
    # Technical documentation directories
    r"(^|/)docs?/",
    r"(^|/)documentation/",
    r"(^|/)wiki/",
    r"(^|/)guides?/",
    r"(^|/)tutorials?/",
    r"(^|/)examples?/",
    r"(^|/)man/",
    r"(^|/)api/",
    r"(^|/)reference/",
    
    # Binary/media files
    r"\.(png|jpg|jpeg|gif|svg|ico|pdf)$",
    r"\.(zip|tar|gz|bz2|7z|rar)$",
    r"\.(exe|dll|so|dylib|bin)$",
    
    # Source code files (CRITICAL - should NEVER be docs)
    r"\.(java|py|js|ts|go|rs|cpp|c|h|hpp)$",
    r"\.(scala|kt|swift|rb|php|cs|fs)$",
    
    # Test/resource files
    r"(^|/)test/",
    r"(^|/)tests/",
    r"/resources/",
    r"\.(cef|json|xml|yaml|yml)\.txt$",  # Test resource files
    
    # Build/config files
    r"(^|/)conf\.py$",
    r"(^|/)_config\.yml$",
    r"(^|/)mkdocs\.yml$",
    r"(^|/)Doxyfile$",
    r"output\.txt$",                     # Build output files
    
    # Translation files
    r"/translations?/",
    r"/i18n/",
    r"/locales?/",
    r"\.(zh|ja|ko|fr|de|es|it|pt|ru)\.md$",
]


@dataclass(frozen=True)
class CommitRecord:
    """Represents a single git commit"""
    sha: str
    commit_dt: datetime
    files: Tuple[str, ...]


@dataclass
class RhythmMetrics:
    """Rhythm metrics for a repository at a given granularity"""
    granularity: str
    window_count: int
    health_file_commits: int  # Renamed from doc_touch_commits
    mu: float
    sigma: float
    cv: Optional[float]
    active_window_rate: float
    phi_c: float
    label: str


@dataclass
class HealthFileDetail:
    """Details about a health-file commit"""
    sha: str
    commit_date: date
    health_files: List[str]


# ----------------------------
# Git helpers
# ----------------------------

def run_git(repo: str, args: List[str]) -> str:
    """Execute git command in repository and return output"""
    cmd = ["git", "-C", repo] + args
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    return out.decode("utf-8", errors="replace")


def iter_commits_with_files(repo: str, since: date, until: date, include_merges: bool) -> Iterator[CommitRecord]:
    """Yield CommitRecord for each commit in date range"""
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
# Health file detection
# ----------------------------

def compile_health_patterns() -> Tuple[List[re.Pattern], List[re.Pattern]]:
    """Compile health file and exclusion patterns
    
    Returns: (health_regexes, exclusion_regexes)
    """
    # STRICT: Only exact root-level files and .github/.gitlab files
    health_rx = [re.compile(p, re.IGNORECASE) for p in ROOT_HEALTH_FILES]
    
    # Comprehensive exclusions
    excl_rx = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_PATTERNS]
    
    return health_rx, excl_rx


def is_excluded(path: str, excl_rx: Sequence[re.Pattern]) -> bool:
    """Check if path should be excluded"""
    p = path.replace("\\", "/")
    return any(rx.search(p) for rx in excl_rx)


def is_health_file(path: str, health_rx: Sequence[re.Pattern]) -> bool:
    """Check if path is a ROOT-LEVEL project health file
    
    ULTRA-STRICT: Only accepts:
    1. Root-level files: README.md, CONTRIBUTING.md, LICENSE, etc.
    2. .github/ health files: .github/SECURITY.md, .github/CONTRIBUTING.md
    3. .gitlab/ health files: .gitlab/CONTRIBUTING.md
    
    REJECTS everything else:
    - libs/gpu-codec/README.md (nested component)
    - modules/apm/NAMING.md (nested module)
    - x-pack/plugin/*/README (nested plugin)
    - *.java, *.py, *.js (source code files)
    - src/test/resources/*.txt (test resources)
    """
    p = path.replace("\\", "/")
    
    # MUST match one of our strict root-level patterns
    return any(rx.match(p) for rx in health_rx)


def get_health_files(files: Sequence[str], health_rx: Sequence[re.Pattern], 
                     excl_rx: Sequence[re.Pattern]) -> List[str]:
    """Get list of project health files from commit"""
    health_files = []
    for f in files:
        # Check exclusions first
        if is_excluded(f, excl_rx):
            continue
        
        # Check if it's a health file
        if is_health_file(f, health_rx):
            health_files.append(f)
    
    return health_files


# ----------------------------
# Windowing + statistics
# ----------------------------

def month_start(d: date) -> date:
    """Get first day of month"""
    return date(d.year, d.month, 1)


def week_start_monday(d: date) -> date:
    """Get Monday of week"""
    return d - timedelta(days=d.weekday())


def build_windows(since: date, until: date, granularity: str) -> List[date]:
    """Build list of time window start dates"""
    g = granularity.lower()
    if g == "week":
        cur = week_start_monday(since)
        end = week_start_monday(until)
        out = []
        while cur <= end:
            out.append(cur)
            cur += timedelta(days=7)
        return out
    if g == "month":
        cur = month_start(since)
        end = month_start(until)
        out = []
        while cur <= end:
            out.append(cur)
            y, m = cur.year, cur.month
            cur = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
        return out
    raise ValueError("granularity must be week or month")


def window_id_for_date(d: date, granularity: str) -> date:
    """Map date to its window identifier"""
    g = granularity.lower()
    return week_start_monday(d) if g == "week" else month_start(d)


def mean(xs: Sequence[float]) -> float:
    """Arithmetic mean"""
    return sum(xs) / len(xs) if xs else 0.0


def stdev_sample(xs: Sequence[float]) -> float:
    """Sample standard deviation"""
    n = len(xs)
    if n < 2:
        return 0.0
    mu = mean(xs)
    var = sum((x - mu) ** 2 for x in xs) / (n - 1)
    return math.sqrt(var)


def calculate_phi_c(cv: Optional[float]) -> float:
    """Calculate stability score φ_c"""
    if cv is None:
        return 0.0
    
    target = 0.25
    tolerance = 0.25
    
    if 0.0 <= cv <= 0.50:
        phi_c = 1.0 - abs(cv - target) / tolerance
        return max(0.0, phi_c)
    else:
        return 0.0


def compute_metrics(counts_by_window: Dict[date, int], windows: List[date],
                    cv_threshold: float, min_total: int, min_active: int, 
                    granularity: str) -> RhythmMetrics:
    """Compute rhythm metrics from window counts"""
    counts = [float(counts_by_window.get(w, 0)) for w in windows]
    total = int(sum(counts))
    nwin = len(windows)

    mu = mean(counts)
    sigma = stdev_sample(counts)
    active = sum(1 for c in counts if c > 0)
    active_rate = active / nwin if nwin else 0.0

    if mu == 0.0:
        return RhythmMetrics(granularity, nwin, 0, 0.0, 0.0, None, 0.0, 0.0, "inactive")

    cv = sigma / mu if mu > 0 else None
    phi_c = calculate_phi_c(cv)

    if total < min_total or active < min_active:
        return RhythmMetrics(granularity, nwin, total, mu, sigma, cv, active_rate, phi_c, "sparse")

    label = "stable" if (cv is not None and cv <= cv_threshold) else "unstable"
    return RhythmMetrics(granularity, nwin, total, mu, sigma, cv, active_rate, phi_c, label)


# ----------------------------
# Main
# ----------------------------

def parse_date(s: str) -> date:
    """Parse YYYY-MM-DD date string"""
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Compute project health file rhythm (community/governance docs only)"
    )
    ap.add_argument("--repo", required=True, help="Path to local git repo")
    ap.add_argument("--since", required=True, type=parse_date, help="Start date YYYY-MM-DD")
    ap.add_argument("--until", required=True, type=parse_date, help="End date YYYY-MM-DD")
    ap.add_argument("--granularities", nargs="+", default=["month"], 
                    choices=["week", "month"], help="Time granularities")
    ap.add_argument("--include_merges", action="store_true")
    ap.add_argument("--cv_threshold", type=float, default=0.5)
    ap.add_argument("--min_total_commits", type=int, default=5)
    ap.add_argument("--min_active_windows", type=int, default=3)
    ap.add_argument("--out_prefix", default="health_files")
    ap.add_argument("--write_timeseries", action="store_true")
    ap.add_argument("--write_file_details", action="store_true")
    args = ap.parse_args()

    repo = os.path.abspath(args.repo)
    repo_name = os.path.basename(repo)
    
    # Create output directory
    output_dir = os.path.join("outputs", repo_name)
    os.makedirs(output_dir, exist_ok=True)
    
    # Compile patterns
    health_rx, excl_rx = compile_health_patterns()

    # Collect health file commits
    health_dates: List[date] = []
    health_details: List[HealthFileDetail] = []
    
    print(f"Analyzing {repo_name} - PROJECT HEALTH FILES")
    print(f"  Date range: {args.since} to {args.until}")
    print(f"  Tracking: README, CONTRIBUTING, CHANGELOG, SECURITY, CODE_OF_CONDUCT, etc.")
    
    for cr in iter_commits_with_files(repo, args.since, args.until, args.include_merges):
        health_files = get_health_files(cr.files, health_rx, excl_rx)
        
        if health_files:
            health_dates.append(cr.commit_dt.date())
            health_details.append(HealthFileDetail(
                sha=cr.sha,
                commit_date=cr.commit_dt.date(),
                health_files=health_files
            ))

    print(f"  Found {len(health_details)} health-file commits")

    # Compute metrics
    metrics_rows = []
    ts_rows = []

    for g in args.granularities:
        windows = build_windows(args.since, args.until, g)
        counts_by_window = defaultdict(int)
        for d in health_dates:
            counts_by_window[window_id_for_date(d, g)] += 1

        m = compute_metrics(counts_by_window, windows, args.cv_threshold,
                            args.min_total_commits, args.min_active_windows, g)

        cv_str = f"{m.cv:.2f}" if m.cv is not None else "N/A"
        print(f"  Granularity '{g}': {m.window_count} windows, μ={m.mu:.2f}, σ={m.sigma:.2f}, "
              f"CV={cv_str}, φ_c={m.phi_c:.2f}, label={m.label}")

        metrics_rows.append({
            "repo": repo_name,
            "granularity": g,
            "window_count": m.window_count,
            "health_file_commits": m.health_file_commits,
            "mu": m.mu,
            "sigma": m.sigma,
            "cv": m.cv,
            "phi_c": m.phi_c,
            "active_window_rate": m.active_window_rate,
            "label": m.label,
        })

        if args.write_timeseries:
            for w in windows:
                ts_rows.append({
                    "repo": repo_name,
                    "granularity": g,
                    "window_start": w.isoformat(),
                    "health_file_commits": int(counts_by_window.get(w, 0)),
                })

    # Write outputs
    metrics_csv = os.path.join(output_dir, f"{args.out_prefix}_rhythm_metrics.csv")
    with open(metrics_csv, "w", newline="") as f:
        fn = ["repo","granularity","window_count","health_file_commits","mu","sigma","cv","phi_c","active_window_rate","label"]
        w = csv.DictWriter(f, fieldnames=fn)
        w.writeheader()
        w.writerows(metrics_rows)

    ts_csv = None
    if args.write_timeseries:
        ts_csv = os.path.join(output_dir, f"{args.out_prefix}_window_counts.csv")
        with open(ts_csv, "w", newline="") as f:
            fn = ["repo","granularity","window_start","health_file_commits"]
            w = csv.DictWriter(f, fieldnames=fn)
            w.writeheader()
            w.writerows(ts_rows)

    file_details_csv = None
    if args.write_file_details:
        file_details_csv = os.path.join(output_dir, f"{args.out_prefix}_file_details.csv")
        with open(file_details_csv, "w", newline="") as f:
            fn = ["repo", "commit_sha", "commit_date", "health_file"]
            w = csv.DictWriter(f, fieldnames=fn)
            w.writeheader()
            for detail in health_details:
                for health_file in detail.health_files:
                    w.writerow({
                        "repo": repo_name,
                        "commit_sha": detail.sha,
                        "commit_date": detail.commit_date.isoformat(),
                        "health_file": health_file,
                    })

    summary = {
        "repo": repo_name,
        "since": args.since.isoformat(),
        "until": args.until.isoformat(),
        "analysis_type": "project_health_files",
        "tracked_files": [
            "README", "CONTRIBUTING", "CHANGELOG", "SECURITY", 
            "CODE_OF_CONDUCT", "LICENSE", "GOVERNANCE", "etc."
        ],
        "granularities": args.granularities,
        "output_directory": output_dir,
        "total_health_file_commits": len(health_details),
        "outputs": {
            "metrics_csv": metrics_csv,
            "timeseries_csv": ts_csv,
            "file_details_csv": file_details_csv,
        },
    }
    
    summary_path = os.path.join(output_dir, f"{args.out_prefix}_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\nAll outputs saved to: {output_dir}/")
    print(f"  ✓ {metrics_csv}")
    if ts_csv:
        print(f"  ✓ {ts_csv}")
    if file_details_csv:
        print(f"  ✓ {file_details_csv}")
    print(f"  ✓ {summary_path}")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())