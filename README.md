# DocStability

Replication package for the empirical study of health documentation governance
across 100 open source repositories (rhythm, intention, ownership, and
outcome-validation lenses), over a decade-long observation window
(2016-05-30 to 2026-05-29).

---

## Reproducing the paper's results

`scripts/reproduce/reproduce_all_results.py` reproduces every statistic
reported in the paper (RQ1-RQ4) from the already-extracted intermediate CSVs
in `analysis_outputs/`. It does not re-clone or re-mine the 100 repositories;
that extraction step is the expensive, environment-dependent part, while the
statistical layer reproduced here — clustering, regressions, trend tests — is
fully deterministic given the same input CSVs, and is the part a reviewer
most needs to check.

```bash
pip install pandas numpy scipy statsmodels scikit-learn pymannkendall

# reproduce everything (RQ1-RQ4)
python3 scripts/reproduce/reproduce_all_results.py

# or just one section
python3 scripts/reproduce/reproduce_all_results.py --section rq1
python3 scripts/reproduce/reproduce_all_results.py --section rq2
python3 scripts/reproduce/reproduce_all_results.py --section rq3              # ownership descriptives
python3 scripts/reproduce/reproduce_all_results.py --section rq3_predictive   # predictive-validity 
python3 scripts/reproduce/reproduce_all_results.py --section rq4             # evolution: transitions + trend tests
```

Each printed value is shown next to the paper's published value so results
can be diffed by eye. RQ1's living/process-vs-static/attribution robustness
check and the four-way purpose taxonomy (Table IV) additionally regenerate
from raw per-commit data (`outputs/<repo>/<repo>_2016_2026_file_details.csv`
+ `full_commit_logs/<repo>_full_commit_log.csv`); if those directories aren't
present, that sub-check is skipped with a message rather than failing the run.

Run with `--data_dir` to point at a different `analysis_outputs/` location,
or `--repo_root` if `outputs/` and `full_commit_logs/` live somewhere other
than two directories above the script.

---

## Repository List

See `repos-names.csv` for the full list of the 100 studied repositories
(`repo,owner` columns; clone URL is `https://github.com/<owner>/<repo>`).

---

## Structure

```
.
├── repos-names.csv           # 100 study repositories (repo, owner)
├── main.tex                  # paper source (IEEEtran, SANER submission)
├── outputs/<repo>/            # raw per-commit health-file touch records, decade window
│   └── <repo>_2016_2026_file_details.csv    # repo, commit_sha, commit_date, health_file
├── full_commit_logs/          # full per-author commit history + bot flags, decade window
│   └── <repo>_full_commit_log.csv
├── analysis_outputs/          # canonical intermediate CSVs the paper's numbers are built from
│   ├── decade_archetype_assignments_final.csv     # RQ1: per-repo entropy/AWR/archetype
│   ├── decade_intent_agg.csv                      # RQ2: DocOnly/DocDominant/DocNonDominant per repo
│   ├── decade_rq3_full.csv                        # RQ3: participation/concentration/bus-factor per repo
│   ├── decade_staleness.csv                       # RQ3 outcome: documentation staleness
│   ├── decade_newcomer_retention.csv              # RQ3 outcome: documentation-newcomer retention
│   ├── decade_general_outcomes.csv                # RQ3 outcome: repo-wide activity trend, etc.
│   ├── annual_archetype_classified.csv            # RQ4: per-repo-year archetype label
│   ├── annual_bus50_doconly.csv                   # RQ4: per-repo-year DocOnly rate
│   └── annual_participation.csv                   # RQ4: per-repo-year participation rate
├── scripts/
│   ├── extraction/            # per-repo git-mining scripts (operate on a local clone)
│   │   └── extract_full_commit_log.py   # produces full_commit_logs/
│   ├── analysis/               # supporting statistical analysis scripts
│   └── reproduce/
│       └── reproduce_all_results.py    # reproduces every reported RQ1-RQ4 number
├── notebooks/
│   └── Detailed_Analysis_Scripts.ipynb
└── figures/
    └── silhouette_heatmap_combined.pdf
```

---

## Extending the dataset locally

`scripts/extraction/extract_full_commit_log.py` re-clones each repo (blobless,
bare -- no file contents downloaded) and extracts full per-author commit
history with bot flags; this is what produced `full_commit_logs/`. Run:

```
python3 scripts/extraction/extract_full_commit_log.py \
    --repos-csv repos-names.csv \
    --clone-dir ./_clones \
    --out-dir ./full_commit_logs \
    --resume
```

`_clones/` is gitignored (100 full clones, large and regenerable).

Bot handling is an intentional split: rhythm metrics (entropy, AWR,
archetypes -- RQ1) are computed bot-inclusive, while ownership and
structural-fragility metrics (participation rate, top-k concentration, bus
factor -- RQ3) exclude bots throughout, since the risk of interest there is
specific to human maintainership.
