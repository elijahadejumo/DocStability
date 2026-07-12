# DocStability

Replication package for the empirical study of health documentation governance
across 100 open source repositories (rhythm, intention, ownership, and
outcome-validation lenses), over a 5-year observation window (2020-2025).

---

## Repository List

See `repos-names.csv` for the full list of the 100 studied repositories
(`repo,owner` columns; clone URL is `https://github.com/<owner>/<repo>`).

---

## Structure

```
.
├── repos-names.csv          # list of 100 study repositories (repo, owner)
├── per_repo/                # one folder per repository (x100), raw per-repo extraction outputs
│   └── <repo_name>/
│       ├── 5yr_summary.csv / .json
│       ├── 5yr_contributors.csv
│       ├── 5yr_bots.csv
│       ├── <repo>_health_2020_2025_file_details.csv
│       ├── <repo>_health_2020_2025_rhythm_metrics.csv
│       ├── <repo>_2020_2025_entropy_summary.csv
│       ├── <repo>_2020_2025_health_docs_intention_summary.csv
│       ├── <repo>_2020_2025_health_docs_ownership_summary.csv
│       └── <repo>_2020_2025_monthly_distribution.csv
├── combined/                 # cross-repo aggregated CSVs (outputs of scripts/aggregation/*)
│   ├── combined_doc_stability_metrics.csv
│   ├── combined_docs_intention.csv
│   ├── combined_doc_contributors.csv
│   ├── combined_contributors.csv
│   ├── combined_reactive_analysis.csv
│   ├── combined_doc_done.csv
│   └── archetype_*.csv
├── scripts/
│   ├── extraction/            # per-repo git-mining scripts (operate on a local clone)
│   │   ├── Doc_rhythm.py                    # rhythm metrics (entropy, AWR)
│   │   ├── doc_entropy.py                   # normalized Shannon entropy + monthly distribution
│   │   ├── doc_commit_ownership.py          # ownership/concentration metrics (Bus-50/80)
│   │   ├── Intention_docs.py                # DocOnly / DocDominant / DocNonDominant classification
│   │   ├── commit_message_external_links.py # external coordination linkage heuristic
│   │   ├── contrib_concentration.py         # contributor concentration + bot filtering
│   │   └── extract_full_commit_log.py       # full per-author commit history (retention/RQ4 + bot-filter join key)
│   ├── aggregation/            # combine_*.py: merge per-repo outputs into cross-repo CSVs
│   └── analysis/               # cross-repo statistical analysis
│       ├── Archetype.py                # k-means rhythm archetype assignment
│       ├── artifact_stratification.py  # living vs. static/attribution doc-type stratification + robustness
│       ├── artifact_reclustering.py    # re-clusters archetypes on living-only rhythm, compares to combined
│       └── confounding_controls.py     # regression: do RQ2/RQ3 findings survive controlling for size?
├── analysis_outputs/          # outputs of scripts/analysis/*
├── notebooks/
│   └── Detailed_Analysis_Scripts.ipynb   # full statistical analysis, figures, and tables
└── figures/
    └── silhouette_heatmap_combined_v2.pdf
```

---

## Extending the dataset locally

`scripts/extraction/extract_full_commit_log.py` re-clones each repo (blobless,
bare -- no file contents downloaded) and extracts full per-author commit
history. This is needed for anything the existing per-repo CSVs don't already
capture at commit-level granularity with author identity (e.g. contributor
retention analysis, or bot-filtering the rhythm computation, which -- unlike
the ownership scripts -- does not currently exclude bot commits). Run:

```
python3 scripts/extraction/extract_full_commit_log.py \
    --repos-csv repos-names.csv \
    --clone-dir ./_clones \
    --out-dir ./full_commit_logs \
    --resume
```

`_clones/` and `full_commit_logs/` are gitignored (large, regenerable).

---

## Known replication-package/paper consistency notes

- `commit_message_external_links.py` includes a third classification rule
  (`^revert\b` -> reactive) beyond the two criteria described in the paper's
  external-coordination-linkage methodology section. Flagged for reconciliation
  before the next submission.
- `Doc_rhythm.py` / `doc_entropy.py` do not filter bot commits (unlike
  `contrib_concentration.py` / `doc_commit_ownership.py`, which do). A small
  number of rhythm archetype labels are affected by bot-automated
  `AUTHORS`/`CONTRIBUTORS` file churn -- see `scripts/analysis/artifact_stratification.py`
  and `artifact_reclustering.py` for the diagnostic.
