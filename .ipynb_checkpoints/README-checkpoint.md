# DocStability
DocStability Experiments
# Replication Package

This package contains the data and analysis scripts for the submitted paper. It covers **100 open source repositories** over a **5-year observation window (2020–2025)**, examining documentation governance through rhythm, intention, and ownership lenses.

---

## Repository List

See `repos-names.csv` for the full list of the 100 studied repositories.

---

## Scripts

| Script | Description |
|--------|-------------|
| `doc_rhythm.py` | Computes rhythm metrics (AWR, active months) |
| `doc_entropy.py` | Computes Shannon entropy over contributor distributions |
| `doc_commit_ownership.py` | Computes ownership metrics (Bus-50, contributor type classification) |
| `intention_docs.py` | Classifies commits as DocOnly, DocDominant, or DocNonDominant |
| `commit_message_reactive.py` | Extracts external coordination linkage signals from commit messages |
| `contrib_concentration.py` | Contributor concentration and bus factor analysis |
| `Archetype.py` | Assigns rhythm archetypes via k-means clustering |
| `combine_*.py` | Aggregation scripts that merge per-repo outputs into cross-repo CSVs |

---

## Structure

```
.
├── repos-names.csv                  # List of 100 study repositories
├── *.py                             # Analysis and aggregation scripts
├── Detailed_Analysis_Scripts.ipynb  # Full statistical analysis, figures, and tables
├── combined_*.csv                   # Cross-repo aggregated outputs
├── archetype_*.csv                  # Archetype assignment and summary outputs
├── silhouette_heatmap_combined_v2.pdf
│
└── <repo_name>/                     # One folder per repository (×100)
    ├── 5yr_summary.csv / .json
    ├── 5yr_contributors.csv
    ├── 5yr_bots.csv
    ├── <repo>_health_2020_2025_file_details.csv
    ├── <repo>_health_2020_2025_rhythm_metrics.csv
    ├── <repo>_2020_2025_entropy_summary.csv
    ├── <repo>_2020_2025_health_docs_intention_summary.csv
    ├── <repo>_2020_2025_health_docs_ownership_summary.csv
    └── <repo>_2020_2025_monthly_distribution.csv
```