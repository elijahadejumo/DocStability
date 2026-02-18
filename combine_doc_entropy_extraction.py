#!/usr/bin/env python3
"""
combine_entropy_summaries.py

Find each project's health-doc entropy summary CSV in an outputs/ folder
and combine them into a single CSV (one row per project).

Designed to mirror your combine_rhythm_metrics.py style.

Usage:
    python combine_entropy_summaries.py <outputs_directory> [output_file]

Example:
    python combine_entropy_summaries.py /path/to/Repositories/outputs combined_health_docs_entropy.csv
"""

import os
import sys
import csv
from pathlib import Path


def find_entropy_summary_files(outputs_dir):
    """
    Find all entropy summary CSV files in the outputs directory structure.

    We look for filenames containing BOTH:
      - "entropy" and
      - "summary"
    and ending with .csv

    This will match things like:
      airflow_health_2020_2025_entropy_summary.csv
      health_docs_entropy_entropy_summary.csv
      *_health_docs_entropy_summary.csv
    """
    outputs_path = Path(outputs_dir)
    entropy_files = []

    for project_dir in outputs_path.iterdir():
        if project_dir.is_dir():
            project_name = project_dir.name

            # take the first matching entropy summary per project
            candidates = []
            for file in project_dir.iterdir():
                if not (file.is_file() and file.suffix.lower() == ".csv"):
                    continue
                name = file.name.lower()
                if ("entropy" in name) and ("summary" in name):
                    candidates.append(file)

            if candidates:
                # prefer the most specific name if multiple exist
                # heuristic: longer filename often encodes more context
                candidates.sort(key=lambda p: len(p.name), reverse=True)
                chosen = candidates[0]
                entropy_files.append((project_name, chosen))
                print(f"Found: {project_name} -> {chosen.name}")

    return entropy_files


def read_first_data_row(csv_file):
    """
    Read header + first data row from a CSV file.
    Returns: (headers, row) or (None, None)
    """
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
        if len(rows) < 2:
            return None, None
        return rows[0], rows[1]


def combine_entropy(outputs_dir, output_file="combined_health_docs_entropy.csv"):
    entropy_files = find_entropy_summary_files(outputs_dir)

    if not entropy_files:
        print(f"No entropy summary CSV files found in {outputs_dir}")
        return

    print(f"\nFound {len(entropy_files)} project(s) with entropy summaries")

    # get headers from first file
    first_project, first_file = entropy_files[0]
    headers, _row = read_first_data_row(first_file)
    if headers is None:
        print(f"Error: Could not read headers from {first_file}")
        return

    output_headers = ["project_name"] + headers
    all_rows = []

    for project_name, csv_file in entropy_files:
        headers2, row = read_first_data_row(csv_file)
        if headers2 is None or row is None:
            print(f"Warning: Could not read data from {csv_file}")
            continue

        # if schema differs, pad/truncate safely
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        elif len(row) > len(headers):
            row = row[: len(headers)]

        all_rows.append([project_name] + row)

    output_path = Path(output_file)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(output_headers)
        writer.writerows(all_rows)

    print(f"\n✓ Successfully created {output_file}")
    print(f"  Total rows: {len(all_rows)} (from {len(entropy_files)} projects)")
    print(f"  Columns: {len(output_headers)}")

    return output_path


def main():
    if len(sys.argv) < 2:
        print("Usage: python combine_entropy_summaries.py <outputs_directory> [output_file]")
        print("\nExample:")
        print("  python combine_entropy_summaries.py /path/to/Repositories/outputs")
        print("  python combine_entropy_summaries.py /path/to/Repositories/outputs combined_health_docs_entropy.csv")
        sys.exit(1)

    outputs_dir = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "combined_health_docs_entropy.csv"

    if not os.path.exists(outputs_dir):
        print(f"Error: Directory '{outputs_dir}' does not exist")
        sys.exit(1)

    if not os.path.isdir(outputs_dir):
        print(f"Error: '{outputs_dir}' is not a directory")
        sys.exit(1)

    combine_entropy(outputs_dir, output_file)


if __name__ == "__main__":
    main()
