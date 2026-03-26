#!/usr/bin/env python3
"""
combine_health_docs_ownership.py

Combine per-project health_docs ownership CSVs (one row per repo) into a single sheet.

Looks inside:
  <outputs_dir>/<project_name>/*.csv

and selects the FIRST CSV per project whose filename contains the given keyword
(default: "health_docs_ownership_summary") and ends with .csv.

Usage:
  python combine_health_docs_ownership.py <outputs_directory> [output_file] [--keyword KEY]

Examples:
  python combine_health_docs_ownership.py ./outputs combined_health_docs_ownership.csv
  python combine_health_docs_ownership.py ./outputs combined_ownership.csv --keyword ownership_summary
"""

import os
import sys
import csv
from pathlib import Path
import argparse


def find_ownership_files(outputs_dir: Path, keyword: str):
    """
    Find all ownership summary CSV files in the outputs directory structure.

    Returns:
      List of tuples: (project_name, csv_file_path)
    """
    ownership_files = []

    for project_dir in outputs_dir.iterdir():
        if not project_dir.is_dir():
            continue

        project_name = project_dir.name
        chosen = None

        for f in project_dir.iterdir():
            if f.is_file() and f.suffix.lower() == ".csv":
                name = f.name.lower()
                if keyword.lower() in name:
                    chosen = f
                    break

        if chosen:
            ownership_files.append((project_name, chosen))
            print(f"Found: {project_name} -> {chosen.name}")

    return ownership_files


def read_header_and_first_row(csv_file: Path):
    """
    Read header and first data row from a CSV file (ownership summary is 1-row).
    Returns:
      (headers, row) or (None, None)
    """
    with open(csv_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if len(rows) < 2:
        return None, None

    headers = rows[0]
    row = rows[1]
    return headers, row


def combine_ownership(outputs_dir: str, output_file: str, keyword: str) -> Path | None:
    outputs_path = Path(outputs_dir)

    ownership_files = find_ownership_files(outputs_path, keyword)
    if not ownership_files:
        print(f"No ownership CSV files found in {outputs_dir} using keyword='{keyword}'")
        return None

    print(f"\nFound {len(ownership_files)} project(s) with ownership summaries")

    # Use headers from first file
    first_project, first_file = ownership_files[0]
    headers, _ = read_header_and_first_row(first_file)
    if headers is None:
        print(f"Error: Could not read headers from {first_file}")
        return None

    output_headers = ["project_name"] + headers
    all_rows = []

    for project_name, csv_file in ownership_files:
        h, row = read_header_and_first_row(csv_file)
        if h is None or row is None:
            print(f"Warning: Could not read data from {csv_file}")
            continue

        # If headers differ across repos, warn and skip (keeps combined sheet consistent)
        if h != headers:
            print(f"Warning: Header mismatch for {project_name} ({csv_file.name}). Skipping.")
            continue

        all_rows.append([project_name] + row)

    output_path = Path(output_file)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(output_headers)
        writer.writerows(all_rows)

    print(f"\n✓ Successfully created {output_file}")
    print(f"  Total rows: {len(all_rows)} (from {len(ownership_files)} projects found)")
    print(f"  Columns: {len(output_headers)}")
    return output_path


def main():
    ap = argparse.ArgumentParser(description="Combine health_docs ownership summaries across project folders.")
    ap.add_argument("outputs_directory", help="Path to the outputs directory containing project folders")
    ap.add_argument("output_file", nargs="?", default="combined_health_docs_ownership_summary.csv",
                    help="Output CSV filename (default: combined_health_docs_ownership_summary.csv)")
    ap.add_argument("--keyword", default="health_docs_ownership_summary",
                    help="Filename keyword to match (default: health_docs_ownership_summary)")

    args = ap.parse_args()

    outputs_dir = args.outputs_directory
    if not os.path.exists(outputs_dir):
        print(f"Error: Directory '{outputs_dir}' does not exist")
        sys.exit(1)
    if not os.path.isdir(outputs_dir):
        print(f"Error: '{outputs_dir}' is not a directory")
        sys.exit(1)

    combine_ownership(outputs_dir, args.output_file, args.keyword)


if __name__ == "__main__":
    main()
