#!/usr/bin/env python3
"""
Script to extract first two rows from each project's Contributors_summary CSV
and combine them into a comprehensive single sheet.

Usage:
    python combine_contributors_summary.py <outputs_directory> [output_file]
    
Example:
    python combine_contributors_summary.py /path/to/outputs combined_contributors.csv
"""

import os
import sys
import csv
from pathlib import Path


def find_contributors_files(outputs_dir):
    """
    Find all Contributors_summary CSV files in the outputs directory structure.
    Handles filenames like: project_5yr_Contributors_summary.csv
    
    Args:
        outputs_dir: Path to the outputs directory containing project folders
        
    Returns:
        List of tuples: (project_name, csv_file_path)
    """
    outputs_path = Path(outputs_dir)
    contributors_files = []
    
    # Iterate through each project folder in outputs
    for project_dir in outputs_path.iterdir():
        if project_dir.is_dir():
            project_name = project_dir.name
            
            # Look for Contributors_summary CSV file directly in the project folder
            # File pattern: {project_name}_*_Contributors_summary.csv or Contributors_summary.csv
            for file in project_dir.iterdir():
                if file.is_file() and file.suffix == ".csv":
                    filename_lower = file.name.lower()
                    
                    # Check if it's a Contributors_summary file
                    if "contributors_summary" in filename_lower or ("contributors" in filename_lower and "summary" in filename_lower):
                        # Verify the filename starts with the project name (case-insensitive)
                        # or is just Contributors_summary.csv
                        if filename_lower.startswith(project_name.lower() + "_") or filename_lower.startswith("contributors"):
                            contributors_files.append((project_name, file))
                            print(f"Found: {project_name} -> {file.name}")
                            break  # Only take the first contributors_summary file found per project
    
    return contributors_files


def extract_first_two_rows(csv_file):
    """
    Extract the header and first two data rows from a CSV file.
    
    Args:
        csv_file: Path to the CSV file
        
    Returns:
        Tuple: (headers, row1, row2) or (headers, row1, None) if only one data row exists
    """
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
        
        if len(rows) < 2:
            return None, None, None
        
        headers = rows[0]
        row1 = rows[1] if len(rows) > 1 else None
        row2 = rows[2] if len(rows) > 2 else None
        
        return headers, row1, row2


def combine_contributors(outputs_dir, output_file="combined_contributors_summary.csv"):
    """
    Combine contributors summary from all projects into a single CSV file.
    
    Args:
        outputs_dir: Path to the outputs directory
        output_file: Name of the output CSV file
    """
    # Find all contributors_summary files
    contributors_files = find_contributors_files(outputs_dir)
    
    if not contributors_files:
        print(f"No Contributors_summary CSV files found in {outputs_dir}")
        return
    
    print(f"\nFound {len(contributors_files)} project(s) with contributors summary")
    
    # Get headers from the first file (assuming all have the same structure)
    first_project, first_file = contributors_files[0]
    headers, _, _ = extract_first_two_rows(first_file)
    
    if headers is None:
        print(f"Error: Could not read headers from {first_file}")
        return
    
    # Prepare output headers with "project_name" as the first column
    output_headers = ["project_name"] + headers
    
    # Collect all data rows
    all_rows = []
    
    for project_name, csv_file in contributors_files:
        headers, row1, row2 = extract_first_two_rows(csv_file)
        
        if headers is None:
            print(f"Warning: Could not read data from {csv_file}")
            continue
        
        # Add project name to each row
        if row1:
            all_rows.append([project_name] + row1)
        if row2:
            all_rows.append([project_name] + row2)
    
    # Write combined CSV
    output_path = Path(output_file)
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(output_headers)
        writer.writerows(all_rows)
    
    print(f"\n✓ Successfully created {output_file}")
    print(f"  Total rows: {len(all_rows)} (from {len(contributors_files)} projects)")
    print(f"  Columns: {len(output_headers)}")
    
    return output_path


def main():
    if len(sys.argv) < 2:
        print("Usage: python combine_contributors_summary.py <outputs_directory> [output_file]")
        print("\nExample:")
        print("  python combine_contributors_summary.py /path/to/Repositories/outputs")
        print("  python combine_contributors_summary.py /path/to/Repositories/outputs my_combined_contributors.csv")
        sys.exit(1)
    
    outputs_dir = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "combined_contributors_summary.csv"
    
    if not os.path.exists(outputs_dir):
        print(f"Error: Directory '{outputs_dir}' does not exist")
        sys.exit(1)
    
    if not os.path.isdir(outputs_dir):
        print(f"Error: '{outputs_dir}' is not a directory")
        sys.exit(1)
    
    combine_contributors(outputs_dir, output_file)


if __name__ == "__main__":
    main()