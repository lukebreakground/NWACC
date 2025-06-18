#!/usr/bin/env python3
"""
Filter municipal staff CSV to only include entries with departments related to parks, recreation, or outdoor recreation.
- Reads department names from parks_departments.txt
- Reads municipal_staff_results_no_org.csv
- Outputs municipal_staff_results_parks_only.csv with only matching entries
"""

import pandas as pd

# Load parks-related department names into a set for fast lookup
def load_parks_departments(filename='parks_departments.txt'):
    """Read parks department names from a text file, one per line."""
    with open(filename, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

# Filter the staff CSV by department
def filter_staff_by_department(staff_csv='municipal_staff_results_no_org.csv',
                              parks_departments_file='parks_departments.txt',
                              output_csv='municipal_staff_results_parks_only.csv'):
    """
    Filter the staff CSV to only include rows where Department matches a parks department.
    """
    parks_departments = load_parks_departments(parks_departments_file)
    print(f"Loaded {len(parks_departments)} parks-related department names.")

    # Read the staff CSV
    print(f"Reading {staff_csv}...")
    df = pd.read_csv(staff_csv, dtype=str)
    print(f"Loaded {len(df)} rows.")

    # Filter rows
    filtered_df = df[df['Department'].isin(parks_departments)].copy()
    print(f"Filtered down to {len(filtered_df)} rows with matching departments.")

    # Save to output CSV
    filtered_df.to_csv(output_csv, index=False)
    print(f"Saved filtered results to {output_csv}.")

if __name__ == "__main__":
    filter_staff_by_department() 