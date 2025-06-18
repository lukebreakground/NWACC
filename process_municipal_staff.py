#!/usr/bin/env python3
"""
Process municipal staff results CSV:
1. Create a copy with organization column emptied but header preserved
2. Extract unique titles to a text file
"""

import pandas as pd
import csv

def process_municipal_staff_csv():
    """
    Process the municipal staff CSV file:
    - Create copy with organization column emptied
    - Extract unique titles to txt file
    """
    
    # Read the original CSV file
    print("Reading municipal_staff_results.csv...")
    df = pd.read_csv('municipal_staff_results.csv')
    
    print(f"Original CSV has {len(df)} rows and {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)}")
    
    # Filter rows - keep only those that have at least phone OR email (not both empty)
    # Remove rows where both phone AND email are empty/null
    original_count = len(df)
    df_filtered = df[
        (df['Phone'].notna() & (df['Phone'].astype(str).str.strip() != '')) | 
        (df['Email'].notna() & (df['Email'].astype(str).str.strip() != ''))
    ].copy()
    
    filtered_count = len(df_filtered)
    removed_count = original_count - filtered_count
    print(f"Filtered dataset: {filtered_count} rows (removed {removed_count} rows without phone or email)")
    
    # Create a copy with organization column emptied but header preserved
    df_copy = df_filtered.copy()
    df_copy['Organization'] = ''  # Empty the organization column but keep the header
    
    # Save the modified CSV
    output_csv = 'municipal_staff_results_no_org.csv'
    df_copy.to_csv(output_csv, index=False)
    print(f"Created {output_csv} with organization column emptied")
    
    # Extract unique titles from filtered data
    unique_titles = df_filtered['Title'].dropna().unique()
    unique_titles_sorted = sorted(unique_titles)
    
    print(f"Found {len(unique_titles_sorted)} unique titles")
    
    # Save unique titles to text file
    output_txt = 'unique_titles.txt'
    with open(output_txt, 'w') as f:
        for title in unique_titles_sorted:
            f.write(f"{title}\n")
    
    print(f"Created {output_txt} with all unique titles")
    
    # Extract unique departments from filtered data
    unique_departments = df_filtered['Department'].dropna().unique()
    unique_departments_sorted = sorted(unique_departments)
    
    print(f"Found {len(unique_departments_sorted)} unique departments")
    
    # Save unique departments to text file
    output_dept_txt = 'unique_departments.txt'
    with open(output_dept_txt, 'w') as f:
        for dept in unique_departments_sorted:
            f.write(f"{dept}\n")
    
    print(f"Created {output_dept_txt} with all unique departments")
    
    # Show some sample titles
    print("\nSample of unique titles:")
    for i, title in enumerate(unique_titles_sorted[:10]):
        print(f"  {i+1}. {title}")
    
    if len(unique_titles_sorted) > 10:
        print(f"  ... and {len(unique_titles_sorted) - 10} more titles")
    
    # Show some sample departments
    print("\nSample of unique departments:")
    for i, dept in enumerate(unique_departments_sorted[:10]):
        print(f"  {i+1}. {dept}")
    
    if len(unique_departments_sorted) > 10:
        print(f"  ... and {len(unique_departments_sorted) - 10} more departments")

def filter_by_parks_departments():
    """
    Filter municipal_staff_results_no_org.csv to only include rows where the Department matches any entry in parks_departments.txt.
    Save the filtered results to a new CSV file: municipal_staff_results_parks_only.csv
    """
    
    # Read the parks departments into a set for fast lookup
    with open('parks_departments.txt', 'r', encoding='utf-8') as f:
        parks_departments = set(line.strip() for line in f if line.strip())
    print(f"Loaded {len(parks_departments)} parks-related department names.")

    # Read the municipal staff CSV
    print("Reading municipal_staff_results_no_org.csv...")
    df = pd.read_csv('municipal_staff_results_no_org.csv', dtype=str)
    print(f"Loaded {len(df)} rows.")

    # Filter rows where Department matches any in parks_departments
    filtered_df = df[df['Department'].isin(parks_departments)].copy()
    print(f"Filtered down to {len(filtered_df)} rows with matching departments.")

    # Save to new CSV
    output_csv = 'municipal_staff_results_parks_only.csv'
    filtered_df.to_csv(output_csv, index=False)
    print(f"Saved filtered results to {output_csv}.")

if __name__ == "__main__":
    process_municipal_staff_csv() 