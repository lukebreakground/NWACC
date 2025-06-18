#!/usr/bin/env python3
"""
Clean civicplus_results.csv by:
1. Removing rows where has_civicplus is "False"
2. Keeping all remaining rows (no duplicate removal)
"""

import pandas as pd
import sys

def clean_civicplus_results(input_file='civicplus_results.csv', output_file='civicplus_results_cleaned.csv'):
    """
    Clean the civicplus results CSV file.
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path to save the cleaned CSV file
    """
    try:
        # Read the CSV file
        print(f"Reading {input_file}...")
        df = pd.read_csv(input_file)
        
        print(f"Original data shape: {df.shape}")
        print(f"Original unique websites: {df['website'].nunique()}")
        
        # Filter out rows where has_civicplus is False
        print("Filtering out rows with has_civicplus = False...")
        df_filtered = df[df['has_civicplus'] == True].copy()
        
        print(f"After filtering False values: {df_filtered.shape}")
        
        # Keep all rows (no duplicate removal)
        df_cleaned = df_filtered.copy()
        
        print(f"Keeping all rows: {df_cleaned.shape}")
        print(f"Total websites: {len(df_cleaned)}, Unique websites: {df_cleaned['website'].nunique()}")
        
        # Split place into municipality and state
        print("Splitting place into municipality and state...")
        df_cleaned[['municipality', 'state']] = df_cleaned['place'].str.split(', ', n=1, expand=True)
        
        # Remove unwanted columns and reorder
        print("Removing has_civicplus and error columns...")
        df_final = df_cleaned[['municipality', 'state', 'website']].copy()
        
        print(f"Final data shape: {df_final.shape}")
        
        # Save the cleaned data
        df_final.to_csv(output_file, index=False)
        print(f"Cleaned data saved to {output_file}")
        
        # Print summary statistics
        print("\n=== Summary ===")
        print(f"Rows removed (False has_civicplus): {len(df) - len(df_filtered)}")
        print(f"Total rows retained: {len(df_final)}")
        print(f"Unique websites in final data: {df_final['website'].nunique()}")
        
        # Show sample of cleaned data
        print("\n=== Sample of cleaned data (municipality, state, website) ===")
        print(df_final.head(10))
        
        return df_final
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Allow command line arguments for input and output files
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'civicplus_results.csv'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'civicplus_results_cleaned.csv'
    
    clean_civicplus_results(input_file, output_file) 