"""
Script to remove duplicate rows from Excel file based on ticker (Symbol) name.
Keeps the first occurrence of each ticker.
"""

import pandas as pd
import os

# File paths
INPUT_EXCEL_FILE = 'undervalued_stocks_with_regions.xlsx'
OUTPUT_EXCEL_FILE = 'undervalued_stocks_with_regions_cleaned.xlsx'

def remove_duplicates():
    """
    Remove duplicate rows from Excel file based on Symbol (ticker) column.
    """
    print("=" * 80)
    print("Removing Duplicate Rows from Excel File")
    print("=" * 80)
    
    # Check if input file exists
    if not os.path.exists(INPUT_EXCEL_FILE):
        print(f"Error: Input file not found: {INPUT_EXCEL_FILE}")
        return None
    
    print(f"Reading Excel file: {INPUT_EXCEL_FILE}")
    
    try:
        # Read the Excel file
        df = pd.read_excel(INPUT_EXCEL_FILE, engine='openpyxl')
        
        print(f"Original file contains {len(df)} rows")
        print(f"Columns: {list(df.columns)}")
        
        # Check if 'Symbol' column exists
        if 'Symbol' not in df.columns:
            print("Error: 'Symbol' column not found in the Excel file.")
            print(f"Available columns: {list(df.columns)}")
            return None
        
        # Count duplicates before removal
        duplicate_count = df.duplicated(subset=['Symbol']).sum()
        unique_count = df['Symbol'].nunique()
        
        print(f"\nDuplicate Analysis:")
        print(f"  Total rows: {len(df)}")
        print(f"  Unique tickers: {unique_count}")
        print(f"  Duplicate rows to remove: {duplicate_count}")
        
        if duplicate_count == 0:
            print("\nNo duplicates found. File is already clean!")
            return df
        
        # Remove duplicates based on Symbol column, keeping the first occurrence
        print("\nRemoving duplicates (keeping first occurrence of each ticker)...")
        df_cleaned = df.drop_duplicates(subset=['Symbol'], keep='first')
        
        # Show what was removed
        removed_count = len(df) - len(df_cleaned)
        print(f"\nRemoved {removed_count} duplicate row(s)")
        print(f"Cleaned file contains {len(df_cleaned)} rows")
        
        # Show which symbols had duplicates (if any)
        if duplicate_count > 0:
            duplicates = df[df.duplicated(subset=['Symbol'], keep=False)]['Symbol'].unique()
            print(f"\nSymbols that had duplicates ({len(duplicates)}):")
            for symbol in sorted(duplicates)[:20]:  # Show first 20
                count = len(df[df['Symbol'] == symbol])
                print(f"  {symbol}: {count} occurrences (kept 1, removed {count-1})")
            if len(duplicates) > 20:
                print(f"  ... and {len(duplicates) - 20} more")
        
        # Save cleaned file
        print(f"\nSaving cleaned file: {OUTPUT_EXCEL_FILE}")
        df_cleaned.to_excel(OUTPUT_EXCEL_FILE, index=False, engine='openpyxl')
        print("Cleaned file saved successfully!")
        
        # Summary
        print("\n" + "=" * 80)
        print("Summary:")
        print(f"  Original rows: {len(df)}")
        print(f"  Duplicate rows removed: {removed_count}")
        print(f"  Final rows: {len(df_cleaned)}")
        print(f"  Unique tickers: {df_cleaned['Symbol'].nunique()}")
        print(f"  Cleaned file: {OUTPUT_EXCEL_FILE}")
        print("=" * 80)
        
        return df_cleaned
        
    except FileNotFoundError:
        print(f"Error: File not found: {INPUT_EXCEL_FILE}")
        return None
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return None

def remove_duplicates_in_place():
    """
    Remove duplicates and overwrite the original file.
    """
    print("=" * 80)
    print("Removing Duplicate Rows (Overwriting Original File)")
    print("=" * 80)
    
    if not os.path.exists(INPUT_EXCEL_FILE):
        print(f"Error: Input file not found: {INPUT_EXCEL_FILE}")
        return None
    
    try:
        df = pd.read_excel(INPUT_EXCEL_FILE, engine='openpyxl')
        original_count = len(df)
        
        print(f"Original file contains {original_count} rows")
        
        if 'Symbol' not in df.columns:
            print("Error: 'Symbol' column not found.")
            return None
        
        # Remove duplicates
        df_cleaned = df.drop_duplicates(subset=['Symbol'], keep='first')
        removed_count = original_count - len(df_cleaned)
        
        # Overwrite original file
        print(f"Removing {removed_count} duplicate row(s)...")
        df_cleaned.to_excel(INPUT_EXCEL_FILE, index=False, engine='openpyxl')
        
        print(f"\nSuccess! Removed {removed_count} duplicate(s).")
        print(f"Original file updated: {INPUT_EXCEL_FILE}")
        print("=" * 80)
        
        return df_cleaned
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == '--overwrite':
        # Overwrite original file
        remove_duplicates_in_place()
    else:
        # Create new cleaned file (default)
        remove_duplicates()

