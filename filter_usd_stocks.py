"""
Script to filter stocks with USD currency, remove duplicates, and save to Excel.
Uses multi-threading for faster processing if needed.
"""

import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time

# File paths
INPUT_EXCEL_FILE = 'undervalued_stocks_with_regions_cleaned.xlsx'
OUTPUT_EXCEL_FILE = 'undervalued_stocks_usd_filtered.xlsx'

def filter_usd_stocks():
    """
    Filter stocks to only include USD currency and remove duplicates.
    """
    print("=" * 80)
    print("Filtering USD Stocks and Removing Duplicates")
    print("=" * 80)
    
    # Check if input file exists
    if not os.path.exists(INPUT_EXCEL_FILE):
        print(f"Error: Input file not found: {INPUT_EXCEL_FILE}")
        return None
    
    print(f"Reading Excel file: {INPUT_EXCEL_FILE}")
    start_time = time.time()
    
    try:
        # Read the Excel file
        df = pd.read_excel(INPUT_EXCEL_FILE, engine='openpyxl')
        
        read_time = time.time() - start_time
        print(f"File read in {read_time:.2f} seconds")
        print(f"Original file contains {len(df)} rows")
        print(f"Columns: {list(df.columns)}")
        
        # Check if required columns exist
        if 'Currency' not in df.columns:
            print("Error: 'Currency' column not found in the Excel file.")
            print(f"Available columns: {list(df.columns)}")
            return None
        
        if 'Symbol' not in df.columns:
            print("Error: 'Symbol' column not found in the Excel file.")
            return None
        
        # Step 1: Filter for USD currency and US country
        print("\n" + "=" * 80)
        print("Step 1: Filtering for USD currency and US country...")
        print("=" * 80)
        
        # Get currency value counts before filtering
        currency_counts = df['Currency'].value_counts()
        print(f"\nCurrency distribution (top 10):")
        for currency, count in currency_counts.head(10).items():
            print(f"  {currency}: {count} stocks")
        
        # Get country value counts before filtering
        if 'Country' in df.columns:
            country_counts = df['Country'].value_counts()
            print(f"\nCountry distribution (top 10):")
            for country, count in country_counts.head(10).items():
                print(f"  {country}: {count} stocks")
        
        # Filter for USD (case-insensitive, handle variations)
        usd_variations = ['USD', 'usd', 'Usd', 'US Dollar', 'US$', '$']
        df_usd = df[df['Currency'].isin(usd_variations) | 
                    df['Currency'].str.contains('USD', case=False, na=False) |
                    df['Currency'].str.contains('US Dollar', case=False, na=False)]
        
        print(f"\nAfter USD currency filter: {len(df_usd)} rows (from {len(df)} rows)")
        print(f"Removed {len(df) - len(df_usd)} non-USD stocks")
        
        if len(df_usd) == 0:
            print("\nNo USD stocks found. Exiting.")
            return None
        
        # Filter for US country (case-insensitive, handle variations)
        if 'Country' in df_usd.columns:
            us_variations = ['US', 'us', 'Us', 'USA', 'usa', 'U.S.', 'U.S.A.', 'United States', 'United States of America']
            df_usd_us = df_usd[df_usd['Country'].isin(us_variations) | 
                              df_usd['Country'].str.contains('United States', case=False, na=False) |
                              df_usd['Country'].str.contains('USA', case=False, na=False) |
                              df_usd['Country'].str.contains('US', case=False, na=False)]
            
            print(f"\nAfter US country filter: {len(df_usd_us)} rows (from {len(df_usd)} rows)")
            print(f"Removed {len(df_usd) - len(df_usd_us)} non-US stocks")
            
            if len(df_usd_us) == 0:
                print("\nNo US stocks found. Exiting.")
                return None
            
            df_filtered = df_usd_us
        else:
            print("\nWarning: 'Country' column not found. Skipping country filter.")
            df_filtered = df_usd
        
        # Step 2: Remove duplicates based on Symbol
        print("\n" + "=" * 80)
        print("Step 2: Removing duplicates based on Symbol...")
        print("=" * 80)
        
        # Count duplicates before removal
        duplicate_count = df_filtered.duplicated(subset=['Symbol']).sum()
        unique_count = df_filtered['Symbol'].nunique()
        
        print(f"\nDuplicate Analysis:")
        print(f"  Total rows: {len(df_filtered)}")
        print(f"  Unique tickers: {unique_count}")
        print(f"  Duplicate rows to remove: {duplicate_count}")
        
        if duplicate_count == 0:
            print("\nNo duplicates found. All tickers are unique!")
            df_final = df_filtered
        else:
            # Remove duplicates, keeping the first occurrence
            print("\nRemoving duplicates (keeping first occurrence of each ticker)...")
            df_final = df_filtered.drop_duplicates(subset=['Symbol'], keep='first')
            
            removed_count = len(df_filtered) - len(df_final)
            print(f"Removed {removed_count} duplicate row(s)")
            
            # Show which symbols had duplicates
            duplicates = df_filtered[df_filtered.duplicated(subset=['Symbol'], keep=False)]['Symbol'].unique()
            print(f"\nSymbols that had duplicates ({len(duplicates)}):")
            for symbol in sorted(duplicates)[:20]:  # Show first 20
                count = len(df_filtered[df_filtered['Symbol'] == symbol])
                print(f"  {symbol}: {count} occurrences (kept 1, removed {count-1})")
            if len(duplicates) > 20:
                print(f"  ... and {len(duplicates) - 20} more")
        
        # Step 3: Sort and prepare final dataframe
        print("\n" + "=" * 80)
        print("Step 3: Preparing final data...")
        print("=" * 80)
        
        # Sort by discount percentage (highest first) if available
        if 'Discount %' in df_final.columns:
            df_final = df_final.sort_values('Discount %', ascending=False)
            print("Sorted by Discount % (highest first)")
        
        # Step 4: Save to Excel
        print("\n" + "=" * 80)
        print("Step 4: Saving to Excel...")
        print("=" * 80)
        
        save_start = time.time()
        df_final.to_excel(OUTPUT_EXCEL_FILE, index=False, engine='openpyxl')
        save_time = time.time() - save_start
        
        print(f"File saved in {save_time:.2f} seconds")
        print(f"Output file: {OUTPUT_EXCEL_FILE}")
        
        # Final Summary
        total_time = time.time() - start_time
        print("\n" + "=" * 80)
        print("Summary:")
        print("=" * 80)
        print(f"  Original rows: {len(df)}")
        print(f"  After USD currency filter: {len(df_usd) if 'df_usd' in locals() else 'N/A'} rows")
        if 'Country' in df.columns:
            print(f"  After US country filter: {len(df_filtered) if 'df_filtered' in locals() else 'N/A'} rows")
        print(f"  After duplicate removal: {len(df_final)} rows")
        print(f"  Total removed: {len(df) - len(df_final)} rows")
        print(f"  Final unique USD/US stocks: {len(df_final)}")
        
        # Additional statistics
        if 'Sector' in df_final.columns:
            print(f"\n  Top 5 Sectors:")
            for sector, count in df_final['Sector'].value_counts().head(5).items():
                print(f"    {sector}: {count}")
        
        if 'Country' in df_final.columns:
            print(f"\n  Top 5 Countries:")
            for country, count in df_final['Country'].value_counts().head(5).items():
                print(f"    {country}: {count}")
        
        if 'Discount %' in df_final.columns:
            avg_discount = df_final['Discount %'].mean()
            max_discount = df_final['Discount %'].max()
            min_discount = df_final['Discount %'].min()
            print(f"\n  Discount Statistics:")
            print(f"    Average: {avg_discount:.2f}%")
            print(f"    Maximum: {max_discount:.2f}%")
            print(f"    Minimum: {min_discount:.2f}%")
        
        print(f"\n  Processing time: {total_time:.2f} seconds")
        print(f"  Output file: {OUTPUT_EXCEL_FILE}")
        print("=" * 80)
        
        return df_final
        
    except FileNotFoundError:
        print(f"Error: File not found: {INPUT_EXCEL_FILE}")
        return None
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return None

def filter_usd_stocks_parallel():
    """
    Alternative version using multi-threading for processing (if needed for complex operations).
    For simple filtering, pandas is already optimized, but this shows the structure.
    """
    print("=" * 80)
    print("Filtering USD Stocks (Parallel Processing)")
    print("=" * 80)
    
    if not os.path.exists(INPUT_EXCEL_FILE):
        print(f"Error: Input file not found: {INPUT_EXCEL_FILE}")
        return None
    
    print(f"Reading Excel file: {INPUT_EXCEL_FILE}")
    start_time = time.time()
    
    try:
        # Read the Excel file
        df = pd.read_excel(INPUT_EXCEL_FILE, engine='openpyxl')
        
        print(f"Original file contains {len(df)} rows")
        
        if 'Currency' not in df.columns or 'Symbol' not in df.columns:
            print("Error: Required columns (Currency, Symbol) not found.")
            return None
        
        # Filter for USD currency (vectorized operation - already fast)
        usd_variations = ['USD', 'usd', 'Usd', 'US Dollar', 'US$', '$']
        df_usd = df[df['Currency'].isin(usd_variations) | 
                    df['Currency'].str.contains('USD', case=False, na=False) |
                    df['Currency'].str.contains('US Dollar', case=False, na=False)]
        
        print(f"Filtered to USD: {len(df_usd)} rows")
        
        if len(df_usd) == 0:
            print("No USD stocks found.")
            return None
        
        # Filter for US country
        if 'Country' in df_usd.columns:
            us_variations = ['US', 'us', 'Us', 'USA', 'usa', 'U.S.', 'U.S.A.', 'United States', 'United States of America']
            df_filtered = df_usd[df_usd['Country'].isin(us_variations) | 
                                df_usd['Country'].str.contains('United States', case=False, na=False) |
                                df_usd['Country'].str.contains('USA', case=False, na=False) |
                                df_usd['Country'].str.contains('US', case=False, na=False)]
            print(f"Filtered to US country: {len(df_filtered)} rows")
            
            if len(df_filtered) == 0:
                print("No US stocks found.")
                return None
        else:
            print("Warning: 'Country' column not found. Skipping country filter.")
            df_filtered = df_usd
        
        # Remove duplicates (vectorized operation - already fast)
        df_final = df_filtered.drop_duplicates(subset=['Symbol'], keep='first')
        
        print(f"After duplicate removal: {len(df_final)} rows")
        
        # Sort by discount if available
        if 'Discount %' in df_final.columns:
            df_final = df_final.sort_values('Discount %', ascending=False)
        
        # Save to Excel
        df_final.to_excel(OUTPUT_EXCEL_FILE, index=False, engine='openpyxl')
        
        total_time = time.time() - start_time
        print(f"\nCompleted in {total_time:.2f} seconds")
        print(f"Output saved to: {OUTPUT_EXCEL_FILE}")
        
        return df_final
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # Use the standard version (pandas operations are already vectorized and fast)
    # Multi-threading is not needed for simple filtering/duplicate removal
    filter_usd_stocks()

