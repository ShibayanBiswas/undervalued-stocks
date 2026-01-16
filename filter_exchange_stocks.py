"""
Script to filter stocks by exchange (NYSE or NASDAQ) from sector-wise Excel files
and save filtered results to a new folder.
"""

import os
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

# File paths
INPUT_FOLDER = 'undervalued_stocks_by_sector'
OUTPUT_FOLDER = 'undervalued_stocks_by_sector_filtered'

# Allowed exchanges
ALLOWED_EXCHANGES = ['NYSE', 'NASDAQ', 'Nasdaq', 'nasdaq', 'nyse', 'NYS', 'NSDQ']

# Setup logging
def setup_logging():
    """Configure logging to both file and console."""
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'exchange_filter_{timestamp}.log')
    
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

logger = setup_logging()

def filter_stocks_by_exchange(input_file, output_file):
    """
    Filter stocks from an Excel file to keep only NYSE or NASDAQ stocks.
    
    Args:
        input_file: Path to input Excel file
        output_file: Path to output Excel file
    
    Returns:
        tuple: (total_count, filtered_count, removed_count)
    """
    try:
        # Read Excel file
        df = pd.read_excel(input_file, engine='openpyxl')
        total_count = len(df)
        
        if total_count == 0:
            logger.warning(f"Empty file: {input_file}")
            return (0, 0, 0)
        
        # Check for Exchange column (try different possible column names)
        exchange_column = None
        possible_names = ['Exchange', 'exchange', 'EXCHANGE', 'Stock Exchange', 'Stock_Exchange']
        
        for col_name in possible_names:
            if col_name in df.columns:
                exchange_column = col_name
                break
        
        if exchange_column is None:
            logger.warning(f"No Exchange column found in {input_file}. Available columns: {list(df.columns)}")
            # Try to find column containing 'exchange' in name (case-insensitive)
            for col in df.columns:
                if 'exchange' in str(col).lower():
                    exchange_column = col
                    logger.info(f"Found exchange column: {col}")
                    break
        
        if exchange_column is None:
            logger.error(f"Cannot find Exchange column in {input_file}. Skipping.")
            return (total_count, 0, total_count)
        
        # Filter for NYSE or NASDAQ
        # Handle case-insensitive matching and variations
        df_filtered = df[
            df[exchange_column].astype(str).str.upper().str.contains('NYSE|NASDAQ|NYS|NSDQ', case=False, na=False, regex=True)
        ]
        
        filtered_count = len(df_filtered)
        removed_count = total_count - filtered_count
        
        # Save filtered results
        if filtered_count > 0:
            df_filtered.to_excel(output_file, index=False, engine='openpyxl')
            logger.info(f"Filtered {input_file}: {total_count} -> {filtered_count} stocks (removed {removed_count})")
        else:
            logger.warning(f"No NYSE/NASDAQ stocks found in {input_file}. File not created.")
        
        return (total_count, filtered_count, removed_count)
        
    except Exception as e:
        logger.error(f"Error processing {input_file}: {e}")
        return (0, 0, 0)

def main():
    """Main function to filter all Excel files by exchange."""
    print("=" * 80)
    print("Filtering Stocks by Exchange (NYSE/NASDAQ)")
    print("=" * 80)
    
    # Check if input folder exists
    if not os.path.exists(INPUT_FOLDER):
        logger.error(f"Input folder not found: {INPUT_FOLDER}")
        print(f"Error: Input folder not found: {INPUT_FOLDER}")
        return
    
    # Create output folder
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"\nCreated output folder: {OUTPUT_FOLDER}")
    else:
        print(f"\nUsing existing output folder: {OUTPUT_FOLDER}")
    
    # Get all Excel files from input folder
    excel_files = list(Path(INPUT_FOLDER).glob('*.xlsx'))
    
    # Exclude summary files
    excel_files = [f for f in excel_files if not f.name.startswith('_')]
    
    if len(excel_files) == 0:
        logger.warning(f"No Excel files found in {INPUT_FOLDER}")
        print(f"Error: No Excel files found in {INPUT_FOLDER}")
        return
    
    print(f"\nFound {len(excel_files)} Excel files to process")
    print("=" * 80)
    
    # Process each file
    total_stats = {
        'total_stocks': 0,
        'filtered_stocks': 0,
        'removed_stocks': 0,
        'files_processed': 0,
        'files_with_results': 0
    }
    
    print("\nProcessing files...")
    print("-" * 80)
    
    for excel_file in sorted(excel_files):
        file_name = excel_file.name
        input_path = str(excel_file)
        output_path = os.path.join(OUTPUT_FOLDER, file_name)
        
        print(f"\nProcessing: {file_name}")
        
        total, filtered, removed = filter_stocks_by_exchange(input_path, output_path)
        
        total_stats['total_stocks'] += total
        total_stats['filtered_stocks'] += filtered
        total_stats['removed_stocks'] += removed
        total_stats['files_processed'] += 1
        
        if filtered > 0:
            total_stats['files_with_results'] += 1
            print(f"  ✓ {total} -> {filtered} stocks (removed {removed})")
        else:
            print(f"  ✗ No NYSE/NASDAQ stocks found (removed all {total})")
    
    # Create summary file
    summary_file = os.path.join(OUTPUT_FOLDER, '_summary.txt')
    with open(summary_file, 'w') as f:
        f.write("Exchange Filter Summary (NYSE/NASDAQ)\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Filter Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder: {INPUT_FOLDER}\n")
        f.write(f"Output Folder: {OUTPUT_FOLDER}\n\n")
        f.write("Statistics:\n")
        f.write("-" * 80 + "\n")
        f.write(f"Files Processed: {total_stats['files_processed']}\n")
        f.write(f"Files with Results: {total_stats['files_with_results']}\n")
        f.write(f"Total Stocks (Before Filter): {total_stats['total_stocks']}\n")
        f.write(f"Stocks After Filter (NYSE/NASDAQ): {total_stats['filtered_stocks']}\n")
        f.write(f"Stocks Removed: {total_stats['removed_stocks']}\n")
        if total_stats['total_stocks'] > 0:
            retention_rate = (total_stats['filtered_stocks'] / total_stats['total_stocks']) * 100
            f.write(f"Retention Rate: {retention_rate:.2f}%\n")
    
    print("\n" + "=" * 80)
    print("Filtering Complete!")
    print("=" * 80)
    print(f"\nSummary:")
    print(f"  Files Processed: {total_stats['files_processed']}")
    print(f"  Files with Results: {total_stats['files_with_results']}")
    print(f"  Total Stocks (Before): {total_stats['total_stocks']}")
    print(f"  Stocks After Filter (NYSE/NASDAQ): {total_stats['filtered_stocks']}")
    print(f"  Stocks Removed: {total_stats['removed_stocks']}")
    if total_stats['total_stocks'] > 0:
        retention_rate = (total_stats['filtered_stocks'] / total_stats['total_stocks']) * 100
        print(f"  Retention Rate: {retention_rate:.2f}%")
    print(f"\nResults saved in: {OUTPUT_FOLDER}")
    print(f"Summary saved to: {summary_file}")
    print("=" * 80)

if __name__ == '__main__':
    main()

