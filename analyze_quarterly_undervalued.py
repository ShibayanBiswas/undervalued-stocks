"""
Script to add market cap to undervalued stocks and organize them sector-wise.
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv
import time
import logging
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore

# Load environment variables
load_dotenv()

# FMP API Configuration
API_KEY = os.getenv('FMP_API_KEY')
if API_KEY:
    API_KEY = API_KEY.strip()
BASE_URL = 'https://financialmodelingprep.com/api/v3'

# Rate limiting configuration
MAX_RETRIES = 1
INITIAL_DELAY = 0.2
RATE_LIMIT_DELAY = 1.0

# Multi-threading configuration
MAX_WORKERS = 20
API_SEMAPHORE = Semaphore(MAX_WORKERS)

# File paths
INPUT_EXCEL_FILE = 'undervalued_stocks_usd_filtered.xlsx'
OUTPUT_FOLDER = 'undervalued_stocks_by_sector'

# Setup logging
def setup_logging():
    """Configure logging to both file and console."""
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'sector_organization_{timestamp}.log')
    
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

def make_api_request(url, params=None):
    """
    Make an API request with rate limiting and error handling.
    """
    if params is None:
        params = {}
    params['apikey'] = API_KEY
    
    with API_SEMAPHORE:
        try:
            time.sleep(INITIAL_DELAY)
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                logger.warning(f"Rate limit hit for {url}. Waiting {RATE_LIMIT_DELAY}s...")
                time.sleep(RATE_LIMIT_DELAY)
                return None
            elif response.status_code in [401, 403]:
                logger.error(f"API authentication error for {url}. Status: {response.status_code}")
                return None
            else:
                logger.warning(f"API request failed for {url}. Status: {response.status_code}")
                return None
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout for {url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error: {e} for URL: {url}")
            return None


def get_market_cap(symbol):
    """
    Fetch market capitalization for a stock.
    Tries multiple endpoints: key metrics, profile, or quote.
    """
    # Try key metrics first (most reliable for market cap)
    url = f"{BASE_URL}/key-metrics/{symbol}"
    response = make_api_request(url)
    if response:
        try:
            data = response.json()
            if data and len(data) > 0:
                # Get most recent market cap
                latest = data[0]
                market_cap = latest.get('marketCap', None)
                if market_cap and market_cap > 0:
                    return float(market_cap)
        except (ValueError, IndexError, TypeError) as e:
            logger.debug(f"Error parsing key metrics for {symbol}: {e}")
    
    # Try profile endpoint
    url = f"{BASE_URL}/profile/{symbol}"
    response = make_api_request(url)
    if response:
        try:
            data = response.json()
            if data and len(data) > 0:
                market_cap = data[0].get('mktCap', None)
                if market_cap and market_cap > 0:
                    return float(market_cap)
        except (ValueError, IndexError, TypeError) as e:
            logger.debug(f"Error parsing profile for {symbol}: {e}")
    
    # Try quote endpoint (shares outstanding * price)
    url = f"{BASE_URL}/quote/{symbol}"
    response = make_api_request(url)
    if response:
        try:
            data = response.json()
            if data and len(data) > 0:
                price = data[0].get('price', None)
                shares_outstanding = data[0].get('sharesOutstanding', None)
                if price and shares_outstanding:
                    market_cap = price * shares_outstanding
                    if market_cap > 0:
                        return float(market_cap)
        except (ValueError, IndexError, TypeError) as e:
            logger.debug(f"Error parsing quote for {symbol}: {e}")
    
    return None

def process_stock(row, stats_lock, processed_counter, total_stocks):
    """
    Process a single stock: fetch market cap and prepare data for sector organization.
    """
    symbol = row.get('Symbol', '')
    
    if not symbol:
        logger.warning("Row missing Symbol, skipping")
        return None
    
    # Fetch market cap
    market_cap = get_market_cap(symbol)
    
    # Create result row
    result_row = row.copy()
    
    # Remove Region_Fetched column if it exists
    if 'Region_Fetched' in result_row:
        del result_row['Region_Fetched']
    
    # Add Market Cap column
    result_row['Market Cap'] = market_cap if market_cap else None
    
    # Update statistics
    with stats_lock:
        processed_counter['total'] += 1
        if processed_counter['total'] % 50 == 0:
            print(f"Processed {processed_counter['total']}/{total_stocks} stocks...")
    
    logger.info(f"{symbol}: Market Cap={market_cap/1e9:.2f}B" if market_cap else f"{symbol}: Market Cap=N/A")
    
    return result_row

def main():
    """Main function to add market cap and organize stocks by sector."""
    print("=" * 80)
    print("Adding Market Cap and Organizing Stocks by Sector")
    print("=" * 80)
    
    # Check API key
    if not API_KEY:
        logger.error("FMP_API_KEY not found in .env file")
        print("Error: FMP_API_KEY not found in .env file")
        return
    
    # Read input Excel file
    print(f"\nReading input file: {INPUT_EXCEL_FILE}")
    if not os.path.exists(INPUT_EXCEL_FILE):
        logger.error(f"Input file not found: {INPUT_EXCEL_FILE}")
        print(f"Error: Input file not found: {INPUT_EXCEL_FILE}")
        return
    
    try:
        df = pd.read_excel(INPUT_EXCEL_FILE, engine='openpyxl')
        print(f"Loaded {len(df)} stocks from input file")
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        print(f"Error reading Excel file: {e}")
        return
    
    # Check required columns
    required_columns = ['Symbol']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        print(f"Error: Missing required columns: {missing_columns}")
        print(f"Available columns: {list(df.columns)}")
        return
    
    # Filter out rows with missing symbols
    initial_count = len(df)
    df = df.dropna(subset=['Symbol'])
    print(f"After filtering: {len(df)} stocks (removed {initial_count - len(df)} invalid rows)")
    
    if len(df) == 0:
        print("No valid stocks to process")
        return
    
    # Process stocks with multi-threading
    print("\n" + "=" * 80)
    print("Processing stocks (fetching market cap)...")
    print("=" * 80)
    
    stats_lock = Lock()
    processed_counter = {'total': 0}
    total_stocks = len(df)
    
    processed_stocks = []
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_stock, row.to_dict(), stats_lock, processed_counter, total_stocks): idx
            for idx, row in df.iterrows()
        }
        
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    processed_stocks.append(result)
            except Exception as e:
                idx = futures[future]
                symbol = df.iloc[idx].get('Symbol', 'Unknown')
                logger.error(f"Error processing {symbol}: {e}")
    
    processing_time = time.time() - start_time
    
    print(f"\nProcessing completed in {processing_time:.2f} seconds")
    print(f"Processed {len(processed_stocks)} stocks")
    
    if len(processed_stocks) == 0:
        print("No stocks processed successfully")
        return
    
    # Convert to DataFrame
    df_processed = pd.DataFrame(processed_stocks)
    
    # Remove Region_Fetched column if it exists
    if 'Region_Fetched' in df_processed.columns:
        df_processed = df_processed.drop(columns=['Region_Fetched'])
        print("Removed 'Region_Fetched' column")
    
    # Reorder columns: keep Current Price and DCF Price together
    # Get all columns
    all_columns = list(df_processed.columns)
    
    # Define priority columns (in desired order)
    priority_columns = ['Symbol', 'Company Name']
    
    # Add Current Price if it exists
    if 'Current Price' in all_columns:
        priority_columns.append('Current Price')
    
    # Add DCF Price if it exists
    if 'DCF Price' in all_columns:
        priority_columns.append('DCF Price')
    
    # Get columns that exist in the dataframe
    ordered_columns = []
    for col in priority_columns:
        if col in all_columns:
            ordered_columns.append(col)
            all_columns.remove(col)
    
    # Add remaining important columns
    remaining_important = ['Market Cap', 'Price Difference', 'Price Difference %', 'Sector', 'Industry', 'Country', 'Currency']
    for col in remaining_important:
        if col in all_columns:
            ordered_columns.append(col)
            all_columns.remove(col)
    
    # Add all other remaining columns
    ordered_columns.extend([col for col in all_columns if col not in ordered_columns])
    
    # Reorder dataframe
    df_processed = df_processed[ordered_columns]
    
    # Remove invalid rows (missing Symbol or other critical data)
    print("\n" + "=" * 80)
    print("Removing invalid rows...")
    print("=" * 80)
    
    initial_count = len(df_processed)
    
    # Remove rows with missing Symbol
    df_processed = df_processed.dropna(subset=['Symbol'])
    
    # Remove rows with empty Symbol strings
    df_processed = df_processed[df_processed['Symbol'].astype(str).str.strip() != '']
    
    # Filter for market cap >= 1 billion (1,000,000,000)
    if 'Market Cap' in df_processed.columns:
        # Convert Market Cap to numeric, handling any string values
        df_processed['Market Cap'] = pd.to_numeric(df_processed['Market Cap'], errors='coerce')
        
        # Filter for market cap >= 1 billion
        before_mcap_filter = len(df_processed)
        df_processed = df_processed[df_processed['Market Cap'] >= 1_000_000_000]
        after_mcap_filter = len(df_processed)
        
        print(f"Removed {initial_count - before_mcap_filter} rows with missing/invalid Symbol")
        print(f"Removed {before_mcap_filter - after_mcap_filter} rows with market cap < 1 billion")
        print(f"Final count: {after_mcap_filter} stocks (from {initial_count} initial)")
    else:
        print(f"Warning: 'Market Cap' column not found. Skipping market cap filter.")
        print(f"Removed {initial_count - len(df_processed)} rows with missing/invalid Symbol")
        print(f"Final count: {len(df_processed)} stocks (from {initial_count} initial)")
    
    if len(df_processed) == 0:
        print("No valid stocks remaining after filtering. Exiting.")
        return
    
    # Ensure Sector column exists
    if 'Sector' not in df_processed.columns:
        df_processed['Sector'] = 'Unknown'
    
    # Fill missing sectors
    df_processed['Sector'] = df_processed['Sector'].fillna('Unknown')
    
    # Create output folder
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"\nCreated output folder: {OUTPUT_FOLDER}")
    
    # Group by sector and save to separate Excel files
    print("\n" + "=" * 80)
    print("Organizing stocks by sector and saving to Excel files...")
    print("=" * 80)
    
    sectors = df_processed['Sector'].unique()
    print(f"\nFound {len(sectors)} sectors:")
    
    for sector in sorted(sectors):
        sector_df = df_processed[df_processed['Sector'] == sector].copy()
        
        # Sort by Discount % if available (highest discount first)
        if 'Discount %' in sector_df.columns:
            sector_df = sector_df.sort_values('Discount %', ascending=False)
        
        # Create safe filename from sector name
        safe_sector_name = "".join(c for c in str(sector) if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_sector_name = safe_sector_name.replace(' ', '_')
        if not safe_sector_name:
            safe_sector_name = 'Unknown'
        
        output_file = os.path.join(OUTPUT_FOLDER, f"{safe_sector_name}.xlsx")
        
        try:
            sector_df.to_excel(output_file, index=False, engine='openpyxl')
            print(f"  {sector}: {len(sector_df)} stocks -> {output_file}")
            logger.info(f"Saved {len(sector_df)} stocks for sector '{sector}' to {output_file}")
        except Exception as e:
            logger.error(f"Error saving sector '{sector}': {e}")
            print(f"  Error saving {sector}: {e}")
    
    # Create summary file
    summary_file = os.path.join(OUTPUT_FOLDER, '_summary.txt')
    with open(summary_file, 'w') as f:
        f.write("Stock Organization by Sector Summary\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Stocks Processed: {total_stocks}\n")
        f.write(f"Stocks with Market Cap Added: {len(processed_stocks)}\n")
        f.write(f"Stocks after filtering (Market Cap >= 1B, valid rows): {len(df_processed)}\n")
        f.write(f"Processing Time: {processing_time:.2f} seconds\n\n")
        f.write("Filtering Criteria:\n")
        f.write("  - Removed rows with missing/invalid Symbol\n")
        f.write("  - Market Cap >= 1 billion USD\n\n")
        f.write("Sector Breakdown:\n")
        f.write("-" * 80 + "\n")
        for sector in sorted(sectors):
            count = len(df_processed[df_processed['Sector'] == sector])
            f.write(f"  {sector}: {count} stocks\n")
    
    print(f"\nSummary saved to: {summary_file}")
    print("\n" + "=" * 80)
    print("Analysis complete!")
    print(f"Results saved in folder: {OUTPUT_FOLDER}")
    print("=" * 80)

if __name__ == '__main__':
    main()

