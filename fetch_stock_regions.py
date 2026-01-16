"""
Script to fetch region information for undervalued stocks from cache
and create an Excel file with the enhanced data.
Uses multi-threading for faster processing.
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
INITIAL_DELAY = 0.2  # Initial delay between requests (seconds)
MAX_RETRIES = 1

# Multi-threading configuration
MAX_WORKERS = 20  # Number of concurrent threads
API_SEMAPHORE = Semaphore(MAX_WORKERS)  # Limit concurrent API requests

# File paths
UNDERVALUED_CACHE_FILE = 'undervalued_stocks_cache.json'
OUTPUT_EXCEL_FILE = 'undervalued_stocks_with_regions.xlsx'

# Setup logging
def setup_logging():
    """
    Configure logging to both file and console.
    """
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'stock_regions_{timestamp}.log')
    
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
    Make an API request with error handling.
    Returns response object or None if failed.
    """
    if params is None:
        params = {}
    params['apikey'] = API_KEY
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 401 or response.status_code == 403:
            try:
                error_data = response.json()
                if 'Error Message' in error_data:
                    logger.error(f"API Key Error: {error_data['Error Message']}")
            except:
                logger.error(f"Authentication failed. Status: {response.status_code}")
            return None
        
        if response.status_code == 429:
            logger.warning(f"Rate limit hit for URL: {url}")
            return None
        
        if response.status_code >= 400:
            logger.warning(f"HTTP error {response.status_code} for URL: {url}")
            return None
        
        response.raise_for_status()
        return response
        
    except requests.exceptions.Timeout:
        logger.warning(f"Request timeout for URL: {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"Request error: {e} for URL: {url}")
        return None

def get_stock_region(symbol):
    """
    Fetch region/country information for a stock from FMP API profile endpoint.
    Returns dict with region information or None if failed.
    """
    url = f"{BASE_URL}/profile/{symbol}"
    
    response = make_api_request(url)
    if response:
        try:
            data = response.json()
            if data and len(data) > 0 and isinstance(data[0], dict):
                profile_data = data[0]
                region_info = {
                    'country': profile_data.get('country', 'N/A'),
                    'city': profile_data.get('city', 'N/A'),
                    'state': profile_data.get('state', 'N/A'),
                    'address': profile_data.get('address', 'N/A'),
                    'phone': profile_data.get('phone', 'N/A'),
                    'website': profile_data.get('website', 'N/A'),
                    'exchange': profile_data.get('exchangeShortName', 'N/A'),
                    'currency': profile_data.get('currency', 'N/A')
                }
                logger.debug(f"Region info for {symbol}: {region_info.get('country')}")
                return region_info
        except (ValueError, IndexError, TypeError) as e:
            logger.debug(f"Error parsing profile response for {symbol}: {e}")
    return None

def process_stock(stock_data, stats_lock, processed_counter, results_list):
    """
    Process a single stock to fetch region information.
    Thread-safe function for parallel processing.
    """
    symbol = stock_data.get('Symbol', '')
    if not symbol:
        return None
    
    # Use semaphore to limit concurrent API requests
    with API_SEMAPHORE:
        region_info = get_stock_region(symbol)
        time.sleep(INITIAL_DELAY)
        
        # Combine existing stock data with region information
        enhanced_stock = stock_data.copy()
        
        if region_info:
            enhanced_stock['Country'] = region_info.get('country', 'N/A')
            enhanced_stock['City'] = region_info.get('city', 'N/A')
            enhanced_stock['State'] = region_info.get('state', 'N/A')
            enhanced_stock['Address'] = region_info.get('address', 'N/A')
            enhanced_stock['Phone'] = region_info.get('phone', 'N/A')
            enhanced_stock['Website'] = region_info.get('website', 'N/A')
            enhanced_stock['Exchange'] = region_info.get('exchange', 'N/A')
            enhanced_stock['Currency'] = region_info.get('currency', 'N/A')
            enhanced_stock['Region_Fetched'] = True
        else:
            # No region data available
            enhanced_stock['Country'] = 'N/A'
            enhanced_stock['City'] = 'N/A'
            enhanced_stock['State'] = 'N/A'
            enhanced_stock['Address'] = 'N/A'
            enhanced_stock['Phone'] = 'N/A'
            enhanced_stock['Website'] = 'N/A'
            enhanced_stock['Exchange'] = 'N/A'
            enhanced_stock['Currency'] = 'N/A'
            enhanced_stock['Region_Fetched'] = False
        
        enhanced_stock['Region_Fetch_Timestamp'] = datetime.now().isoformat()
        
        # Thread-safe append
        with stats_lock:
            results_list.append(enhanced_stock)
            processed_counter['value'] += 1
        
        logger.info(f"Processed {symbol}: {enhanced_stock.get('Company Name', 'N/A')} - Country: {enhanced_stock.get('Country', 'N/A')}")
        print(f"Processed: {symbol} - {enhanced_stock.get('Company Name', 'N/A')} - Country: {enhanced_stock.get('Country', 'N/A')}")
        
        return enhanced_stock

def load_undervalued_stocks():
    """
    Load undervalued stocks from cache file.
    """
    if not os.path.exists(UNDERVALUED_CACHE_FILE):
        logger.error(f"Cache file not found: {UNDERVALUED_CACHE_FILE}")
        return None
    
    try:
        with open(UNDERVALUED_CACHE_FILE, 'r', encoding='utf-8') as f:
            stocks = json.load(f)
        logger.info(f"Loaded {len(stocks)} stocks from {UNDERVALUED_CACHE_FILE}")
        return stocks
    except Exception as e:
        logger.error(f"Error loading cache file: {e}")
        return None

def fetch_regions_for_stocks():
    """
    Main function to fetch region information for all undervalued stocks.
    Uses multi-threading and batch processing.
    """
    logger.info("=" * 80)
    logger.info("Starting region data fetch for undervalued stocks")
    logger.info("=" * 80)
    
    # Load undervalued stocks from cache
    stocks = load_undervalued_stocks()
    if not stocks:
        logger.error("No stocks to process. Exiting.")
        return None
    
    logger.info(f"Processing {len(stocks)} stocks...")
    logger.info(f"Multi-threading: {MAX_WORKERS} concurrent threads")
    logger.info(f"Batch size: 2000 stocks per batch")
    logger.info("=" * 80)
    
    start_time = time.time()
    results = []
    stats_lock = Lock()
    processed_counter = {'value': 0}
    
    # Process stocks in batches of 2000
    BATCH_SIZE = 2000
    total_batches = (len(stocks) + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"Processing {len(stocks)} stocks in {total_batches} batches of {BATCH_SIZE}")
    print(f"Processing {len(stocks)} stocks in {total_batches} batches of {BATCH_SIZE} (using {MAX_WORKERS} threads)")
    
    for batch_num in range(total_batches):
        batch_start = batch_num * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, len(stocks))
        batch_stocks = stocks[batch_start:batch_end]
        
        logger.info(f"Processing batch {batch_num + 1}/{total_batches} (stocks {batch_start + 1}-{batch_end})...")
        print(f"Processing batch {batch_num + 1}/{total_batches} (stocks {batch_start + 1}-{batch_end})...")
        
        batch_results = []
        batch_start_time = time.time()
        
        # Process stocks in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all stocks in the batch for processing
            future_to_stock = {
                executor.submit(
                    process_stock,
                    stock,
                    stats_lock,
                    processed_counter,
                    batch_results
                ): stock
                for stock in batch_stocks
            }
            
            # Process completed futures as they finish
            for future in as_completed(future_to_stock):
                stock = future_to_stock[future]
                symbol = stock.get('Symbol', 'Unknown')
                
                try:
                    result = future.result()
                    if result:
                        batch_results.append(result)
                except Exception as e:
                    logger.error(f"Error processing stock {symbol}: {e}")
        
        # Add batch results to main results
        results.extend(batch_results)
        
        batch_time = time.time() - batch_start_time
        logger.info(f"Batch {batch_num + 1}/{total_batches} complete. Processed {len(batch_results)} stocks in {batch_time:.1f} seconds")
        print(f"Batch {batch_num + 1}/{total_batches} complete. Processed {len(batch_results)} stocks")
        
        # Show progress
        elapsed = time.time() - start_time
        rate = processed_counter['value'] / elapsed if elapsed > 0 else 0
        remaining = (len(stocks) - processed_counter['value']) / rate if rate > 0 else 0
        logger.info(f"Overall Progress: {processed_counter['value']}/{len(stocks)} stocks ({rate:.1f} stocks/sec) | ETA: {remaining/60:.1f} minutes")
    
    total_time = time.time() - start_time
    logger.info("=" * 80)
    logger.info(f"Region fetch complete! Processed {len(results)} stocks in {total_time/60:.1f} minutes")
    logger.info("=" * 80)
    
    # Create DataFrame
    if results:
        # Define column order for better readability
        column_order = [
            'Symbol',
            'Company Name',
            'Current Price',
            'DCF Price',
            'Discount %',
            'Premium %',
            'Valuation Status',
            'Sector',
            'Industry',
            'Country',
            'City',
            'State',
            'Exchange',
            'Currency',
            'Address',
            'Phone',
            'Website',
            'Region_Fetched',
            'Timestamp',
            'Region_Fetch_Timestamp'
        ]
        
        df = pd.DataFrame(results)
        
        # Reorder columns (only include columns that exist)
        existing_columns = [col for col in column_order if col in df.columns]
        other_columns = [col for col in df.columns if col not in column_order]
        df = df[existing_columns + other_columns]
        
        # Sort by discount percentage (highest discount first)
        if 'Discount %' in df.columns:
            df = df.sort_values('Discount %', ascending=False)
        
        # Save to Excel
        try:
            df.to_excel(OUTPUT_EXCEL_FILE, index=False, engine='openpyxl')
            logger.info(f"Results saved to {OUTPUT_EXCEL_FILE}")
            print(f"\nResults saved to {OUTPUT_EXCEL_FILE}")
            
            # Print summary statistics
            logger.info("\n" + "=" * 80)
            logger.info("Summary Statistics:")
            logger.info(f"Total stocks processed: {len(df)}")
            
            if 'Country' in df.columns:
                country_counts = df['Country'].value_counts()
                logger.info(f"\nTop 10 Countries by Stock Count:")
                for country, count in country_counts.head(10).items():
                    logger.info(f"  {country}: {count}")
            
            if 'Region_Fetched' in df.columns:
                fetched_count = df['Region_Fetched'].sum()
                logger.info(f"\nRegion data successfully fetched: {fetched_count}/{len(df)} ({fetched_count/len(df)*100:.1f}%)")
            
            logger.info("=" * 80)
            
            # Also print to console
            print("\n" + "=" * 80)
            print(f"Summary:")
            print(f"Total stocks: {len(df)}")
            if 'Region_Fetched' in df.columns:
                fetched_count = df['Region_Fetched'].sum()
                print(f"Region data fetched: {fetched_count}/{len(df)} ({fetched_count/len(df)*100:.1f}%)")
            print(f"Output file: {OUTPUT_EXCEL_FILE}")
            print("=" * 80)
            
            return df
        except Exception as e:
            logger.error(f"Error saving Excel file: {e}")
            print(f"\nError saving Excel file: {e}")
            return None
    else:
        logger.warning("No results to save.")
        print("\nNo results to save.")
        return None

def validate_api_key():
    """
    Test the API key by making a simple request.
    Returns True if valid, False otherwise.
    """
    logger.info("Validating API key...")
    test_url = f"{BASE_URL}/profile/AAPL"
    test_params = {'apikey': API_KEY}
    
    try:
        response = requests.get(test_url, params=test_params, timeout=10)
        
        if response.status_code == 200:
            logger.info("API key is valid!")
            return True
        elif response.status_code in [401, 403]:
            try:
                error_data = response.json()
                error_msg = error_data.get('Error Message', 'Unknown error')
                logger.error(f"API key validation failed: {error_msg}")
                print(f"\n❌ API Key Validation Failed!")
                print(f"Error: {error_msg}")
            except:
                logger.error(f"API key validation failed with status {response.status_code}")
                print(f"\n❌ API Key Validation Failed! Status code: {response.status_code}")
            return False
        else:
            logger.warning(f"API key validation returned status {response.status_code}")
            return True
    except Exception as e:
        logger.error(f"Error validating API key: {e}")
        print(f"\n⚠️  Could not validate API key: {e}")
        print("Proceeding anyway, but API calls may fail...")
        return True

if __name__ == "__main__":
    if not API_KEY:
        logger.error("FMP_API_KEY not found in .env file")
        print("Error: FMP_API_KEY not found in .env file")
        print("Please create a .env file with: FMP_API_KEY=your_api_key_here")
    elif len(API_KEY) < 10:
        logger.error(f"API key appears to be invalid (too short: {len(API_KEY)} chars)")
        print(f"Error: API key appears to be invalid. Please check your .env file.")
    else:
        logger.info(f"API Key loaded (length: {len(API_KEY)} chars, starts with: {API_KEY[:5]}...)")
        
        # Validate API key before proceeding
        if validate_api_key():
            try:
                fetch_regions_for_stocks()
            except KeyboardInterrupt:
                logger.warning("Process interrupted by user")
                print("\n\nProcess interrupted by user.")
            except Exception as e:
                logger.exception(f"Unexpected error occurred: {e}")
                print(f"\nAn error occurred. Check the log file for details: {e}")
        else:
            print("\nPlease fix your API key before running the script.")
            print("You can get a free API key at: https://site.financialmodelingprep.com/")

