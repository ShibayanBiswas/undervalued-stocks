"""
Script to fetch stocks from FMP API where current price is lower than DCF price.
Also fetches sector, industry, and sub-industry information.
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
    API_KEY = API_KEY.strip()  # Remove any whitespace
# Using v3 API endpoint (stable requires paid subscription for many endpoints)
BASE_URL = 'https://financialmodelingprep.com/api/v3'

# Rate limiting configuration
MAX_RETRIES = 1  # Only 1 try per request
INITIAL_DELAY = 0.2  # Initial delay between requests (seconds)
MAX_DELAY = 60  # Maximum delay between retries (seconds)
RATE_LIMIT_DELAY = 1.0  # Delay after rate limit hit (seconds)
CONSECUTIVE_ERROR_PAUSE = 1  # Pause time after consecutive errors (seconds)

# Multi-threading configuration
MAX_WORKERS = 20  # Number of concurrent threads
API_SEMAPHORE = Semaphore(MAX_WORKERS)  # Limit concurrent API requests

# Cache configuration
CACHE_FILE = 'stock_cache.json'
UNDERVALUED_CACHE_FILE = 'undervalued_stocks_cache.json'

# Setup logging
def setup_logging():
    """
    Configure logging to both file and console.
    """
    # Create logs directory if it doesn't exist
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'fmp_stocks_{timestamp}.log')
    
    # Configure logging format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Set up root logger
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # Console output
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger

# Initialize logger
logger = setup_logging()

# Stock data cache
stock_cache = {}

def load_cache():
    """
    Load stock cache from file.
    Cache structure: {symbol: {'price': float, 'dcf': float, 'profile': dict, 'timestamp': str}}
    Also includes '_undervalued_stocks' and '_fair_stocks' lists
    """
    global stock_cache
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                stock_cache = json.load(f)
            # Initialize lists if they don't exist
            if '_undervalued_stocks' not in stock_cache:
                stock_cache['_undervalued_stocks'] = []
            if '_fair_stocks' not in stock_cache:
                stock_cache['_fair_stocks'] = []
            # Count actual stocks (excluding special keys)
            stock_count = len([k for k in stock_cache.keys() if not k.startswith('_')])
            logger.info(f"Loaded cache with {stock_count} stocks, {len(stock_cache.get('_undervalued_stocks', []))} undervalued, {len(stock_cache.get('_fair_stocks', []))} fair")
        except Exception as e:
            logger.warning(f"Error loading cache: {e}. Starting with empty cache.")
            stock_cache = {'_undervalued_stocks': [], '_fair_stocks': []}
    else:
        stock_cache = {'_undervalued_stocks': [], '_fair_stocks': []}

def save_cache():
    """
    Save stock cache to file.
    """
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(stock_cache, f, indent=2)
        stock_count = len([k for k in stock_cache.keys() if not k.startswith('_')])
        logger.debug(f"Saved cache with {stock_count} stocks, {len(stock_cache.get('_undervalued_stocks', []))} undervalued, {len(stock_cache.get('_fair_stocks', []))} fair")
    except Exception as e:
        logger.error(f"Error saving cache: {e}")

def get_cached_stock(symbol):
    """
    Get cached stock data if available.
    Returns dict with 'price', 'dcf', and 'profile' or None if not cached.
    """
    if symbol in stock_cache:
        return stock_cache[symbol]
    return None

def cache_stock(symbol, price=None, dcf=None, profile=None):
    """
    Cache stock data.
    """
    if symbol not in stock_cache:
        stock_cache[symbol] = {}
    
    if price is not None:
        stock_cache[symbol]['price'] = price
    if dcf is not None:
        stock_cache[symbol]['dcf'] = dcf
    if profile is not None:
        stock_cache[symbol]['profile'] = profile
    
    stock_cache[symbol]['timestamp'] = datetime.now().isoformat()

# Undervalued stocks cache (separate file)
undervalued_stocks_cache = []

def load_undervalued_cache():
    """
    Load undervalued stocks cache from separate file.
    """
    global undervalued_stocks_cache
    if os.path.exists(UNDERVALUED_CACHE_FILE):
        try:
            with open(UNDERVALUED_CACHE_FILE, 'r') as f:
                undervalued_stocks_cache = json.load(f)
            logger.info(f"Loaded undervalued stocks cache with {len(undervalued_stocks_cache)} stocks")
        except Exception as e:
            logger.warning(f"Error loading undervalued cache: {e}. Starting with empty cache.")
            undervalued_stocks_cache = []
    else:
        undervalued_stocks_cache = []

def save_undervalued_cache():
    """
    Save undervalued stocks cache to separate file.
    """
    try:
        with open(UNDERVALUED_CACHE_FILE, 'w') as f:
            json.dump(undervalued_stocks_cache, f, indent=2)
        logger.debug(f"Saved undervalued stocks cache with {len(undervalued_stocks_cache)} stocks")
    except Exception as e:
        logger.error(f"Error saving undervalued cache: {e}")

# Load cache on startup
load_cache()
load_undervalued_cache()

def make_api_request(url, params=None, max_retries=MAX_RETRIES):
    """
    Make an API request with only 1 try (no retries).
    Returns response object or None if failed.
    """
    if params is None:
        params = {}
    params['apikey'] = API_KEY
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        # Check for API key errors in response
        if response.status_code == 401 or response.status_code == 403:
            try:
                error_data = response.json()
                if 'Error Message' in error_data:
                    logger.error(f"API Key Error: {error_data['Error Message']}")
                    logger.error(f"Please check your API key in .env file. Current key (first 10 chars): {API_KEY[:10] if API_KEY and len(API_KEY) > 10 else 'INVALID'}")
            except:
                logger.error(f"Authentication failed. Status: {response.status_code}")
            return None
        
        # Handle rate limiting (HTTP 429) - just log and return None
        if response.status_code == 429:
            logger.warning(f"Rate limit hit for URL: {url}")
            return None
        
        # Handle other HTTP errors - log response body for debugging
        if response.status_code >= 400:
            try:
                error_data = response.json()
                if 'Error Message' in error_data:
                    logger.error(f"API Error: {error_data['Error Message']}")
                else:
                    logger.warning(f"HTTP error {response.status_code} for URL: {url}. Response: {str(error_data)[:200]}")
            except:
                # Try to get text response
                try:
                    error_text = response.text[:200]
                    logger.warning(f"HTTP error {response.status_code} for URL: {url}. Response text: {error_text}")
                except:
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

def get_all_stocks():
    """
    Fetch all available stocks from FMP API.
    Returns a list of stock symbols.
    Reference: https://site.financialmodelingprep.com/developer/docs#stock-directory
    """
    logger.info("Fetching all stocks from FMP API...")
    # Try v3 API stock list endpoint
    url = f"{BASE_URL}/stock/list"
    
    response = make_api_request(url)
    if response:
        try:
            stocks = response.json()
            if stocks and len(stocks) > 0:
                logger.info(f"Successfully fetched {len(stocks)} stocks")
                return stocks
            else:
                logger.warning("Received empty stock list from API")
        except ValueError as e:
            logger.error(f"Error parsing response: {e}")
    
    # If stock-list fails, use popular stocks as fallback
    logger.warning("stock/list endpoint failed or unavailable, using fallback list of popular stocks...")
    
    # Try to get popular US stocks as fallback
    popular_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'BRK.B', 'V', 'JNJ', 
                       'WMT', 'JPM', 'MA', 'PG', 'UNH', 'HD', 'DIS', 'BAC', 'ADBE', 'NFLX',
                       'PYPL', 'CMCSA', 'KO', 'PFE', 'TMO', 'COST', 'AVGO', 'CSCO', 'PEP', 'ABT',
                       'NKE', 'MRK', 'T', 'VZ', 'CVX', 'XOM', 'LLY', 'ABBV', 'ACN', 'DHR']
    
    logger.info(f"Using fallback list of {len(popular_symbols)} popular stocks")
    # Convert to the format expected by the rest of the code
    stocks = [{'symbol': sym} for sym in popular_symbols]
    return stocks

def get_dcf_bulk():
    """
    Fetch DCF (Discounted Cash Flow) values for all stocks using bulk API.
    Also populates cache with fetched data.
    Reference: https://site.financialmodelingprep.com/developer/docs#bulk
    Returns a dictionary mapping symbol to DCF value.
    """
    logger.info("Fetching DCF values using bulk API...")
    url = f"{BASE_URL}/dcf-bulk"
    
    response = make_api_request(url)
    if response:
        try:
            data = response.json()
            dcf_dict = {}
            for item in data:
                symbol = item.get('symbol', '')
                dcf_value = item.get('dcf', None)
                if symbol and dcf_value:
                    dcf_dict[symbol] = dcf_value
                    # Cache the DCF value
                    cache_stock(symbol, dcf=dcf_value)
            logger.info(f"Successfully fetched DCF values for {len(dcf_dict)} stocks and cached them")
            return dcf_dict
        except ValueError as e:
            logger.error(f"Error parsing DCF bulk response: {e}")
            return {}
    logger.warning("Failed to fetch DCF bulk data, will use individual API calls")
    return None

def get_dcf_value(symbol):
    """
    Fetch DCF (Discounted Cash Flow) value for a single stock symbol.
    Also extracts Stock Price from the DCF response if available.
    Checks cache first, then fetches from API if not cached.
    Reference: https://site.financialmodelingprep.com/developer/docs#discounted-cash-flow
    Returns tuple: (dcf_value, stock_price_from_dcf) or (None, None)
    """
    # Check cache first
    cached = get_cached_stock(symbol)
    if cached and 'dcf' in cached and cached['dcf'] is not None:
        logger.debug(f"Using cached DCF for {symbol}: {cached['dcf']}")
        # Also return cached price if available
        cached_price = cached.get('price', None)
        return cached['dcf'], cached_price
    
    # Fetch from API - using correct endpoint format per FMP documentation
    # Reference: https://site.financialmodelingprep.com/developer/docs#discounted-cash-flow
    url = f"{BASE_URL}/discounted-cash-flow/{symbol}"
    
    response = make_api_request(url)
    if response:
        try:
            data = response.json()
            if data and len(data) > 0 and isinstance(data[0], dict):
                dcf_value = data[0].get('dcf', None)
                # Extract Stock Price from DCF response (field name is "Stock Price")
                stock_price_from_dcf = data[0].get('Stock Price', None)
                dcf_date = data[0].get('date', None)
                
                if dcf_value and dcf_value > 0:
                    # Cache both DCF and price if available
                    if stock_price_from_dcf and stock_price_from_dcf > 0:
                        cache_stock(symbol, dcf=dcf_value, price=stock_price_from_dcf)
                        logger.debug(f"DCF value for {symbol}: {dcf_value}, Stock Price from DCF: {stock_price_from_dcf}, Date: {dcf_date}")
                    else:
                        cache_stock(symbol, dcf=dcf_value)
                        logger.debug(f"DCF value for {symbol}: {dcf_value}, Date: {dcf_date}")
                    return dcf_value, stock_price_from_dcf
            else:
                logger.debug(f"No DCF data available for {symbol} (empty response)")
        except (ValueError, IndexError, TypeError) as e:
            logger.debug(f"Error parsing DCF response for {symbol}: {e}")
    return None, None

def get_stock_price(symbol):
    """
    Fetch current stock price for a symbol.
    Checks cache first, then fetches from API if not cached.
    Reference: https://site.financialmodelingprep.com/developer/docs#quote
    """
    # Check cache first
    cached = get_cached_stock(symbol)
    if cached and 'price' in cached and cached['price'] is not None:
        logger.debug(f"Using cached price for {symbol}: {cached['price']}")
        return cached['price']
    
    # Fetch from API - using path parameter format for v3 API
    url = f"{BASE_URL}/quote/{symbol}"
    
    response = make_api_request(url)
    if response:
        try:
            data = response.json()
            if data and len(data) > 0 and isinstance(data[0], dict):
                price = data[0].get('price', None)
                if price and price > 0:
                    cache_stock(symbol, price=price)
                    logger.debug(f"Price for {symbol}: {price}")
                    return price
        except (ValueError, IndexError, TypeError) as e:
            logger.debug(f"Error parsing price response for {symbol}: {e}")
    return None

def get_profiles_bulk():
    """
    Fetch company profiles for all stocks using bulk API.
    Also populates cache with fetched data.
    Reference: https://site.financialmodelingprep.com/developer/docs#bulk
    Returns a dictionary mapping symbol to profile data.
    """
    logger.info("Fetching company profiles using bulk API...")
    profiles_dict = {}
    part = 0
    
    while True:
        url = f"{BASE_URL}/profile-bulk"
        params = {'part': part}
        
        response = make_api_request(url, params)
        if not response:
            break
            
        try:
            data = response.json()
            if not data or len(data) == 0:
                break
                
            for item in data:
                symbol = item.get('symbol', '')
                if symbol:
                    profile = {
                        'sector': item.get('sector', 'N/A'),
                        'industry': item.get('industry', 'N/A'),
                        'companyName': item.get('companyName', 'N/A')
                    }
                    profiles_dict[symbol] = profile
                    # Cache the profile
                    cache_stock(symbol, profile=profile)
            
            # Check if there are more parts
            if len(data) < 1000:  # Assuming 1000 items per part
                break
            part += 1
            time.sleep(INITIAL_DELAY)  # Small delay between bulk requests
            
        except ValueError as e:
            logger.error(f"Error parsing profile bulk response: {e}")
            break
    
    logger.info(f"Successfully fetched profiles for {len(profiles_dict)} stocks and cached them")
    return profiles_dict if profiles_dict else None

def get_company_profile(symbol):
    """
    Fetch company profile including sector, industry, and sub-industry for a single symbol.
    Checks cache first, then fetches from API if not cached.
    Reference: https://site.financialmodelingprep.com/developer/docs#company-information
    """
    # Check cache first
    cached = get_cached_stock(symbol)
    if cached and 'profile' in cached and cached['profile'] is not None:
        logger.debug(f"Using cached profile for {symbol}")
        return cached['profile']
    
    # Fetch from API - using path parameter format for v3 API
    url = f"{BASE_URL}/profile/{symbol}"
    
    response = make_api_request(url)
    if response:
        try:
            data = response.json()
            if data and len(data) > 0 and isinstance(data[0], dict):
                profile = {
                    'sector': data[0].get('sector', 'N/A'),
                    'industry': data[0].get('industry', 'N/A'),
                    'companyName': data[0].get('companyName', 'N/A')
                }
                cache_stock(symbol, profile=profile)
                logger.debug(f"Profile for {symbol}: {profile.get('companyName')} - {profile.get('sector')}")
                return profile
        except (ValueError, IndexError, TypeError) as e:
            logger.debug(f"Error parsing profile response for {symbol}: {e}")
    return None

def process_stock(stock, use_bulk, dcf_bulk, profiles_bulk, OVERVALUED_BUFFER, stats_lock, undervalued_stocks, fair_stocks, undervalued_stocks_cache, processed_counter):
    """
    Process a single stock to determine if it's undervalued, fair, or overvalued.
    Thread-safe function for parallel processing.
    Returns stock_detail dictionary.
    """
    symbol = stock.get('symbol', '')
    if not symbol:
        return None
    
    stock_detail = {
        'symbol': symbol,
        'company_name': '',
        'price': None,
        'dcf': None,
        'status': 'UNKNOWN',
        'has_data': False
    }
    
    # Use semaphore to limit concurrent API requests
    with API_SEMAPHORE:
        # Try to get company name early for logging
        profile_display = None
        if use_bulk and profiles_bulk:
            profile_display = profiles_bulk.get(symbol)
        if not profile_display:
            cached = get_cached_stock(symbol)
            if cached and 'profile' in cached:
                profile_display = cached['profile']
        company_name = profile_display.get('companyName', symbol) if profile_display else symbol
        
        # Get DCF value (from bulk or individual, cache is checked in get_dcf_value)
        stock_price_from_dcf = None
        if use_bulk and dcf_bulk:
            dcf_value = dcf_bulk.get(symbol)
            # If not in bulk, try cache or individual API
            if dcf_value is None:
                dcf_value, stock_price_from_dcf = get_dcf_value(symbol)
                time.sleep(INITIAL_DELAY)
        else:
            dcf_value, stock_price_from_dcf = get_dcf_value(symbol)
            time.sleep(INITIAL_DELAY)
        
        # Get current price - use price from DCF response if available, otherwise fetch separately
        if stock_price_from_dcf and stock_price_from_dcf > 0:
            current_price = stock_price_from_dcf
        else:
            current_price = get_stock_price(symbol)
        
        # Log/print information for ALL stocks checked, regardless of data availability
        if dcf_value is None or dcf_value <= 0:
            if current_price is None or current_price <= 0:
                # No DCF and no price
                log_msg = f"Stock: {company_name} ({symbol}) - DCF: N/A, Price: N/A - Status: DATA_UNAVAILABLE"
                logger.info(log_msg)
                print(f"DATA_UNAVAILABLE: {company_name} ({symbol}) - DCF: N/A, Price: N/A")
                stock_detail['status'] = 'DATA_UNAVAILABLE'
            else:
                # No DCF but have price
                log_msg = f"Stock: {company_name} ({symbol}) - DCF: N/A, Price: ${current_price:.2f} - Status: NO_DCF_DATA"
                logger.info(log_msg)
                print(f"NO_DCF_DATA: {company_name} ({symbol}) - DCF: N/A, Price: ${current_price:.2f}")
                stock_detail['status'] = 'NO_DCF_DATA'
                stock_detail['price'] = current_price
            
            stock_detail['company_name'] = company_name
            return stock_detail
        
        if current_price is None or current_price <= 0:
            # Have DCF but no price
            log_msg = f"Stock: {company_name} ({symbol}) - DCF: ${dcf_value:.2f}, Price: N/A - Status: NO_PRICE_DATA"
            logger.info(log_msg)
            print(f"NO_PRICE_DATA: {company_name} ({symbol}) - DCF: ${dcf_value:.2f}, Price: N/A")
            stock_detail['status'] = 'NO_PRICE_DATA'
            stock_detail['dcf'] = dcf_value
            stock_detail['company_name'] = company_name
            return stock_detail
        
        # Both DCF and price available - log complete information
        premium_pct = round(((current_price - dcf_value) / dcf_value) * 100, 2) if current_price > dcf_value else 0
        discount_pct = round(((dcf_value - current_price) / dcf_value) * 100, 2) if current_price < dcf_value else 0
        
        if current_price < dcf_value:
            status = "UNDERVALUED"
        elif current_price > dcf_value * (1 + OVERVALUED_BUFFER):
            status = "OVERVALUED (>20%)"
        else:
            status = "FAIR"
        
        log_msg = f"Stock: {company_name} ({symbol}) - Price: ${current_price:.2f}, DCF: ${dcf_value:.2f}, Diff: {abs(discount_pct) if discount_pct > 0 else premium_pct:.2f}% - Status: {status}"
        logger.info(log_msg)
        print(f"{status}: {company_name} ({symbol}) - Price: ${current_price:.2f}, DCF: ${dcf_value:.2f}, Diff: {abs(discount_pct) if discount_pct > 0 else premium_pct:.2f}%")
        
        # Update stock detail for batch tracking
        stock_detail['company_name'] = company_name
        stock_detail['price'] = current_price
        stock_detail['dcf'] = dcf_value
        stock_detail['status'] = status
        stock_detail['has_data'] = True
        
        # Get company profile for all stocks
        profile = None
        if use_bulk and profiles_bulk:
            profile = profiles_bulk.get(symbol)
        if profile is None:
            profile = get_company_profile(symbol)
            time.sleep(INITIAL_DELAY)
        
        if profile:
            company_name = profile.get('companyName', 'N/A')
            
            # Check if undervalued
            if current_price < dcf_value:
                discount_pct = round(((dcf_value - current_price) / dcf_value) * 100, 2)
                stock_data = {
                    'Symbol': symbol,
                    'Company Name': company_name,
                    'Current Price': round(current_price, 2),
                    'DCF Price': round(dcf_value, 2),
                    'Discount %': discount_pct,
                    'Premium %': 0,
                    'Valuation Status': 'UNDERVALUED',
                    'Sector': profile.get('sector', 'N/A'),
                    'Industry': profile.get('industry', 'N/A'),
                    'Timestamp': datetime.now().isoformat()
                }
                # Thread-safe append
                with stats_lock:
                    undervalued_stocks.append(stock_data)
                    # Add to separate undervalued cache (avoid duplicates)
                    existing_idx = next((i for i, s in enumerate(undervalued_stocks_cache) if s.get('Symbol') == symbol), None)
                    if existing_idx is not None:
                        undervalued_stocks_cache[existing_idx] = stock_data
                    else:
                        undervalued_stocks_cache.append(stock_data)
                # Update cache with stock data
                cache_stock(symbol, price=current_price, dcf=dcf_value, profile=profile)
                logger.info(f"Found undervalued: {symbol} - Price: ${current_price:.2f} < DCF: ${dcf_value:.2f} "
                           f"({discount_pct}% discount) - {company_name}")
                print(f"UNDERVALUED: {company_name} ({symbol}) - Price: ${current_price:.2f}, DCF: ${dcf_value:.2f}, Discount: {discount_pct}%")
            
            # Check if fair value (between DCF and DCF * 1.20)
            elif current_price >= dcf_value and current_price <= dcf_value * (1 + OVERVALUED_BUFFER):
                premium_pct = round(((current_price - dcf_value) / dcf_value) * 100, 2)
                stock_data = {
                    'Symbol': symbol,
                    'Company Name': company_name,
                    'Current Price': round(current_price, 2),
                    'DCF Price': round(dcf_value, 2),
                    'Discount %': 0,
                    'Premium %': premium_pct,
                    'Valuation Status': 'FAIR',
                    'Sector': profile.get('sector', 'N/A'),
                    'Industry': profile.get('industry', 'N/A'),
                    'Timestamp': datetime.now().isoformat()
                }
                # Thread-safe append
                with stats_lock:
                    fair_stocks.append(stock_data)
                    # Add to cache list (avoid duplicates)
                    if '_fair_stocks' not in stock_cache:
                        stock_cache['_fair_stocks'] = []
                    existing_idx = next((i for i, s in enumerate(stock_cache['_fair_stocks']) if s.get('Symbol') == symbol), None)
                    if existing_idx is not None:
                        stock_cache['_fair_stocks'][existing_idx] = stock_data
                    else:
                        stock_cache['_fair_stocks'].append(stock_data)
                # Update cache with stock data
                cache_stock(symbol, price=current_price, dcf=dcf_value, profile=profile)
                logger.info(f"Found fair value: {symbol} - Price: ${current_price:.2f}, DCF: ${dcf_value:.2f} "
                           f"({premium_pct}% premium) - {company_name}")
                print(f"FAIR: {company_name} ({symbol}) - Price: ${current_price:.2f}, DCF: ${dcf_value:.2f}, Premium: {premium_pct}%")
            
            # Skip overvalued stocks (price > DCF * 1.20) - not including in results
            else:
                # Update cache with stock data even for overvalued
                cache_stock(symbol, price=current_price, dcf=dcf_value, profile=profile)
        else:
            # No profile available, but still track the stock and update cache
            cache_stock(symbol, price=current_price, dcf=dcf_value)
    
    return stock_detail

def find_undervalued_stocks():
    """
    Main function to find stocks where price < DCF price.
    Uses bulk APIs where possible for efficiency.
    """
    logger.info("=" * 80)
    logger.info("Starting undervalued stocks analysis")
    logger.info("=" * 80)
    
    # Get all stocks
    all_stocks = get_all_stocks()
    
    if not all_stocks:
        logger.error("No stocks found. Exiting.")
        return
    
    # Try to fetch bulk data first (much faster)
    logger.info("Attempting to fetch bulk data for faster processing...")
    dcf_bulk = get_dcf_bulk()
    profiles_bulk = get_profiles_bulk()
    
    use_bulk = dcf_bulk is not None
    
    if use_bulk:
        logger.info("Using bulk API endpoints for faster processing")
    else:
        logger.info("Bulk APIs not available, using individual API calls")
    
    undervalued_stocks = []
    fair_stocks = []  # Stocks fairly valued (between DCF and DCF * 1.20)
    processed = 0
    OVERVALUED_BUFFER = 0.20  # 20% buffer for fair value range
    
    # Thread-safe lock for shared data structures
    stats_lock = Lock()
    processed_counter = {'value': 0}  # Use dict to allow modification in threads
    
    logger.info(f"Analyzing {len(all_stocks)} stocks...")
    logger.info(f"Multi-threading: {MAX_WORKERS} concurrent threads")
    logger.info(f"Rate limiting: {INITIAL_DELAY}s delay between requests, max {MAX_RETRIES} retries per request")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    # Process stocks in batches of 2000
    BATCH_SIZE = 2000
    total_batches = (len(all_stocks) + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"Processing {len(all_stocks)} stocks in {total_batches} batches of {BATCH_SIZE}")
    print(f"Processing {len(all_stocks)} stocks in {total_batches} batches of {BATCH_SIZE} (using {MAX_WORKERS} threads)")
    
    for batch_num in range(total_batches):
        batch_start = batch_num * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, len(all_stocks))
        batch_stocks = all_stocks[batch_start:batch_end]
        
        logger.info(f"Processing batch {batch_num + 1}/{total_batches} (stocks {batch_start + 1}-{batch_end})...")
        print(f"Processing batch {batch_num + 1}/{total_batches} (stocks {batch_start + 1}-{batch_end})...")
        
        # Track batch statistics
        batch_data = {
            'processed': 0,
            'skipped': 0,
            'with_data': 0,
            'no_data': 0,
            'undervalued': 0,
            'fair': 0,
            'overvalued': 0,
            'stocks_details': []
        }
        
        # Process stocks in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all stocks in the batch for processing
            future_to_stock = {
                executor.submit(
                    process_stock,
                    stock,
                    use_bulk,
                    dcf_bulk,
                    profiles_bulk,
                    OVERVALUED_BUFFER,
                    stats_lock,
                    undervalued_stocks,
                    fair_stocks,
                    undervalued_stocks_cache,
                    processed_counter
                ): stock
                for stock in batch_stocks
            }
            
            # Process completed futures as they finish
            for future in as_completed(future_to_stock):
                stock = future_to_stock[future]
                symbol = stock.get('symbol', '')
                
                try:
                    stock_detail = future.result()
                    
                    if stock_detail is None:
                        # Stock was skipped (no symbol)
                        with stats_lock:
                            batch_data['skipped'] += 1
                        continue
                    
                    # Update batch statistics
                    with stats_lock:
                        processed_counter['value'] += 1
                        processed = processed_counter['value']
                        batch_data['processed'] += 1
                        
                        if stock_detail['has_data']:
                            batch_data['with_data'] += 1
                            if stock_detail['status'] == 'UNDERVALUED':
                                batch_data['undervalued'] += 1
                            elif stock_detail['status'] == 'FAIR':
                                batch_data['fair'] += 1
                            elif 'OVERVALUED' in stock_detail['status']:
                                batch_data['overvalued'] += 1
                        else:
                            batch_data['no_data'] += 1
                        
                        batch_data['stocks_details'].append(stock_detail)
                        
                        # Show progress every 50 stocks
                        if processed % 50 == 0:
                            elapsed = time.time() - start_time
                            rate = processed / elapsed if elapsed > 0 else 0
                            remaining = (len(all_stocks) - processed) / rate if rate > 0 else 0
                            logger.info(f"Progress: {processed}/{len(all_stocks)} stocks ({rate:.1f} stocks/sec) | "
                                       f"Found {len(undervalued_stocks)} undervalued, {len(fair_stocks)} fair | "
                                       f"ETA: {remaining/60:.1f} minutes")
                        
                        # Save cache every 100 stocks to prevent data loss
                        if processed % 100 == 0:
                            save_cache()
                            save_undervalued_cache()
                            stock_count = len([k for k in stock_cache.keys() if not k.startswith('_')])
                            logger.info(f"Cache saved: {stock_count} stocks, {len(undervalued_stocks_cache)} undervalued in separate cache, {len(stock_cache.get('_fair_stocks', []))} fair")
                
                except Exception as e:
                    logger.error(f"Error processing stock {symbol}: {e}")
                    with stats_lock:
                        batch_data['skipped'] += 1
        
        # Save cache after each batch completes
        save_cache()
        save_undervalued_cache()
        stock_count = len([k for k in stock_cache.keys() if not k.startswith('_')])
        
        # Log comprehensive batch summary
        logger.info("=" * 80)
        logger.info(f"BATCH {batch_num + 1}/{total_batches} SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Batch Range: Stocks {batch_start + 1}-{batch_end} ({len(batch_stocks)} stocks)")
        logger.info(f"Processed: {batch_data['processed']} stocks")
        logger.info(f"Skipped (no symbol): {batch_data['skipped']} stocks")
        valid_stocks = batch_data['processed']
        if valid_stocks > 0:
            logger.info(f"With Complete Data: {batch_data['with_data']} stocks ({batch_data['with_data']/valid_stocks*100:.1f}% of processed)")
            logger.info(f"No Data/Missing: {batch_data['no_data']} stocks ({batch_data['no_data']/valid_stocks*100:.1f}% of processed)")
        else:
            logger.info(f"With Complete Data: {batch_data['with_data']} stocks")
            logger.info(f"No Data/Missing: {batch_data['no_data']} stocks")
        logger.info(f"Undervalued: {batch_data['undervalued']} stocks")
        logger.info(f"Fair Value: {batch_data['fair']} stocks")
        logger.info(f"Overvalued: {batch_data['overvalued']} stocks")
        logger.info(f"Total Found So Far: {len(undervalued_stocks)} undervalued, {len(fair_stocks)} fair")
        logger.info(f"Cache Status: {stock_count} stocks cached, {len(undervalued_stocks_cache)} undervalued in separate cache")
        logger.info("=" * 80)
        
        # Log all stock details for this batch
        logger.info(f"\nDETAILED STOCK DATA FOR BATCH {batch_num + 1}:")
        logger.info("-" * 80)
        for detail in batch_data['stocks_details']:
            if detail['has_data']:
                logger.info(f"{detail['symbol']} | {detail['company_name']} | Price: ${detail['price']:.2f} | DCF: ${detail['dcf']:.2f} | Status: {detail['status']}")
            else:
                logger.info(f"{detail['symbol']} | {detail['company_name']} | Price: {detail['price']} | DCF: {detail['dcf']} | Status: {detail['status']}")
        logger.info("-" * 80)
        
        # Validation check - account for skipped stocks
        expected_count = len(batch_stocks)
        actual_processed = batch_data['processed'] + batch_data['skipped']
        if actual_processed != expected_count:
            logger.warning(f"⚠️  BATCH VALIDATION: Expected {expected_count} stocks, but processed {batch_data['processed']} + skipped {batch_data['skipped']} = {actual_processed} stocks")
        else:
            logger.info(f"BATCH VALIDATION: All {expected_count} stocks handled (processed: {batch_data['processed']}, skipped: {batch_data['skipped']})")
            if batch_data['with_data'] == batch_data['processed']:
                logger.info(f"DATA VALIDATION: All {batch_data['processed']} processed stocks have complete data")
            else:
                logger.warning(f"⚠️  DATA VALIDATION: Only {batch_data['with_data']}/{batch_data['processed']} processed stocks have complete data")
        
        print(f"Batch {batch_num + 1}/{total_batches} complete. Processed: {batch_data['processed']}, With Data: {batch_data['with_data']}, Cache saved.")
    
    total_time = time.time() - start_time
    final_processed = processed_counter['value']
    logger.info("=" * 80)
    logger.info(f"Analysis complete! Processed {final_processed} stocks in {total_time/60:.1f} minutes")
    logger.info(f"Found {len(undervalued_stocks)} undervalued stocks")
    logger.info(f"Found {len(fair_stocks)} fair value stocks")
    logger.info("=" * 80)
    
    # Combine undervalued and fair stocks only
    all_selected_stocks = undervalued_stocks + fair_stocks
    
    # Create DataFrame and save to CSV
    if all_selected_stocks:
        df = pd.DataFrame(all_selected_stocks)
        # Sort by valuation status (undervalued first, then fair) then by discount/premium
        df['Status Order'] = df['Valuation Status'].map({'UNDERVALUED': 0, 'FAIR': 1})
        df['Sort Value'] = df.apply(lambda row: -row['Discount %'] if row['Valuation Status'] == 'UNDERVALUED' else row['Premium %'], axis=1)
        df = df.sort_values(['Status Order', 'Sort Value'], ascending=[True, False])
        df = df.drop(['Status Order', 'Sort Value'], axis=1)
        
        # Save to CSV
        output_file = 'stock_valuations.csv'
        try:
            df.to_csv(output_file, index=False, encoding='utf-8')
            logger.info(f"Results saved to {output_file}")
        except Exception as e:
            logger.error(f"Error saving CSV file: {e}")
            return None
        
        # Log summary for undervalued stocks
        if undervalued_stocks:
            df_undervalued = df[df['Valuation Status'] == 'UNDERVALUED']
            logger.info("\nTop 10 Undervalued Stocks:")
            logger.info("=" * 80)
            print("\n" + "=" * 80)
            print("Top 10 Undervalued Stocks:")
            print("=" * 80)
            for idx, row in df_undervalued.head(10).iterrows():
                log_msg = f"{row['Symbol']}: {row['Company Name']} - " \
                         f"Price: ${row['Current Price']:.2f}, DCF: ${row['DCF Price']:.2f}, " \
                         f"Discount: {row['Discount %']:.2f}%"
                logger.info(log_msg)
                print(log_msg)
            
            # Summary statistics for undervalued
            logger.info("\n" + "=" * 80)
            logger.info("Undervalued Stocks Summary:")
            logger.info(f"Total undervalued stocks: {len(df_undervalued)}")
            logger.info(f"Average discount: {df_undervalued['Discount %'].mean():.2f}%")
            logger.info(f"Max discount: {df_undervalued['Discount %'].max():.2f}%")
            logger.info(f"Min discount: {df_undervalued['Discount %'].min():.2f}%")
        
        # Log summary for fair value stocks
        if fair_stocks:
            df_fair = df[df['Valuation Status'] == 'FAIR']
            logger.info("\n" + "=" * 80)
            logger.info("Fair Value Stocks Summary:")
            logger.info(f"Total fair value stocks: {len(df_fair)}")
            logger.info(f"Average premium: {df_fair['Premium %'].mean():.2f}%")
            logger.info(f"Max premium: {df_fair['Premium %'].max():.2f}%")
            logger.info(f"Min premium: {df_fair['Premium %'].min():.2f}%")
        
        # Sector breakdown for all selected stocks
        logger.info("\n" + "=" * 80)
        logger.info("Sector Breakdown (All Selected Stocks):")
        for sector, count in df['Sector'].value_counts().head(10).items():
            logger.info(f"  {sector}: {count}")
        
        # Save cache before exiting
        save_cache()
        save_undervalued_cache()
        
        # Also print to console for immediate visibility
        stock_count = len([k for k in stock_cache.keys() if not k.startswith('_')])
        print("\n" + "=" * 80)
        print(f"\nAnalysis complete!")
        print(f"Found {len(undervalued_stocks)} undervalued stocks")
        print(f"Found {len(fair_stocks)} fair value stocks")
        print(f"Total: {len(all_selected_stocks)} stocks (undervalued + fair only)")
        print(f"Results saved to {output_file}")
        print(f"Cache saved to {CACHE_FILE} ({stock_count} stocks cached, {len(stock_cache.get('_fair_stocks', []))} fair in cache)")
        print(f"Undervalued stocks cache saved to {UNDERVALUED_CACHE_FILE} ({len(undervalued_stocks_cache)} stocks)")
        print(f"Log file: logs/fmp_stocks_*.log")
        print("=" * 80)
        
        return df
    else:
        # Save cache even if no results
        save_cache()
        save_undervalued_cache()
        logger.warning("No stocks found matching criteria.")
        print("\nNo stocks found matching criteria.")
        return None

def validate_api_key():
    """
    Test the API key by making a simple request.
    Returns True if valid, False otherwise.
    """
    logger.info("Validating API key...")
    # Use a simple endpoint to test the API key - using v3 API path format
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
                print(f"\nPlease:")
                print("1. Verify your API key at https://site.financialmodelingprep.com/")
                print("2. Make sure your API key is active and not expired")
                print("3. Check that the API key in .env file matches your account")
                print(f"\nCurrent API key (first 10 chars): {API_KEY[:10]}...")
            except:
                logger.error(f"API key validation failed with status {response.status_code}")
                print(f"\n❌ API Key Validation Failed! Status code: {response.status_code}")
            return False
        else:
            logger.warning(f"API key validation returned status {response.status_code}")
            # Might still be valid, just log the warning
            return True
    except Exception as e:
        logger.error(f"Error validating API key: {e}")
        print(f"\n⚠️  Could not validate API key: {e}")
        print("Proceeding anyway, but API calls may fail...")
        return True  # Proceed anyway

if __name__ == "__main__":
    if not API_KEY:
        logger.error("FMP_API_KEY not found in .env file")
        print("Error: FMP_API_KEY not found in .env file")
        print("Please create a .env file with: FMP_API_KEY=your_api_key_here")
        print("\nTo get a free API key, visit: https://site.financialmodelingprep.com/")
    elif len(API_KEY) < 10:
        logger.error(f"API key appears to be invalid (too short: {len(API_KEY)} chars)")
        print(f"Error: API key appears to be invalid. Please check your .env file.")
        print(f"Current key length: {len(API_KEY)} characters")
    else:
        logger.info(f"API Key loaded (length: {len(API_KEY)} chars, starts with: {API_KEY[:5]}...)")
        
        # Validate API key before proceeding
        if validate_api_key():
            try:
                find_undervalued_stocks()
            except KeyboardInterrupt:
                logger.warning("Process interrupted by user")
                save_cache()  # Save cache before exiting
                save_undervalued_cache()
                print("\n\nProcess interrupted by user. Cache saved. Partial results may be available.")
            except Exception as e:
                logger.exception(f"Unexpected error occurred: {e}")
                save_cache()  # Save cache even on error
                save_undervalued_cache()
                print(f"\nAn error occurred. Check the log file for details: {e}")
        else:
            print("\nPlease fix your API key before running the script.")
            print("You can get a free API key at: https://site.financialmodelingprep.com/")

