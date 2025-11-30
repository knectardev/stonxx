"""
Fetch historical data for NYSE symbols that are between $1-$20 per share
"""
from fetch_historical_data import fetch_bars
from database import init_database, insert_bars_batch
import requests
import yaml

# Load config
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

ALPACA_API_KEY = config['alpaca']['api_key']
ALPACA_SECRET_KEY = config['alpaca']['api_secret']
BASE_URL = config['alpaca']['data_url']
TRADING_URL = config['alpaca']['trading_url']

def get_headers():
    return {
        'APCA-API-KEY-ID': ALPACA_API_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET_KEY
    }

def get_nyse_symbols():
    """Fetch NYSE symbols from Alpaca API"""
    headers = get_headers()
    try:
        url = f'{TRADING_URL}/assets'
        response = requests.get(
            url,
            headers=headers,
            params={'status': 'active', 'exchange': 'NYSE', 'asset_class': 'us_equity'}
        )
        if response.status_code == 200:
            assets = response.json()
            symbols = [asset['symbol'] for asset in assets if asset.get('status') == 'active']
            return symbols
        return []
    except Exception as e:
        print(f"Error fetching NYSE symbols: {e}")
        return []
from datetime import datetime, timedelta
import time

def get_current_price(symbol: str) -> float:
    """Get current price for a symbol from Alpaca API"""
    headers = get_headers()
    try:
        response = requests.get(
            f'{BASE_URL}/stocks/snapshots',
            headers=headers,
            params={'symbols': symbol}
        )
        if response.status_code == 200:
            data = response.json()
            if symbol in data and 'latestTrade' in data[symbol] and data[symbol]['latestTrade']:
                return float(data[symbol]['latestTrade']['p'])
    except Exception as e:
        pass
    return None

def filter_symbols_by_price(symbols: list, min_price: float = 1.0, max_price: float = 20.0) -> list:
    """Filter symbols by current price"""
    print(f"\n{'='*60}")
    print(f"STEP 1: Filtering symbols by price range: ${min_price} - ${max_price}")
    print(f"{'='*60}")
    print(f"Total symbols to check: {len(symbols):,}\n")
    
    filtered = []
    checked = 0
    start_time = time.time()
    
    # Process in chunks to check prices
    chunk_size = 100
    total_chunks = (len(symbols) + chunk_size - 1) // chunk_size
    
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        checked += len(chunk)
        
        # Get prices for chunk
        symbols_str = ','.join(chunk)
        headers = get_headers()
        try:
            response = requests.get(
                f'{BASE_URL}/stocks/snapshots',
                headers=headers,
                params={'symbols': symbols_str}
            )
            
            if response.status_code == 200:
                data = response.json()
                chunk_filtered = 0
                for symbol in chunk:
                    if symbol in data and 'latestTrade' in data[symbol] and data[symbol]['latestTrade']:
                        price = float(data[symbol]['latestTrade']['p'])
                        if min_price <= price <= max_price:
                            filtered.append(symbol)
                            chunk_filtered += 1
                
                # Progress update with percentage
                percent = (checked / len(symbols)) * 100
                elapsed = time.time() - start_time
                rate = checked / elapsed if elapsed > 0 else 0
                remaining = (len(symbols) - checked) / rate if rate > 0 else 0
                
                print(f"[{chunk_num}/{total_chunks}] Checked {checked:,}/{len(symbols):,} symbols ({percent:.1f}%) | "
                      f"Found: {len(filtered):,} in range | "
                      f"Rate: {rate:.0f} symbols/sec | "
                      f"ETA: {remaining:.0f}s", end='\r')
            
            time.sleep(0.1)  # Rate limiting
            
        except Exception as e:
            print(f"\n  Error checking chunk {chunk_num}: {e}")
            continue
    
    elapsed_total = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"✓ Filtering complete in {elapsed_total:.1f} seconds")
    print(f"✓ Found {len(filtered):,} symbols in price range ${min_price}-${max_price}")
    print(f"{'='*60}\n")
    return filtered

def fetch_historical_data_filtered(days: int = 7, timeframe: str = '1Min', 
                                   min_price: float = 1.0, max_price: float = 20.0):
    """
    Fetch historical data only for symbols in the specified price range
    
    Args:
        days: Number of days of historical data to fetch (default: 7 for 1 week)
        timeframe: Bar timeframe (e.g., '1Min')
        min_price: Minimum price per share
        max_price: Maximum price per share
    """
    # Initialize database
    print("Initializing database...")
    init_database()
    
    # Get NYSE symbols
    print("Fetching NYSE symbols...")
    symbols = get_nyse_symbols()
    
    if not symbols:
        print("No symbols found. Exiting.")
        return
    
    print(f"Found {len(symbols)} NYSE symbols")
    
    # Filter by price
    filtered_symbols = filter_symbols_by_price(symbols, min_price, max_price)
    
    if not filtered_symbols:
        print("No symbols in the specified price range. Exiting.")
        return
    
    # Calculate date range (1 week = 7 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    print(f"\nFetching {timeframe} bars from {start_date.date()} to {end_date.date()} ({days} days)")
    print(f"Processing {len(filtered_symbols)} symbols...\n")
    
    total_bars = 0
    successful = 0
    failed = 0
    start_time = time.time()
    
    print(f"{'='*60}")
    print(f"STEP 2: Fetching historical data")
    print(f"{'='*60}\n")
    
    for i, symbol in enumerate(filtered_symbols, 1):
        try:
            percent = (i / len(filtered_symbols)) * 100
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(filtered_symbols) - i) / rate if rate > 0 else 0
            
            print(f"[{i}/{len(filtered_symbols)}] ({percent:.1f}%) Fetching {symbol}...", end=' ', flush=True)
            
            # Fetch bars
            symbol_start = time.time()
            bars = fetch_bars(symbol, timeframe, start_date, end_date)
            fetch_time = time.time() - symbol_start
            
            if bars:
                # Insert into database
                inserted = insert_bars_batch(bars)
                total_bars += inserted
                successful += 1
                print(f"✓ {len(bars):,} bars ({inserted:,} new) in {fetch_time:.1f}s | "
                      f"Total: {total_bars:,} bars | "
                      f"ETA: {remaining/60:.1f}min")
            else:
                failed += 1
                print(f"✗ No data | ETA: {remaining/60:.1f}min")
            
            # Rate limiting
            time.sleep(0.2)
            
        except KeyboardInterrupt:
            print("\n\nInterrupted by user. Stopping...")
            break
        except Exception as e:
            failed += 1
            print(f"✗ Error: {e} | ETA: {remaining/60:.1f}min")
            continue
    
    elapsed_total = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"STEP 2 COMPLETE")
    print(f"{'='*60}")
    print(f"Summary:")
    print(f"  Symbols processed: {len(filtered_symbols):,}")
    print(f"  Successful: {successful:,}")
    print(f"  Failed: {failed:,}")
    print(f"  Total bars stored: {total_bars:,}")
    print(f"  Time elapsed: {elapsed_total/60:.1f} minutes ({elapsed_total:.0f} seconds)")
    print(f"  Average: {elapsed_total/len(filtered_symbols):.1f} seconds per symbol")
    print(f"{'='*60}\n")

def main():
    """Main function"""
    print("=" * 60)
    print("Fetch Historical Data for $1-$20 NYSE Stocks")
    print("=" * 60)
    
    # Ask user for confirmation
    print("\nThis will:")
    print("  1. Fetch all NYSE symbols")
    print("  2. Filter to only symbols priced $1-$20")
    print("  3. Fetch 2 weeks (14 days) of 1-minute historical data")
    print("  4. Store in database")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
        return
    
    # Fetch data (2 weeks = 14 days)
    fetch_historical_data_filtered(
        days=14,
        timeframe='1Min',
        min_price=1.0,
        max_price=20.0
    )
    
    print("Done!")

if __name__ == '__main__':
    main()

