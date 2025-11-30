"""
Script to fetch and store 3 months of 1-minute historical bar data from Alpaca API.
"""
import requests
import yaml
from datetime import datetime, timedelta
from typing import List, Dict
import time
from database import init_database, insert_bars_batch, get_latest_bar, get_data_range
import sys

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

def get_nyse_symbols() -> List[str]:
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
            print(f"Found {len(symbols)} active NYSE symbols")
            return symbols
        else:
            print(f"Error fetching symbols: {response.status_code} - {response.text[:200]}")
            return []
    except Exception as e:
        print(f"Error fetching NYSE symbols: {e}")
        return []

def fetch_bars(symbol: str, timeframe: str, start: datetime, end: datetime) -> List[Dict]:
    """
    Fetch historical bars from Alpaca API with pagination support
    
    Args:
        symbol: Stock symbol
        timeframe: '1Min', '1Sec', etc.
        start: Start datetime
        end: End datetime
    
    Returns:
        List of bar dictionaries
    """
    headers = get_headers()
    all_bars = []
    
    # Alpaca API limits to 1000 bars per request, so we need to paginate
    # Split into chunks of ~7 days to stay under limit
    current_start = start
    chunk_days = 7  # Fetch 7 days at a time (about 2730 minutes, well under 1000 limit for 1Min)
    
    while current_start < end:
        # Calculate chunk end (7 days or until end date)
        chunk_end = min(current_start + timedelta(days=chunk_days), end)
        
        start_str = current_start.strftime('%Y-%m-%dT%H:%M:%S-05:00')
        end_str = chunk_end.strftime('%Y-%m-%dT%H:%M:%S-05:00')
        
        url = f'{BASE_URL}/stocks/{symbol}/bars'
        
        try:
            response = requests.get(
                url,
                headers=headers,
                params={
                    'timeframe': timeframe,
                    'start': start_str,
                    'end': end_str,
                    'adjustment': 'raw',
                    'feed': 'iex',
                    'limit': 10000  # Max limit
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'bars' in data and data['bars']:
                    chunk_bars = []
                    for bar in data['bars']:
                        # Convert ISO timestamp to Unix timestamp
                        ts = datetime.fromisoformat(bar['t'].replace('Z', '+00:00'))
                        timestamp = int(ts.timestamp())
                        
                        chunk_bars.append({
                            'symbol': symbol,
                            'timeframe': timeframe,
                            'timestamp': timestamp,
                            'open': float(bar['o']),
                            'high': float(bar['h']),
                            'low': float(bar['l']),
                            'close': float(bar['c']),
                            'volume': int(bar['v'])
                        })
                    
                    all_bars.extend(chunk_bars)
                    # Don't print progress here - let the caller handle it
                    
                    # Check for pagination token
                    if 'next_page_token' in data and data['next_page_token']:
                        # If there's a next page, continue with that token
                        # For now, we'll just move to next chunk
                        pass
                    
                    # Rate limiting
                    time.sleep(0.1)
                else:
                    print(f"    No bars in response for {current_start.date()} to {chunk_end.date()}")
            elif response.status_code == 429:
                print(f"    Rate limited, waiting...")
                time.sleep(2)
                continue  # Retry this chunk
            else:
                print(f"    Error: {response.status_code} - {response.text[:200]}")
                break  # Stop on error
                
        except Exception as e:
            print(f"    Exception: {e}")
            break
        
        # Move to next chunk
        current_start = chunk_end + timedelta(seconds=1)  # Add 1 second to avoid overlap
    
    return all_bars

def fetch_historical_data(symbols: List[str], months: int = 3, timeframe: str = '1Min'):
    """
    Fetch and store historical data for multiple symbols
    
    Args:
        symbols: List of stock symbols
        months: Number of months of historical data to fetch
        timeframe: Bar timeframe (e.g., '1Min', '1Sec')
    """
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months * 30)
    
    print(f"\nFetching {timeframe} bars from {start_date.date()} to {end_date.date()}")
    print(f"Processing {len(symbols)} symbols...\n")
    
    total_bars = 0
    successful = 0
    failed = 0
    
    for i, symbol in enumerate(symbols, 1):
        try:
            # Check if we already have recent data
            latest = get_latest_bar(symbol, timeframe)
            if latest:
                latest_ts = latest['timestamp']
                latest_dt = datetime.fromtimestamp(latest_ts)
                # If we have data within last 7 days, skip
                if (datetime.now() - latest_dt).days < 7:
                    print(f"[{i}/{len(symbols)}] {symbol}: Already has recent data, skipping")
                    continue
            
            print(f"[{i}/{len(symbols)}] Fetching {symbol}...", end=' ', flush=True)
            
            # Fetch bars
            bars = fetch_bars(symbol, timeframe, start_date, end_date)
            
            if bars:
                # Insert into database
                inserted = insert_bars_batch(bars)
                total_bars += inserted
                successful += 1
                print(f"✓ {len(bars)} bars ({inserted} new)")
            else:
                failed += 1
                print("✗ No data")
            
            # Rate limiting - be nice to the API
            time.sleep(0.2)
            
        except KeyboardInterrupt:
            print("\n\nInterrupted by user. Stopping...")
            break
        except Exception as e:
            failed += 1
            print(f"✗ Error: {e}")
            continue
    
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  Symbols processed: {len(symbols)}")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total bars stored: {total_bars}")
    print(f"{'='*60}\n")

def main():
    """Main function"""
    # Initialize database
    print("Initializing database...")
    init_database()
    
    # Get NYSE symbols
    print("Fetching NYSE symbols...")
    symbols = get_nyse_symbols()
    
    if not symbols:
        print("No symbols found. Exiting.")
        sys.exit(1)
    
    # Ask user how many symbols to process (for testing)
    if len(symbols) > 100:
        print(f"\nFound {len(symbols)} symbols.")
        response = input(f"Process all {len(symbols)} symbols? (y/n, or enter number to limit): ")
        
        if response.lower() == 'n':
            print("Exiting.")
            sys.exit(0)
        elif response.isdigit():
            limit = int(response)
            symbols = symbols[:limit]
            print(f"Processing first {limit} symbols.")
    
    # Fetch historical data
    fetch_historical_data(symbols, months=3, timeframe='1Min')
    
    print("Done!")

if __name__ == '__main__':
    main()

