"""
Clean up database to only keep symbols that are:
1. Active (status = 'active')
2. Tradable (tradable = True)
3. US Equity (asset_class = 'us_equity')
4. In $1-$20 price range

This removes non-tradable symbols from the database to avoid wasting API calls.
"""
from database import get_connection, get_symbols_with_data, get_latest_bar
import requests
import yaml
import time
from typing import List, Tuple

# Load config
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

ALPACA_API_KEY = config['alpaca']['api_key']
ALPACA_SECRET_KEY = config['alpaca']['api_secret']
TRADING_URL = config['alpaca']['trading_url']

def get_headers():
    return {
        'APCA-API-KEY-ID': ALPACA_API_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET_KEY
    }

def check_asset_tradable(symbol: str) -> Tuple[bool, str]:
    """
    Check if a symbol is tradable by fetching its asset metadata.
    
    Returns:
        (is_tradable, reason) tuple
    """
    headers = get_headers()
    try:
        # Use Alpaca's asset endpoint to get metadata
        url = f'{TRADING_URL}/assets/{symbol}'
        response = requests.get(url, headers=headers)
        
        if response.status_code == 404:
            return (False, "Symbol not found/delisted")
        
        if response.status_code != 200:
            return (False, f"API error: {response.status_code}")
        
        asset = response.json()
        
        # Check asset class
        if asset.get('asset_class') != 'us_equity':
            return (False, f"Not US equity: {asset.get('asset_class')}")
        
        # Check status
        if asset.get('status') != 'active':
            return (False, f"Not active: {asset.get('status')}")
        
        # Check if tradable
        if not asset.get('tradable', False):
            return (False, "Not tradable")
        
        return (True, "OK")
        
    except Exception as e:
        return (False, f"Error: {str(e)}")

def cleanup_database(min_price: float = 1.0, max_price: float = 20.0, 
                     check_tradable: bool = True):
    """
    Remove symbols from database that don't meet trading criteria.
    
    Args:
        min_price: Minimum price per share
        max_price: Maximum price per share
        check_tradable: Whether to check if symbol is tradable via API
    """
    conn = get_connection()
    
    print("=" * 80)
    print("CLEANUP TRADABLE SYMBOLS")
    print("=" * 80)
    print(f"\nCriteria:")
    print(f"  Price range: ${min_price} - ${max_price}")
    print(f"  Status: active")
    print(f"  Asset class: us_equity")
    print(f"  Tradable: True")
    print(f"  Check tradable via API: {check_tradable}")
    
    # Get all symbols with data
    symbols = get_symbols_with_data('1Min')
    print(f"\nFound {len(symbols)} symbols in database")
    
    if not symbols:
        print("No symbols found. Exiting.")
        conn.close()
        return
    
    symbols_to_remove = []
    symbols_to_keep = []
    symbols_to_check = []
    
    print(f"\n{'='*80}")
    print("STEP 1: Checking prices from database")
    print(f"{'='*80}\n")
    
    # First pass: Check prices from database (no API calls needed)
    for i, symbol in enumerate(symbols, 1):
        latest_bar = get_latest_bar(symbol, '1Min')
        if latest_bar:
            price = latest_bar['close']
            if price < min_price or price > max_price:
                symbols_to_remove.append((symbol, f"Price ${price:.2f} outside range"))
                if i <= 10:
                    print(f"[{i}/{len(symbols)}] {symbol}: ${price:.2f} - OUT OF PRICE RANGE")
            else:
                symbols_to_check.append((symbol, price))
                if i <= 10:
                    print(f"[{i}/{len(symbols)}] {symbol}: ${price:.2f} - Price OK, checking tradability...")
        else:
            symbols_to_check.append((symbol, None))
            if i <= 10:
                print(f"[{i}/{len(symbols)}] {symbol}: No price data, checking tradability...")
    
    if len(symbols_to_check) > 10:
        print(f"  ... checking {len(symbols_to_check)} more symbols for tradability")
    
    if not check_tradable:
        # Skip tradable check, keep all price-valid symbols
        symbols_to_keep = [s[0] for s in symbols_to_check]
        print(f"\n✓ Skipping tradable check - keeping {len(symbols_to_keep)} symbols with valid prices")
    else:
        print(f"\n{'='*80}")
        print("STEP 2: Checking tradability via API")
        print(f"{'='*80}\n")
        print(f"Checking {len(symbols_to_check)} symbols...\n")
        
        # Second pass: Check tradability via API
        checked = 0
        start_time = time.time()
        
        for i, (symbol, price) in enumerate(symbols_to_check, 1):
            checked += 1
            
            is_tradable, reason = check_asset_tradable(symbol)
            
            if is_tradable:
                symbols_to_keep.append(symbol)
                status = "✓ KEEP"
            else:
                symbols_to_remove.append((symbol, reason))
                status = "✗ REMOVE"
            
            # Progress update
            percent = (checked / len(symbols_to_check)) * 100
            elapsed = time.time() - start_time
            rate = checked / elapsed if elapsed > 0 else 0
            remaining = (len(symbols_to_check) - checked) / rate if rate > 0 else 0
            
            price_str = f"${price:.2f}" if price else "N/A"
            print(f"[{checked}/{len(symbols_to_check)}] ({percent:.1f}%) {symbol}: {price_str} - {status} ({reason}) | "
                  f"ETA: {remaining/60:.1f}min", end='\r')
            
            # Rate limiting - be nice to the API
            time.sleep(0.1)
        
        print()  # New line after progress
    
    # Remove symbols from database
    if symbols_to_remove:
        print(f"\n{'='*80}")
        print("STEP 3: Removing non-tradable/invalid symbols")
        print(f"{'='*80}\n")
        print(f"Removing {len(symbols_to_remove)} symbols from database...")
        
        cursor = conn.cursor()
        removed_count = 0
        
        for symbol, reason in symbols_to_remove:
            cursor.execute('DELETE FROM bars WHERE symbol = ?', (symbol,))
            removed_count += cursor.rowcount
        
        conn.commit()
        
        print(f"✓ Removed {removed_count} bar records for {len(symbols_to_remove)} symbols")
        
        # Show some examples
        print(f"\nRemoved symbols (first 20):")
        for symbol, reason in symbols_to_remove[:20]:
            print(f"  {symbol}: {reason}")
        if len(symbols_to_remove) > 20:
            print(f"  ... and {len(symbols_to_remove) - 20} more")
    else:
        print(f"\n✓ No symbols to remove")
    
    print(f"\n{'='*80}")
    print("CLEANUP COMPLETE")
    print(f"{'='*80}")
    print(f"Summary:")
    print(f"  Symbols checked: {len(symbols)}")
    print(f"  Symbols kept: {len(symbols_to_keep)}")
    print(f"  Symbols removed: {len(symbols_to_remove)}")
    print(f"{'='*80}\n")
    
    conn.close()

if __name__ == '__main__':
    print("=" * 80)
    print("Cleanup Tradable Symbols")
    print("=" * 80)
    print("\nThis will remove symbols from the database that are:")
    print("  - Outside $1-$20 price range")
    print("  - Not tradable")
    print("  - Not active")
    print("  - Not US equity")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
    else:
        cleanup_database(
            min_price=1.0,
            max_price=20.0,
            check_tradable=True
        )
        print("Done!")

