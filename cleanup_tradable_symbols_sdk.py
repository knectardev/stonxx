"""
Clean up database using Alpaca SDK (ChatGPT's recommended approach).
This requires: pip install alpaca-py

Filters out symbols that are:
- Not active
- Not tradable  
- Not US equity
- Outside $1-$20 price range
"""
from alpaca.trading.client import TradingClient
from database import get_connection, get_symbols_with_data, get_latest_bar
import yaml
import time

# Load config
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

ALPACA_API_KEY = config['alpaca']['api_key']
ALPACA_SECRET_KEY = config['alpaca']['api_secret']

def cleanup_database_sdk(min_price: float = 1.0, max_price: float = 20.0):
    """
    Clean up database using Alpaca SDK (ChatGPT's recommended approach).
    
    Args:
        min_price: Minimum price per share
        max_price: Maximum price per share
    """
    # Initialize TradingClient
    trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    
    conn = get_connection()
    
    print("=" * 80)
    print("CLEANUP TRADABLE SYMBOLS (SDK Approach)")
    print("=" * 80)
    print(f"\nCriteria:")
    print(f"  Price range: ${min_price} - ${max_price}")
    print(f"  Status: active")
    print(f"  Asset class: us_equity")
    print(f"  Tradable: True")
    
    # Get all symbols with data
    symbols = get_symbols_with_data('1Min')
    print(f"\nFound {len(symbols)} symbols in database")
    
    if not symbols:
        print("No symbols found. Exiting.")
        conn.close()
        return
    
    clean_symbols = []
    symbols_to_remove = []
    
    print(f"\n{'='*80}")
    print("Checking symbols for tradability")
    print(f"{'='*80}\n")
    
    checked = 0
    start_time = time.time()
    
    for symbol in symbols:
        checked += 1
        
        try:
            # Get asset metadata (ChatGPT's approach)
            asset = trading_client.get_asset(symbol)
            
            # Check all criteria
            if asset.asset_class != "us_equity":
                symbols_to_remove.append((symbol, f"Not US equity: {asset.asset_class}"))
                continue
            
            if not asset.tradable:
                symbols_to_remove.append((symbol, "Not tradable"))
                continue
            
            if asset.status != "active":
                symbols_to_remove.append((symbol, f"Not active: {asset.status}"))
                continue
            
            # Check price from database
            latest_bar = get_latest_bar(symbol, '1Min')
            if latest_bar:
                price = latest_bar['close']
                if price < min_price or price > max_price:
                    symbols_to_remove.append((symbol, f"Price ${price:.2f} outside range"))
                    continue
            else:
                # No price data, but still tradable - keep it for now
                pass
            
            clean_symbols.append(symbol)
            status = "✓ KEEP"
            reason = "OK"
            
        except Exception as e:
            # Symbol doesn't exist / delisted
            symbols_to_remove.append((symbol, f"Not found/delisted: {str(e)}"))
            status = "✗ REMOVE"
            reason = str(e)
        
        # Progress update
        percent = (checked / len(symbols)) * 100
        elapsed = time.time() - start_time
        rate = checked / elapsed if elapsed > 0 else 0
        remaining = (len(symbols) - checked) / rate if rate > 0 else 0
        
        print(f"[{checked}/{len(symbols)}] ({percent:.1f}%) {symbol}: {status} ({reason[:30]}) | "
              f"ETA: {remaining/60:.1f}min", end='\r')
        
        # Rate limiting
        time.sleep(0.1)
    
    print()  # New line after progress
    
    # Remove symbols from database
    if symbols_to_remove:
        print(f"\n{'='*80}")
        print("Removing non-tradable/invalid symbols")
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
    print(f"  Symbols kept: {len(clean_symbols)}")
    print(f"  Symbols removed: {len(symbols_to_remove)}")
    print(f"{'='*80}\n")
    
    conn.close()

if __name__ == '__main__':
    print("=" * 80)
    print("Cleanup Tradable Symbols (Alpaca SDK)")
    print("=" * 80)
    print("\nThis uses ChatGPT's recommended approach with Alpaca SDK.")
    print("\nRequirements: pip install alpaca-py")
    print("\nThis will remove symbols from the database that are:")
    print("  - Outside $1-$20 price range")
    print("  - Not tradable")
    print("  - Not active")
    print("  - Not US equity")
    
    try:
        from alpaca.trading.client import TradingClient
    except ImportError:
        print("\n❌ ERROR: alpaca-py not installed!")
        print("Install it with: pip install alpaca-py")
        exit(1)
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
    else:
        cleanup_database_sdk(
            min_price=1.0,
            max_price=20.0
        )
        print("Done!")

