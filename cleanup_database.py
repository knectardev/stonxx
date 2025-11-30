"""
Remove symbols from database that are outside the $1-$20 price range
"""
from database import get_connection, get_symbols_with_data, get_latest_bar
import requests
import yaml

# Load config for API calls
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

ALPACA_API_KEY = config['alpaca']['api_key']
ALPACA_SECRET_KEY = config['alpaca']['api_secret']
BASE_URL = config['alpaca']['data_url']

def get_headers():
    return {
        'APCA-API-KEY-ID': ALPACA_API_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET_KEY
    }

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
        print(f"  Error fetching price for {symbol}: {e}")
    return None

def cleanup_database(min_price: float = 1.0, max_price: float = 20.0):
    """Remove symbols outside price range from database"""
    print(f"\nCleaning database: Removing symbols with price < ${min_price} or > ${max_price}")
    print("=" * 60)
    
    # Get all symbols with data
    symbols = get_symbols_with_data('1Min')
    print(f"Found {len(symbols)} symbols in database")
    
    symbols_to_remove = []
    symbols_to_keep = []
    
    print("\nChecking prices...")
    for i, symbol in enumerate(symbols, 1):
        # First try to get latest close from database
        latest_bar = get_latest_bar(symbol, '1Min')
        price = None
        
        if latest_bar:
            price = latest_bar['close']
        else:
            # Fallback to API
            price = get_current_price(symbol)
        
        if price is None:
            print(f"[{i}/{len(symbols)}] {symbol}: No price found, keeping in database")
            symbols_to_keep.append(symbol)
        elif price < min_price or price > max_price:
            print(f"[{i}/{len(symbols)}] {symbol}: ${price:.2f} - REMOVING")
            symbols_to_remove.append(symbol)
        else:
            print(f"[{i}/{len(symbols)}] {symbol}: ${price:.2f} - KEEPING")
            symbols_to_keep.append(symbol)
    
    # Remove symbols from database
    if symbols_to_remove:
        print(f"\nRemoving {len(symbols_to_remove)} symbols from database...")
        conn = get_connection()
        cursor = conn.cursor()
        
        for symbol in symbols_to_remove:
            cursor.execute('DELETE FROM bars WHERE symbol = ?', (symbol,))
        
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        
        print(f"✓ Removed {deleted} bar records for {len(symbols_to_remove)} symbols")
        print(f"  Removed symbols: {', '.join(symbols_to_remove)}")
    else:
        print("\n✓ No symbols to remove")
    
    print(f"\n✓ Kept {len(symbols_to_keep)} symbols in database")
    print("=" * 60)

if __name__ == '__main__':
    cleanup_database(min_price=1.0, max_price=20.0)

