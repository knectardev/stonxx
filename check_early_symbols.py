"""
Check which symbols have data going back to Nov 17
"""
from database import get_symbols_with_data, get_data_range
from datetime import datetime, timedelta

def check_early_symbols():
    """Find symbols with data from Nov 17"""
    symbols = get_symbols_with_data('1Min')
    
    cutoff_date = datetime(2025, 11, 17, 0, 0, 0)
    cutoff_timestamp = int(cutoff_date.timestamp())
    
    print("=" * 80)
    print("CHECKING SYMBOLS WITH DATA FROM NOV 17")
    print("=" * 80)
    
    symbols_with_early_data = []
    symbols_missing_early = []
    
    for symbol in symbols:
        date_range = get_data_range(symbol, '1Min')
        if date_range:
            min_dt = datetime.fromtimestamp(date_range[0])
            if min_dt.timestamp() <= cutoff_timestamp:
                symbols_with_early_data.append((symbol, min_dt.date()))
            else:
                symbols_missing_early.append((symbol, min_dt.date()))
    
    print(f"\nTotal symbols: {len(symbols)}")
    print(f"Symbols with data from Nov 17 or earlier: {len(symbols_with_early_data)}")
    print(f"Symbols starting after Nov 17: {len(symbols_missing_early)}")
    
    if symbols_with_early_data:
        print(f"\n{'='*80}")
        print("Symbols with early data (first 30):")
        print(f"{'='*80}")
        for symbol, start_date in symbols_with_early_data[:30]:
            print(f"  {symbol}: starts {start_date}")
        
        if len(symbols_with_early_data) > 30:
            print(f"  ... and {len(symbols_with_early_data) - 30} more")
    
    # Check what dates actually exist in the database
    print(f"\n{'='*80}")
    print("Checking what dates actually exist in database...")
    print(f"{'='*80}")
    
    from database import get_connection
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get all unique dates
    cursor.execute('''
        SELECT DISTINCT DATE(datetime(timestamp, 'unixepoch')) as date
        FROM bars
        WHERE timeframe = '1Min'
        ORDER BY date
    ''')
    
    dates = [row[0] for row in cursor.fetchall()]
    print(f"\nUnique dates in database:")
    for date in dates:
        cursor.execute('''
            SELECT COUNT(DISTINCT symbol) as symbol_count, COUNT(*) as bar_count
            FROM bars
            WHERE timeframe = '1Min' AND DATE(datetime(timestamp, 'unixepoch')) = ?
        ''', (date,))
        row = cursor.fetchone()
        symbol_count, bar_count = row
        print(f"  {date}: {symbol_count} symbols, {bar_count:,} bars")
    
    conn.close()
    
    print(f"\n{'='*80}")
    print("CONCLUSION")
    print(f"{'='*80}")
    
    if len(symbols_with_early_data) == 0:
        print("⚠ No symbols have data from Nov 17 or earlier.")
        print("This suggests the data was only fetched starting Nov 24.")
        print("To get 14 days of data, you would need to fetch historical data")
        print("going back 14 days from yesterday for all symbols.")
    else:
        print(f"✓ {len(symbols_with_early_data)} symbols have early data.")
        print(f"  But {len(symbols_missing_early)} symbols are missing early data.")

if __name__ == '__main__':
    check_early_symbols()

