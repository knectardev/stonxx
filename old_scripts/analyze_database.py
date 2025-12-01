"""
Analyze the database to see what data we currently have
"""
from database import get_connection, get_symbols_with_data, get_latest_bar, get_data_range, get_bar_count
from datetime import datetime, timedelta

def analyze_database():
    """Analyze current database contents"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("=" * 60)
    print("DATABASE ANALYSIS")
    print("=" * 60)
    
    # Total bars
    cursor.execute('SELECT COUNT(*) FROM bars')
    total_bars = cursor.fetchone()[0]
    print(f"\nTotal bars in database: {total_bars:,}")
    
    # By timeframe
    cursor.execute('''
        SELECT timeframe, COUNT(*) as count
        FROM bars
        GROUP BY timeframe
        ORDER BY count DESC
    ''')
    print(f"\nBars by timeframe:")
    by_timeframe = cursor.fetchall()
    for tf, count in by_timeframe:
        print(f"  {tf}: {count:,}")
    
    # Unique symbols
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM bars')
    unique_symbols = cursor.fetchone()[0]
    print(f"\nUnique symbols: {unique_symbols}")
    
    # Date range
    cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM bars')
    row = cursor.fetchone()
    min_ts = row[0]
    max_ts = row[1]
    
    if min_ts and max_ts:
        min_dt = datetime.fromtimestamp(min_ts)
        max_dt = datetime.fromtimestamp(max_ts)
        days = (max_dt - min_dt).days
        print(f"\nOverall date range:")
        print(f"  Earliest: {min_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Latest: {max_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Span: {days} days")
    
    # Check 1Min data only
    cursor.execute('''
        SELECT COUNT(*) FROM bars WHERE timeframe = '1Min'
    ''')
    one_min_count = cursor.fetchone()[0]
    print(f"\n1Min bars: {one_min_count:,}")
    
    # Date range for 1Min data
    cursor.execute('''
        SELECT MIN(timestamp), MAX(timestamp) 
        FROM bars 
        WHERE timeframe = '1Min'
    ''')
    row = cursor.fetchone()
    if row[0] and row[1]:
        min_dt = datetime.fromtimestamp(row[0])
        max_dt = datetime.fromtimestamp(row[1])
        days = (max_dt - min_dt).days
        print(f"\n1Min data date range:")
        print(f"  Earliest: {min_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Latest: {max_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Span: {days} days")
    
    # Calculate 14 days from yesterday
    yesterday = datetime.now() - timedelta(days=1)
    cutoff_date = yesterday - timedelta(days=14)
    cutoff_timestamp = int(cutoff_date.timestamp())
    
    print(f"\nTarget date range (14 days from yesterday):")
    print(f"  Yesterday: {yesterday.strftime('%Y-%m-%d')}")
    print(f"  Cutoff date: {cutoff_date.strftime('%Y-%m-%d')}")
    print(f"  Cutoff timestamp: {cutoff_timestamp}")
    
    # Count bars outside date range
    cursor.execute('''
        SELECT COUNT(*) FROM bars 
        WHERE timeframe = '1Min' AND timestamp < ?
    ''', (cutoff_timestamp,))
    old_bars = cursor.fetchone()[0]
    print(f"\n1Min bars older than cutoff: {old_bars:,}")
    
    # Get symbols with 1Min data and their latest prices
    symbols = get_symbols_with_data('1Min')
    print(f"\nSymbols with 1Min data: {len(symbols)}")
    
    print(f"\nSample symbols (first 20) with latest prices:")
    price_ranges = {'under_1': [], '1_to_20': [], 'over_20': []}
    
    for symbol in symbols[:20]:
        latest = get_latest_bar(symbol, '1Min')
        if latest:
            price = latest['close']
            date_range = get_data_range(symbol, '1Min')
            count = get_bar_count(symbol=symbol, timeframe='1Min')
            
            if price < 1.0:
                price_ranges['under_1'].append((symbol, price))
                status = "UNDER $1"
            elif price <= 20.0:
                price_ranges['1_to_20'].append((symbol, price))
                status = "OK"
            else:
                price_ranges['over_20'].append((symbol, price))
                status = "OVER $20"
            
            if date_range:
                min_dt = datetime.fromtimestamp(date_range[0])
                max_dt = datetime.fromtimestamp(date_range[1])
                days = (max_dt - min_dt).days
                print(f"  {symbol}: ${price:.2f} ({status}) - {count:,} bars - {min_dt.date()} to {max_dt.date()} ({days} days)")
    
    # Count all symbols by price range
    print(f"\nAnalyzing ALL symbols for price ranges...")
    for symbol in symbols:
        latest = get_latest_bar(symbol, '1Min')
        if latest:
            price = latest['close']
            if price < 1.0:
                price_ranges['under_1'].append((symbol, price))
            elif price <= 20.0:
                price_ranges['1_to_20'].append((symbol, price))
            else:
                price_ranges['over_20'].append((symbol, price))
    
    print(f"\nPrice range summary:")
    print(f"  Under $1: {len(price_ranges['under_1'])} symbols")
    print(f"  $1-$20: {len(price_ranges['1_to_20'])} symbols")
    print(f"  Over $20: {len(price_ranges['over_20'])} symbols")
    
    conn.close()
    print("\n" + "=" * 60)

if __name__ == '__main__':
    analyze_database()

