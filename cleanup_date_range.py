"""
Clean up database to only keep:
1. 1Min timeframe data (already all 1Min)
2. Last 14 days from yesterday
3. Symbols with prices in $1-$20 range (verified from latest bar)
"""
from database import get_connection, get_symbols_with_data, get_latest_bar
from datetime import datetime, timedelta

def cleanup_database():
    """Clean database to only keep 14 days of 1Min data for $1-$20 symbols"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("=" * 60)
    print("DATABASE CLEANUP")
    print("=" * 60)
    
    # Calculate cutoff date: 14 days before yesterday
    yesterday = datetime.now() - timedelta(days=1)
    cutoff_date = yesterday - timedelta(days=14)
    cutoff_timestamp = int(cutoff_date.timestamp())
    
    # Round cutoff to start of day for clarity
    cutoff_start_of_day = cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff_timestamp = int(cutoff_start_of_day.timestamp())
    
    print(f"\nCleanup criteria:")
    print(f"  Timeframe: 1Min only")
    print(f"  Price range: $1-$20 (based on latest bar close price)")
    print(f"  Date range: Last 14 days from yesterday")
    print(f"  Yesterday: {yesterday.strftime('%Y-%m-%d')}")
    print(f"  Cutoff date: {cutoff_start_of_day.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Cutoff timestamp: {cutoff_timestamp}")
    
    # Step 1: Count current state
    cursor.execute('SELECT COUNT(*) FROM bars')
    total_before = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT COUNT(*) FROM bars WHERE timeframe = '1Min'
    ''')
    one_min_before = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT COUNT(*) FROM bars 
        WHERE timeframe = '1Min' AND timestamp < ?
    ''', (cutoff_timestamp,))
    old_bars_count = cursor.fetchone()[0]
    
    print(f"\nCurrent state:")
    print(f"  Total bars: {total_before:,}")
    print(f"  1Min bars: {one_min_before:,}")
    print(f"  Bars older than cutoff: {old_bars_count:,}")
    
    # Step 2: Delete bars older than cutoff date (keep only 14 days)
    print(f"\n{'='*60}")
    print("STEP 1: Removing data older than 14 days")
    print(f"{'='*60}")
    
    cursor.execute('''
        DELETE FROM bars 
        WHERE timeframe = '1Min' AND timestamp < ?
    ''', (cutoff_timestamp,))
    
    deleted_old = cursor.rowcount
    conn.commit()
    print(f"✓ Deleted {deleted_old:,} bars older than cutoff date")
    
    # Step 3: Delete any non-1Min data (shouldn't be any, but just in case)
    cursor.execute('''
        SELECT COUNT(*) FROM bars WHERE timeframe != '1Min'
    ''')
    non_1min_count = cursor.fetchone()[0]
    
    if non_1min_count > 0:
        print(f"\n{'='*60}")
        print("STEP 2: Removing non-1Min timeframe data")
        print(f"{'='*60}")
        cursor.execute('DELETE FROM bars WHERE timeframe != "1Min"')
        deleted_non_1min = cursor.rowcount
        conn.commit()
        print(f"✓ Deleted {deleted_non_1min:,} bars with non-1Min timeframe")
    else:
        print(f"\n✓ No non-1Min data found (all data is already 1Min)")
    
    # Step 4: Check symbols and remove those outside $1-$20 price range
    print(f"\n{'='*60}")
    print("STEP 3: Checking symbols for $1-$20 price range")
    print(f"{'='*60}")
    
    symbols = get_symbols_with_data('1Min')
    print(f"Found {len(symbols)} symbols with data")
    
    symbols_to_remove = []
    symbols_to_keep = []
    
    print(f"\nChecking latest prices...")
    for i, symbol in enumerate(symbols, 1):
        latest = get_latest_bar(symbol, '1Min')
        if latest:
            price = latest['close']
            if price < 1.0 or price > 20.0:
                symbols_to_remove.append((symbol, price))
                if i <= 10:  # Show first 10
                    print(f"  {symbol}: ${price:.2f} - OUT OF RANGE (will remove)")
            else:
                symbols_to_keep.append((symbol, price))
        else:
            # No latest bar, probably should remove this symbol too
            symbols_to_remove.append((symbol, None))
            if i <= 10:
                print(f"  {symbol}: No price data - will remove")
    
    if len(symbols_to_remove) > 10:
        print(f"  ... and {len(symbols_to_remove) - 10} more symbols to remove")
    
    # Remove symbols outside price range
    if symbols_to_remove:
        print(f"\nRemoving {len(symbols_to_remove)} symbols outside $1-$20 range...")
        for symbol, price in symbols_to_remove:
            cursor.execute('DELETE FROM bars WHERE symbol = ?', (symbol,))
        deleted_symbols = cursor.rowcount
        conn.commit()
        print(f"✓ Removed all bars for {len(symbols_to_remove)} symbols")
        if len(symbols_to_remove) <= 20:
            removed_symbols_list = [s[0] for s in symbols_to_remove]
            print(f"  Removed: {', '.join(removed_symbols_list)}")
        else:
            removed_symbols_list = [s[0] for s in symbols_to_remove[:10]]
            print(f"  Removed (first 10): {', '.join(removed_symbols_list)} ... and {len(symbols_to_remove) - 10} more")
    else:
        print(f"\n✓ All symbols are in $1-$20 range")
    
    # Step 5: Final statistics
    cursor.execute('SELECT COUNT(*) FROM bars')
    total_after = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM bars')
    symbols_after = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT MIN(timestamp), MAX(timestamp) FROM bars
    ''')
    row = cursor.fetchone()
    
    print(f"\n{'='*60}")
    print("CLEANUP COMPLETE")
    print(f"{'='*60}")
    print(f"\nFinal statistics:")
    print(f"  Bars before: {total_before:,}")
    print(f"  Bars after: {total_after:,}")
    print(f"  Bars deleted: {total_before - total_after:,}")
    print(f"  Symbols remaining: {symbols_after}")
    print(f"  Symbols removed: {len(symbols) - symbols_after}")
    
    if row[0] and row[1]:
        min_dt = datetime.fromtimestamp(row[0])
        max_dt = datetime.fromtimestamp(row[1])
        days = (max_dt - min_dt).days
        print(f"\nDate range after cleanup:")
        print(f"  Earliest: {min_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Latest: {max_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Span: {days} days")
    
    # Verify date range is correct
    if row[0]:
        earliest_ts = row[0]
        earliest_date = datetime.fromtimestamp(earliest_ts)
        if earliest_date < cutoff_start_of_day:
            print(f"\n⚠ WARNING: Some data is still older than cutoff date!")
        else:
            print(f"\n✓ All remaining data is within the 14-day window")
    
    conn.close()
    print(f"\n{'='*60}\n")

if __name__ == '__main__':
    response = input("This will delete old data from the database. Continue? (yes/no): ").strip().lower()
    if response == 'yes':
        cleanup_database()
    else:
        print("Cancelled.")

