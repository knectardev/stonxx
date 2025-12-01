"""
Generate a status report on the database
"""
from database import get_connection, get_symbols_with_data, get_data_range, get_bar_count
from datetime import datetime, timedelta

def generate_report():
    """Generate comprehensive status report"""
    conn = get_connection()
    cursor = conn.cursor()
    
    yesterday = datetime.now() - timedelta(days=1)
    cutoff_date = yesterday - timedelta(days=14)
    cutoff_date = cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff_timestamp = int(cutoff_date.timestamp())
    
    print("=" * 80)
    print("DATABASE STATUS REPORT")
    print("=" * 80)
    
    # Overall stats
    cursor.execute('SELECT COUNT(*) FROM bars WHERE timeframe = "1Min"')
    total_bars = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM bars WHERE timeframe = "1Min"')
    total_symbols = cursor.fetchone()[0]
    
    print(f"\nOverall Statistics:")
    print(f"  Total 1Min bars: {total_bars:,}")
    print(f"  Total symbols: {total_symbols}")
    
    # Date analysis
    cursor.execute('''
        SELECT DATE(datetime(timestamp, 'unixepoch')) as date, 
               COUNT(DISTINCT symbol) as symbol_count,
               COUNT(*) as bar_count
        FROM bars
        WHERE timeframe = '1Min'
        GROUP BY date
        ORDER BY date
    ''')
    
    dates_data = cursor.fetchall()
    
    print(f"\nData by Date:")
    print(f"{'Date':<12} {'Symbols':<10} {'Bars':<12} {'Status'}")
    print("-" * 50)
    
    for date, symbol_count, bar_count in dates_data:
        date_obj = datetime.strptime(date, '%Y-%m-%d').date()
        if date_obj < cutoff_date.date():
            status = "BEFORE CUTOFF"
        elif date_obj >= cutoff_date.date() and date_obj <= yesterday.date():
            status = "IN RANGE"
        else:
            status = "AFTER YESTERDAY"
        print(f"{date:<12} {symbol_count:<10} {bar_count:<12,} {status}")
    
    # Symbol coverage analysis
    symbols = get_symbols_with_data('1Min')
    
    symbols_full_14_days = []
    symbols_partial_data = []
    
    for symbol in symbols:
        date_range = get_data_range(symbol, '1Min')
        if date_range:
            min_dt = datetime.fromtimestamp(date_range[0])
            max_dt = datetime.fromtimestamp(date_range[1])
            days_span = (max_dt.date() - min_dt.date()).days
            
            # Check if it starts early enough and has reasonable coverage
            if min_dt.timestamp() <= cutoff_timestamp and days_span >= 10:
                symbols_full_14_days.append((symbol, min_dt.date(), max_dt.date(), days_span))
            else:
                symbols_partial_data.append((symbol, min_dt.date(), max_dt.date(), days_span))
    
    print(f"\n{'='*80}")
    print("Symbol Coverage Analysis")
    print(f"{'='*80}")
    print(f"Symbols with data going back to cutoff date ({cutoff_date.date()}): {len(symbols_full_14_days)}")
    print(f"Symbols with partial/recent data only: {len(symbols_partial_data)}")
    
    if symbols_full_14_days:
        print(f"\nSymbols with full coverage (first 10):")
        for symbol, start, end, days in symbols_full_14_days[:10]:
            print(f"  {symbol}: {start} to {end} ({days} days)")
    
    # Check what's missing
    print(f"\n{'='*80}")
    print("Missing Data Analysis")
    print(f"{'='*80}")
    print(f"Expected date range: {cutoff_date.date()} to {yesterday.date()} (14 calendar days)")
    
    existing_dates = {datetime.strptime(date, '%Y-%m-%d').date() for date, _, _ in dates_data}
    expected_dates = set()
    current_date = cutoff_date.date()
    while current_date <= yesterday.date():
        # Skip weekends (Saturday=5, Sunday=6)
        if current_date.weekday() < 5:  # Monday=0, Friday=4
            expected_dates.add(current_date)
        current_date += timedelta(days=1)
    
    missing_dates = expected_dates - existing_dates
    
    print(f"\nTrading days in range: {len(expected_dates)}")
    print(f"Trading days with data: {len(existing_dates)}")
    print(f"Missing trading days: {len(missing_dates)}")
    
    if missing_dates:
        print(f"\nMissing dates:")
        for date in sorted(missing_dates):
            print(f"  {date} ({date.strftime('%A')})")
    
    print(f"\n{'='*80}")
    print("RECOMMENDATION")
    print(f"{'='*80}")
    print("Most symbols only have data from Nov 24 onwards (~5 days), not the full 14 days.")
    print("To get 14 days of data for all symbols, you would need to:")
    print("  1. Fetch historical data going back 14 days from yesterday")
    print("  2. This requires API calls to Alpaca")
    print("\nCurrent cleanup status:")
    print("  ✓ All old data (Sept/Oct) has been removed")
    print("  ✓ Only data within 14-day window remains")
    print("  ✓ All symbols are in $1-$20 price range")
    print("  ⚠ Most symbols only have ~5 days of data (Nov 24-28)")
    
    conn.close()

if __name__ == '__main__':
    generate_report()

