"""
Check date ranges for specific symbols to see what data they have
"""
from database import get_symbols_with_data, get_data_range, get_bar_count
from datetime import datetime, timedelta

def check_symbol_ranges():
    """Check date ranges for symbols"""
    symbols = get_symbols_with_data('1Min')
    
    # Calculate expected date range
    yesterday = datetime.now() - timedelta(days=1)
    cutoff_date = yesterday - timedelta(days=14)
    cutoff_date = cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    print("=" * 80)
    print("SYMBOL DATE RANGE ANALYSIS")
    print("=" * 80)
    print(f"\nExpected date range: {cutoff_date.strftime('%Y-%m-%d')} to {yesterday.strftime('%Y-%m-%d')} (14 days)")
    print(f"\nChecking {len(symbols)} symbols...\n")
    
    # Check ABEV specifically
    print("Checking ABEV specifically:")
    date_range = get_data_range('ABEV', '1Min')
    count = get_bar_count(symbol='ABEV', timeframe='1Min')
    if date_range:
        min_dt = datetime.fromtimestamp(date_range[0])
        max_dt = datetime.fromtimestamp(date_range[1])
        days = (max_dt - min_dt).days
        print(f"  ABEV: {count:,} bars from {min_dt.strftime('%Y-%m-%d %H:%M')} to {max_dt.strftime('%Y-%m-%d %H:%M')} ({days} calendar days)")
        
        # Check if it starts before cutoff
        if min_dt.date() > cutoff_date.date():
            missing_days = (min_dt.date() - cutoff_date.date()).days
            print(f"  ⚠ WARNING: Missing {missing_days} days of data before {min_dt.date()}")
    
    # Sample some symbols to see date ranges
    print(f"\n{'='*80}")
    print("Sample of 30 random symbols and their date ranges:")
    print(f"{'='*80}")
    print(f"{'Symbol':<10} {'Bars':<8} {'Start Date':<12} {'End Date':<12} {'Days':<6} {'Status'}")
    print("-" * 80)
    
    symbols_to_check = ['ABEV'] + [s for s in symbols if s != 'ABEV'][:29]
    
    missing_early_data = []
    
    for symbol in symbols_to_check:
        date_range = get_data_range(symbol, '1Min')
        count = get_bar_count(symbol=symbol, timeframe='1Min')
        if date_range:
            min_dt = datetime.fromtimestamp(date_range[0])
            max_dt = datetime.fromtimestamp(date_range[1])
            days = (max_dt - min_dt).days
            
            start_str = min_dt.strftime('%Y-%m-%d')
            end_str = max_dt.strftime('%Y-%m-%d')
            
            # Check if starts early enough
            if min_dt.date() > cutoff_date.date():
                status = "MISSING EARLY"
                missing_early_data.append((symbol, min_dt.date(), cutoff_date.date()))
            elif min_dt.date() <= cutoff_date.date():
                status = "OK"
            else:
                status = "?"
            
            print(f"{symbol:<10} {count:<8,} {start_str:<12} {end_str:<12} {days:<6} {status}")
    
    # Statistics
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    
    symbols_with_full_range = 0
    symbols_missing_early = 0
    all_date_ranges = []
    
    for symbol in symbols:
        date_range = get_data_range(symbol, '1Min')
        if date_range:
            min_dt = datetime.fromtimestamp(date_range[0])
            all_date_ranges.append(min_dt.date())
            if min_dt.date() <= cutoff_date.date():
                symbols_with_full_range += 1
            else:
                symbols_missing_early += 1
    
    if all_date_ranges:
        earliest_date = min(all_date_ranges)
        latest_date = max(all_date_ranges)
        
        print(f"Earliest data start across all symbols: {earliest_date}")
        print(f"Latest data start across all symbols: {latest_date}")
        print(f"\nSymbols with data going back to cutoff ({cutoff_date.date()}): {symbols_with_full_range}")
        print(f"Symbols missing early data (start after {cutoff_date.date()}): {symbols_missing_early}")
    
    if missing_early_data:
        print(f"\n⚠ Symbols missing early data (first 20):")
        for symbol, start_date, cutoff in missing_early_data[:20]:
            missing_days = (start_date - cutoff).days
            print(f"  {symbol}: starts {start_date} (missing {missing_days} days before cutoff)")
    
    print(f"\n{'='*80}")

if __name__ == '__main__':
    check_symbol_ranges()

