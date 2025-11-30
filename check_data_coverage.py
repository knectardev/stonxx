"""
Check actual data coverage for all symbols - see if they truly have 14 days of data
"""
from database import get_symbols_with_data, get_data_range, get_bar_count
from datetime import datetime, timedelta

def check_coverage():
    """Check if symbols actually have complete 14-day coverage"""
    # Calculate target date range: 14 days from yesterday
    yesterday = datetime.now() - timedelta(days=1)
    target_end = yesterday.replace(hour=23, minute=59, second=59)
    target_start = (target_end - timedelta(days=14)).replace(hour=0, minute=0, second=0)
    target_start_ts = int(target_start.timestamp())
    target_end_ts = int(target_end.timestamp())
    
    print("=" * 80)
    print("DATA COVERAGE ANALYSIS")
    print("=" * 80)
    print(f"\nTarget date range: {target_start.date()} to {target_end.date()} (14 days)")
    
    symbols = get_symbols_with_data('1Min')
    print(f"\nTotal symbols: {len(symbols)}")
    
    symbols_complete = []
    symbols_incomplete = []
    symbols_missing_early = []
    symbols_missing_data = []
    
    print(f"\nAnalyzing coverage for each symbol...\n")
    
    for i, symbol in enumerate(symbols, 1):
        date_range = get_data_range(symbol, '1Min')
        
        if not date_range:
            symbols_missing_data.append((symbol, None, None))
            continue
        
        existing_start = datetime.fromtimestamp(date_range[0])
        existing_end = datetime.fromtimestamp(date_range[1])
        
        # Check if starts early enough
        starts_early_enough = date_range[0] <= target_start_ts
        ends_recent_enough = date_range[1] >= (target_end_ts - 86400)  # Within last day
        
        # Count bars in target range
        from database import get_bars
        bars_in_range = get_bars(symbol, '1Min', 
                                start_time=target_start_ts,
                                end_time=target_end_ts)
        
        # Estimate expected bars (14 days * ~390 bars per trading day = ~2730 bars minimum)
        # But we'll be more lenient - check if it covers the date range
        days_coverage = (existing_end.date() - existing_start.date()).days
        
        if starts_early_enough and ends_recent_enough and len(bars_in_range) > 100:
            symbols_complete.append((symbol, existing_start.date(), existing_end.date(), len(bars_in_range)))
        elif not starts_early_enough:
            symbols_missing_early.append((symbol, existing_start.date(), existing_end.date(), len(bars_in_range)))
        else:
            symbols_incomplete.append((symbol, existing_start.date(), existing_end.date(), len(bars_in_range)))
        
        if i % 50 == 0:
            print(f"  Analyzed {i}/{len(symbols)}...", end='\r')
    
    print()  # New line
    
    print(f"\n{'='*80}")
    print("COVERAGE SUMMARY")
    print(f"{'='*80}\n")
    
    print(f"Symbols with complete 14-day coverage: {len(symbols_complete)}")
    print(f"Symbols missing early data: {len(symbols_missing_early)}")
    print(f"Symbols with incomplete data: {len(symbols_incomplete)}")
    print(f"Symbols with no data: {len(symbols_missing_data)}")
    
    total_needs_data = len(symbols_missing_early) + len(symbols_incomplete) + len(symbols_missing_data)
    
    if symbols_missing_early:
        print(f"\n{'='*80}")
        print("SYMBOLS MISSING EARLY DATA (first 20):")
        print(f"{'='*80}")
        for symbol, start, end, bars in symbols_missing_early[:20]:
            days = (end - start).days if end and start else 0
            print(f"  {symbol}: starts {start}, has {bars:,} bars in range ({days} days)")
        if len(symbols_missing_early) > 20:
            print(f"  ... and {len(symbols_missing_early) - 20} more")
    
    if symbols_incomplete:
        print(f"\n{'='*80}")
        print("SYMBOLS WITH INCOMPLETE DATA (first 20):")
        print(f"{'='*80}")
        for symbol, start, end, bars in symbols_incomplete[:20]:
            days = (end - start).days if end and start else 0
            print(f"  {symbol}: {start} to {end} ({days} days), {bars:,} bars")
        if len(symbols_incomplete) > 20:
            print(f"  ... and {len(symbols_incomplete) - 20} more")
    
    print(f"\n{'='*80}")
    print("RECOMMENDATION")
    print(f"{'='*80}")
    print(f"Symbols needing data fetch: {total_needs_data}/{len(symbols)}")
    print(f"  ({total_needs_data/len(symbols)*100:.1f}% need data)")
    
    if total_needs_data > 0:
        print(f"\nRecommendation: Fetch 14-day data for all {len(symbols)} symbols")
        print("This will ensure complete coverage and fill any gaps.")
    else:
        print(f"\n✓ All symbols appear to have complete coverage!")
    
    print(f"\n{'='*80}\n")
    
    return total_needs_data > 0

if __name__ == '__main__':
    needs_data = check_coverage()

