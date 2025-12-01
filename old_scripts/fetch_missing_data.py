"""
Fetch missing historical data to fill gaps and ensure 14 full days of data
for all $1-$20 symbols.
"""
from fetch_historical_data import fetch_bars
from database import init_database, insert_bars_batch, get_symbols_with_data, get_data_range, get_bars
import requests
import yaml
from datetime import datetime, timedelta
import time
from typing import List, Tuple, Optional

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

def get_missing_date_ranges(symbol: str, timeframe: str, target_start: datetime, target_end: datetime) -> List[Tuple[datetime, datetime]]:
    """
    Identify missing date ranges for a symbol within the target date range.
    
    Returns list of (start, end) tuples for missing ranges.
    """
    existing_range = get_data_range(symbol, timeframe)
    
    if not existing_range:
        # No data exists, need to fetch entire range
        return [(target_start, target_end)]
    
    existing_start = datetime.fromtimestamp(existing_range[0])
    existing_end = datetime.fromtimestamp(existing_range[1])
    
    missing_ranges = []
    
    # Check for missing data before existing start
    if existing_start > target_start:
        missing_ranges.append((target_start, existing_start - timedelta(minutes=1)))
    
    # Check for gaps within existing data by looking at actual bars
    existing_bars = get_bars(symbol, timeframe, 
                            start_time=int(target_start.timestamp()),
                            end_time=int(target_end.timestamp()))
    
    if not existing_bars:
        # Have range info but no bars in target range, fetch all
        return [(target_start, target_end)]
    
    # Sort bars by timestamp
    existing_bars.sort(key=lambda x: x['timestamp'])
    
    # Check for gaps in the data (more than 2 hours gap suggests missing data)
    max_gap_minutes = 120
    last_timestamp = None
    
    for bar in existing_bars:
        bar_time = datetime.fromtimestamp(bar['timestamp'])
        if last_timestamp:
            gap_minutes = (bar_time - last_timestamp).total_seconds() / 60
            if gap_minutes > max_gap_minutes:
                # Significant gap found
                gap_start = last_timestamp + timedelta(minutes=1)
                gap_end = bar_time - timedelta(minutes=1)
                if gap_start < gap_end and gap_start >= target_start and gap_end <= target_end:
                    missing_ranges.append((gap_start, gap_end))
        last_timestamp = bar_time
    
    # Check for missing data after existing end
    if existing_end < target_end:
        missing_ranges.append((existing_end + timedelta(minutes=1), target_end))
    
    return missing_ranges

def analyze_data_coverage(symbol: str, target_start: datetime, target_end: datetime) -> dict:
    """Analyze what data exists vs what's needed"""
    existing_range = get_data_range(symbol, '1Min')
    
    if not existing_range:
        return {
            'has_data': False,
            'coverage_pct': 0.0,
            'missing_ranges': [(target_start, target_end)]
        }
    
    existing_start = datetime.fromtimestamp(existing_range[0])
    existing_end = datetime.fromtimestamp(existing_range[1])
    
    # Count existing bars in target range
    existing_bars = get_bars(symbol, '1Min',
                            start_time=int(target_start.timestamp()),
                            end_time=int(target_end.timestamp()))
    
    # Estimate expected bars (assuming ~390 bars per trading day, 14 days = ~2730 bars)
    # But we'll use a simpler approach - check date coverage
    target_days = (target_end.date() - target_start.date()).days + 1
    coverage_days = (min(existing_end.date(), target_end.date()) - max(existing_start.date(), target_start.date())).days + 1
    coverage_pct = (coverage_days / target_days) * 100 if target_days > 0 else 0
    
    missing_ranges = get_missing_date_ranges(symbol, '1Min', target_start, target_end)
    
    return {
        'has_data': True,
        'existing_start': existing_start,
        'existing_end': existing_end,
        'coverage_pct': coverage_pct,
        'bar_count': len(existing_bars),
        'missing_ranges': missing_ranges
    }

def fetch_missing_data_for_symbol(symbol: str, timeframe: str, 
                                   target_start: datetime, target_end: datetime) -> dict:
    """Fetch missing data for a single symbol"""
    result = {
        'symbol': symbol,
        'bars_fetched': 0,
        'bars_inserted': 0,
        'ranges_fetched': 0,
        'errors': []
    }
    
    # Identify missing ranges
    missing_ranges = get_missing_date_ranges(symbol, timeframe, target_start, target_end)
    
    if not missing_ranges:
        return result
    
    # Fetch each missing range
    for range_start, range_end in missing_ranges:
        try:
            result['ranges_fetched'] += 1
            bars = fetch_bars(symbol, timeframe, range_start, range_end)
            result['bars_fetched'] += len(bars) if bars else 0
            
            if bars:
                inserted = insert_bars_batch(bars)
                result['bars_inserted'] += inserted
                
            # Rate limiting between ranges
            time.sleep(0.2)
            
        except Exception as e:
            result['errors'].append(f"Range {range_start.date()} to {range_end.date()}: {str(e)}")
            continue
    
    return result

def fetch_all_missing_data(days: int = 14, timeframe: str = '1Min',
                           min_price: float = 1.0, max_price: float = 20.0):
    """
    Fetch missing historical data to ensure full 14-day coverage for all symbols.
    
    Args:
        days: Number of days to fetch (default: 14)
        timeframe: Bar timeframe (default: '1Min')
        min_price: Minimum price for symbols (default: 1.0)
        max_price: Maximum price for symbols (default: 20.0)
    """
    # Initialize database
    print("Initializing database...")
    init_database()
    
    # Calculate target date range (14 days from yesterday)
    yesterday = datetime.now() - timedelta(days=1)
    target_end = yesterday.replace(hour=23, minute=59, second=59)
    target_start = (target_end - timedelta(days=days)).replace(hour=0, minute=0, second=0)
    
    print(f"\n{'='*80}")
    print("FETCH MISSING DATA - 14 DAYS COMPLETE COVERAGE")
    print(f"{'='*80}")
    print(f"Target date range: {target_start.date()} to {target_end.date()} ({days} days)")
    print(f"Timeframe: {timeframe}")
    print(f"Price range: ${min_price} - ${max_price}")
    print(f"{'='*80}\n")
    
    # Get symbols from database (already filtered to $1-$20)
    print("Getting symbols from database...")
    db_symbols = get_symbols_with_data(timeframe)
    
    if not db_symbols:
        print("No symbols found in database. Please run fetch_filtered_data.py first.")
        return
    
    print(f"Found {len(db_symbols)} symbols in database\n")
    
    # Analyze coverage for all symbols
    print(f"{'='*80}")
    print("STEP 1: Analyzing data coverage")
    print(f"{'='*80}\n")
    
    symbols_needing_data = []
    coverage_stats = []
    
    for i, symbol in enumerate(db_symbols, 1):
        analysis = analyze_data_coverage(symbol, target_start, target_end)
        coverage_stats.append(analysis)
        
        if analysis['missing_ranges']:
            symbols_needing_data.append((symbol, analysis))
        
        if i % 100 == 0:
            print(f"  Analyzed {i}/{len(db_symbols)} symbols... ({len(symbols_needing_data)} need data)", end='\r')
    
    print(f"\n  ✓ Analysis complete: {len(symbols_needing_data)}/{len(db_symbols)} symbols need additional data\n")
    
    if not symbols_needing_data:
        print("✓ All symbols already have complete 14-day coverage!")
        return
    
    # Show summary
    symbols_with_gaps = len([s for s in coverage_stats if s['coverage_pct'] < 95])
    symbols_missing_early = len([s for s in coverage_stats if s.get('has_data') and s.get('existing_start') and s['existing_start'].date() > target_start.date()])
    
    print(f"Summary:")
    print(f"  Symbols needing data: {len(symbols_needing_data)}")
    print(f"  Symbols with gaps: {symbols_with_gaps}")
    print(f"  Symbols missing early data: {symbols_missing_early}")
    print()
    
    # Fetch missing data
    print(f"{'='*80}")
    print("STEP 2: Fetching missing data")
    print(f"{'='*80}\n")
    
    total_bars_fetched = 0
    total_bars_inserted = 0
    successful = 0
    failed = 0
    start_time = time.time()
    
    for i, (symbol, analysis) in enumerate(symbols_needing_data, 1):
        try:
            percent = (i / len(symbols_needing_data)) * 100
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(symbols_needing_data) - i) / rate if rate > 0 else 0
            
            # Show what's missing
            missing_days = sum([(end.date() - start.date()).days for start, end in analysis['missing_ranges']])
            print(f"[{i}/{len(symbols_needing_data)}] ({percent:.1f}%) {symbol}: "
                  f"coverage {analysis['coverage_pct']:.0f}%, "
                  f"missing ~{missing_days} days...", end=' ', flush=True)
            
            # Fetch missing data
            result = fetch_missing_data_for_symbol(symbol, timeframe, target_start, target_end)
            
            if result['bars_inserted'] > 0:
                total_bars_fetched += result['bars_fetched']
                total_bars_inserted += result['bars_inserted']
                successful += 1
                print(f"✓ {result['bars_inserted']:,} bars | "
                      f"Total: {total_bars_inserted:,} | "
                      f"ETA: {remaining/60:.1f}min")
            elif result['errors']:
                failed += 1
                print(f"✗ Errors: {len(result['errors'])}")
            else:
                successful += 1
                print(f"✓ No additional data needed")
            
            # Rate limiting
            time.sleep(0.2)
            
        except KeyboardInterrupt:
            print("\n\nInterrupted by user. Stopping...")
            break
        except Exception as e:
            failed += 1
            print(f"✗ Error: {e}")
            continue
    
    elapsed_total = time.time() - start_time
    
    print(f"\n{'='*80}")
    print("FETCH COMPLETE")
    print(f"{'='*80}")
    print(f"Summary:")
    print(f"  Symbols processed: {len(symbols_needing_data):,}")
    print(f"  Successful: {successful:,}")
    print(f"  Failed: {failed:,}")
    print(f"  Bars fetched: {total_bars_fetched:,}")
    print(f"  Bars inserted: {total_bars_inserted:,}")
    print(f"  Time elapsed: {elapsed_total/60:.1f} minutes")
    print(f"  Average: {elapsed_total/len(symbols_needing_data):.2f} seconds per symbol")
    print(f"{'='*80}\n")

def main():
    """Main function"""
    print("=" * 80)
    print("Fetch Missing Historical Data - 14 Days Complete Coverage")
    print("=" * 80)
    
    print("\nThis script will:")
    print("  1. Check existing data in database for all symbols")
    print("  2. Identify missing date ranges within the last 14 days")
    print("  3. Fetch only the missing data to fill gaps")
    print("  4. Ensure complete 14-day coverage for all $1-$20 symbols")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
        return
    
    fetch_all_missing_data(
        days=14,
        timeframe='1Min',
        min_price=1.0,
        max_price=20.0
    )
    
    print("Done!")

if __name__ == '__main__':
    main()

