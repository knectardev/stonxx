"""
Fetch complete 14 days of 1-minute data for all symbols currently in database.
Resume from a specific symbol.
This will fill any gaps automatically since the database uses INSERT OR IGNORE.
"""
from fetch_historical_data import fetch_bars
from database import init_database, insert_bars_batch, get_symbols_with_data
from datetime import datetime, timedelta
import time
import sys

def fetch_full_14_days_resume(start_symbol: str = None):
    """
    Fetch complete 14 days of data for all symbols in database.
    
    Args:
        start_symbol: Symbol to start from (None = start from beginning)
    """
    # Initialize database
    print("Initializing database...")
    init_database()
    
    # Calculate date range: 14 days from yesterday
    yesterday = datetime.now() - timedelta(days=1)
    target_end = yesterday.replace(hour=23, minute=59, second=59)
    target_start = (target_end - timedelta(days=14)).replace(hour=0, minute=0, second=0)
    
    print(f"\n{'='*80}")
    print("FETCH FULL 14 DAYS OF DATA")
    print(f"{'='*80}")
    print(f"Date range: {target_start.date()} to {target_end.date()} (14 days)")
    print(f"Timeframe: 1Min")
    if start_symbol:
        print(f"Resuming from symbol: {start_symbol}")
    print(f"{'='*80}\n")
    
    # Get all symbols from database
    symbols = get_symbols_with_data('1Min')
    
    if not symbols:
        print("No symbols found in database. Please add symbols first.")
        return
    
    # Sort symbols to ensure consistent order
    symbols = sorted(symbols)
    
    # Find starting index if start_symbol is provided
    start_index = 0
    if start_symbol:
        start_symbol_upper = start_symbol.upper()
        try:
            start_index = symbols.index(start_symbol_upper)
            print(f"Found {start_symbol_upper} at position {start_index + 1}/{len(symbols)}")
            symbols = symbols[start_index:]
        except ValueError:
            print(f"Warning: Symbol '{start_symbol}' not found in database.")
            print(f"Starting from beginning instead.")
            start_symbol = None
    
    print(f"Found {len(symbols)} symbols in database")
    if start_symbol:
        print(f"Processing {len(symbols)} symbols starting from {start_symbol}")
    else:
        print(f"Fetching complete 14-day data for each symbol...")
    print()
    
    total_bars = 0
    successful = 0
    failed = 0
    start_time = time.time()
    
    for i, symbol in enumerate(symbols, 1):
        actual_index = start_index + i if start_index > 0 else i
        try:
            percent = (actual_index / (start_index + len(symbols))) * 100
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(symbols) - i) / rate if rate > 0 else 0
            
            print(f"[{actual_index}/{start_index + len(symbols)}] ({percent:.1f}%) {symbol}...", end=' ', flush=True)
            
            # Fetch full 14 days
            symbol_start = time.time()
            bars = fetch_bars(symbol, '1Min', target_start, target_end)
            fetch_time = time.time() - symbol_start
            
            if bars:
                # Insert into database (INSERT OR IGNORE handles duplicates)
                inserted = insert_bars_batch(bars)
                total_bars += inserted
                successful += 1
                print(f"✓ {len(bars):,} bars ({inserted:,} new) in {fetch_time:.1f}s | "
                      f"Total: {total_bars:,} | "
                      f"ETA: {remaining/60:.1f}min")
            else:
                failed += 1
                print(f"✗ No data | ETA: {remaining/60:.1f}min")
            
            # Rate limiting
            time.sleep(0.2)
            
        except KeyboardInterrupt:
            print("\n\nInterrupted by user. Stopping...")
            print(f"\nTo resume from {symbol}, run:")
            print(f"  python fetch_full_14_days_resume.py {symbol}")
            break
        except Exception as e:
            failed += 1
            print(f"✗ Error: {e} | ETA: {remaining/60:.1f}min")
            continue
    
    elapsed_total = time.time() - start_time
    
    print(f"\n{'='*80}")
    print("FETCH COMPLETE")
    print(f"{'='*80}")
    print(f"Summary:")
    print(f"  Symbols processed: {len(symbols):,}")
    print(f"  Successful: {successful:,}")
    print(f"  Failed: {failed:,}")
    print(f"  Total bars inserted: {total_bars:,}")
    print(f"  Time elapsed: {elapsed_total/60:.1f} minutes")
    if successful > 0:
        print(f"  Average: {elapsed_total/successful:.2f} seconds per symbol")
    print(f"{'='*80}\n")

if __name__ == '__main__':
    # Get start symbol from command line argument
    start_symbol = None
    if len(sys.argv) > 1:
        start_symbol = sys.argv[1].upper()
    
    print("=" * 80)
    print("Fetch Full 14 Days of 1-Minute Data (Resume Capable)")
    print("=" * 80)
    
    if start_symbol:
        print(f"\nResuming from symbol: {start_symbol}")
    else:
        print("\nThis will fetch complete 14-day data for all symbols in database.")
    print("Existing data will be preserved (duplicates ignored).")
    
    if not start_symbol:
        response = input("\nContinue? (y/n): ").strip().lower()
        if response != 'y':
            print("Cancelled.")
            sys.exit(0)
    
    fetch_full_14_days_resume(start_symbol)
    print("Done!")

