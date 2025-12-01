"""
Fetch complete 14 days of 1-minute data for all symbols currently in database.
This will fill any gaps automatically since the database uses INSERT OR IGNORE.
"""
from fetch_historical_data import fetch_bars
from database import init_database, insert_bars_batch, get_symbols_with_data
from datetime import datetime, timedelta
import time

def fetch_full_14_days():
    """Fetch complete 14 days of data for all symbols in database"""
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
    print(f"{'='*80}\n")
    
    # Get all symbols from database
    symbols = get_symbols_with_data('1Min')
    
    if not symbols:
        print("No symbols found in database. Please add symbols first.")
        return
    
    print(f"Found {len(symbols)} symbols in database")
    print(f"Fetching complete 14-day data for each symbol...\n")
    
    total_bars = 0
    successful = 0
    failed = 0
    start_time = time.time()
    
    for i, symbol in enumerate(symbols, 1):
        try:
            percent = (i / len(symbols)) * 100
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(symbols) - i) / rate if rate > 0 else 0
            
            print(f"[{i}/{len(symbols)}] ({percent:.1f}%) {symbol}...", end=' ', flush=True)
            
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
    print(f"  Average: {elapsed_total/len(symbols):.2f} seconds per symbol")
    print(f"{'='*80}\n")

if __name__ == '__main__':
    print("=" * 80)
    print("Fetch Full 14 Days of 1-Minute Data")
    print("=" * 80)
    print("\nThis will fetch complete 14-day data for all symbols in database.")
    print("Existing data will be preserved (duplicates ignored).")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
    else:
        fetch_full_14_days()
        print("Done!")

