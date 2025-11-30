"""
Remove low-volume REITs (Real Estate Investment Trusts) from the database.
REITs often behave like bonds and have extremely low volatility/volume.
Remove those with <4M daily volume.
"""
from database import get_connection, get_symbols_with_data

# List of REITs to check (will remove if <4M daily volume)
REIT_SYMBOLS = [
    'APLE', 'ARR', 'BRT', 'BRSP', 'CIO', 'CLDT', 'COLD', 'DHF', 'DOC',
    'DRH', 'GEO', 'GMN', 'IRT', 'KREF', 'NREF', 'NTST', 'O', 'PEB',
    'PINE', 'RITM', 'STWD'
]

# Normalize to uppercase set for fast lookup
REIT_SYMBOLS_SET = {s.upper() for s in REIT_SYMBOLS}

def remove_low_volume_reits():
    """Remove REITs with low volume (<4M daily) from database"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("REMOVE LOW-VOLUME REITs")
    print("=" * 80)
    print("\nWhy remove low-volume REITs?")
    print("  - Behave like bonds")
    print("  - Extremely low volatility")
    print("  - Low volume (hard to trade)")
    print("  - Not suitable for active trading")
    print("\nRemoving REITs with <4M daily volume")
    
    # Get all symbols in database
    symbols = get_symbols_with_data('1Min')
    print(f"\nFound {len(symbols)} symbols in database")
    
    # Find REITs in database
    reits_in_db = [s for s in symbols if s.upper() in REIT_SYMBOLS_SET]
    
    print(f"\nREIT symbols found: {len(reits_in_db)}")
    
    if not reits_in_db:
        print("\n✓ No REIT symbols found. Nothing to remove.")
        conn.close()
        return
    
    # Check volume and decide which to remove
    volume_threshold = 4_000_000  # 4M shares
    reits_to_remove = []
    reits_to_keep = []
    
    print(f"\n{'='*80}")
    print("Checking daily volume (latest trading day)")
    print(f"{'='*80}\n")
    
    for symbol in sorted(reits_in_db):
        # Get latest trading day's total volume
        cursor.execute('''
            SELECT SUM(volume) as daily_volume, COUNT(*) as bar_count,
                   DATE(datetime(MIN(timestamp), 'unixepoch')) as date
            FROM bars
            WHERE symbol = ? AND timeframe = '1Min'
            GROUP BY DATE(datetime(timestamp, 'unixepoch'))
            ORDER BY date DESC
            LIMIT 1
        ''', (symbol,))
        
        row = cursor.fetchone()
        if row and row[0]:
            daily_volume = row[0] or 0
            
            if daily_volume < volume_threshold:
                reits_to_remove.append((symbol, daily_volume))
                print(f"  {symbol}: {daily_volume:>12,} volume - REMOVE")
            else:
                reits_to_keep.append((symbol, daily_volume))
                print(f"  {symbol}: {daily_volume:>12,} volume - KEEP (above 4M)")
        else:
            # No data - remove it
            reits_to_remove.append((symbol, 0))
            print(f"  {symbol}: No volume data - REMOVE")
    
    if not reits_to_remove:
        print("\n✓ All REITs have sufficient volume. Nothing to remove.")
        conn.close()
        return
    
    # Remove from database
    print(f"\n{'='*80}")
    print(f"Removing {len(reits_to_remove)} low-volume REITs")
    print(f"{'='*80}\n")
    
    removed_count = 0
    for symbol, volume in reits_to_remove:
        cursor.execute('DELETE FROM bars WHERE symbol = ?', (symbol,))
        removed_count += cursor.rowcount
    
    conn.commit()
    
    # Get remaining count
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM bars WHERE timeframe = "1Min"')
    remaining_symbols = cursor.fetchone()[0]
    
    print(f"{'='*80}")
    print("REMOVAL COMPLETE")
    print(f"{'='*80}")
    print(f"Removed {removed_count:,} bar records for {len(reits_to_remove)} REITs")
    if reits_to_keep:
        print(f"Kept {len(reits_to_keep)} REITs with sufficient volume (>4M)")
    print(f"Regular stocks remaining: {remaining_symbols}")
    print(f"{'='*80}\n")
    
    conn.close()

if __name__ == '__main__':
    print("=" * 80)
    print("Remove Low-Volume REITs")
    print("=" * 80)
    print("\nThis will remove REITs with <4M daily volume that:")
    print("  - Behave like bonds")
    print("  - Have extremely low volatility")
    print("  - Have low volume")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
    else:
        remove_low_volume_reits()
        print("Done!")

