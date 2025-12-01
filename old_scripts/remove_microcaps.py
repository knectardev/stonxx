"""
Remove microcaps (<$200M market cap) with no/low volume from the database.
These are tiny companies with minimal trading activity.
"""
from database import get_connection, get_symbols_with_data

# List of microcaps to remove (examples provided)
MICROCAP_SYMBOLS = [
    'CULP', 'LION', 'LOCL', 'MEC', 'MEGI', 'MCS', 'MITT', 'NRDY', 'NTZ',
    'OPAD', 'SHCO', 'SOUL', 'STEM', 'TBI', 'VFRC', 'XIFR', 'YALA', 'ZVIA'
]

# Normalize to uppercase set for fast lookup
MICROCAP_SYMBOLS_SET = {s.upper() for s in MICROCAP_SYMBOLS}

def remove_microcaps():
    """Remove microcaps from database"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("REMOVE MICROCAPS (<$200M)")
    print("=" * 80)
    print("\nWhy remove microcaps?")
    print("  - Tiny companies (<$200M market cap)")
    print("  - No trading activity")
    print("  - Very low liquidity")
    print("  - Not suitable for trading")
    
    # Get all symbols in database
    symbols = get_symbols_with_data('1Min')
    print(f"\nFound {len(symbols)} symbols in database")
    
    # Find microcaps in database
    microcaps_in_db = [s for s in symbols if s.upper() in MICROCAP_SYMBOLS_SET]
    
    print(f"\nMicrocap symbols to remove: {len(microcaps_in_db)}")
    print(f"This represents {(len(microcaps_in_db)/len(symbols)*100):.1f}% of your database")
    
    if not microcaps_in_db:
        print("\n✓ No microcap symbols found. Nothing to remove.")
        conn.close()
        return
    
    # Show symbols to remove with volume info
    print(f"\n{'='*80}")
    print("Microcaps to remove:")
    print(f"{'='*80}\n")
    
    for symbol in sorted(microcaps_in_db):
        # Check volume for reference
        cursor.execute('''
            SELECT SUM(volume) as daily_volume
            FROM bars
            WHERE symbol = ? AND timeframe = '1Min'
            GROUP BY DATE(datetime(timestamp, 'unixepoch'))
            ORDER BY DATE(datetime(timestamp, 'unixepoch')) DESC
            LIMIT 1
        ''', (symbol,))
        
        row = cursor.fetchone()
        volume = row[0] if row and row[0] else 0
        volume_str = f"{volume:,}" if volume > 0 else "No volume"
        print(f"  {symbol}: {volume_str} daily volume")
    
    # Remove from database
    print(f"\n{'='*80}")
    print(f"Removing {len(microcaps_in_db)} microcap symbols")
    print(f"{'='*80}\n")
    
    removed_count = 0
    for symbol in microcaps_in_db:
        cursor.execute('DELETE FROM bars WHERE symbol = ?', (symbol,))
        removed_count += cursor.rowcount
    
    conn.commit()
    
    # Get remaining count
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM bars WHERE timeframe = "1Min"')
    remaining_symbols = cursor.fetchone()[0]
    
    print(f"{'='*80}")
    print("REMOVAL COMPLETE")
    print(f"{'='*80}")
    print(f"Removed {removed_count:,} bar records for {len(microcaps_in_db)} symbols")
    print(f"Regular stocks remaining: {remaining_symbols}")
    print(f"Removed {len(microcaps_in_db)} microcaps ({(len(microcaps_in_db)/(len(microcaps_in_db)+remaining_symbols)*100):.1f}% of total)")
    print(f"{'='*80}\n")
    
    conn.close()

if __name__ == '__main__':
    print("=" * 80)
    print("Remove Microcaps (<$200M)")
    print("=" * 80)
    print("\nThis will remove microcap companies that are:")
    print("  - Tiny companies (<$200M market cap)")
    print("  - Have no trading activity")
    print("  - Very low liquidity")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
    else:
        remove_microcaps()
        print("Done!")

