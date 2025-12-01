"""
Check microcap symbols in the database and their volume
"""
from database import get_connection, get_symbols_with_data

# List of microcap examples to check/remove
MICROCAP_SYMBOLS = [
    'CULP', 'LION', 'LOCL', 'MEC', 'MEGI', 'MCS', 'MITT', 'NRDY', 'NTZ',
    'OPAD', 'SHCO', 'SOUL', 'STEM', 'TBI', 'VFRC', 'XIFR', 'YALA', 'ZVIA'
]

# Normalize to uppercase
MICROCAP_SYMBOLS_SET = {s.upper() for s in MICROCAP_SYMBOLS}

# Get symbols in database
db_symbols = get_symbols_with_data('1Min')
db_symbols_set = set(s.upper() for s in db_symbols)

# Find matches
matches = [s for s in MICROCAP_SYMBOLS_SET if s in db_symbols_set]

print("=" * 80)
print("CHECKING FOR MICROCAPS")
print("=" * 80)
print(f"\nTotal microcap symbols in list: {len(MICROCAP_SYMBOLS)}")
print(f"Total symbols in database: {len(db_symbols)}")
print(f"\nMicrocap symbols found in database: {len(matches)}")
print(f"Percentage of database: {(len(matches)/len(db_symbols)*100):.1f}%")

if matches:
    print(f"\nFound in database: {', '.join(matches)}")
    
    # Check volume for each
    print(f"\n{'='*80}")
    print("Checking daily volume (latest trading day)")
    print(f"{'='*80}\n")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    microcaps_to_remove = []
    microcaps_to_keep = []
    
    for symbol in sorted(matches):
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
            
            if daily_volume == 0 or daily_volume < 10000:  # No volume or very low
                microcaps_to_remove.append((symbol, daily_volume))
                print(f"  {symbol}: {daily_volume:>12,} volume (REMOVE - no/low volume)")
            else:
                microcaps_to_keep.append((symbol, daily_volume))
                print(f"  {symbol}: {daily_volume:>12,} volume (KEEP - has volume)")
        else:
            # No data
            microcaps_to_remove.append((symbol, 0))
            print(f"  {symbol}: No volume data (REMOVE)")
    
    conn.close()
    
    print(f"\n{'='*80}")
    print("Summary:")
    print(f"{'='*80}")
    print(f"Microcaps to remove (no/low volume): {len(microcaps_to_remove)}")
    print(f"Microcaps to keep (has volume): {len(microcaps_to_keep)}")
else:
    print("\n✓ No microcap symbols found in database")

print("\n" + "=" * 80)

