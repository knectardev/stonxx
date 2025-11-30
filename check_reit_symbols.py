"""
Check which REITs are in the database and their average daily volume
"""
from database import get_connection, get_symbols_with_data

# List of low-vol/low-volume REITs to check/remove
REIT_SYMBOLS = [
    'APLE', 'ARR', 'BRT', 'BRSP', 'CIO', 'CLDT', 'COLD', 'DHF', 'DOC',
    'DRH', 'GEO', 'GMN', 'IRT', 'KREF', 'NREF', 'NTST', 'O', 'PEB',
    'PINE', 'RITM', 'STWD'
]

# Normalize to uppercase
REIT_SYMBOLS_SET = {s.upper() for s in REIT_SYMBOLS}

# Get symbols in database
db_symbols = get_symbols_with_data('1Min')
db_symbols_set = set(s.upper() for s in db_symbols)

# Find matches
matches = [s for s in REIT_SYMBOLS_SET if s in db_symbols_set]

print("=" * 80)
print("CHECKING FOR LOW-VOLUME REITs")
print("=" * 80)
print(f"\nTotal REIT symbols in list: {len(REIT_SYMBOLS)}")
print(f"Total symbols in database: {len(db_symbols)}")
print(f"\nREIT symbols found in database: {len(matches)}")
print(f"Percentage of database: {(len(matches)/len(db_symbols)*100):.1f}%")

if matches:
    print(f"\nFound in database: {', '.join(matches)}")
    
    # Check average daily volume for each
    print(f"\n{'='*80}")
    print("Checking average daily volume (from latest trading day)")
    print(f"{'='*80}\n")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    reits_to_remove = []
    reits_to_keep = []
    volume_threshold = 4_000_000  # 4M shares
    
    for symbol in sorted(matches):
        # Get latest trading day's bars to calculate daily volume
        cursor.execute('''
            SELECT SUM(volume) as daily_volume, COUNT(*) as bar_count,
                   datetime(MIN(timestamp), 'unixepoch') as date
            FROM bars
            WHERE symbol = ? AND timeframe = '1Min'
            GROUP BY DATE(datetime(timestamp, 'unixepoch'))
            ORDER BY date DESC
            LIMIT 1
        ''', (symbol,))
        
        row = cursor.fetchone()
        if row and row[0]:
            daily_volume = row[0] or 0
            bar_count = row[1] or 0
            date = row[2] or 'Unknown'
            
            if daily_volume < volume_threshold:
                reits_to_remove.append((symbol, daily_volume, date))
                print(f"  {symbol}: {daily_volume:>12,} volume (REMOVE - below 4M)")
            else:
                reits_to_keep.append((symbol, daily_volume, date))
                print(f"  {symbol}: {daily_volume:>12,} volume (KEEP - above 4M)")
        else:
            # No data or can't calculate
            reits_to_remove.append((symbol, 0, 'No data'))
            print(f"  {symbol}: No volume data (REMOVE)")
    
    conn.close()
    
    print(f"\n{'='*80}")
    print("Summary:")
    print(f"{'='*80}")
    print(f"REITs to remove (<4M volume): {len(reits_to_remove)}")
    print(f"REITs to keep (>4M volume): {len(reits_to_keep)}")
else:
    print("\n✓ No REIT symbols found in database")

print("\n" + "=" * 80)

