"""
Remove SPAC (Special Purpose Acquisition Company) remnants from the database.
SPAC shells often have no liquidity or volume.
"""
from database import get_connection, get_symbols_with_data

# List of SPAC remnants to remove
SPAC_SYMBOLS = [
    'AXIA', 'AEXA', 'BNH', 'BNJ', 'BRSL', 'CINT', 'CMDB', 'GCTS', 'GCTD',
    'RBOT', 'PERF', 'SHCO', 'YALA', 'ZVIA'
]

# Normalize to uppercase set for fast lookup
SPAC_SYMBOLS_SET = {s.upper() for s in SPAC_SYMBOLS}

def remove_spac_remnants():
    """Remove SPAC remnants from database"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("REMOVE SPAC REMNANTS")
    print("=" * 80)
    print("\nWhy remove SPAC remnants?")
    print("  - SPAC shells with no liquidity")
    print("  - Little to no volume")
    print("  - Not suitable for trading")
    
    # Get all symbols in database
    symbols = get_symbols_with_data('1Min')
    print(f"\nFound {len(symbols)} symbols in database")
    
    # Find SPACs in database
    spacs_in_db = [s for s in symbols if s.upper() in SPAC_SYMBOLS_SET]
    
    print(f"\nSPAC remnant symbols to remove: {len(spacs_in_db)}")
    print(f"This represents {(len(spacs_in_db)/len(symbols)*100):.1f}% of your database")
    
    if not spacs_in_db:
        print("\n✓ No SPAC remnant symbols found. Nothing to remove.")
        conn.close()
        return
    
    # Show symbols to remove
    print(f"\n{'='*80}")
    print("Symbols to remove:")
    print(f"{'='*80}")
    print(f"  {', '.join(spacs_in_db)}")
    
    # Remove from database
    print(f"\n{'='*80}")
    print(f"Removing {len(spacs_in_db)} SPAC remnant symbols")
    print(f"{'='*80}\n")
    
    removed_count = 0
    for symbol in spacs_in_db:
        cursor.execute('DELETE FROM bars WHERE symbol = ?', (symbol,))
        removed_count += cursor.rowcount
    
    conn.commit()
    
    # Get remaining count
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM bars WHERE timeframe = "1Min"')
    remaining_symbols = cursor.fetchone()[0]
    
    print(f"{'='*80}")
    print("REMOVAL COMPLETE")
    print(f"{'='*80}")
    print(f"Removed {removed_count:,} bar records for {len(spacs_in_db)} symbols")
    print(f"Regular stocks remaining: {remaining_symbols}")
    print(f"Removed {len(spacs_in_db)} SPAC remnants ({(len(spacs_in_db)/(len(spacs_in_db)+remaining_symbols)*100):.1f}% of total)")
    print(f"{'='*80}\n")
    
    conn.close()

if __name__ == '__main__':
    print("=" * 80)
    print("Remove SPAC Remnants")
    print("=" * 80)
    print("\nThis will remove SPAC shell remnants that have:")
    print("  - No liquidity")
    print("  - Little to no volume")
    print("  - Not suitable for trading")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
    else:
        remove_spac_remnants()
        print("Done!")

