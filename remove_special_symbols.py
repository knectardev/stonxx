"""
Remove preferred shares, warrants, and units from database.
These are often less liquid and harder to trade than regular stocks.
"""
from database import get_connection, get_symbols_with_data

def remove_special_symbols():
    """Remove preferred shares (.PR*), warrants (.WS), and units (.U) from database"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("REMOVE SPECIAL SYMBOLS")
    print("=" * 80)
    print("\nThis will remove:")
    print("  - Preferred shares (symbols with .PR* patterns)")
    print("  - Warrants (symbols ending in .WS)")
    print("  - Units (symbols ending in .U)")
    print("\nThese are often less liquid and harder to trade than regular stocks.\n")
    
    # Get all symbols
    symbols = get_symbols_with_data('1Min')
    print(f"Found {len(symbols)} symbols in database\n")
    
    # Categorize symbols
    preferred_shares = []
    warrants = []
    units = []
    regular_stocks = []
    
    for symbol in symbols:
        if '.WS' in symbol:
            warrants.append(symbol)
        elif '.U' in symbol:
            units.append(symbol)
        elif '.' in symbol:
            # Likely a preferred share (e.g., .PR*, .PRE, .PRA, etc.)
            preferred_shares.append(symbol)
        else:
            regular_stocks.append(symbol)
    
    symbols_to_remove = preferred_shares + warrants + units
    
    print(f"Symbol breakdown:")
    print(f"  Regular stocks: {len(regular_stocks)}")
    print(f"  Preferred shares: {len(preferred_shares)}")
    print(f"  Warrants: {len(warrants)}")
    print(f"  Units: {len(units)}")
    print(f"  Total special symbols to remove: {len(symbols_to_remove)}")
    
    if not symbols_to_remove:
        print("\n✓ No special symbols found. Nothing to remove.")
        conn.close()
        return
    
    print(f"\n{'='*80}")
    print(f"Removing {len(symbols_to_remove)} special symbols")
    print(f"{'='*80}\n")
    
    # Show examples
    if preferred_shares:
        print(f"Preferred shares to remove ({len(preferred_shares)}):")
        print(f"  Examples: {', '.join(preferred_shares[:10])}")
        if len(preferred_shares) > 10:
            print(f"  ... and {len(preferred_shares) - 10} more")
    
    if warrants:
        print(f"\nWarrants to remove ({len(warrants)}):")
        print(f"  {', '.join(warrants)}")
    
    if units:
        print(f"\nUnits to remove ({len(units)}):")
        print(f"  {', '.join(units)}")
    
    # Remove from database
    removed_count = 0
    for symbol in symbols_to_remove:
        cursor.execute('DELETE FROM bars WHERE symbol = ?', (symbol,))
        removed_count += cursor.rowcount
    
    conn.commit()
    
    print(f"\n{'='*80}")
    print("REMOVAL COMPLETE")
    print(f"{'='*80}")
    print(f"Removed {removed_count:,} bar records for {len(symbols_to_remove)} symbols")
    print(f"Regular stocks remaining: {len(regular_stocks)}")
    print(f"{'='*80}\n")
    
    conn.close()

if __name__ == '__main__':
    print("=" * 80)
    print("Remove Preferred Shares, Warrants, and Units")
    print("=" * 80)
    print("\nThis will remove symbols that are less liquid/harder to trade:")
    print("  - Preferred shares (.PR*, .PRE, .PRA, etc.)")
    print("  - Warrants (.WS)")
    print("  - Units (.U)")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
    else:
        remove_special_symbols()
        print("Done!")

