"""
Remove ADRs (American Depositary Receipts) from the database.
ADRs often have terrible volume, show fake premarket gaps, and are dangerous to scalp.
"""
from database import get_connection, get_symbols_with_data

# List of ADRs to remove
ADR_SYMBOLS = [
    'ABEV', 'AEG', 'AGRO', 'ALTG', 'AMTD', 'ARCO', 'ASX', 'BCS', 'BBAR', 'BBD',
    'BBDO', 'BEPH', 'BEPI', 'BFK', 'CANG', 'CCU', 'CEE', 'CEPU', 'CIG', 'CIG.C',
    'CNO', 'CYH', 'DAO', 'EB', 'EC', 'EBS', 'EEX', 'EGY', 'ELP', 'ENIC',
    'ENR', 'EOI', 'GGB', 'GENI', 'GEO', 'GPK', 'GPRK', 'HLX', 'HMY', 'HRTG',
    'IBN', 'ICL', 'INFY', 'IRS', 'IAG', 'ITUB', 'JBS', 'KEP', 'KOS', 'KT',
    'KVUE', 'LND', 'LPL', 'LU', 'MANU', 'MFG', 'MIC', 'MNSO', 'MUFG', 'MX',
    'NEXA', 'NHY', 'NIO', 'NMR', 'NOK', 'NOMD', 'NOTES', 'OEC', 'OGN', 'PAGS',
    'PBR', 'PBR.A', 'PBT', 'PL', 'PUJ', 'QD', 'QS', 'RLX', 'RERE', 'SBSW',
    'SID', 'SOJE', 'SOL', 'TAC', 'TAK', 'TEF', 'TEO', 'TK', 'TKC', 'TU',
    'TME', 'TV', 'UTZ', 'VALE', 'VET', 'VIPS', 'VIV', 'WIT', 'YMM', 'YRD',
    'YSG', 'ZH'
]

# Normalize to uppercase set for fast lookup
ADR_SYMBOLS_SET = {s.upper() for s in ADR_SYMBOLS}

def remove_adrs():
    """Remove ADRs from database"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("REMOVE ADRs (AMERICAN DEPOSITARY RECEIPTS)")
    print("=" * 80)
    print("\nWhy remove ADRs?")
    print("  - Low liquidity (terrible volume)")
    print("  - Show fake premarket gaps")
    print("  - Dangerous to scalp")
    print("  - Behave differently than US equities")
    
    # Get all symbols in database
    symbols = get_symbols_with_data('1Min')
    print(f"\nFound {len(symbols)} symbols in database")
    
    # Find ADRs in database
    adrs_in_db = [s for s in symbols if s.upper() in ADR_SYMBOLS_SET]
    
    print(f"\nADR symbols to remove: {len(adrs_in_db)}")
    print(f"This represents {(len(adrs_in_db)/len(symbols)*100):.1f}% of your database")
    
    if not adrs_in_db:
        print("\n✓ No ADR symbols found. Nothing to remove.")
        conn.close()
        return
    
    # Show breakdown
    print(f"\n{'='*80}")
    print("Symbols to remove (first 30):")
    print(f"{'='*80}")
    for i in range(0, min(30, len(adrs_in_db)), 10):
        print(f"  {', '.join(adrs_in_db[i:i+10])}")
    if len(adrs_in_db) > 30:
        print(f"  ... and {len(adrs_in_db) - 30} more")
    
    # Check if MANU is in the list (was going to resume from there)
    if 'MANU' in adrs_in_db:
        print(f"\n⚠ Note: MANU is in this list - will need to resume data fetch from a different symbol")
    
    # Remove from database
    print(f"\n{'='*80}")
    print(f"Removing {len(adrs_in_db)} ADR symbols")
    print(f"{'='*80}\n")
    
    removed_count = 0
    for symbol in adrs_in_db:
        cursor.execute('DELETE FROM bars WHERE symbol = ?', (symbol,))
        removed_count += cursor.rowcount
    
    conn.commit()
    
    # Get remaining count
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM bars WHERE timeframe = "1Min"')
    remaining_symbols = cursor.fetchone()[0]
    
    print(f"{'='*80}")
    print("REMOVAL COMPLETE")
    print(f"{'='*80}")
    print(f"Removed {removed_count:,} bar records for {len(adrs_in_db)} symbols")
    print(f"Regular stocks remaining: {remaining_symbols}")
    print(f"Removed {len(adrs_in_db)} ADRs ({(len(adrs_in_db)/(len(adrs_in_db)+remaining_symbols)*100):.1f}% of total)")
    print(f"{'='*80}\n")
    
    conn.close()

if __name__ == '__main__':
    print("=" * 80)
    print("Remove ADRs (American Depositary Receipts)")
    print("=" * 80)
    print("\nThis will remove ~90 ADR symbols that are:")
    print("  - Low liquidity (terrible volume)")
    print("  - Show fake premarket gaps")
    print("  - Dangerous to scalp")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
    else:
        remove_adrs()
        print("Done!")

