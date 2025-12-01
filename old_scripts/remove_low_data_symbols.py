"""
Remove symbols with insufficient 1-minute data from the database.
"""
from database import get_connection, get_symbols_with_data, get_bar_count

# List of symbols with insufficient 1-minute data
LOW_DATA_SYMBOLS = [
    'PDCC', 'SRL', 'BCSS', 'BIPH', 'STG', 'VOC', 'CPAC', 'DTB', 'AIIA', 'BIPI',
    'CTDD', 'FTW', 'HBB', 'MPX', 'GDIV', 'SABA', 'GHG', 'WBX', 'BWMX', 'CFND',
    'KKRS', 'NUW', 'NL', 'SGU', 'CTBB', 'MDV', 'NMI', 'TNGY', 'CMCM', 'SOS',
    'AVAL', 'IDE', 'KORE', 'RLTY', 'KFS', 'MCN', 'MCR', 'BGSF', 'AIZN', 'DTG',
    'CATO', 'CRT', 'AHT', 'SPMC', 'SPXX', 'FINS', 'CLPR', 'SPE', 'TGE', 'CBAN',
    'GROV', 'UFI', 'OPP', 'LVWR', 'SPH', 'NRT', 'BXMX', 'TLYS', 'BALY', 'SKLZ',
    'JILL', 'PSBD', 'SLAI', 'CCIF', 'CLCO', 'ALUR', 'ASIC', 'NXP', 'SKIL', 'TRC',
    'AVBC', 'SMHI', 'SDHC', 'CVLG', 'AP', 'TRAK', 'GFR', 'SI', 'PKST', 'PKE',
    'PAXS', 'NOTE', 'CIA', 'OOMA', 'SPRU', 'NOAH', 'RIV', 'EBF', 'LXFR', 'SCM',
    'RNGR', 'GNE', 'AVK', 'WDH', 'USNA', 'AUNA', 'AMWL', 'NPB', 'GHI', 'BOC',
    'OIA', 'HGTY', 'HSHP', 'EAF', 'SRI', 'RPT', 'SRG', 'KNOP', 'SKYH', 'TG',
    'MED', 'XYF', 'CLW', 'RMAX', 'CMP', 'FVR', 'AOMR', 'GPMT', 'NUV', 'AERO',
    'FLOC', 'MYE', 'OWLT', 'ASG', 'SQNS', 'QUAD', 'MG', 'CURV', 'CTO', 'BHR',
    'NOA', 'DHX', 'SJT', 'ONL', 'MTUS', 'ORN'
]

# Normalize to uppercase set for fast lookup
LOW_DATA_SYMBOLS_SET = {s.upper() for s in LOW_DATA_SYMBOLS}

def remove_low_data_symbols():
    """Remove symbols with insufficient 1-minute data"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("REMOVE LOW-DATA SYMBOLS")
    print("=" * 80)
    print("\nWhy remove these?")
    print("  - Insufficient 1-minute ticker data")
    print("  - Not enough data points for reliable trading analysis")
    print("  - Low liquidity / trading activity")
    
    # Get all symbols in database
    symbols = get_symbols_with_data('1Min')
    print(f"\nFound {len(symbols)} symbols in database")
    
    # Find low-data symbols in database
    low_data_in_db = [s for s in symbols if s.upper() in LOW_DATA_SYMBOLS_SET]
    
    print(f"\nLow-data symbols to remove: {len(low_data_in_db)}")
    print(f"This represents {(len(low_data_in_db)/len(symbols)*100):.1f}% of your database")
    
    if not low_data_in_db:
        print("\n✓ No low-data symbols found. Nothing to remove.")
        conn.close()
        return
    
    # Show breakdown
    print(f"\n{'='*80}")
    print("Symbols to remove (first 20 with bar counts):")
    print(f"{'='*80}")
    for symbol in sorted(low_data_in_db)[:20]:
        bar_count = get_bar_count(symbol, '1Min')
        print(f"  {symbol}: {bar_count:,} bars")
    if len(low_data_in_db) > 20:
        print(f"  ... and {len(low_data_in_db) - 20} more")
    
    # Remove from database
    print(f"\n{'='*80}")
    print(f"Removing {len(low_data_in_db)} low-data symbols")
    print(f"{'='*80}\n")
    
    removed_count = 0
    for symbol in low_data_in_db:
        cursor.execute('DELETE FROM bars WHERE symbol = ?', (symbol,))
        removed_count += cursor.rowcount
    
    conn.commit()
    
    # Get remaining count
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM bars WHERE timeframe = "1Min"')
    remaining_symbols = cursor.fetchone()[0]
    
    print(f"{'='*80}")
    print("REMOVAL COMPLETE")
    print(f"{'='*80}")
    print(f"Removed {removed_count:,} bar records for {len(low_data_in_db)} symbols")
    print(f"Regular stocks remaining: {remaining_symbols}")
    print(f"Removed {len(low_data_in_db)} low-data symbols ({(len(low_data_in_db)/(len(low_data_in_db)+remaining_symbols)*100):.1f}% of total)")
    print(f"{'='*80}\n")
    
    conn.close()

if __name__ == '__main__':
    print("=" * 80)
    print("Remove Low-Data Symbols")
    print("=" * 80)
    print("\nThis will remove 136 symbols with insufficient 1-minute ticker data.")
    print("These symbols don't have enough data points for reliable trading.")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
    else:
        remove_low_data_symbols()
        print("Done!")

