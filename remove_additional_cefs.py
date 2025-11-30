"""
Remove additional CEFs and Bond Funds from the database.
"""
from database import get_connection, get_symbols_with_data

# Additional list of CEFs and Bond Funds to remove
CEF_SYMBOLS = [
    'AFB', 'AFGC', 'AFGE', 'AGD', 'BFZ', 'BLE', 'BNY', 'BYM', 'CAF', 'CDE',
    'CMTG', 'DBL', 'DFH', 'DIAX', 'DLNG', 'EDD', 'EDF', 'ECAT', 'EFC', 'EFXT',
    'ELME', 'EPC', 'EQS', 'ESRT', 'FCRS', 'FF', 'FFWM', 'FPH', 'FPI', 'FSCO',
    'FSK', 'FSM', 'FSSL', 'GGT', 'GGZ', 'GHY', 'GNT', 'GOF', 'GRX', 'GSBD',
    'GUG', 'HAFN', 'HEQ', 'HGLB', 'HPF', 'HPI', 'HPS', 'HQH', 'HQL', 'IH',
    'IHS', 'INN', 'INR', 'IVR', 'JACS', 'JBGS', 'LZM', 'MFM', 'MGF', 'MGRB',
    'MGRD', 'MH', 'MIY', 'MLP', 'MPA', 'MQT', 'MQY', 'MSD', 'MSDL', 'MSIF',
    'MUA', 'MUC', 'MUE', 'MUJ', 'MXF', 'NBXG', 'NCDL', 'NMAI', 'NMAX', 'NMCO',
    'NPCT', 'NPFD', 'NPKI', 'NPWR', 'NRGV', 'NRK', 'NXDT', 'NXE', 'PNI', 'PSTL',
    'PTA', 'PVL', 'QXO', 'QVCC', 'RFL', 'RMI', 'RMM', 'RMMZ', 'RSI', 'RWT',
    'SB', 'SBDS', 'SBXD', 'SBI', 'SCD', 'SF', 'SST', 'STEW', 'STLA', 'STUB',
    'SUPV', 'SVV', 'SWZ', 'TIC', 'TPVG', 'TSI', 'TSQ', 'TTAM', 'UTZ', 'VEL',
    'VGI', 'VGM', 'VKQ', 'VRE', 'VSH', 'VTN', 'VYX', 'WDS', 'WDI', 'WEAV',
    'WHG', 'WLKP', 'WSR', 'XZO', 'YCY'
]

# Normalize to uppercase set for fast lookup
CEF_SYMBOLS_SET = {s.upper() for s in CEF_SYMBOLS}

def remove_additional_cefs():
    """Remove additional CEFs and Bond Funds from database"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("REMOVE ADDITIONAL CEFs AND BOND FUNDS")
    print("=" * 80)
    print("\nWhy remove CEFs/Bond Funds?")
    print("  - Trade thin (low liquidity)")
    print("  - Barely move intraday")
    print("  - Behave nothing like equities")
    print("  - Clutter your screener")
    print("  - Waste API calls")
    
    # Get all symbols in database
    symbols = get_symbols_with_data('1Min')
    print(f"\nFound {len(symbols)} symbols in database")
    
    # Find CEFs in database
    cefs_in_db = [s for s in symbols if s.upper() in CEF_SYMBOLS_SET]
    
    print(f"\nAdditional CEF/Bond Fund symbols to remove: {len(cefs_in_db)}")
    print(f"This represents {(len(cefs_in_db)/len(symbols)*100):.1f}% of your database")
    
    if not cefs_in_db:
        print("\n✓ No additional CEF/Bond Fund symbols found. Nothing to remove.")
        conn.close()
        return
    
    # Show breakdown
    print(f"\n{'='*80}")
    print("Symbols to remove (first 30):")
    print(f"{'='*80}")
    for i in range(0, min(30, len(cefs_in_db)), 10):
        print(f"  {', '.join(cefs_in_db[i:i+10])}")
    if len(cefs_in_db) > 30:
        print(f"  ... and {len(cefs_in_db) - 30} more")
    
    # Remove from database
    print(f"\n{'='*80}")
    print(f"Removing {len(cefs_in_db)} additional CEF/Bond Fund symbols")
    print(f"{'='*80}\n")
    
    removed_count = 0
    for symbol in cefs_in_db:
        cursor.execute('DELETE FROM bars WHERE symbol = ?', (symbol,))
        removed_count += cursor.rowcount
    
    conn.commit()
    
    # Get remaining count
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM bars WHERE timeframe = "1Min"')
    remaining_symbols = cursor.fetchone()[0]
    
    print(f"{'='*80}")
    print("REMOVAL COMPLETE")
    print(f"{'='*80}")
    print(f"Removed {removed_count:,} bar records for {len(cefs_in_db)} symbols")
    print(f"Regular stocks remaining: {remaining_symbols}")
    print(f"Removed {len(cefs_in_db)} additional CEFs/Bond Funds ({(len(cefs_in_db)/(len(cefs_in_db)+remaining_symbols)*100):.1f}% of total)")
    print(f"{'='*80}\n")
    
    conn.close()

if __name__ == '__main__':
    print("=" * 80)
    print("Remove Additional CEFs and Bond Funds")
    print("=" * 80)
    print("\nThis will remove 130+ additional CEF/Bond Fund symbols that:")
    print("  - Trade thin (low liquidity)")
    print("  - Barely move intraday")
    print("  - Behave nothing like equities")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
    else:
        remove_additional_cefs()
        print("Done!")

