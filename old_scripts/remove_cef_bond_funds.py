"""
Remove Closed-End Funds (CEFs) and Bond Funds from the database.
These trade thin, barely move intraday, and behave nothing like equities.
"""
from database import get_connection, get_symbols_with_data

# List of CEFs and Bond Funds to remove (from ChatGPT's recommendation)
CEF_SYMBOLS = [
    'ACP', 'AOD', 'ARDC', 'ARR', 'AWF', 'AWP', 'BBDC', 'BBN', 'BCAT', 'BCSF',
    'BCX', 'BDJ', 'BGB', 'BGH', 'BGR', 'BGT', 'BGX', 'BGY', 'BHK', 'BKN',
    'BKT', 'BLW', 'BMEZ', 'BOE', 'BRW', 'BSL', 'BSM', 'BTA', 'BTZ', 'BWG',
    'CCF', 'CGO', 'CHMI', 'CHCT', 'CIF', 'CLDT', 'CMU', 'CNF', 'CNS', 'CXE',
    'CXH', 'DHF', 'DLY', 'DMA', 'DMB', 'DMO', 'DNP', 'DPG', 'DSL', 'DSM',
    'DSU', 'EARN', 'ECC', 'EFR', 'EFT', 'EGY', 'EHI', 'EIC', 'EMD', 'EMF',
    'EOD', 'EOI', 'EOT', 'ETB', 'ETJ', 'ETV', 'ETW', 'ETX', 'ETY', 'EVF',
    'EVG', 'EVH', 'EVN', 'EXG', 'FCT', 'FFC', 'FLC', 'FMN', 'FOF', 'FPF',
    'FRA', 'FT', 'FTHY', 'FXED', 'GAB', 'GBAB', 'GDL', 'GDO', 'GF', 'GHY',
    'GUT', 'GYLD', 'HFRO', 'HIO', 'HIX', 'HYI', 'HYT', 'IAE', 'IFN', 'IGA',
    'IGD', 'IGI', 'IGR', 'IHD', 'IIM', 'IQI', 'IRR', 'ISD', 'JCE', 'JOF',
    'JPC', 'JQC', 'JRI', 'JRS', 'JFR', 'JGH', 'JHI', 'JLS', 'KIO', 'KTF',
    'KYN', 'LFT', 'LGI', 'MHD', 'MHF', 'MHN', 'MIN', 'MMD', 'MMT', 'MMU',
    'MNR', 'MVO', 'MVT', 'MVF', 'MYD', 'MYI', 'MYN', 'NAC', 'NAD', 'NAN',
    'NAZ', 'NBB', 'NCA', 'NCV', 'NCZ', 'NDMO', 'NEA', 'NFJ', 'NGL', 'NIM',
    'NIQ', 'NKX', 'NMS', 'NMT', 'NMZ', 'NNY', 'NPV', 'NQP', 'NVG', 'NXC',
    'NXDR', 'NXDT.PRA', 'NXJ', 'NXN', 'NZF', 'PCF', 'PCM', 'PCN', 'PCQ',
    'PDI', 'PDO', 'PDT', 'PFD', 'PFH', 'PFL', 'PFLT', 'PFN', 'PFO', 'PGP',
    'PGZ', 'PHK', 'PIM', 'PML', 'PMM', 'PMO', 'PPT', 'PTY', 'QVC', 'QVCD',
    'RA', 'RCS', 'RFI', 'RFM', 'RFMZ', 'RGT', 'RMT', 'RQI', 'RSF', 'RVT',
    'SBI', 'SCD', 'SCE.PRG', 'SCE.PRL', 'SDHF', 'SDHY', 'THQ', 'THW', 'TDF',
    'TEI', 'USA', 'USB.PRH', 'USB.PRQ', 'USB.PRR', 'USB.PRS', 'UZE', 'VCV',
    'VBF', 'VG', 'VLN', 'VLT', 'VMO', 'VNO.PRL', 'VNO.PRM', 'VNO.PRN', 'VNO.PRO',
    'VPV', 'VVR', 'WEA', 'WIA', 'WIW', 'XFLT', 'ZTR'
]

# Normalize to uppercase set for fast lookup
CEF_SYMBOLS_SET = {s.upper() for s in CEF_SYMBOLS}

def remove_cef_bond_funds():
    """Remove CEFs and Bond Funds from database"""
    conn = get_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("REMOVE CLOSED-END FUNDS (CEFs) AND BOND FUNDS")
    print("=" * 80)
    print("\nWhy remove these?")
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
    
    print(f"\nCEF/Bond Fund symbols to remove: {len(cefs_in_db)}")
    print(f"This represents {(len(cefs_in_db)/len(symbols)*100):.1f}% of your database")
    
    if not cefs_in_db:
        print("\n✓ No CEF/Bond Fund symbols found. Nothing to remove.")
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
    print(f"Removing {len(cefs_in_db)} CEF/Bond Fund symbols")
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
    print(f"Removed {len(cefs_in_db)} CEFs/Bond Funds ({(len(cefs_in_db)/(len(cefs_in_db)+remaining_symbols)*100):.1f}% of total)")
    print(f"{'='*80}\n")
    
    conn.close()

if __name__ == '__main__':
    print("=" * 80)
    print("Remove Closed-End Funds (CEFs) and Bond Funds")
    print("=" * 80)
    print("\nThis will remove 215+ symbols that are:")
    print("  - Closed-End Funds (CEFs)")
    print("  - Bond Funds")
    print("  - Low liquidity / thin trading")
    print("  - Minimal intraday movement")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
    else:
        remove_cef_bond_funds()
        print("Done!")

