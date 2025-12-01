"""
Check which low-data symbols are in the database
"""
from database import get_symbols_with_data, get_bar_count

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

# Get symbols in database
db_symbols = get_symbols_with_data('1Min')
db_symbols_set = set(s.upper() for s in db_symbols)

# Find matches
matches = [s for s in LOW_DATA_SYMBOLS_SET if s in db_symbols_set]

print("=" * 80)
print("CHECKING FOR LOW-DATA SYMBOLS")
print("=" * 80)
print(f"\nTotal low-data symbols in list: {len(LOW_DATA_SYMBOLS)}")
print(f"Total symbols in database: {len(db_symbols)}")
print(f"\nLow-data symbols found in database: {len(matches)}")
print(f"Percentage of database: {(len(matches)/len(db_symbols)*100):.1f}%")

if matches:
    print(f"\nFound in database:")
    # Show symbols with their bar counts
    print(f"\n{'Symbol':<10} {'Bar Count':<15} {'Reason'}")
    print("-" * 80)
    for symbol in sorted(matches):
        bar_count = get_bar_count(symbol, '1Min')
        print(f"{symbol:<10} {bar_count:<15,} Insufficient 1-minute data")
    
    print(f"\n{'='*80}")
    print(f"Total to remove: {len(matches)} symbols")
else:
    print("\n✓ No low-data symbols found in database")

print("\n" + "=" * 80)

