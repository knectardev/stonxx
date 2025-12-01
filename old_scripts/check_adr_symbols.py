"""
Check how many ADR (American Depositary Receipt) symbols are in the database
"""
from database import get_symbols_with_data

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

# Normalize to uppercase
ADR_SYMBOLS_SET = {s.upper() for s in ADR_SYMBOLS}

# Get symbols in database
db_symbols = get_symbols_with_data('1Min')
db_symbols_set = set(s.upper() for s in db_symbols)

# Find matches
matches = [s for s in ADR_SYMBOLS_SET if s in db_symbols_set]

print("=" * 80)
print("CHECKING FOR ADRs (AMERICAN DEPOSITARY RECEIPTS)")
print("=" * 80)
print(f"\nTotal ADR symbols in list: {len(ADR_SYMBOLS)}")
print(f"Total symbols in database: {len(db_symbols)}")
print(f"\nADR symbols found in database: {len(matches)}")
print(f"Percentage of database: {(len(matches)/len(db_symbols)*100):.1f}%")

if matches:
    print(f"\nFound in database:")
    # Group by first letter for readability
    for i in range(0, len(matches), 10):
        print(f"  {', '.join(matches[i:i+10])}")
    if len(matches) > 10:
        print(f"  ... and {len(matches) - ((len(matches)-1)//10 + 1)*10 if len(matches) > 10 else 0} more")
else:
    print("\n✓ No ADR symbols found in database")

print("\n" + "=" * 80)

