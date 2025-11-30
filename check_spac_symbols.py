"""
Check how many SPAC remnant symbols are in the database
"""
from database import get_symbols_with_data

# List of SPAC remnants to remove
SPAC_SYMBOLS = [
    'AXIA', 'AEXA', 'BNH', 'BNJ', 'BRSL', 'CINT', 'CMDB', 'GCTS', 'GCTD',
    'RBOT', 'PERF', 'SHCO', 'YALA', 'ZVIA'
]

# Normalize to uppercase
SPAC_SYMBOLS_SET = {s.upper() for s in SPAC_SYMBOLS}

# Get symbols in database
db_symbols = get_symbols_with_data('1Min')
db_symbols_set = set(s.upper() for s in db_symbols)

# Find matches
matches = [s for s in SPAC_SYMBOLS_SET if s in db_symbols_set]

print("=" * 80)
print("CHECKING FOR SPAC REMNANTS")
print("=" * 80)
print(f"\nTotal SPAC remnant symbols in list: {len(SPAC_SYMBOLS)}")
print(f"Total symbols in database: {len(db_symbols)}")
print(f"\nSPAC remnant symbols found in database: {len(matches)}")
print(f"Percentage of database: {(len(matches)/len(db_symbols)*100):.1f}%")

if matches:
    print(f"\nFound in database:")
    print(f"  {', '.join(matches)}")
else:
    print("\n✓ No SPAC remnant symbols found in database")

print("\n" + "=" * 80)

