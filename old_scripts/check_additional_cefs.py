"""
Check which additional CEFs and Bond Funds are in the database
"""
from database import get_symbols_with_data

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

# Normalize to uppercase
CEF_SYMBOLS_SET = {s.upper() for s in CEF_SYMBOLS}

# Get symbols in database
db_symbols = get_symbols_with_data('1Min')
db_symbols_set = set(s.upper() for s in db_symbols)

# Find matches
matches = [s for s in CEF_SYMBOLS_SET if s in db_symbols_set]

print("=" * 80)
print("CHECKING FOR ADDITIONAL CEFs AND BOND FUNDS")
print("=" * 80)
print(f"\nTotal CEF/Bond Fund symbols in list: {len(CEF_SYMBOLS)}")
print(f"Total symbols in database: {len(db_symbols)}")
print(f"\nCEF/Bond Fund symbols found in database: {len(matches)}")
print(f"Percentage of database: {(len(matches)/len(db_symbols)*100):.1f}%")

if matches:
    print(f"\nFound in database:")
    # Group by first letter for readability
    for i in range(0, len(matches), 10):
        print(f"  {', '.join(matches[i:i+10])}")
    if len(matches) > 10:
        print(f"  ... and {len(matches) - ((len(matches)-1)//10 + 1)*10 if len(matches) > 10 else 0} more")
else:
    print("\n✓ No additional CEF/Bond Fund symbols found in database")

print("\n" + "=" * 80)

