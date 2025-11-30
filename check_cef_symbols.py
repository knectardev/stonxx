"""
Check how many Closed-End Funds (CEFs) and Bond Funds from the list are in the database
"""
from database import get_symbols_with_data

# List of CEFs and Bond Funds to remove (from ChatGPT's recommendation)
cef_symbols = [
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

# Normalize to uppercase
cef_symbols = [s.upper() for s in cef_symbols]

# Get symbols in database
db_symbols = get_symbols_with_data('1Min')
db_symbols_set = set(s.upper() for s in db_symbols)

# Find matches
matches = [s for s in cef_symbols if s in db_symbols_set]

print("=" * 80)
print("CHECKING FOR CLOSED-END FUNDS (CEFs) AND BOND FUNDS")
print("=" * 80)
print(f"\nTotal CEF/Bond Fund symbols in list: {len(cef_symbols)}")
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
    print("\n✓ No CEF/Bond Fund symbols found in database")

print("\n" + "=" * 80)

