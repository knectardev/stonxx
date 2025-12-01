"""
Check which specific symbols have data on Nov 17
"""
from database import get_connection
from datetime import datetime

conn = get_connection()
cursor = conn.cursor()

print("Symbols with data on Nov 17:")
cursor.execute('''
    SELECT DISTINCT symbol, COUNT(*) as bar_count
    FROM bars 
    WHERE timeframe = '1Min' AND DATE(datetime(timestamp, 'unixepoch')) = '2025-11-17'
    GROUP BY symbol
    ORDER BY symbol
''')

rows = cursor.fetchall()
for symbol, count in rows:
    print(f"  {symbol}: {count} bars")

print(f"\nTotal: {len(rows)} symbols have data on Nov 17")

conn.close()

