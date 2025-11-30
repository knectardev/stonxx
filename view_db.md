# Viewing the Database

There are several ways to view the SQLite database (`stonxx.db`):

## Option 1: Interactive Python Browser (Recommended)

Run the interactive browser script:

```bash
python browse_db.py
```

This provides a menu-driven interface to:
- View database statistics
- List all symbols
- Query bars for specific symbols
- Run custom SQL queries

## Option 2: SQLite Command Line

If you have SQLite installed, you can use the command line:

```bash
sqlite3 stonxx.db
```

Then run SQL queries:

```sql
-- View all tables
.tables

-- View schema
.schema bars

-- Count total bars
SELECT COUNT(*) FROM bars;

-- View bars for a symbol
SELECT * FROM bars WHERE symbol='AAPL' LIMIT 10;

-- View statistics by symbol
SELECT symbol, COUNT(*) as bar_count 
FROM bars 
WHERE timeframe='1Min' 
GROUP BY symbol 
ORDER BY bar_count DESC;

-- View date range for a symbol
SELECT 
    symbol,
    datetime(MIN(timestamp), 'unixepoch') as start_date,
    datetime(MAX(timestamp), 'unixepoch') as end_date,
    COUNT(*) as bar_count
FROM bars
WHERE symbol='AAPL' AND timeframe='1Min'
GROUP BY symbol;

-- Exit
.quit
```

## Option 3: GUI Tools

### DB Browser for SQLite (Free, Cross-platform)
- Download: https://sqlitebrowser.org/
- Open `stonxx.db` file
- Browse tables, run queries, export data

### SQLiteStudio (Free, Cross-platform)
- Download: https://sqlitestudio.pl/
- Similar features to DB Browser

### VS Code Extension
- Install "SQLite Viewer" extension
- Right-click `stonxx.db` → "Open Database"

## Option 4: Quick Python Scripts

### View all symbols:
```python
from database import get_symbols_with_data
print(get_symbols_with_data('1Min'))
```

### View bars for a symbol:
```python
from database import get_bars
from datetime import datetime

bars = get_bars('AAPL', '1Min', limit=10)
for bar in bars:
    dt = datetime.fromtimestamp(bar['timestamp'])
    print(f"{dt}: O={bar['open']:.2f} H={bar['high']:.2f} L={bar['low']:.2f} C={bar['close']:.2f} V={bar['volume']}")
```

### Export to CSV:
```python
from database import get_bars
import csv
from datetime import datetime

bars = get_bars('AAPL', '1Min')
with open('aapl_bars.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['symbol', 'timeframe', 'timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume'])
    for bar in bars:
        dt = datetime.fromtimestamp(bar['timestamp'])
        writer.writerow([
            bar['symbol'], bar['timeframe'], bar['timestamp'], dt.isoformat(),
            bar['open'], bar['high'], bar['low'], bar['close'], bar['volume']
        ])
print(f"Exported {len(bars)} bars to aapl_bars.csv")
```

