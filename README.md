# NYSE Stock Filter

A minimalist web application for filtering New York Stock Exchange (NYSE) stocks by price range using the Alpaca API.

## Features

- Filter NYSE stocks by price range (min/max)
- Visual price distribution graph
- Interactive slider for price range selection
- Real-time match count
- Dark-themed UI matching the design specifications
- Table display of filtered stocks with Symbol and Price columns

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure your `config.yml` file contains valid Alpaca API credentials:
```yaml
alpaca:
  api_key: "YOUR_API_KEY"
  api_secret: "YOUR_API_SECRET"
  data_url: "https://data.alpaca.markets/v2"
```

3. Run the application:
```bash
python app.py
```

4. Open your browser and navigate to:
```
http://localhost:5000
```

## Usage

1. Adjust the minimum and maximum price inputs, or use the slider handles
2. The distribution graph shows the price distribution of all NYSE stocks
3. The match count displays how many stocks fall within your selected range
4. The table below shows all matching stocks sorted by price

## API Endpoints

- `GET /` - Main page
- `GET /api/stocks` - Fetch all NYSE stocks with prices
- `POST /api/filter` - Filter stocks by price range (JSON body: `{min_price: float, max_price: float}`)

## Database - Historical Data Storage

The application includes a database layer for storing historical stock bar data.

### Database Schema

**bars table** - Raw data layer storing OHLCV data:
- `symbol` - Stock ticker symbol
- `timeframe` - Bar timeframe (e.g., '1m', '1s', '1Min', '1Sec')
- `timestamp` - Unix timestamp (seconds since epoch)
- `open`, `high`, `low`, `close` - OHLC prices
- `volume` - Trading volume
- `created_at` - Record creation timestamp

The schema supports multiple timeframes including 1-second and 1-minute bars.

### Initializing the Database

```bash
python database.py
```

This creates the SQLite database file (`stonxx.db`) with the required schema and indexes.

### Fetching Historical Data

To fetch and store 3 months of 1-minute historical data:

```bash
python fetch_historical_data.py
```

This script will:
1. Fetch all active NYSE symbols from Alpaca API
2. Download 3 months of 1-minute bar data for each symbol
3. Store the data in the database
4. Skip symbols that already have recent data (within last 7 days)

You can limit the number of symbols processed by entering a number when prompted.

### Database Utilities

View database statistics:
```bash
python db_utils.py
```

Query bars programmatically:
```python
from database import get_bars, get_latest_bar
from datetime import datetime, timedelta

# Get latest bar for a symbol
latest = get_latest_bar('AAPL', '1Min')

# Get bars for a date range
start = int((datetime.now() - timedelta(days=7)).timestamp())
end = int(datetime.now().timestamp())
bars = get_bars('AAPL', '1Min', start_time=start, end_time=end)
```

### Database Files

- `stonxx.db` - SQLite database file (created automatically)
- `database.py` - Database schema and core functions
- `fetch_historical_data.py` - Script to populate historical data
- `db_utils.py` - Utility functions for querying data

