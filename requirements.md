# Project Requirements

## Overview

This is a Flask-based web application for filtering and viewing NYSE (New York Stock Exchange) stocks by price range, with capabilities for fetching, storing, and analyzing historical stock bar data using the Alpaca API.

## System Requirements

### Operating System
- Windows, macOS, or Linux
- Python 3.7 or higher

### Python Version
- **Required**: Python 3.7+
- **Recommended**: Python 3.9 or higher

## Python Dependencies

All Python dependencies are listed in `requirements.txt`. Install them using:

```bash
pip install -r requirements.txt
```

### Core Dependencies

1. **Flask** (3.0.0)
   - Web framework for the application
   - Provides HTTP server and routing
   - Used for serving the web interface and API endpoints

2. **requests** (2.31.0)
   - HTTP library for making API calls to Alpaca
   - Used for fetching stock symbols, prices, and historical bar data

3. **PyYAML** (6.0.1)
   - YAML parser for configuration file handling
   - Used to read `config.yml` containing API credentials

### Standard Library Dependencies

The following are included in Python standard library (no installation needed):
- `sqlite3` - Database operations
- `datetime` - Date and time handling
- `typing` - Type hints
- `time` - Time-related functions and rate limiting
- `os` - Operating system interface
- `json` - JSON parsing (via requests)
- `csv` - CSV export functionality (in db_utils.py)

## External Services & APIs

### Alpaca API

The application requires an active Alpaca API account:

- **API Type**: Paper trading or Live trading account
- **Required Endpoints**:
  - Trading API: `https://paper-api.alpaca.markets/v2` (for assets/account info)
  - Data API: `https://data.alpaca.markets/v2` (for historical data and snapshots)

- **Required API Credentials**:
  - API Key ID
  - API Secret Key

- **API Features Used**:
  - `/assets` - Fetch NYSE stock symbols
  - `/stocks/snapshots` - Get latest stock prices
  - `/stocks/{symbol}/bars` - Fetch historical OHLCV bar data

- **Rate Limiting**:
  - Application implements rate limiting (0.1-0.2 second delays)
  - Handles 429 (Too Many Requests) responses with retry logic
  - Batch processing in chunks (100-200 symbols at a time)

**Note**: Sign up at https://alpaca.markets/ to obtain API credentials.

## Database Requirements

### SQLite Database

- **Database File**: `stonxx.db` (created automatically)
- **Database Engine**: SQLite3 (included with Python)
- **Storage**: Local file system

### Database Schema

**bars table**:
- Stores historical OHLCV (Open, High, Low, Close, Volume) bar data
- Fields:
  - `id` (INTEGER PRIMARY KEY AUTOINCREMENT)
  - `symbol` (TEXT) - Stock ticker symbol
  - `timeframe` (TEXT) - Bar timeframe (e.g., '1Min', '1Sec')
  - `timestamp` (INTEGER) - Unix timestamp in seconds
  - `open` (REAL) - Opening price
  - `high` (REAL) - High price
  - `low` (REAL) - Low price
  - `close` (REAL) - Closing price
  - `volume` (INTEGER) - Trading volume
  - `created_at` (INTEGER) - Record creation timestamp

- **Indexes**:
  - Composite index on (symbol, timeframe, timestamp) for efficient queries
  - Index on timestamp for date range queries
  - Index on symbol for symbol-based queries

- **Constraints**:
  - UNIQUE constraint on (symbol, timeframe, timestamp) to prevent duplicates

### Database Storage Requirements

- Disk space depends on:
  - Number of symbols stored
  - Number of days/weeks of historical data
  - Bar timeframe (1-minute bars generate ~390 bars per trading day per symbol)
  - Example: 100 symbols × 90 days × 390 bars/day ≈ 3.5 million rows

## Configuration Requirements

### Configuration File: `config.yml`

Required YAML configuration file with the following structure:

```yaml
alpaca:
  api_key: "YOUR_API_KEY_HERE"
  api_secret: "YOUR_API_SECRET_HERE"
  data_url: "https://data.alpaca.markets/v2"
  trading_url: "https://paper-api.alpaca.markets/v2"

universe:
  tickers:
    - AFRM
    - DDOG
    # ... additional symbols

screener:
  timeframe: "1Min"
  lookback_bars: 400

alerts:
  email_enabled: false
  slack_enabled: false

vwap_screener:
  timeframe_base: "1Min"
  slope_lookback_bars: 3
  min_price: 2.0
  max_price: 10.0
  min_slope_pct_per_bar: 0.3
  nyse_symbols_file: "nyse_1to15_symbols.txt"
```

**Security Note**: This file contains sensitive credentials and should NEVER be committed to version control. It is already in `.gitignore`.

## Network Requirements

- **Internet Connection**: Required for API calls to Alpaca
- **Port**: 5000 (default Flask port) must be available for web server
- **Outbound HTTPS**: Access to `alpaca.markets` domains
- **Firewall**: Must allow outbound HTTPS connections on port 443

## Browser Requirements (for Web Interface)

- Modern web browser with JavaScript enabled
- Supported browsers:
  - Chrome/Edge (latest)
  - Firefox (latest)
  - Safari (latest)
- Features used:
  - Fetch API
  - ES6 JavaScript features
  - CSS Grid/Flexbox
  - HTML5

## File System Requirements

### Required Files

1. **Source Code Files**:
   - `app.py` - Flask web application
   - `database.py` - Database schema and core functions
   - `fetch_historical_data.py` - Historical data fetching script
   - `fetch_filtered_data.py` - Filtered data fetching (price range filtering)
   - `browse_db.py` - Interactive database browser
   - `cleanup_database.py` - Database cleanup utility
   - `db_utils.py` - Database utility functions

2. **Templates**:
   - `templates/index.html` - Web interface HTML/CSS/JavaScript

3. **Configuration**:
   - `config.yml` - API credentials (must be created by user)

4. **Documentation**:
   - `README.md` - Project documentation
   - `view_db.md` - Database viewing guide
   - `requirements.md` - This file

### Generated Files (can be ignored in git)

- `stonxx.db` - SQLite database (created automatically)
- `__pycache__/` - Python bytecode cache
- `*.pyc` - Compiled Python files
- `bar_count_histogram.png` - Generated charts (if any)

## Disk Space Requirements

- **Minimum**: 100 MB for application files and small database
- **Recommended**: 1 GB+ if storing significant historical data
- Database growth: ~50-100 KB per symbol per month (for 1-minute bars)

## Performance Considerations

- **Memory**: 
  - Base application: ~50-100 MB
  - Large data fetches: +200-500 MB
  - Web server: +50-100 MB

- **CPU**: 
  - Minimal CPU usage for web server
  - Moderate CPU usage during bulk data fetching
  - Processing 1000+ symbols may take significant time

- **API Rate Limits**:
  - Alpaca API has rate limits
  - Application includes delays and chunk processing
  - Bulk operations may take hours for large symbol lists

## Development Tools (Optional)

The following tools are helpful but not required:

- **Git** - Version control
- **Virtual Environment** (`venv`) - Python environment isolation
- **SQLite Browser** - GUI tool for database inspection
  - DB Browser for SQLite: https://sqlitebrowser.org/
  - SQLiteStudio: https://sqlitestudio.pl/
- **VS Code / IDE** - Code editing
- **Python Linter** (pylint, flake8) - Code quality

## Setup Steps

1. Install Python 3.7+ if not already installed
2. Clone or download the project
3. Create virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
5. Create `config.yml` with your Alpaca API credentials
6. Initialize database:
   ```bash
   python database.py
   ```
7. (Optional) Fetch historical data:
   ```bash
   python fetch_historical_data.py
   ```
8. Start web application:
   ```bash
   python app.py
   ```
9. Open browser to `http://localhost:5000`

## Troubleshooting

### Common Issues

1. **"No module named 'flask'"**
   - Solution: Run `pip install -r requirements.txt`

2. **API Authentication Errors**
   - Solution: Verify `config.yml` has correct API credentials

3. **Database locked errors**
   - Solution: Ensure only one process accesses the database at a time

4. **Port 5000 already in use**
   - Solution: Change port in `app.py` or stop other Flask applications

5. **Rate limit errors (429)**
   - Solution: Application handles this automatically with retries, but may slow down

## Security Considerations

- Never commit `config.yml` with real API keys to version control
- Use environment variables for sensitive data in production
- Database file may contain sensitive financial data
- API keys have trading capabilities (even in paper trading mode)
- Consider using read-only API keys if available

## Additional Resources

- Alpaca API Documentation: https://alpaca.markets/docs/
- Flask Documentation: https://flask.palletsprojects.com/
- SQLite Documentation: https://www.sqlite.org/docs.html
- Python Documentation: https://docs.python.org/3/

