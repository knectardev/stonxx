from flask import Flask, render_template, jsonify, request
import requests
import yaml
from typing import List, Dict
import time
from database import get_bars, get_latest_bar, get_data_range, get_symbols_with_data, get_connection
from datetime import datetime, timedelta

app = Flask(__name__)

# Load config
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

ALPACA_API_KEY = config['alpaca']['api_key']
ALPACA_SECRET_KEY = config['alpaca']['api_secret']
BASE_URL = config['alpaca']['data_url']

def get_headers():
    return {
        'APCA-API-KEY-ID': ALPACA_API_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET_KEY
    }

def get_nyse_symbols() -> List[str]:
    """Fetch NYSE symbols from Alpaca API"""
    headers = get_headers()
    try:
        # Use the trading API endpoint for assets (it already has /v2)
        trading_url = config['alpaca']['trading_url']
        url = f'{trading_url}/assets'
        print(f"Fetching assets from: {url}")
        response = requests.get(
            url,
            headers=headers,
            params={'status': 'active', 'exchange': 'NYSE', 'asset_class': 'us_equity'}
        )
        print(f"Response status: {response.status_code}")
        if response.status_code == 200:
            assets = response.json()
            # Get all active NYSE symbols (not just tradable, as many are still valid)
            symbols = [asset['symbol'] for asset in assets if asset.get('status') == 'active']
            print(f"Found {len(symbols)} active NYSE symbols")
            return symbols
        else:
            print(f"Error response: {response.text[:200]}")
            # Try alternative: maybe we need to use a different endpoint or approach
            # For now, return a small sample for testing
            return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'JNJ']
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error fetching NYSE symbols: {e}")
        # Return sample symbols for testing
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'JNJ']

def get_stock_prices(symbols: List[str]) -> Dict[str, float]:
    """Get latest prices for a list of symbols using snapshots endpoint"""
    headers = get_headers()
    prices = {}
    
    # Alpaca API allows batch requests, but we'll process in chunks
    # Increased chunk size for better performance with large symbol lists
    chunk_size = 200
    total_chunks = (len(symbols) + chunk_size - 1) // chunk_size
    
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        try:
            # Get snapshots for the chunk (includes latest trade price)
            symbols_str = ','.join(chunk)
            response = requests.get(
                f'{BASE_URL}/stocks/snapshots',
                headers=headers,
                params={'symbols': symbols_str}
            )
            if response.status_code == 200:
                data = response.json()
                # Data is a dict keyed by symbol
                for symbol in chunk:
                    if symbol in data:
                        snapshot = data[symbol]
                        # Get price from latestTrade
                        if 'latestTrade' in snapshot and snapshot['latestTrade']:
                            trade_data = snapshot['latestTrade']
                            if 'p' in trade_data:
                                prices[symbol] = float(trade_data['p'])
                if chunk_num % 5 == 0:  # Progress update every 5 chunks
                    print(f"  Fetched prices for {len(prices)}/{len(symbols)} symbols...")
            elif response.status_code == 429:
                # Rate limited, wait longer
                print(f"  Rate limited, waiting...")
                time.sleep(2)
                continue  # Retry this chunk
            time.sleep(0.1)  # Rate limiting
        except Exception as e:
            print(f"Error fetching prices for chunk {chunk_num}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    return prices

@app.route('/')
def index():
    try:
        return render_template('index.html')
    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"Error loading template: {str(e)}", 500

@app.route('/health')
def health():
    return jsonify({'status': 'ok'})

@app.route('/api/stocks', methods=['GET'])
def get_stocks():
    """Get stocks from database only (symbols with historical data)"""
    try:
        # Only use symbols from database
        db_symbols = get_symbols_with_data('1Min')
        print(f"Found {len(db_symbols)} symbols with historical data in database")
        
        if not db_symbols:
            return jsonify({'stocks': [], 'message': 'No symbols in database. Please add historical data first.'})
        
        # Fetch latest prices and min/max timestamps per symbol efficiently
        stocks = []

        # Single query to get min/max and count per symbol
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts, COUNT(*) AS bar_count
            FROM bars
            WHERE timeframe = '1Min'
            GROUP BY symbol
        """)
        ranges = {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}

        # Latest prices
        for symbol in db_symbols:
            latest_bar = get_latest_bar(symbol, '1Min')
            if not latest_bar:
                continue
            min_ts, max_ts, count = ranges.get(symbol, (None, None, 0))
            stocks.append({
                'symbol': symbol,
                'price': latest_bar['close'],
                'range_start_ts': int(min_ts) if min_ts is not None else None,
                'range_end_ts': int(max_ts) if max_ts is not None else None,
                'bar_count': int(count) if count is not None else 0
            })
        conn.close()
        
        print(f"Returning {len(stocks)} stocks from database")
        return jsonify({'stocks': stocks})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/filter', methods=['POST'])
def filter_stocks():
    """Filter stocks by price range"""
    try:
        data = request.json
        min_price = float(data.get('min_price', 0))
        max_price = float(data.get('max_price', 1000))
        
        symbols = get_nyse_symbols()
        prices = get_stock_prices(symbols)
        
        filtered = [
            {'symbol': symbol, 'price': price}
            for symbol, price in prices.items()
            if min_price <= price <= max_price
        ]
        
        # Sort by price
        filtered.sort(key=lambda x: x['price'])
        
        return jsonify({
            'stocks': filtered,
            'count': len(filtered)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bars/<symbol>', methods=['GET'])
def get_symbol_bars(symbol):
    """Get historical bars for a symbol"""
    try:
        timeframe = request.args.get('timeframe', '1Min')
        limit = request.args.get('limit', type=int)
        
        # Get bars (most recent first if limit specified)
        bars = get_bars(symbol, timeframe, limit=limit)
        
        # Convert timestamps to ISO format for frontend
        bars_data = []
        for bar in bars:
            dt = datetime.fromtimestamp(bar['timestamp'])
            bars_data.append({
                'timestamp': bar['timestamp'],
                'datetime': dt.isoformat(),
                'open': bar['open'],
                'high': bar['high'],
                'low': bar['low'],
                'close': bar['close'],
                'volume': bar['volume']
            })
        
        # If limit specified, reverse to show most recent first
        if limit:
            bars_data.reverse()
        
        # Get date range info
        date_range = get_data_range(symbol, timeframe)
        range_info = None
        if date_range:
            range_info = {
                'start': datetime.fromtimestamp(date_range[0]).isoformat(),
                'end': datetime.fromtimestamp(date_range[1]).isoformat()
            }
        
        return jsonify({
            'symbol': symbol,
            'timeframe': timeframe,
            'bars': bars_data,
            'count': len(bars_data),
            'date_range': range_info
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/symbol/<symbol>')
def symbol_detail(symbol):
    """Render full-page detail view for a symbol"""
    try:
        return render_template('symbol.html', symbol=symbol)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return f"Error loading symbol page: {str(e)}", 500

if __name__ == '__main__':
    try:
        app.run(debug=True, port=5000, host='127.0.0.1')
    except Exception as e:
        print(f"Error starting server: {e}")
        import traceback
        traceback.print_exc()

