"""
Interactive database browser for viewing bar data
"""
import sqlite3
from datetime import datetime
from database import get_connection, get_bar_count, get_symbols_with_data, get_data_range

def print_stats():
    """Print database statistics"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Total bars
    cursor.execute('SELECT COUNT(*) FROM bars')
    total = cursor.fetchone()[0]
    
    # By timeframe
    cursor.execute('SELECT timeframe, COUNT(*) FROM bars GROUP BY timeframe')
    by_tf = dict(cursor.fetchall())
    
    # By symbol
    cursor.execute('SELECT symbol, COUNT(*) FROM bars GROUP BY symbol ORDER BY COUNT(*) DESC LIMIT 10')
    top_symbols = cursor.fetchall()
    
    # Date range
    cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM bars')
    min_ts, max_ts = cursor.fetchone()
    
    conn.close()
    
    print("\n" + "="*60)
    print("DATABASE STATISTICS")
    print("="*60)
    print(f"Total bars: {total:,}")
    print(f"\nBy timeframe: {by_tf}")
    print(f"\nTop 10 symbols by bar count:")
    for symbol, count in top_symbols:
        print(f"  {symbol}: {count:,} bars")
    
    if min_ts and max_ts:
        min_dt = datetime.fromtimestamp(min_ts)
        max_dt = datetime.fromtimestamp(max_ts)
        days = (max_dt - min_dt).days
        print(f"\nDate range: {min_dt.date()} to {max_dt.date} ({days} days)")
    print("="*60 + "\n")

def query_bars(symbol=None, timeframe='1Min', limit=20):
    """Query and display bars"""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = '''
        SELECT symbol, timeframe, timestamp, open, high, low, close, volume
        FROM bars
        WHERE 1=1
    '''
    params = []
    
    if symbol:
        query += ' AND symbol = ?'
        params.append(symbol)
    
    if timeframe:
        query += ' AND timeframe = ?'
        params.append(timeframe)
    
    query += ' ORDER BY timestamp DESC LIMIT ?'
    params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print("No bars found.")
        return
    
    print(f"\n{'Symbol':<8} {'Timeframe':<10} {'Date/Time':<20} {'Open':<10} {'High':<10} {'Low':<10} {'Close':<10} {'Volume':<12}")
    print("-" * 100)
    
    for row in rows:
        symbol, tf, ts, open_p, high, low, close, volume = row
        dt = datetime.fromtimestamp(ts)
        print(f"{symbol:<8} {tf:<10} {dt.strftime('%Y-%m-%d %H:%M'):<20} {open_p:<10.2f} {high:<10.2f} {low:<10.2f} {close:<10.2f} {volume:<12,}")

def interactive_mode():
    """Interactive database browser"""
    print("\n" + "="*60)
    print("SQLite Database Browser")
    print("="*60)
    
    while True:
        print("\nOptions:")
        print("  1. Show statistics")
        print("  2. List all symbols")
        print("  3. Query bars for a symbol")
        print("  4. Run custom SQL query")
        print("  5. Exit")
        
        choice = input("\nEnter choice (1-5): ").strip()
        
        if choice == '1':
            print_stats()
        
        elif choice == '2':
            symbols = get_symbols_with_data('1Min')
            print(f"\nSymbols with data ({len(symbols)}):")
            for i, sym in enumerate(symbols, 1):
                count = get_bar_count(symbol=sym, timeframe='1Min')
                date_range = get_data_range(sym, '1Min')
                if date_range:
                    start_dt = datetime.fromtimestamp(date_range[0])
                    end_dt = datetime.fromtimestamp(date_range[1])
                    days = (end_dt - start_dt).days
                    print(f"  {sym}: {count:,} bars ({days} days)")
        
        elif choice == '3':
            symbol = input("Enter symbol (e.g., AAPL): ").strip().upper()
            limit = input("Number of bars to show (default 20): ").strip()
            limit = int(limit) if limit.isdigit() else 20
            query_bars(symbol=symbol, limit=limit)
        
        elif choice == '4':
            print("\nEnter SQL query (or 'back' to return):")
            print("Example: SELECT * FROM bars WHERE symbol='AAPL' LIMIT 10")
            sql = input("SQL> ").strip()
            if sql.lower() == 'back':
                continue
            
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute(sql)
                
                # Get column names
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                conn.close()
                
                if rows:
                    # Print header
                    print("\n" + " | ".join(f"{col:<15}" for col in columns))
                    print("-" * (len(columns) * 18))
                    
                    # Print rows
                    for row in rows:
                        print(" | ".join(f"{str(val):<15}" for val in row))
                    print(f"\n{len(rows)} row(s) returned")
                else:
                    print("No rows returned")
            except Exception as e:
                print(f"Error: {e}")
        
        elif choice == '5':
            print("Goodbye!")
            break
        
        else:
            print("Invalid choice. Please try again.")

if __name__ == '__main__':
    try:
        interactive_mode()
    except KeyboardInterrupt:
        print("\n\nGoodbye!")

