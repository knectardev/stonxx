"""
Database layer for storing historical stock bar data.
Supports multiple timeframes (1s, 1m, etc.) for NYSE stocks.
"""
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import os

DB_PATH = 'stonxx.db'

def get_connection():
    """Get database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Initialize database schema"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Bars table - stores raw OHLCV data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            UNIQUE(symbol, timeframe, timestamp)
        )
    ''')
    
    # Indexes for efficient querying
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_bars_symbol_timeframe 
        ON bars(symbol, timeframe, timestamp)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_bars_timestamp 
        ON bars(timestamp)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_bars_symbol 
        ON bars(symbol)
    ''')
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")

def insert_bar(symbol: str, timeframe: str, timestamp: int, 
               open_price: float, high: float, low: float, 
               close: float, volume: int):
    """Insert a single bar into the database"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO bars 
            (symbol, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (symbol, timeframe, timestamp, open_price, high, low, close, volume))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Error inserting bar: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def insert_bars_batch(bars: List[Dict]):
    """Insert multiple bars in a batch transaction"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.executemany('''
            INSERT OR IGNORE INTO bars 
            (symbol, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            (bar['symbol'], bar['timeframe'], bar['timestamp'], 
             bar['open'], bar['high'], bar['low'], bar['close'], bar['volume'])
            for bar in bars
        ])
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        print(f"Error inserting bars batch: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()

def get_bars(symbol: str, timeframe: str, 
             start_time: Optional[int] = None, 
             end_time: Optional[int] = None,
             limit: Optional[int] = None) -> List[Dict]:
    """Get bars for a symbol and timeframe"""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = '''
        SELECT symbol, timeframe, timestamp, open, high, low, close, volume
        FROM bars
        WHERE symbol = ? AND timeframe = ?
    '''
    params = [symbol, timeframe]
    
    if start_time:
        query += ' AND timestamp >= ?'
        params.append(start_time)
    
    if end_time:
        query += ' AND timestamp <= ?'
        params.append(end_time)
    
    query += ' ORDER BY timestamp ASC'
    
    if limit:
        query += ' LIMIT ?'
        params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]

def get_latest_bar(symbol: str, timeframe: str) -> Optional[Dict]:
    """Get the most recent bar for a symbol and timeframe"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT symbol, timeframe, timestamp, open, high, low, close, volume
        FROM bars
        WHERE symbol = ? AND timeframe = ?
        ORDER BY timestamp DESC
        LIMIT 1
    ''', (symbol, timeframe))
    
    row = cursor.fetchone()
    conn.close()
    
    return dict(row) if row else None

def get_symbols_with_data(timeframe: str) -> List[str]:
    """Get list of symbols that have data for a given timeframe"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT DISTINCT symbol
        FROM bars
        WHERE timeframe = ?
        ORDER BY symbol
    ''', (timeframe,))
    
    symbols = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    return symbols

def get_data_range(symbol: str, timeframe: str) -> Optional[Tuple[int, int]]:
    """Get the earliest and latest timestamps for a symbol/timeframe"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts
        FROM bars
        WHERE symbol = ? AND timeframe = ?
    ''', (symbol, timeframe))
    
    row = cursor.fetchone()
    conn.close()
    
    if row and row[0] and row[1]:
        return (row[0], row[1])
    return None

def get_bar_count(symbol: Optional[str] = None, timeframe: Optional[str] = None) -> int:
    """Get total count of bars, optionally filtered by symbol and/or timeframe"""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = 'SELECT COUNT(*) FROM bars WHERE 1=1'
    params = []
    
    if symbol:
        query += ' AND symbol = ?'
        params.append(symbol)
    
    if timeframe:
        query += ' AND timeframe = ?'
        params.append(timeframe)
    
    cursor.execute(query, params)
    count = cursor.fetchone()[0]
    conn.close()
    
    return count

def delete_old_bars(days_to_keep: int = 90):
    """Delete bars older than specified days"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cutoff_timestamp = int((datetime.now() - timedelta(days=days_to_keep)).timestamp())
    
    cursor.execute('''
        DELETE FROM bars
        WHERE timestamp < ?
    ''', (cutoff_timestamp,))
    
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    
    return deleted

if __name__ == '__main__':
    # Initialize database when run directly
    init_database()
    print("Database schema created successfully!")

