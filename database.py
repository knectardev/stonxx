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

    # Ratings table - stores a single 0-5 star rating per symbol
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS symbol_ratings (
            symbol TEXT PRIMARY KEY,
            rating INTEGER NOT NULL CHECK (rating BETWEEN 0 AND 5),
            updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    ''')

    # Ingest runs table - tracks background/on-demand ingest processes
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ingest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timeframe TEXT NOT NULL,
            mode TEXT NOT NULL,                  -- e.g., 'catchup', 'backfill_8w'
            status TEXT NOT NULL,                -- 'running', 'finished', 'error'
            started_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            ended_at INTEGER,
            window_start INTEGER,                -- unix seconds
            window_end INTEGER,                  -- unix seconds
            inserted_rows INTEGER NOT NULL DEFAULT 0,
            pid INTEGER
        )
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ingest_runs_timeframe_status
        ON ingest_runs(timeframe, status, started_at)
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

def create_ingest_run(timeframe: str, mode: str, window_start: int, window_end: int, pid: int) -> int:
    """Create an ingest run record and return its ID"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO ingest_runs(timeframe, mode, status, started_at, window_start, window_end, pid)
        VALUES (?, ?, 'running', strftime('%s','now'), ?, ?, ?)
    ''', (timeframe, mode, window_start, window_end, pid))
    conn.commit()
    run_id = cur.lastrowid
    conn.close()
    return run_id

def update_ingest_run(run_id: int, status: Optional[str] = None, inserted_rows_increment: Optional[int] = None, ended_at_now: bool = False):
    """Update status and/or increment inserted_rows for an ingest run"""
    conn = get_connection()
    cur = conn.cursor()
    if inserted_rows_increment:
        cur.execute('UPDATE ingest_runs SET inserted_rows = inserted_rows + ? WHERE id = ?', (inserted_rows_increment, run_id))
    if status is not None:
        if ended_at_now and status in ('finished', 'error'):
            cur.execute('UPDATE ingest_runs SET status = ?, ended_at = strftime(\'%s\',\'now\') WHERE id = ?', (status, run_id))
        else:
            cur.execute('UPDATE ingest_runs SET status = ? WHERE id = ?', (status, run_id))
    conn.commit()
    conn.close()

def get_timeframe_freshness(timeframe: str) -> Optional[int]:
    """
    Return the earliest 'latest timestamp' across all symbols for a timeframe.
    Interpreted as how far behind the system could be at worst.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        WITH latest AS (
            SELECT symbol, MAX(timestamp) AS max_ts
            FROM bars
            WHERE timeframe = ?
            GROUP BY symbol
        )
        SELECT MIN(max_ts) FROM latest
    ''', (timeframe,))
    row = cur.fetchone()
    conn.close()
    if row and row[0]:
        return int(row[0])
    return None

def get_ingest_overview() -> Dict:
    """Return running and last-finished run info plus freshness by timeframe."""
    conn = get_connection()
    cur = conn.cursor()

    # Running
    cur.execute('''
        SELECT id, timeframe, mode, status, started_at, window_start, window_end, inserted_rows, pid
        FROM ingest_runs
        WHERE status = 'running'
        ORDER BY started_at DESC
    ''')
    running = [dict(row) for row in cur.fetchall()]

    # Last finished per timeframe
    last_finished: Dict[str, Dict] = {}
    for tf in ('1Min', '5Min', '30Min'):
        cur.execute('''
            SELECT id, timeframe, mode, status, started_at, ended_at, window_start, window_end, inserted_rows, pid
            FROM ingest_runs
            WHERE timeframe = ? AND status = 'finished'
            ORDER BY ended_at DESC
            LIMIT 1
        ''', (tf,))
        row = cur.fetchone()
        last_finished[tf] = dict(row) if row else None

    conn.close()

    return {
        'running': running,
        'last_finished': last_finished,
        'freshness': {
            '1Min': get_timeframe_freshness('1Min'),
            '5Min': get_timeframe_freshness('5Min'),
            '30Min': get_timeframe_freshness('30Min'),
        }
    }

def has_running_ingest(timeframe: Optional[str] = None, mode: Optional[str] = None) -> bool:
    """Return True if there is a running ingest, optionally filtered by timeframe and mode."""
    conn = get_connection()
    cur = conn.cursor()
    query = "SELECT COUNT(*) FROM ingest_runs WHERE status = 'running'"
    params: List = []
    if timeframe:
        query += " AND timeframe = ?"
        params.append(timeframe)
    if mode:
        query += " AND mode = ?"
        params.append(mode)
    cur.execute(query, params)
    cnt = cur.fetchone()[0]
    conn.close()
    return cnt > 0

def get_symbol_rating(symbol: str) -> int:
    """Return the 0-5 rating for a symbol (0 if unrated)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT rating FROM symbol_ratings WHERE symbol = ?', (symbol,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else 0

def set_symbol_rating(symbol: str, rating: int) -> int:
    """
    Upsert and return the sanitized rating (0-5) for the symbol.
    """
    r = max(0, min(5, int(rating)))
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO symbol_ratings(symbol, rating, updated_at)
        VALUES (?, ?, strftime('%s','now'))
        ON CONFLICT(symbol) DO UPDATE SET
            rating = excluded.rating,
            updated_at = excluded.updated_at
    ''', (symbol, r))
    conn.commit()
    conn.close()
    return r

def get_ratings_map() -> Dict[str, int]:
    """Return a dict of symbol -> rating for all rated symbols."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT symbol, rating FROM symbol_ratings')
    out: Dict[str, int] = {row[0]: int(row[1]) for row in cur.fetchall()}
    conn.close()
    return out

if __name__ == '__main__':
    # Initialize database when run directly
    init_database()
    print("Database schema created successfully!")

