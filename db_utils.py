"""
Database utility functions for querying and managing bar data.
"""
from database import get_connection, get_bars, get_bar_count, get_symbols_with_data
from datetime import datetime, timedelta
from typing import List, Dict, Optional

def get_recent_bars(symbol: str, timeframe: str, hours: int = 24) -> List[Dict]:
    """Get bars from the last N hours"""
    end_time = int(datetime.now().timestamp())
    start_time = int((datetime.now() - timedelta(hours=hours)).timestamp())
    return get_bars(symbol, timeframe, start_time, end_time)

def get_bars_for_date_range(symbol: str, timeframe: str, 
                            start_date: datetime, end_date: datetime) -> List[Dict]:
    """Get bars for a specific date range"""
    start_time = int(start_date.timestamp())
    end_time = int(end_date.timestamp())
    return get_bars(symbol, timeframe, start_time, end_time)

def get_database_stats() -> Dict:
    """Get database statistics"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Total bars
    cursor.execute('SELECT COUNT(*) FROM bars')
    total_bars = cursor.fetchone()[0]
    
    # Bars by timeframe
    cursor.execute('''
        SELECT timeframe, COUNT(*) as count
        FROM bars
        GROUP BY timeframe
    ''')
    by_timeframe = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Unique symbols
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM bars')
    unique_symbols = cursor.fetchone()[0]
    
    # Date range
    cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM bars')
    row = cursor.fetchone()
    min_ts = row[0]
    max_ts = row[1]
    
    conn.close()
    
    return {
        'total_bars': total_bars,
        'by_timeframe': by_timeframe,
        'unique_symbols': unique_symbols,
        'date_range': {
            'start': datetime.fromtimestamp(min_ts).isoformat() if min_ts else None,
            'end': datetime.fromtimestamp(max_ts).isoformat() if max_ts else None
        }
    }

def export_bars_to_csv(symbol: str, timeframe: str, output_file: str):
    """Export bars to CSV file"""
    import csv
    
    bars = get_bars(symbol, timeframe)
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['symbol', 'timeframe', 'timestamp', 'datetime', 
                        'open', 'high', 'low', 'close', 'volume'])
        
        for bar in bars:
            dt = datetime.fromtimestamp(bar['timestamp']).isoformat()
            writer.writerow([
                bar['symbol'],
                bar['timeframe'],
                bar['timestamp'],
                dt,
                bar['open'],
                bar['high'],
                bar['low'],
                bar['close'],
                bar['volume']
            ])
    
    print(f"Exported {len(bars)} bars to {output_file}")

if __name__ == '__main__':
    # Print database statistics
    stats = get_database_stats()
    print("Database Statistics:")
    print(f"  Total bars: {stats['total_bars']:,}")
    print(f"  Unique symbols: {stats['unique_symbols']}")
    print(f"  By timeframe: {stats['by_timeframe']}")
    print(f"  Date range: {stats['date_range']['start']} to {stats['date_range']['end']}")

