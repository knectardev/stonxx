"""
On-demand catch-up ingest for 1m, 5m, and 30m bars.

Goals:
- Efficient API usage: call Alpaca multi-symbol bars endpoint in chunks (not per symbol).
- Compute a conservative catch-up window per timeframe based on latest stored bars,
  with a small overlap buffer to cover late data and avoid gaps.
- Insert rows in batch; rely on UNIQUE(symbol,timeframe,timestamp) to drop duplicates.

Usage examples:
  python -u ingest_catchup.py               # run all timeframes (1m,5m,30m)
  python -u ingest_catchup.py --tfs 1m,5m   # run a subset
  python -u ingest_catchup.py --chunk 120   # change symbol chunk size

Notes:
- Time is handled in UTC. Alpaca v2 supports ISO8601 with 'Z' suffix.
"""
import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple
import requests
import yaml

from database import (
	get_connection,
	init_database,
	insert_bars_batch,
	get_symbols_with_data,
	create_ingest_run,
	update_ingest_run,
)
import os


# --- Config / Headers ---
with open('config.yml', 'r') as f:
	_cfg = yaml.safe_load(f)

BASE_URL = _cfg['alpaca']['data_url']
HEADERS = {
	'APCA-API-KEY-ID': _cfg['alpaca']['api_key'],
	'APCA-API-SECRET-KEY': _cfg['alpaca']['api_secret']
}


# --- Helpers ---
def utcnow() -> datetime:
	return datetime.now(timezone.utc)

def parse_tf(tf: str) -> str:
	tf = tf.strip().lower()
	if tf in ('1m', '1min', '1'):
		return '1Min'
	if tf in ('5m', '5min', '5'):
		return '5Min'
	if tf in ('30m', '30min', '30'):
		return '30Min'
	raise ValueError(f"Unsupported timeframe: {tf}")

def chunked(lst: List[str], size: int):
	for i in range(0, len(lst), size):
		yield lst[i:i+size]


def fetch_bars_multi(symbols: List[str], timeframe: str, start: datetime, end: datetime) -> List[Dict]:
	"""
	Fetch bars for multiple symbols via Alpaca v2 multi-symbol endpoint.
	Normalizes the payload into rows ready for DB insert.
	"""
	if not symbols:
		return []

	params = {
		'timeframe': timeframe,
		'symbols': ','.join(symbols),
		'start': start.isoformat().replace('+00:00', 'Z'),
		'end': end.isoformat().replace('+00:00', 'Z'),
		'adjustment': 'raw',
		'feed': 'iex',
		'limit': 10000
	}

	resp = requests.get(f'{BASE_URL}/stocks/bars', headers=HEADERS, params=params, timeout=30)
	if resp.status_code == 429:
		# signal caller to retry later
		raise requests.HTTPError("429 Too Many Requests")
	resp.raise_for_status()
	data = resp.json()

	rows: List[Dict] = []

	# Shape A: {"bars":[{"S":"SYM","t":"...","o":...}, ...]}
	if isinstance(data.get('bars'), list):
		for bar in data['bars']:
			sym = bar.get('S') or bar.get('Symbol')
			if not sym:
				continue
			ts = datetime.fromisoformat(bar['t'].replace('Z', '+00:00')).timestamp()
			rows.append({
				'symbol': sym,
				'timeframe': timeframe,
				'timestamp': int(ts),
				'open': float(bar['o']),
				'high': float(bar['h']),
				'low': float(bar['l']),
				'close': float(bar['c']),
				'volume': int(bar['v']),
			})
		return rows

	# Shape B: {"bars":{"SYM":[{...}, ...], "SYM2":[...]}}
	if isinstance(data.get('bars'), dict):
		for sym, bars in data['bars'].items():
			for bar in bars or []:
				ts = datetime.fromisoformat(bar['t'].replace('Z', '+00:00')).timestamp()
				rows.append({
					'symbol': sym,
					'timeframe': timeframe,
					'timestamp': int(ts),
					'open': float(bar['o']),
					'high': float(bar['h']),
					'low': float(bar['l']),
					'close': float(bar['c']),
					'volume': int(bar['v']),
				})
		return rows

	return rows


def get_latest_by_symbol(timeframe: str, symbols: List[str]) -> Dict[str, int]:
	"""
	Return dict mapping symbol -> latest timestamp (unix seconds) for given timeframe.
	Missing symbols won't be present in the map.
	"""
	conn = get_connection()
	cur = conn.cursor()
	cur.execute(
		"""
		SELECT symbol, MAX(timestamp) AS max_ts
		FROM bars
		WHERE timeframe = ?
		GROUP BY symbol
		""",
		(timeframe,),
	)
	rows = cur.fetchall()
	conn.close()
	result: Dict[str, int] = {row[0]: int(row[1]) for row in rows if row[1] is not None}

	# Restrict to requested symbols for cleanliness
	return {s: result[s] for s in symbols if s in result}


def compute_catchup_window(timeframe: str, symbols: List[str]) -> Tuple[datetime, datetime]:
	"""
	Compute a conservative [start,end] for catch-up for this timeframe across all symbols.
	- If some symbols have no data, fall back to default horizon.
	- Apply a small overlap buffer to cover late-arriving bars.
	"""
	now = utcnow()

	# Overlap buffers to safely re-fetch a little recent history
	buffers = {
		'1Min': timedelta(minutes=15),
		'5Min': timedelta(hours=1),
		'30Min': timedelta(hours=6),
	}
	# Fallback horizons if a symbol lacks this timeframe entirely
	fallbacks = {
		'1Min': timedelta(days=3),
		'5Min': timedelta(days=14),
		'30Min': timedelta(days=56),
	}

	latest_map = get_latest_by_symbol(timeframe, symbols)

	if len(latest_map) < len(symbols):
		# Some symbols have no data for this TF; start from a reasonable fallback
		start = now - fallbacks[timeframe]
	else:
		# Start from the earliest "latest" across symbols, minus buffer
		min_latest = min(latest_map.values()) if latest_map else int((now - buffers[timeframe]).timestamp())
		start = datetime.fromtimestamp(max(0, min_latest), tz=timezone.utc) - buffers[timeframe]

	# Ensure start < now
	start = min(start, now - timedelta(seconds=1))
	return start, now


def run_catchup_for_timeframe(symbols: List[str], timeframe: str, chunk_size: int, pause_s: float) -> int:
	"""
	Run catch-up cycle for one timeframe across all symbols.
	Returns number of rows newly inserted.
	"""
	start, end = compute_catchup_window(timeframe, symbols)
	print(f"[{timeframe}] Catch-up window: {start.isoformat()} -> {end.isoformat()}")
	total_inserted = 0

	# create run record
	run_id = create_ingest_run(
		timeframe=timeframe,
		mode='catchup',
		window_start=int(start.timestamp()),
		window_end=int(end.timestamp()),
		pid=os.getpid()
	)

	for group in chunked(symbols, chunk_size):
		try:
			rows = fetch_bars_multi(group, timeframe, start, end)
			if rows:
				inserted = insert_bars_batch(rows) or 0
				total_inserted += inserted
				if inserted:
					update_ingest_run(run_id, inserted_rows_increment=inserted)
			time.sleep(pause_s)
		except requests.HTTPError as e:
			if '429' in str(e):
				print(f"[{timeframe}] Rate limited; backing off...")
				time.sleep(2.0)
				continue
			else:
				print(f"[{timeframe}] HTTP error: {e}")
		except Exception as e:
			print(f"[{timeframe}] Error: {e}")

	print(f"[{timeframe}] Inserted {total_inserted:,} rows")
	# finish run
	update_ingest_run(run_id, status='finished', ended_at_now=True)
	return total_inserted


def main():
	parser = argparse.ArgumentParser(description="On-demand catch-up ingest for multiple timeframes.")
	parser.add_argument('--tfs', type=str, default='1m,5m,30m',
	                    help="Comma-separated list of timeframes: 1m,5m,30m (default: all)")
	parser.add_argument('--chunk', type=int, default=100, help="Symbols per request (default: 100)")
	parser.add_argument('--pause', type=float, default=0.15, help="Pause seconds between chunked requests (default: 0.15)")
	args = parser.parse_args()

	timeframes = [parse_tf(tf) for tf in args.tfs.split(',') if tf.strip()]
	init_database()

	# Symbols "in the system": use 1Min as the baseline membership
	symbols = get_symbols_with_data('1Min')
	if not symbols:
		print("No symbols found in the system (1Min baseline). Nothing to do.")
		return

	print(f"Running catch-up for {len(symbols)} symbols; timeframes: {', '.join(timeframes)}")
	total = 0
	for tf in timeframes:
		total += run_catchup_for_timeframe(symbols, tf, args.chunk, args.pause)

	print(f"Done. Total newly inserted rows across all timeframes: {total:,}")


if __name__ == '__main__':
	main()


