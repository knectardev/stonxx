"""
Fetch the last 8 weeks of 30-minute bars from Alpaca for symbols
that already exist in the local database, and store them.
"""
from datetime import datetime, timedelta
import time
from typing import List
import os

from database import init_database, insert_bars_batch, get_symbols_with_data, create_ingest_run, update_ingest_run
from fetch_historical_data import fetch_bars  # reuse existing Alpaca fetcher


def fetch_for_existing_symbols(days: int = 56, timeframe: str = '30Min') -> None:
	"""
	Fetch recent bars for symbols already present in the database.

	Args:
		days: Number of days to fetch back from now (default 56 = 8 weeks)
		timeframe: Alpaca timeframe string (default '30Min')
	"""
	# Ensure DB schema exists
	init_database()

	# Symbols "in the system": those we already have any timeframe for. Use 1Min as baseline.
	symbols: List[str] = get_symbols_with_data('1Min')
	if not symbols:
		print("No existing symbols found in database. Nothing to fetch.")
		return

	end_dt = datetime.now()
	start_dt = end_dt - timedelta(days=days)

	print(f"Fetching {timeframe} bars for {len(symbols)} symbols "
	      f"from {start_dt.date()} to {end_dt.date()} ({days} days)")

	total_new = 0
	success = 0
	fail = 0

	# create run record
	run_id = create_ingest_run(
		timeframe=timeframe,
		mode='backfill_8w',
		window_start=int(start_dt.timestamp()),
		window_end=int(end_dt.timestamp()),
		pid=os.getpid()
	)

	for i, symbol in enumerate(symbols, 1):
		try:
			print(f"[{i}/{len(symbols)}] {symbol}: fetching...", end=' ', flush=True)
			bars = fetch_bars(symbol, timeframe, start_dt, end_dt)
			if not bars:
				fail += 1
				print("no data")
				continue

			inserted = insert_bars_batch(bars)
			total_new += inserted
			if inserted:
				update_ingest_run(run_id, inserted_rows_increment=inserted)
			success += 1
			print(f"{len(bars):,} bars ({inserted:,} new)")

			# modest pacing
			time.sleep(0.15)
		except Exception as e:
			fail += 1
			print(f"error: {e}")

	print("\nDone.")
	print(f"Symbols: {len(symbols)} | Success: {success} | Fail: {fail} | New rows: {total_new:,}")
	# finish run
	update_ingest_run(run_id, status='finished', ended_at_now=True)


if __name__ == '__main__':
	fetch_for_existing_symbols(days=56, timeframe='30Min')


