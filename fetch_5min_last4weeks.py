"""
Fetch the last 4 weeks of 5-minute bars from Alpaca for symbols
that already exist in the local database, and store them.
"""
from datetime import datetime, timedelta
import time
from typing import List

from database import init_database, insert_bars_batch, get_symbols_with_data
from fetch_historical_data import fetch_bars  # re-use existing Alpaca fetcher


def fetch_for_existing_symbols(days: int = 28, timeframe: str = '5Min') -> None:
	"""
	Fetch recent bars for symbols already present in the database.

	Args:
		days: Number of days to fetch back from now (default 28 = 4 weeks)
		timeframe: Alpaca timeframe string (default '5Min')
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
			success += 1
			print(f"{len(bars):,} bars ({inserted:,} new)")

			# modest pacing
			time.sleep(0.15)
		except Exception as e:
			fail += 1
			print(f"error: {e}")

	print("\nDone.")
	print(f"Symbols: {len(symbols)} | Success: {success} | Fail: {fail} | New rows: {total_new:,}")


if __name__ == '__main__':
	fetch_for_existing_symbols(days=28, timeframe='5Min')


