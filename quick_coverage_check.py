"""
Quick check of current data coverage
"""
from database import get_symbols_with_data, get_data_range
from datetime import datetime, timedelta

symbols = get_symbols_with_data('1Min')
yesterday = datetime.now() - timedelta(days=1)
target_end = yesterday.replace(hour=23, minute=59, second=59)
target_start = (target_end - timedelta(days=14)).replace(hour=0, minute=0, second=0)

print(f"Target date range: {target_start.date()} to {target_end.date()} (14 days)")
print(f"\nChecking first 5 symbols:")
print("-" * 80)

for symbol in symbols[:5]:
    date_range = get_data_range(symbol, '1Min')
    if date_range:
        existing_start = datetime.fromtimestamp(date_range[0])
        existing_end = datetime.fromtimestamp(date_range[1])
        days = (existing_end.date() - existing_start.date()).days
        print(f"{symbol}: {existing_start.date()} to {existing_end.date()} ({days} days)")

print(f"\nTotal symbols: {len(symbols)}")
print("\nRecommendation: Run fetch_full_14_days.py to ensure complete 14-day coverage for all symbols")

