from __future__ import annotations

import random
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import requests
import yaml

from database import get_connection


def load_keys() -> Tuple[str, str, str]:
    with open("config.yml", "r") as f:
        cfg = yaml.safe_load(f)
    return (
        cfg["alpaca"]["api_key"],
        cfg["alpaca"]["api_secret"],
        cfg["alpaca"]["data_url"],
    )


def iso_z(ts: datetime) -> str:
    # Return RFC3339 UTC with Z
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_bar_from_alpaca(symbol: str, when_unix: int, api_key: str, api_secret: str, base_url: str):
    start_dt = datetime.fromtimestamp(when_unix, tz=timezone.utc)
    end_dt = start_dt + timedelta(minutes=1)
    url = f"{base_url}/stocks/{symbol}/bars"
    resp = requests.get(
        url,
        headers={
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
        },
        params={
            "timeframe": "1Min",
            "start": iso_z(start_dt),
            "end": iso_z(end_dt),
            "feed": "iex",
            "limit": 1,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    bars = data.get("bars", [])
    return bars[0] if bars else None


def load_local_bars(symbol: str, days: int) -> List[Tuple[int, float, float, float, float, int]]:
    cutoff = int((datetime.now(tz=timezone.utc) - timedelta(days=days)).timestamp())
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, open, high, low, close, volume
        FROM bars
        WHERE symbol = ? AND timeframe = '1Min' AND timestamp >= ?
        ORDER BY timestamp ASC
        """,
        (symbol, cutoff),
    )
    rows = [(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), int(r[5])) for r in cur.fetchall()]
    conn.close()
    return rows


def summarize_per_day(rows: List[Tuple[int, float, float, float, float, int]]):
    from collections import defaultdict

    per_day = defaultdict(int)
    for ts, *_ in rows:
        day = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        per_day[day] += 1
    print("\nPer-day 1Min bar counts (last window):")
    for day in sorted(per_day):
        print(f"  {day}  ->  {per_day[day]} bars")


def main():
    symbol = sys.argv[1] if len(sys.argv) > 1 else "ABEV"
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 14
    samples = int(sys.argv[3]) if len(sys.argv) > 3 else 5

    print(f"Validating {symbol} over last {days} days with {samples} random spot-checks...\n")

    local_rows = load_local_bars(symbol, days)
    if not local_rows:
        print("No local bars found.")
        sys.exit(1)

    print(f"Local bars found: {len(local_rows)}")
    first_ts = datetime.fromtimestamp(local_rows[0][0], tz=timezone.utc)
    last_ts = datetime.fromtimestamp(local_rows[-1][0], tz=timezone.utc)
    print(f"Local range: {first_ts}  ->  {last_ts}")
    summarize_per_day(local_rows)

    api_key, api_secret, base_url = load_keys()

    print("\nSpot checks vs Alpaca IEX:")
    random.seed(42)
    candidates = [row for row in local_rows if 14_000 < row[5]]  # prefer non-tiny volume bars
    if len(candidates) < samples:
        candidates = local_rows
    picks = random.sample(candidates, k=min(samples, len(candidates)))

    mismatches = 0
    for ts, o, h, l, c, v in picks:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        try:
            remote = fetch_bar_from_alpaca(symbol, ts, api_key, api_secret, base_url)
        except Exception as e:
            print(f"  {dt}Z  local close={c}  -> API error: {e}")
            mismatches += 1
            continue

        if not remote:
            print(f"  {dt}Z  local close={c}  -> API returned no bar")
            mismatches += 1
            continue

        r_close = float(remote["c"])
        diff = abs(r_close - c)
        ok = diff <= 0.01  # 1 cent tolerance for FP/rounding
        status = "OK " if ok else "MISMATCH"
        print(f"  {dt}Z  local={c:.2f}  api={r_close:.2f}  | Δ={diff:.3f}  [{status}]")
        if not ok:
            mismatches += 1

    if mismatches == 0:
        print("\nAll spot checks within tolerance.")
    else:
        print(f"\nSpot checks with mismatches: {mismatches}/{len(picks)}")


if __name__ == "__main__":
    main()


