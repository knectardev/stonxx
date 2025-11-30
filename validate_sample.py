from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import requests
import yaml

from database import get_connection, get_symbols_with_data


@dataclass
class SampleResult:
    symbol: str
    ts_unix: int
    local_close: float
    api_close: Optional[float]
    ok: bool
    error: Optional[str] = None


def load_keys() -> Tuple[str, str, str]:
    with open("config.yml", "r") as f:
        cfg = yaml.safe_load(f)
    return cfg["alpaca"]["api_key"], cfg["alpaca"]["api_secret"], cfg["alpaca"]["data_url"]


def iso_z(ts: datetime) -> str:
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


def get_recent_bars_for_symbol(symbol: str, days: int) -> List[Tuple[int, float, int]]:
    """
    Return (timestamp, close, volume) for last N days for a symbol.
    """
    cutoff = int((datetime.now(tz=timezone.utc) - timedelta(days=days)).timestamp())
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, close, volume
        FROM bars
        WHERE symbol = ? AND timeframe = '1Min' AND timestamp >= ?
        ORDER BY timestamp ASC
        """,
        (symbol, cutoff),
    )
    rows = [(int(r[0]), float(r[1]), int(r[2])) for r in cur.fetchall()]
    conn.close()
    return rows


def compute_day_expected_bars(days: int) -> Dict[datetime.date, int]:
    """
    For each date in the last N days, compute an 'expected bars per day' value
    as the 90th percentile of per-symbol bar counts in the DB for that date.
    This adapts to holidays/half-days without a market calendar.
    """
    cutoff = int((datetime.now(tz=timezone.utc) - timedelta(days=days)).timestamp())
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, timestamp
        FROM bars
        WHERE timeframe = '1Min' AND timestamp >= ?
        """,
        (cutoff,),
    )
    per_symbol_per_day: Dict[Tuple[str, datetime.date], int] = defaultdict(int)
    for sym, ts in cur.fetchall():
        d = datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
        per_symbol_per_day[(sym, d)] += 1
    conn.close()

    # Aggregate per day
    per_day_counts: Dict[datetime.date, List[int]] = defaultdict(list)
    for (sym, d), cnt in per_symbol_per_day.items():
        per_day_counts[d].append(cnt)

    def pct90(values: List[int]) -> int:
        if not values:
            return 0
        values = sorted(values)
        idx = max(0, int(0.9 * (len(values) - 1)))
        return values[idx]

    return {d: pct90(cnts) for d, cnts in per_day_counts.items()}


def sample_symbols(symbols: List[str], k: int) -> List[str]:
    if k >= len(symbols):
        return symbols
    random.seed(42)
    return random.sample(symbols, k)


def run_sampling(
    symbols: List[str],
    days: int = 14,
    per_symbol_minutes: int = 3,
    min_volume: int = 500,  # prefer meaningful prints
) -> Tuple[List[SampleResult], Dict[str, float]]:
    api_key, api_secret, base_url = load_keys()
    day_expected = compute_day_expected_bars(days)

    results: List[SampleResult] = []
    coverage_by_symbol: Dict[str, float] = {}

    for sym in symbols:
        rows = get_recent_bars_for_symbol(sym, days)
        if not rows:
            coverage_by_symbol[sym] = 0.0
            continue

        # Coverage ratio: sum of min(day_count, expected)/sum(expected)
        counts_per_day: Dict[datetime.date, int] = defaultdict(int)
        for ts, _, _ in rows:
            counts_per_day[datetime.fromtimestamp(ts, tz=timezone.utc).date()] += 1

        cov_num = 0
        cov_den = 0
        for d, count in counts_per_day.items():
            exp = day_expected.get(d, 0)
            if exp <= 0:
                continue
            cov_num += min(count, exp)
            cov_den += exp
        coverage_by_symbol[sym] = (cov_num / cov_den) if cov_den > 0 else 0.0

        # Prefer higher-volume minutes for API checks
        hv = [r for r in rows if r[2] >= min_volume]
        pool = hv if len(hv) >= per_symbol_minutes else rows
        random.seed(1337)
        picks = random.sample(pool, k=min(per_symbol_minutes, len(pool)))

        for ts, close, _ in picks:
            try:
                bar = fetch_bar_from_alpaca(sym, ts, api_key, api_secret, base_url)
                if not bar:
                    results.append(
                        SampleResult(symbol=sym, ts_unix=ts, local_close=close, api_close=None, ok=False, error="no_bar")
                    )
                    continue
                api_close = float(bar["c"])
                ok = abs(api_close - close) <= 0.01
                results.append(
                    SampleResult(symbol=sym, ts_unix=ts, local_close=close, api_close=api_close, ok=ok)
                )
            except Exception as e:
                results.append(
                    SampleResult(
                        symbol=sym, ts_unix=ts, local_close=close, api_close=None, ok=False, error=str(e)
                    )
                )

    return results, coverage_by_symbol


def summarize(results: List[SampleResult], coverage_by_symbol: Dict[str, float]):
    total = len(results)
    mismatches = sum(1 for r in results if not r.ok)
    mismatch_rate = (mismatches / total) * 100 if total else 0.0

    print("\nSampling summary")
    print("----------------")
    print(f"Samples checked: {total}")
    print(f"Mismatches (incl. missing bars/API errors): {mismatches} ({mismatch_rate:.2f}%)")

    cov_values = list(coverage_by_symbol.values())
    if cov_values:
        avg_cov = sum(cov_values) / len(cov_values)
        p50 = sorted(cov_values)[len(cov_values) // 2]
        p90 = sorted(cov_values)[int(0.9 * (len(cov_values) - 1))]
        print(f"Coverage ratio vs per-day expected (0–1): avg={avg_cov:.3f}, p50={p50:.3f}, p90={p90:.3f}")

    errs = [r for r in results if not r.ok]
    if errs[:5]:
        print("\nFirst few issues:")
        for r in errs[:5]:
            ts = datetime.fromtimestamp(r.ts_unix, tz=timezone.utc)
            print(f"  {r.symbol} {ts}Z local={r.local_close} api={r.api_close} err={r.error}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Statistical validator for stored minute bars")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--symbols", type=int, default=50)
    parser.add_argument("--per-symbol-minutes", type=int, default=3)
    parser.add_argument("--min-volume", type=int, default=500)
    args = parser.parse_args()

    syms = get_symbols_with_data("1Min")
    if not syms:
        print("No symbols in DB for 1Min.")
        return

    sampled = sample_symbols(syms, args.symbols)
    print(f"Sampling {len(sampled)} symbols out of {len(syms)} with {args.per_symbol_minutes} minutes each...")

    results, cov = run_sampling(
        sampled, days=args.days, per_symbol_minutes=args.per_symbol_minutes, min_volume=args.min_volume
    )
    summarize(results, cov)


if __name__ == "__main__":
    main()


