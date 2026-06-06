"""
Fetch OHLC candle data from TradingView using tvDatafeed.
Supports login (for premium symbols) or anonymous access.
"""

import argparse
import sys
from tvDatafeed import TvDatafeed, Interval


INTERVALS = {
    "1m":   Interval.in_1_minute,
    "3m":   Interval.in_3_minute,
    "5m":   Interval.in_5_minute,
    "15m":  Interval.in_15_minute,
    "30m":  Interval.in_30_minute,
    "45m":  Interval.in_45_minute,
    "1h":   Interval.in_1_hour,
    "2h":   Interval.in_2_hour,
    "3h":   Interval.in_3_hour,
    "4h":   Interval.in_4_hour,
    "1d":   Interval.in_daily,
    "1w":   Interval.in_weekly,
    "1M":   Interval.in_monthly,
}


def get_candles(
    symbol: str,
    exchange: str,
    interval: str = "1d",
    n_bars: int = 100,
    username: str = None,
    password: str = None,
) -> None:
    if interval not in INTERVALS:
        print(f"Invalid interval '{interval}'. Choose from: {', '.join(INTERVALS)}")
        sys.exit(1)

    print(f"Connecting to TradingView {'(authenticated)' if username else '(anonymous)'}...")
    tv = TvDatafeed(username=username, password=password)

    print(f"Fetching {n_bars} bars of {interval} candles for {exchange}:{symbol}...")
    df = tv.get_hist(
        symbol=symbol,
        exchange=exchange,
        interval=INTERVALS[interval],
        n_bars=n_bars,
    )

    if df is None or df.empty:
        print("No data returned. Check symbol/exchange spelling.")
        sys.exit(1)

    # Keep only OHLCV columns and clean up index
    df = df[["open", "high", "low", "close", "volume"]]
    df.index.name = "datetime"

    print(f"\n{symbol} | {exchange} | {interval} — {len(df)} candles\n")
    print(df.to_string())
    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch OHLC candle data from TradingView")
    parser.add_argument("symbol",   help="Ticker symbol, e.g. BTCUSDT")
    parser.add_argument("exchange", help="Exchange name, e.g. BINANCE")
    parser.add_argument("-i", "--interval", default="1d",  help="Candle interval (default: 1d)")
    parser.add_argument("-n", "--bars",     default=100, type=int, help="Number of bars (default: 100)")
    parser.add_argument("-u", "--username", default=None, help="TradingView username (optional)")
    parser.add_argument("-p", "--password", default=None, help="TradingView password (optional)")
    args = parser.parse_args()

    get_candles(
        symbol=args.symbol,
        exchange=args.exchange,
        interval=args.interval,
        n_bars=args.bars,
        username=args.username,
        password=args.password,
    )


if __name__ == "__main__":
    main()
