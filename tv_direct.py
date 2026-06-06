"""
Direct WebSocket connection to TradingView to fetch OHLC candle data.
Reverse-engineered from TradingView's browser WebSocket protocol.
"""

import argparse
import json
import random
import re
import string
import sys
import time

import websocket  # pip install websocket-client
import pandas as pd

TV_WS_URL = "wss://data.tradingview.com/socket.io/websocket"

HEADERS = {
    "Origin": "https://www.tradingview.com",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

INTERVALS = {
    "1m":  "1",
    "3m":  "3",
    "5m":  "5",
    "15m": "15",
    "30m": "30",
    "45m": "45",
    "1h":  "60",
    "2h":  "120",
    "3h":  "180",
    "4h":  "240",
    "1d":  "1D",
    "1w":  "1W",
    "1M":  "1M",
}


# ── Protocol helpers ──────────────────────────────────────────────────────────

def _rand_id(n=12) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=n))


def _wrap(msg: dict) -> str:
    """Wrap a JSON message in the TradingView framing format."""
    body = json.dumps(msg, separators=(",", ":"))
    return f"~m~{len(body)}~m~{body}"


def _send(ws, func: str, args: list):
    ws.send(_wrap({"m": func, "p": args}))


def _heartbeat(raw: str) -> str | None:
    """Return a heartbeat reply if the message is a ping, else None."""
    m = re.match(r"~m~(\d+)~m~(~h~\d+)", raw)
    return f"~m~{m.group(1)}~m~{m.group(2)}" if m else None


def _parse_packets(raw: str) -> list[dict]:
    """Extract all JSON payloads from a (possibly multi-packet) frame."""
    payloads = []
    for body in re.findall(r"~m~\d+~m~({.*})", raw):
        try:
            payloads.append(json.loads(body))
        except json.JSONDecodeError:
            pass
    return payloads


# ── Main fetcher ──────────────────────────────────────────────────────────────

def fetch_candles(
    symbol: str,
    exchange: str,
    interval: str = "1d",
    n_bars: int = 100,
    auth_token: str = "unauthorized_user_token",
    timeout: int = 15,
) -> pd.DataFrame:

    if interval not in INTERVALS:
        raise ValueError(f"Invalid interval '{interval}'. Choose from: {', '.join(INTERVALS)}")

    tv_interval = INTERVALS[interval]
    full_symbol = f"{exchange.upper()}:{symbol.upper()}"

    chart_session = f"cs_{_rand_id()}"
    quote_session = f"qs_{_rand_id()}"

    candles: list[dict] = []
    done = False
    error_msg = None

    def on_open(ws):
        # Authenticate (use "unauthorized_user_token" for anonymous access)
        _send(ws, "set_auth_token", [auth_token])
        # Create sessions
        _send(ws, "chart_create_session", [chart_session, ""])
        _send(ws, "quote_create_session", [quote_session])
        # Subscribe to the symbol
        _send(ws, "resolve_symbol", [
            chart_session,
            "symbol_1",
            f'={{"symbol":"{full_symbol}","adjustment":"splits"}}',
        ])
        # Request historical bars
        _send(ws, "create_series", [
            chart_session, "s1", "s1", "symbol_1", tv_interval, n_bars,
        ])

    def on_message(ws, raw):
        nonlocal done, error_msg

        # Reply to keep-alive pings
        hb = _heartbeat(raw)
        if hb:
            ws.send(hb)
            return

        for packet in _parse_packets(raw):
            m = packet.get("m", "")

            if m == "timescale_update":
                series = (
                    packet.get("p", [{}])[1]
                    .get("s1", {})
                    .get("s", [])
                )
                for bar in series:
                    v = bar.get("v", [])
                    if len(v) >= 5:
                        candles.append({
                            "datetime": pd.Timestamp(v[0], unit="s", tz="UTC"),
                            "open":     v[1],
                            "high":     v[2],
                            "low":      v[3],
                            "close":    v[4],
                            "volume":   v[5] if len(v) > 5 else None,
                        })
                done = True
                ws.close()

            elif m == "series_error":
                error_msg = str(packet.get("p", "unknown series error"))
                done = True
                ws.close()

            elif m == "critical_error":
                error_msg = str(packet.get("p", "critical error"))
                done = True
                ws.close()

    def on_error(ws, err):
        nonlocal error_msg
        error_msg = str(err)

    ws_app = websocket.WebSocketApp(
        TV_WS_URL,
        header=[f"{k}: {v}" for k, v in HEADERS.items()],
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
    )

    # Run in the background and wait for data or timeout
    import threading
    t = threading.Thread(target=ws_app.run_forever, daemon=True)
    t.start()

    deadline = time.time() + timeout
    while not done and time.time() < deadline:
        time.sleep(0.1)

    ws_app.close()

    if error_msg:
        raise RuntimeError(f"TradingView error: {error_msg}")
    if not candles:
        raise RuntimeError("No candles received — check symbol/exchange.")

    df = pd.DataFrame(candles).set_index("datetime")
    return df


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch OHLC candle data directly from TradingView WebSocket"
    )
    parser.add_argument("symbol",   help="Ticker symbol, e.g. BTCUSDT")
    parser.add_argument("exchange", help="Exchange name, e.g. BINANCE")
    parser.add_argument("-i", "--interval",   default="1d",
                        help=f"Candle interval (default: 1d). Choices: {', '.join(INTERVALS)}")
    parser.add_argument("-n", "--bars",       default=100, type=int,
                        help="Number of bars to fetch (default: 100)")
    parser.add_argument("-t", "--auth-token", default="unauthorized_user_token",
                        help="TradingView auth token for premium access (optional)")
    parser.add_argument("--timeout",          default=15, type=int,
                        help="WebSocket timeout in seconds (default: 15)")
    parser.add_argument("-o", "--output",     default=None,
                        help="Save output to CSV file (optional)")
    args = parser.parse_args()

    print(f"Connecting to TradingView WebSocket...")
    print(f"Symbol: {args.exchange.upper()}:{args.symbol.upper()}  |  Interval: {args.interval}  |  Bars: {args.bars}\n")

    try:
        df = fetch_candles(
            symbol=args.symbol,
            exchange=args.exchange,
            interval=args.interval,
            n_bars=args.bars,
            auth_token=args.auth_token,
            timeout=args.timeout,
        )
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(df.to_string())
    print(f"\nFetched {len(df)} candles.")

    if args.output:
        df.to_csv(args.output)
        print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
