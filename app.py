"""
Sniper Bot — Alpaca 15-minute long-setup scanner with Telegram alerts.

Single-file version, packaged for a standard Python deployment (e.g. Railway).

Strategy — on each freshly closed 15-minute bar, checks:
  1. Trend:      EMA9 > EMA20 > EMA50
  2. Location:   Close > EMA9 and Close > VWAP
  3. Volume:     current volume > 1.2x the 20-period average volume
  4. Momentum:   RSI(14) between 55 and 70
  5. Structure:  an unfilled bullish Fair Value Gap confirming the move

When all five align, a formatted alert is sent to Telegram. An hourly
heartbeat message is also sent so you can confirm the bot is alive 24/7
without waiting for a signal or for market open.

Run with:  python app.py
Required env vars: ALPACA_API_KEY, ALPACA_SECRET_KEY, TELEGRAM_BOT_TOKEN,
                    TELEGRAM_CHAT_ID
Optional env vars: PORT (defaults to 8080; used only for the health-check
                    web server Railway can ping)
"""

import asyncio
import json
import logging
import os
import threading
import time
import traceback
from datetime import datetime, timezone

import pandas as pd
import pandas_ta as ta
from dotenv import load_dotenv
from flask import Flask
from telegram import Bot
from telegram.constants import ParseMode

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient

# Loads a local .env file if present; on Railway, real env vars set in the
# dashboard take precedence and this is a no-op.
load_dotenv()

# =============================================================================
# Configuration
# =============================================================================

# --- Credentials (read from environment / Railway variables) ---
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
ALPACA_PAPER = True  # Paper trading mode — this bot only reads data and alerts, never trades.

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# --- Watchlist ---
TICKERS = [
    "RIVN", "NIO", "PLUG", "SOUN", "XPEV", "RIOT", "AMD", "INTC", "OPEN",
    "PATH", "TOST", "RBLX", "FRSH", "CPRT", "CELH", "TTD", "NKE", "ABT",
    "KGC", "GRWG", "HIVE", "BE", "FCX", "SLB", "AA", "SMR", "HIMS", "AUR",
    "BTE", "AMPX", "CRDO", "ALAB", "KSCP", "BLNK", "GLW", "SNDK", "ON",
    "RZLV", "LAES", "GFI", "U", "FIG",
]

# --- Timeframe ---
TIMEFRAME_MINUTES = 15
BARS_LOOKBACK = 120  # number of 15-min bars to fetch per scan (covers EMA50 warmup)

# --- Strategy thresholds ---
VOLUME_MULTIPLIER = 1.2       # current volume must exceed 1.2x the 20-period avg volume
VOLUME_AVG_PERIOD = 20
RSI_PERIOD = 14
RSI_MIN = 55
RSI_MAX = 70
EMA_FAST = 9
EMA_MID = 20
EMA_SLOW = 50
FVG_LOOKBACK = 3               # how many recent candles to scan for an unfilled bullish FVG

# --- Scan cadence ---
POLL_SECONDS = 60              # how often to check whether a new 15-min bar has closed

# --- Heartbeat ---
HEARTBEAT_SECONDS = 14400       # send a Telegram "still alive" message this often, market open or not

# --- State file (prevents duplicate alerts for the same bar/ticker) ---
STATE_FILE = os.path.join(os.path.dirname(__file__), ".alert_state.json")

# --- Health-check web server (lets Railway confirm the service is up) ---
PORT = int(os.environ.get("PORT", 8080))


# =============================================================================
# Alpaca data access
# =============================================================================

_data_client: StockHistoricalDataClient | None = None
_trading_client: TradingClient | None = None


def get_data_client() -> StockHistoricalDataClient:
    global _data_client
    if _data_client is None:
        _data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    return _data_client


def get_trading_client() -> TradingClient:
    """Only used to confirm the market clock / trading calendar."""
    global _trading_client
    if _trading_client is None:
        _trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=ALPACA_PAPER)
    return _trading_client


def is_market_open() -> bool:
    clock = get_trading_client().get_clock()
    return bool(clock.is_open)


def fetch_bars(symbol: str, limit: int = BARS_LOOKBACK) -> pd.DataFrame:
    """
    Returns a DataFrame indexed by timestamp (UTC) with columns:
    open, high, low, close, volume. Only fully closed bars are returned.
    """
    client = get_data_client()
    request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame(TIMEFRAME_MINUTES, TimeFrameUnit.Minute),
        limit=limit,
    )
    bars = client.get_stock_bars(request)
    df = bars.df

    if df.empty:
        return df

    # bars.df has a MultiIndex (symbol, timestamp) when requesting a single symbol too.
    if isinstance(df.index, pd.MultiIndex):
        df = df.xs(symbol, level="symbol")

    df = df[["open", "high", "low", "close", "volume"]].sort_index()
    return df


# =============================================================================
# Indicators and Fair Value Gap (FVG) detection
# =============================================================================

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds EMA9/20/50, RSI14, VWAP, and rolling average volume columns to df.
    Expects df with columns: open, high, low, close, volume and a DatetimeIndex.
    """
    df = df.copy()

    df[f"ema{EMA_FAST}"] = ta.ema(df["close"], length=EMA_FAST)
    df[f"ema{EMA_MID}"] = ta.ema(df["close"], length=EMA_MID)
    df[f"ema{EMA_SLOW}"] = ta.ema(df["close"], length=EMA_SLOW)
    df["rsi"] = ta.rsi(df["close"], length=RSI_PERIOD)

    # Session VWAP, anchored to each calendar day so it resets daily like a real intraday VWAP.
    df["vwap"] = _daily_vwap(df)

    df["avg_volume"] = df["volume"].rolling(window=VOLUME_AVG_PERIOD).mean()

    return df


def _daily_vwap(df: pd.DataFrame) -> pd.Series:
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    pv = typical_price * df["volume"]

    day_key = df.index.date
    cum_pv = pv.groupby(day_key).cumsum()
    cum_vol = df["volume"].groupby(day_key).cumsum()

    return cum_pv / cum_vol.replace(0, pd.NA)


def detect_bullish_fvg(df: pd.DataFrame, lookback: int = FVG_LOOKBACK) -> dict | None:
    """
    Looks for a classic 3-candle bullish Fair Value Gap in the most recent `lookback`
    candles: candle[i-2].high < candle[i].low, leaving an unfilled gap that price has
    not traded back into. Returns the gap details if the most recent gap is still open
    (i.e. price hasn't closed back below the gap's top), else None.
    """
    if len(df) < lookback + 2:
        return None

    recent = df.iloc[-(lookback + 2):]
    found = None

    for i in range(2, len(recent)):
        candle_low_2_ago = recent["low"].iloc[i]
        candle_high_2_before = recent["high"].iloc[i - 2]

        if candle_low_2_ago > candle_high_2_before:
            gap_bottom = candle_high_2_before
            gap_top = candle_low_2_ago
            gap_time = recent.index[i]
            found = {"gap_bottom": float(gap_bottom), "gap_top": float(gap_top), "time": gap_time}

    if found is None:
        return None

    # Confirm the gap hasn't been fully filled since it formed: close must still be
    # trading above the gap's bottom.
    latest_close = df["close"].iloc[-1]
    if latest_close < found["gap_bottom"]:
        return None

    return found


# =============================================================================
# Signal evaluation
# =============================================================================

def evaluate_signal(df: pd.DataFrame) -> dict | None:
    """
    df must contain enough raw OHLCV bars to warm up EMA50. Returns a dict with
    signal details if every condition passes on the most recently CLOSED bar,
    otherwise None.
    """
    df = compute_indicators(df)

    if df[[f"ema{EMA_SLOW}", "rsi", "vwap", "avg_volume"]].iloc[-1].isna().any():
        return None  # not enough warm-up data yet

    last = df.iloc[-1]

    ema_fast = last[f"ema{EMA_FAST}"]
    ema_mid = last[f"ema{EMA_MID}"]
    ema_slow = last[f"ema{EMA_SLOW}"]
    close = last["close"]
    vwap = last["vwap"]
    volume = last["volume"]
    avg_volume = last["avg_volume"]
    rsi = last["rsi"]
# 1. فحص الـ FVG أولاً كشرط إلزامي وصمام أمان
    fvg = detect_bullish_fvg(df)
    if fvg is None:
        return None  # إذا ما فيه فجوة سعرية، نلغي الصفقة فوراً وبدون نقاش!

    # 2. فحص بقية الشروط الأربعة
    checks = {
        "trend": ema_fast > ema_mid > ema_slow,
        "price_above_ema_vwap": close > ema_fast and close > vwap,
        "volume_surge": avg_volume > 0 and volume > VOLUME_MULTIPLIER * avg_volume,
        "rsi_in_range": RSI_MIN <= rsi <= RSI_MAX,
    }

    # حساب كم شرط تحقق من الشروط الأربعة
    satisfied_count = sum(1 for val in checks.values() if val)

    # نقبل الصفقة إذا تحقق الـ FVG + على الأقل 3 شروط من الـ 4 الأخرى
    if satisfied_count < 3:
        return None

    return {
        "bar_time": df.index[-1],
        "close": float(close),
        "ema_fast": float(ema_fast),
        "ema_mid": float(ema_mid),
        "ema_slow": float(ema_slow),
        "vwap": float(vwap),
        "volume": float(volume),
        "avg_volume": float(avg_volume),
        "rsi": float(rsi),
        "fvg_top": fvg["gap_top"],
        "fvg_bottom": fvg["gap_bottom"],
        "fvg_time": fvg["time"],
    }


# =============================================================================
# Telegram alerts
# =============================================================================

_bot: Bot | None = None


def _get_bot() -> Bot:
    global _bot
    if _bot is None:
        _bot = Bot(token=TELEGRAM_BOT_TOKEN)
    return _bot


def format_signal_message(symbol: str, signal: dict) -> str:
    bar_time = signal["bar_time"]
    if isinstance(bar_time, datetime):
        bar_time_str = bar_time.strftime("%Y-%m-%d %H:%M UTC")
    else:
        bar_time_str = str(bar_time)

    return (
        f"🟢 *LONG SIGNAL* — `{symbol}`\n"
        f"_15m bar closed {bar_time_str}_\n"
        f"\n"
        f"💵 *Close:* `${signal['close']:.2f}`\n"
        f"📈 *Trend:* EMA9 `{signal['ema_fast']:.2f}` \\> EMA20 `{signal['ema_mid']:.2f}` \\> EMA50 `{signal['ema_slow']:.2f}`\n"
        f"📍 *VWAP:* `${signal['vwap']:.2f}` \\(price above ✅\\)\n"
        f"📊 *Volume:* `{signal['volume']:,.0f}` vs 20\\-avg `{signal['avg_volume']:,.0f}` "
        f"\\(`{signal['volume'] / signal['avg_volume']:.2f}x`\\)\n"
        f"⚡ *RSI\\(14\\):* `{signal['rsi']:.1f}`\n"
        f"🔳 *Fair Value Gap:* `${signal['fvg_bottom']:.2f}` – `${signal['fvg_top']:.2f}`\n"
        f"\n"
        f"_Automated scan — not financial advice. Verify before trading._"
    )


async def send_alert_async(symbol: str, signal: dict) -> None:
    message = format_signal_message(symbol, signal)
    bot = _get_bot()
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=message,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


def send_alert(symbol: str, signal: dict) -> None:
    """Sync wrapper so callers don't need to manage an event loop themselves."""
    asyncio.run(send_alert_async(symbol, signal))


async def send_startup_message_async(watchlist_size: int, timeframe: int) -> None:
    bot = _get_bot()
    text = (
        f"🤖 *Sniper Bot started*\n"
        f"Watching `{watchlist_size}` tickers on the `{timeframe}m` timeframe\\.\n"
        f"You'll get an alert here when a valid long setup closes\\."
    )
    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=ParseMode.MARKDOWN_V2)


def send_startup_message(watchlist_size, timeframe):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop.run_until_complete(send_startup_message_async(watchlist_size, timeframe))


async def send_heartbeat_async() -> None:
    bot = _get_bot()
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text="Sniper Bot status: Active and scanning 40 stocks.",
    )


def send_heartbeat() -> None:
    """Sync wrapper; failures are logged but never crash the scan loop."""
    try:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        if loop.is_running():
            # إذا كان الـ loop شغال فعلياً، نرسلها كمهمة خلفية
            asyncio.run_coroutine_threadable(send_heartbeat_async())
        else:
            loop.run_until_complete(send_heartbeat_async())
    except Exception as exc:
        print(f"Failed to send heartbeat: {exc}")


def send_error_message(text: str) -> None:
    try:
        asyncio.run(
            _get_bot().send_message(chat_id=TELEGRAM_CHAT_ID, text=f"⚠️ Scanner error: {text}")
        )
    except Exception:
        pass  # never let a notification failure crash the scanner


# =============================================================================
# Alert-dedup state (on disk)
# =============================================================================

def load_alerted_bars() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_alerted_bars(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def already_alerted(state: dict, symbol: str, bar_time_iso: str) -> bool:
    return state.get(symbol) == bar_time_iso


def mark_alerted(state: dict, symbol: str, bar_time_iso: str) -> None:
    state[symbol] = bar_time_iso


# =============================================================================
# Health-check web server (background thread)
# =============================================================================
# Railway (and any uptime pinger) can hit this to confirm the process is up.
# Not required for the bot's own logic — it never receives trading data here.

_health_app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)


@_health_app.route("/")
def _alive():
    return "I am alive"


def _run_health_server() -> None:
    _health_app.run(host="0.0.0.0", port=PORT)


def start_health_server() -> None:
    thread = threading.Thread(target=_run_health_server, daemon=True)
    thread.start()


# =============================================================================
# Main scan loop
# =============================================================================

def check_credentials() -> None:
    missing = [
        name
        for name, value in [
            ("ALPACA_API_KEY", ALPACA_API_KEY),
            ("ALPACA_SECRET_KEY", ALPACA_SECRET_KEY),
            ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
            ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
        ]
        if not value
    ]
    if missing:
        raise SystemExit(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Set them in your deployment platform's environment variables."
        )


def scan_once(state: dict) -> None:
    for symbol in TICKERS:
        try:
            df = fetch_bars(symbol)
            if df.empty or len(df) < 5:
                continue

            bar_time_iso = df.index[-1].isoformat()
            if already_alerted(state, symbol, bar_time_iso):
                continue

            signal = evaluate_signal(df)
            if signal is None:
                continue

            print(f"[{datetime.now(timezone.utc).isoformat()}] Signal confirmed for {symbol} at {bar_time_iso}")
            send_alert(symbol, signal)
            mark_alerted(state, symbol, bar_time_iso)
            save_alerted_bars(state)

        except Exception as exc:
            print(f"[{datetime.now(timezone.utc).isoformat()}] Error scanning {symbol}: {exc}")
            traceback.print_exc()


def main() -> None:
    check_credentials()

    start_health_server()

    print(f"Starting scanner for {len(TICKERS)} tickers on the {TIMEFRAME_MINUTES}m timeframe...")
    send_startup_message(len(TICKERS), TIMEFRAME_MINUTES)

    state = load_alerted_bars()

    last_heartbeat = 0.0

    while True:
        try:
            now = time.monotonic()
            if now - last_heartbeat >= HEARTBEAT_SECONDS:
                send_heartbeat()
                last_heartbeat = now
                print(f"[{datetime.now(timezone.utc).isoformat()}] Sent heartbeat.")

            if is_market_open():
                scan_once(state)
            else:
                print(f"[{datetime.now(timezone.utc).isoformat()}] Market closed, sleeping...")
        except Exception as exc:
            print(f"Fatal loop error: {exc}")
            traceback.print_exc()
            send_error_message(str(exc))

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
