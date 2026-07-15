"""
Sniper Bot — Alpaca 15-minute long-setup scanner with Telegram alerts.
Fully customized with Abdulaziz's strict breakout and momentum rules.

Strategy checks on each freshly closed 15-minute bar:
  1. Strength:   Last candle is strong (body >= 60% of total range).
  2. Breakout:   Close price breaks above the highest high of the previous 10 candles.
  3. Safety:     Stock is NOT up more than 4% from the daily opening price.
  4. Trend:      EMA9 > EMA20 > EMA50.
  5. VWAP:       Close price is above VWAP.
  6. Volume:     RVOL > 1.7 (current volume > 1.7x the 20-period average volume).
  7. Momentum:   RSI(14) is strictly between 58 and 68.
  8. Trend Str:  ADX(14) > 25 (confirming a strong active trend).
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

# Loads env variables
load_dotenv()

# =============================================================================
# Configuration
# =============================================================================

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
ALPACA_PAPER = True  

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

# --- Timeframe & Warmup ---
TIMEFRAME_MINUTES = 15
BARS_LOOKBACK = 120  

# --- Strategy thresholds ---
VOLUME_MULTIPLIER = 1.7       # RVOL > 1.7
VOLUME_AVG_PERIOD = 20
RSI_PERIOD = 14
RSI_MIN = 58
RSI_MAX = 68
EMA_FAST = 9
EMA_MID = 20
EMA_SLOW = 50
ADX_THRESHOLD = 25
MAX_DAILY_RETURN = 0.04       # 4% Max from Daily Open

# --- Scan cadence ---
POLL_SECONDS = 60              
HEARTBEAT_SECONDS = 14400       

# --- State & Ports ---
STATE_FILE = os.path.join(os.path.dirname(__file__), ".alert_state.json")
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
    global _trading_client
    if _trading_client is None:
        _trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=ALPACA_PAPER)
    return _trading_client


def is_market_open() -> bool:
    clock = get_trading_client().get_clock()
    return bool(clock.is_open)


def fetch_bars(symbol: str, limit: int = BARS_LOOKBACK) -> pd.DataFrame:
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

    if isinstance(df.index, pd.MultiIndex):
        df = df.xs(symbol, level="symbol")

    df = df[["open", "high", "low", "close", "volume"]].sort_index()
    return df


# =============================================================================
# Indicators
# =============================================================================

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df[f"ema{EMA_FAST}"] = ta.ema(df["close"], length=EMA_FAST)
    df[f"ema{EMA_MID}"] = ta.ema(df["close"], length=EMA_MID)
    df[f"ema{EMA_SLOW}"] = ta.ema(df["close"], length=EMA_SLOW)
    df["rsi"] = ta.rsi(df["close"], length=RSI_PERIOD)
    df["vwap"] = _daily_vwap(df)
    df["avg_volume"] = df["volume"].rolling(window=VOLUME_AVG_PERIOD).mean()
    
    # ADX Calculation
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=14)
    if adx_df is not None:
        df["adx"] = adx_df["ADX_14"]
    else:
        df["adx"] = 0

    return df


def _daily_vwap(df: pd.DataFrame) -> pd.Series:
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    pv = typical_price * df["volume"]

    day_key = df.index.date
    cum_pv = pv.groupby(day_key).cumsum()
    cum_vol = df["volume"].groupby(day_key).cumsum()

    return cum_pv / cum_vol.replace(0, pd.NA)


# =============================================================================
# Signal evaluation
# =============================================================================

def evaluate_signal(df: pd.DataFrame) -> dict | None:
    df = compute_indicators(df)

    if df[[f"ema{EMA_SLOW}", "rsi", "vwap", "avg_volume", "adx"]].iloc[-1].isna().any():
        return None  

    last = df.iloc[-1]
    
    close = last["close"]
    open_price = last["open"]
    high = last["high"]
    low = last["low"]
    volume = last["volume"]
    avg_volume = last["avg_volume"]
    rsi = last["rsi"]
    vwap = last["vwap"]
    adx = last["adx"]
    
    ema_fast = last[f"ema{EMA_FAST}"]
    ema_mid = last[f"ema{EMA_MID}"]
    ema_slow = last[f"ema{EMA_SLOW}"]

    # 1. فحص الشمعة الصاعدة القوية (جسم الشمعة أكبر من 60% من مداها الكامل)
    strong_candle = (close - open_price) > (high - low) * 0.6
    if not strong_candle:
        return None

    # 2. اختراق أعلى قمة لآخر 10 شمعات (باستثناء الشمعة الحالية المغلقة)
    previous_10_bars = df.iloc[-11:-1]
    highest_of_last_10 = previous_10_bars["high"].max()
    is_breakout = close > highest_of_last_10
    if not is_breakout:
        return None

    # 3. لا يكون مرتفعاً أكثر من 4% عن افتتاح اليوم الحالي
    current_day = df.index[-1].date()
    day_bars = df[df.index.date == current_day]
    if day_bars.empty:
        return None
    daily_open = day_bars["open"].iloc[0]  
    daily_return = (close - daily_open) / daily_open
    if daily_return > MAX_DAILY_RETURN:
        return None

    # 4. الشروط الفنية الصارمة المتبقية (يجب أن تتحقق جميعها)
    checks = {
        "trend": ema_fast > ema_mid > ema_slow,                         # EMA 9 > 20 > 50
        "above_vwap": close > vwap,                                     # الإغلاق فوق الـ VWAP
        "rvol": avg_volume > 0 and volume > VOLUME_MULTIPLIER * avg_volume, # RVOL > 1.7
        "adx_strong": adx > ADX_THRESHOLD,                              # ADX > 25
        "rsi_range": RSI_MIN <= rsi <= RSI_MAX                         # RSI بين 58 و 68
    }

    if not all(checks.values()):
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
        "adx": float(adx),
        "daily_return": float(daily_return * 100),
        "highest_of_last_10": float(highest_of_last_10),
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
    bar_time_str = bar_time.strftime("%Y-%m-%d %H:%M UTC") if isinstance(bar_time, datetime) else str(bar_time)

    return (
        f"🎯 *SNIPER BREAKOUT SIGNAL* — `{symbol}`\n"
        f"_15m bar closed {bar_time_str}_\n"
        f"\n"
        f"💵 *Close Price:* `${signal['close']:.2f}`\n"
        f"🚀 *Daily Change:* `+{signal['daily_return']:.2f}%` \\(<4% Rule ✅\\)\n"
        f"📈 *Trend:* EMA9 `{signal['ema_fast']:.2f}` \\> EMA20 `{signal['ema_mid']:.2f}` \\> EMA50 `{signal['ema_slow']:.2f}`\n"
        f"📍 *VWAP:* `${signal['vwap']:.2f}` \\(price above ✅\\)\n"
        f"📊 *RVOL:* `{signal['volume'] / signal['avg_volume']:.2f}x` \\(Target > 1.7x ✅\\)\n"
        f"⚡ *RSI\\(14\\):* `{signal['rsi']:.1f}` \\(Target: 58-68 ✅\\)\n"
        f"🔥 *ADX Trend Strength:* `{signal['adx']:.1f}` \\(Target > 25 ✅\\)\n"
        f"🔳 *10-Bar Breakout:* Above `${signal['highest_of_last_10']:.2f}` ✅\n"
        f"\n"
        f"_Automated scan — not financial advice. Verify before trading._"
    )


def safe_run_async(coro):
    """طريقة آمنة لتشغيل الدوال غير المتزامنة لحل مشاكل الـ Event Loop نهائياً."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
    if loop.is_running():
        # تشغيل الدالة في الـ Loop الحالي إذا كان قيد التشغيل بالفعل
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()
    else:
        return loop.run_until_complete(coro)


def send_alert(symbol: str, signal: dict) -> None:
    safe_run_async(send_alert_async(symbol, signal))


async def send_alert_async(symbol: str, signal: dict) -> None:
    message = format_signal_message(symbol, signal)
    bot = _get_bot()
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=message,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


def send_startup_message(watchlist_size, timeframe):
    async def _send():
        bot = _get_bot()
        text = (
            f"🤖 *Sniper Bot started*\n"
            f"Watching `{watchlist_size}` tickers on the `{timeframe}m` timeframe\\.\n"
            f"Using Abdulaziz's strict breakout strategy\\."
        )
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=ParseMode.MARKDOWN_V2)
    
    try:
        safe_run_async(_send())
    except Exception as exc:
        print(f"Failed to send startup message: {exc}")


async def send_heartbeat_async() -> None:
    bot = _get_bot()
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text="Sniper Bot status: Active and scanning 42 stocks.",
    )


def send_heartbeat() -> None:
    try:
        safe_run_async(send_heartbeat_async())
    except Exception as exc:
        print(f"Failed to send heartbeat: {exc}")


def send_error_message(text: str) -> None:
    async def _send():
        await _get_bot().send_message(chat_id=TELEGRAM_CHAT_ID, text=f"⚠️ Scanner error: {text}")
    try:
        safe_run_async(_send())
    except Exception:
        pass  


# =============================================================================
# Alert-dedup state
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
# Health-check web server
# =============================================================================

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
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}.")


def scan_once(state: dict) -> None:
    for symbol in TICKERS:
        print(f"scanning {symbol}")
      try:
        df = fetch_bars(symbol)
        print(f"{symbol}: {len(df)} bars")
                continue

            bar_time_iso = df.index[-1].isoformat()
            if already_alerted(state, symbol, bar_time_iso):
                continue

            signal = evaluate_signal(df)
            if signal is None:
              print(f"{symbol}: no signal")
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
