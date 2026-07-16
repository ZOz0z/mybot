"""
Sniper Bot — Alpaca 15-minute long-setup scanner with Telegram alerts.
Fully customized with Abdulaziz's ultra-strict, institutional-grade breakout rules.

Strategy checks on each freshly closed 15-minute bar:
  1. Base Strength: Last candle body >= 60% of total range.
  2. Breakout:      Close price breaks and closes above the highest high of the previous 10 candles.
  3. Safety:        Daily return <= 4% from open.
  4. Trend:         EMA9 > EMA20 > EMA50 and EMA50 > EMA200.
  5. VWAP & EMA:    Close price is above EMA9 and above VWAP.
  6. Distance:      Close price is <= 2% away from EMA9.
  7. Volume Surge:  RVOL > 1.5 AND current volume > 20-period average volume.
  8. Dollar Volume: Close * Volume > $10,000,000.
  9. Exhaustion (2):The last two candles are NOT consecutive bullish candles with body >= 80% of range.
  10. Exhaustion (1):The last candle's total range (High-Low)/Close is <= 6%.
  11. Momentum:     RSI(14) strictly between 58 and 68.
  12. Trend Strength:ADX(14) > 25.
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

# --- Timeframe & Warmup (Increase lookback slightly to warm up EMA200 properly) ---
TIMEFRAME_MINUTES = 15
BARS_LOOKBACK = 250  

# --- Strategy thresholds ---
VOLUME_MULTIPLIER = 1.5       # RVOL > 1.5
VOLUME_AVG_PERIOD = 20
RSI_PERIOD = 14
RSI_MIN = 58
RSI_MAX = 68
EMA_FAST = 9
EMA_MID = 20
EMA_SLOW = 50
EMA_LONG = 200
ADX_THRESHOLD = 25
MAX_DAILY_RETURN = 0.04       # 4% Max from Daily Open
MAX_EMA9_DISTANCE = 0.02      # 2% Max distance from EMA9
MAX_CANDLE_RANGE = 0.06       # 6% Max candle range (High-Low)/Close
MIN_DOLLAR_VOLUME = 15000000.0 # $15,000,000 Min liquidity

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
    df[f"ema{EMA_LONG}"] = ta.ema(df["close"], length=EMA_LONG)
    df["rsi"] = ta.rsi(df["close"], length=RSI_PERIOD)
    df["vwap"] = _daily_vwap(df)
    df["avg_volume"] = df["volume"].rolling(window=VOLUME_AVG_PERIOD).mean()
    df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
    
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

def evaluate_signal(df: pd.DataFrame) -> tuple[dict | None, str | None]:
    """
    Evaluates trading conditions using optimized Early Returns (Early Dismissal)
    to save CPU resources, and records the exact reason if rejected.
    Returns: (signal_dict, rejection_reason_string)
    """
    # 1. أولاً: نقوم بحساب المؤشرات السريعة الأساسية قبل المعقدة لسرعة الرفض
    df = compute_indicators(df)

    # التأكد من توفر البيانات الكافية لـ EMA200 وبقية المؤشرات
    needed_cols = [f"ema{EMA_FAST}", f"ema{EMA_MID}", f"ema{EMA_SLOW}", f"ema{EMA_LONG}", "rsi", "vwap", "avg_volume", "adx", "atr"]
    if df[needed_cols].iloc[-1].isna().any():
        return None, "عدم توفر بيانات المؤشرات الكافية"

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
    atr = last["atr"]
    
    ema_fast = last[f"ema{EMA_FAST}"]
    ema_mid = last[f"ema{EMA_MID}"]
    ema_slow = last[f"ema{EMA_SLOW}"]
    ema_long = last[f"ema{EMA_LONG}"]

    # --- 1) فحص الشمعة الصاعدة القوية (قاعدة الـ 60%) ---
    candle_range = high - low
    if candle_range == 0:
        return None, "شمعة بلا مدى"
    
    strong_candle = (close - open_price) > candle_range * 0.6
    if not strong_candle:
        return None, "❌ الشمعة ليست قوية (أقل من 60% صعود)"

    # --- 2) فلتر الشمعة الكبيرة جداً (High-Low)/Close > 6% ---
    candle_size_pct = candle_range / close
    if candle_size_pct > MAX_CANDLE_RANGE:
        return None, f"❌ شمعة عملاقة جداً ({candle_size_pct*100:.1f}%)"

    # --- 3) فلتر آخر شمعتين (الاستنزاف: شمعتين متتاليتين جسم كل منهما > 80%) ---
    if len(df) >= 2:
        last_2 = df.iloc[-2:]
        candle_1_range = last_2["high"].iloc[0] - last_2["low"].iloc[0]
        candle_2_range = last_2["high"].iloc[1] - last_2["low"].iloc[1]
        
        # التأكد من أنهما صاعدتان وبمدى حقيقي
        if candle_1_range > 0 and candle_2_range > 0:
            c1_bullish = last_2["close"].iloc[0] > last_2["open"].iloc[0]
            c2_bullish = last_2["close"].iloc[1] > last_2["open"].iloc[1]
            
            c1_body_pct = (last_2["close"].iloc[0] - last_2["open"].iloc[0]) / candle_1_range
            c2_body_pct = (last_2["close"].iloc[1] - last_2["open"].iloc[1]) / candle_2_range
            
            if c1_bullish and c2_bullish and c1_body_pct > 0.8 and c2_body_pct > 0.8:
                return None, "❌ شمعتان استنزافيتان متتاليتان (>80% جسم)"

    # --- 4) اختراق وإغلاق فوق أعلى قمة لآخر 10 شمعات سابقة ---
    if len(df) < 12:  # تصحيح منطق الشرط ليكون أقل من 12 وليس أكبر من أو يساوي
        return None, "عدم توفر شمعات كافية للاختراق"
    
    previous_10_bars = df.iloc[-11:-1]
    highest_of_last_10 = previous_10_bars["high"].max()
    is_breakout = close > highest_of_last_10 * 1.001
    if not is_breakout:
        return None, f"❌ لم يغلق فوق قمة الـ 10 شمعات ({highest_of_last_10:.2f})"

    # --- 5) فلتر الـ Dollar Volume والـ ATR ---
    dollar_volume = close * volume
    if dollar_volume < MIN_DOLLAR_VOLUME:
        return None, f"❌ سيولة ضعيفة (${dollar_volume:,.0f})"
    
    atr_percent = atr / close
    if atr_percent < 0.015:
        return None, f"❌ منخفض ATR ({atr_percent*100:.2f}%)"

    # --- 6) لا يكون مرتفعاً أكثر من 4% عن افتتاح اليوم الحالي وفلتر القمة اليومية ---
    current_day = df.index[-1].date()
    day_bars = df[df.index.date == current_day]
    if day_bars.empty:
        return None, "فشل العثور على افتتاح اليوم"
    
    daily_open = day_bars["open"].iloc[0]  
    daily_return = (close - daily_open) / daily_open
    if daily_return > MAX_DAILY_RETURN:
        return None, f"❌ صعود مفرط اليوم ({daily_return*100:.1f}%)"
    
    today_high = day_bars["high"].max()
    if close < today_high * 0.995:
        return None, "بعيد عن اعلى سعر ❌"

    # --- 7) الاتجاه طويل الأجل وصاعد (EMA50 > EMA200) ---
    if not (ema_slow > ema_long):
        return None, f"❌ ترند هابط EMA50 < EMA200"

    # --- 8) ترتيب المتوسطات (EMA9 > EMA20 > EMA50) ---
    if not (ema_fast > ema_mid > ema_slow):
        return None, "❌ ترتيب المتوسطات EMA9 > 20 > 50"

    # --- 9) السعر فوق EMA9 وفوق VWAP ---
    if not (close > ema_fast):
        return None, "❌ الإغلاق تحت EMA9"
    if not (close > vwap):
        return None, "❌ الإغلاق تحت VWAP"

    # --- 10) فلتر المسافة عن الـ EMA9 (أقل من أو يساوي 2%) ---
    distance_from_ema9 = (close - ema_fast) / ema_fast
    if distance_from_ema9 > MAX_EMA9_DISTANCE:
        return None, f"❌ بعيد عن EMA9 ({distance_from_ema9*100:.2f}%)"

    # --- 11) تعديل شرط الحجم (RVOL > 1.5 AND Volume > Avg_Volume) ---
    rvol = volume / avg_volume if avg_volume > 0 else 0
    if not (rvol > VOLUME_MULTIPLIER and volume > avg_volume):
        return None, f"❌ حجم التداول ضعيف (RVOL: {rvol:.2f}x)"

    # --- 12) نطاق RSI (بين 58 و 68) ---
    if not (RSI_MIN <= rsi <= RSI_MAX):
        return None, f"❌ مؤشر RSI خارج النطاق ({rsi:.1f})"

    # --- 13) قوة الترند ADX > 25 ---
    if not (adx > ADX_THRESHOLD):
        return None, f"❌ اتجاه ضعيف ADX ({adx:.1f})"

    # إذا تم اجتياز جميع الشروط الصارمة بنجاح، يُعاد التنبيه
    signal_data = {
        "bar_time": df.index[-1],
        "close": float(close),
        "ema_fast": float(ema_fast),
        "ema_mid": float(ema_mid),
        "ema_slow": float(ema_slow),
        "ema_long": float(ema_long),
        "vwap": float(vwap),
        "volume": float(volume),
        "avg_volume": float(avg_volume),
        "rsi": float(rsi),
        "adx": float(adx),
        "daily_return": float(daily_return * 100),
        "highest_of_last_10": float(highest_of_last_10),
        "dollar_volume": float(dollar_volume),
        "distance_ema9": float(distance_from_ema9 * 100)
    }
    return signal_data, None


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
        f"🛡️ *Trend filter:* EMA50 \\> EMA200 `{signal['ema_long']:.2f}` ✅\n"
        f"📍 *VWAP:* `${signal['vwap']:.2f}` \\(price above ✅\\)\n"
        f"📏 *EMA9 Distance:* `{signal['distance_ema9']:.2f}%` \\(<2% Rule ✅\\)\n"
        f"📊 *RVOL:* `{signal['volume'] / signal['avg_volume']:.2f}x` \\(Target > 1.5x ✅\\)\n"
        f"💰 *Dollar Volume:* `${signal['dollar_volume']:,.0f}` \\(Target > 15M ✅\\)\n"
        f"⚡ *RSI\\(14\\):* `{signal['rsi']:.1f}` \\(Target: 58-68 ✅\\)\n"
        f"🔥 *ADX Trend Strength:* `{signal['adx']:.1f}` \\(Target > 25 ✅\\)\n"
        f"🔳 *10-Bar Breakout:* Above `${signal['highest_of_last_10']:.2f}` ✅\n"
        f"\n"
        f"_Automated scan — not financial advice. Verify before trading._"
    )


def safe_run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
    if loop.is_running():
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
            f"Using Abdulaziz's ultra\\-strict breakout strategy\\."
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
    start_time = time.monotonic()
    market_open = is_market_open()
    
    print("\n=========================")
    print("Starting new scan...")
    print(f"Market Open: {market_open}")
    print(f"Scanning {len(TICKERS)} stocks")
    print("=========================")

    signals_count = 0
    rejected_count = 0

    for symbol in TICKERS:
        try:
            df = fetch_bars(symbol)
            if df.empty or len(df) < 5:
                print(f"{symbol} ❌ لا توجد بيانات كافية")
                rejected_count += 1
                continue

            bar_time_iso = df.index[-1].isoformat()
            if already_alerted(state, symbol, bar_time_iso):
                print(f"{symbol} ❌ تم الإرسال مسبقاً")
                rejected_count += 1
                continue

            # تقييم الإشارة الفنية الصارمة وجلب سبب الرفض
            signal, reject_reason = evaluate_signal(df)
            
            if signal is None:
                print(f"{symbol} {reject_reason}")
                rejected_count += 1
                continue

            # تحسين سجل اللوق عند قبول الإشارة
            print("\n=========================")
            print("✅ SIGNAL FOUND")
            print(f"Ticker:          {symbol}")
            print(f"Price:           ${signal['close']:.2f}")
            print(f"RSI:             {signal['rsi']:.1f}")
            print(f"ADX:             {signal['adx']:.1f}")
            print(f"RVOL:            {signal['volume']/signal['avg_volume']:.2f}x")
            print(f"Dollar Volume:   ${signal['dollar_volume']:,.0f}")
            print(f"Distance EMA9:   {signal['distance_ema9']:.2f}%")
            print(f"Daily Change:    +{signal['daily_return']:.2f}%")
            print(f"Breakout Price:  ${signal['highest_of_last_10']:.2f}")
            print("=========================\n")

            send_alert(symbol, signal)
            mark_alerted(state, symbol, bar_time_iso)
            save_alerted_bars(state)
            signals_count += 1

        except Exception as exc:
            print(f"[{datetime.now(timezone.utc).isoformat()}] Error scanning {symbol}: {exc}")
            traceback.print_exc()
            rejected_count += 1

    elapsed_time = time.monotonic() - start_time
    print("=========================")
    print("Scan Finished")
    print(f"Signals Found: {signals_count}")
    print(f"Rejected: {rejected_count}")
    print(f"Time: {elapsed_time:.1f} sec")
    print("=========================\n")


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
