"""
Sniper Bot — 30-minute long-setup scanner with Telegram alerts.
Customized output report, Filter Statistics, and DEBUG mode control.
Engineered for Railway / Cloud deployment.
"""

import asyncio
from collections import Counter
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
import pandas as pd
import pandas_ta as ta
import yfinance as yf

yf.set_tz_cache_location("/tmp")

from flask import Flask
from telegram import Bot

# =============================================================================
# Configuration & Controls
# =============================================================================

DEBUG = False

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

_raw_tickers = [
    "RIVN", "NIO", "PLUG", "SOUN", "XPEV", "RIOT", "AMD", "INTC", "OPEN", "PATH",
    "TOST", "RBLX", "FRSH", "CPRT", "CELH", "TTD", "NKE", "ABT", "KGC", "GRWG",
    "HIVE", "BE", "FCX", "SLB", "AA", "SMR", "HIMS", "AUR", "BTE", "AMPX",
    "CRDO", "ALAB", "KSCP", "BLNK", "GLW", "SNDK", "ON", "RZLV", "LAES", "GFI",
    "U", "FIG", "IOVA", "ERIC", "CMPS", "RLMD", "ALTO", "HELP", "JLHL", "NN",
    "CCRN", "SONO", "PESI", "SSRM", "PEGA", "SDGR", "TEM", "NBIS", "RKLB", "LUNR",
    "OUST", "AEHR", "ACLS", "CAMT", "PDFS", "FORM", "AMKR", "VECO", "VIAV", "S",
    "DOCN", "ENPH", "SEDG", "MRVL", "MTSI", "ALGM", "COHR", "AAOI", "CARG"
]

TICKERS = sorted(list(set(_raw_tickers)))

# ✅ تعديل 1: تغيير التايم فريم من 15 إلى 30 دقيقة
TIMEFRAME_MINUTES = 30

# ✅ تعديل 2: تخفيف شرط الشمعة من 50% إلى 40% عشان يمر أسهم أكثر
CANDLE_BODY_MIN = 0.4

VOLUME_MULTIPLIER = 1.3
VOLUME_AVG_PERIOD = 20
RSI_PERIOD = 14
RSI_MIN = 55
RSI_MAX = 75

EMA_FAST = 9
EMA_MID = 20
EMA_SLOW = 50
EMA_LONG = 200

ADX_MIN = 20
ADX_MAX = 50

MIN_DOLLAR_VOLUME = 1500000.0
MAX_EMA9_DISTANCE = 0.02
MAX_CANDLE_RANGE = 0.06
MIN_ATR_PCT = 0.006
DISTANCE_FROM_HIGH = 0.985
MAX_DAILY_RETURN = 0.08

POLL_SECONDS = 60
HEARTBEAT_SECONDS = 14400
STATE_FILE = os.path.join(os.path.dirname(__file__), ".alert_state.json")
PORT = int(os.environ.get("PORT", 8080))

# =============================================================================
# Bulk Data Fetching via yfinance
# =============================================================================

def fetch_all_bars_bulk(tickers_list: list) -> tuple[dict[str, pd.DataFrame], list[str]]:
    try:
        tickers_str = " ".join(tickers_list)
        # ✅ تعديل 3: تغيير interval من 15m إلى 30m
        data = yf.download(
            tickers=tickers_str,
            period="1mo",
            interval="30m",
            group_by="ticker",
            progress=False,
            auto_adjust=False
        )

        all_dfs = {}
        missing_tickers = []

        if data.empty:
            return {}, tickers_list

        if isinstance(data.columns, pd.MultiIndex):
            for ticker in tickers_list:
                if ticker in data.columns.levels[0]:
                    df_ticker = data[ticker].copy()
                    df_ticker.dropna(subset=["Close"], inplace=True)
                    if not df_ticker.empty and len(df_ticker) >= 5:
                        df_ticker.columns = [str(col).lower() for col in df_ticker.columns]
                        req_cols = ["open", "high", "low", "close", "volume"]
                        if all(col in df_ticker.columns for col in req_cols):
                            all_dfs[ticker] = df_ticker[req_cols].sort_index()
                        else:
                            missing_tickers.append(ticker)
                    else:
                        missing_tickers.append(ticker)
                else:
                    missing_tickers.append(ticker)
        else:
            df_single = data.copy()
            df_single.dropna(subset=["Close"], inplace=True)
            df_single.columns = [str(col).lower() for col in df_single.columns]
            req_cols = ["open", "high", "low", "close", "volume"]
            if all(col in df_single.columns for col in req_cols):
                all_dfs[tickers_list[0]] = df_single[req_cols].sort_index()
            else:
                missing_tickers.append(tickers_list[0])

        return all_dfs, missing_tickers

    except Exception as e:
        logging.error(f"خطأ أثناء التحميل الجماعي: {e}")
        return {}, tickers_list

# =============================================================================
# Indicators & Signal Evaluation
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
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=14)
    if adx_df is not None and "ADX_14" in adx_df.columns:
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

def evaluate_signal(df: pd.DataFrame) -> tuple[dict | None, str | None, dict]:
    df = compute_indicators(df)
    needed_cols = [f"ema{EMA_FAST}", f"ema{EMA_MID}", f"ema{EMA_SLOW}", f"ema{EMA_LONG}", "rsi", "vwap", "avg_volume", "adx", "atr"]

    last_row = df[needed_cols].iloc[-1]
    missing = last_row[last_row.isna()]

    filter_stats = {
        "EMA Trend": False,
        "VWAP": False,
        "RSI": False,
        "ADX": False,
        "Volume": False,
        "Breakout": False
    }

    if not missing.empty:
        return None, "Missing Indicators", filter_stats

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

    previous_10_bars = df.iloc[-11:-1] if len(df) >= 12 else df.iloc[:-1]
    highest_of_last_10 = previous_10_bars["high"].max() if not previous_10_bars.empty else high
    target_breakout_price = highest_of_last_10 * 1.002

    # إحصائيات الفلاتر
    if (ema_slow > ema_long) and (ema_fast > ema_mid > ema_slow) and (close > ema_fast):
        filter_stats["EMA Trend"] = True
    if close > vwap:
        filter_stats["VWAP"] = True
    if RSI_MIN <= rsi <= RSI_MAX:
        filter_stats["RSI"] = True
    if ADX_MIN <= adx <= ADX_MAX:
        filter_stats["ADX"] = True
    dollar_volume = close * volume
    rvol = volume / avg_volume if avg_volume > 0 else 0
    if (rvol > VOLUME_MULTIPLIER) and (volume > avg_volume) and (dollar_volume >= MIN_DOLLAR_VOLUME):
        filter_stats["Volume"] = True
    if close > target_breakout_price:
        filter_stats["Breakout"] = True

    # شروط الرفض
    candle_range = high - low
    if candle_range == 0:
        return None, "No Range Candle", filter_stats

    # ✅ تعديل 4: شرط الشمعة 40% بدل 50%
    strong_candle = (close - open_price) > candle_range * CANDLE_BODY_MIN
    if not strong_candle:
        return None, "Candle <40%", filter_stats

    candle_size_pct = candle_range / close
    if candle_size_pct > MAX_CANDLE_RANGE:
        return None, "Large Candle (>6%)", filter_stats

    if len(df) >= 2:
        last_2 = df.iloc[-2:]
        candle_1_range = last_2["high"].iloc[0] - last_2["low"].iloc[0]
        candle_2_range = last_2["high"].iloc[1] - last_2["low"].iloc[1]
        if candle_1_range > 0 and candle_2_range > 0:
            c1_bullish = last_2["close"].iloc[0] > last_2["open"].iloc[0]
            c2_bullish = last_2["close"].iloc[1] > last_2["open"].iloc[1]
            c1_body_pct = (last_2["close"].iloc[0] - last_2["open"].iloc[0]) / candle_1_range
            c2_body_pct = (last_2["close"].iloc[1] - last_2["open"].iloc[1]) / candle_2_range
            if c1_bullish and c2_bullish and c1_body_pct > 0.8 and c2_body_pct > 0.8:
                return None, "Exhaustion Candles", filter_stats

    if len(df) < 12:
        return None, "Insufficient Bars", filter_stats

    if close <= target_breakout_price:
        return None, "Breakout Failed", filter_stats

    if dollar_volume < MIN_DOLLAR_VOLUME:
        return None, "Low Dollar Volume", filter_stats

    atr_percent = atr / close
    if atr_percent < MIN_ATR_PCT:
        return None, "Low ATR", filter_stats

    current_day = df.index[-1].date()
    day_bars = df[df.index.date == current_day]
    if day_bars.empty:
        return None, "Daily Open Fail", filter_stats

    daily_open = day_bars["open"].iloc[0]
    daily_return = (close - daily_open) / daily_open
    if daily_return > MAX_DAILY_RETURN:
        return None, "Excessive Return (>8%)", filter_stats

    today_high = day_bars["high"].max()
    if close < today_high * DISTANCE_FROM_HIGH:
        return None, "Far From High", filter_stats

    if not (ema_slow > ema_long):
        return None, "EMA50 < EMA200", filter_stats

    if not (ema_fast > ema_mid > ema_slow):
        return None, "EMA Order Wrong", filter_stats

    if not (close > ema_fast):
        return None, "Below EMA9", filter_stats

    if not (close > vwap):
        return None, "Below VWAP", filter_stats

    distance_from_ema9 = (close - ema_fast) / ema_fast
    if distance_from_ema9 > MAX_EMA9_DISTANCE:
        return None, "Far From EMA9 (>2%)", filter_stats

    if not (rvol > VOLUME_MULTIPLIER and volume > avg_volume):
        return None, "Low Volume", filter_stats

    if not (RSI_MIN <= rsi <= RSI_MAX):
        return None, "RSI Out of Range", filter_stats

    if not (ADX_MIN <= adx <= ADX_MAX):
        return None, "ADX Out of Range", filter_stats

    return {
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
    }, None, filter_stats

# =============================================================================
# Telegram Alerts
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
        f"🎯 SNIPER BREAKOUT — {symbol}\n"
        f"سهم: {symbol}\n"
        f"الوقت: {bar_time_str}\n\n"
        f"💵 سعر الدخول: ${signal['close']:.2f}\n"
        f"🚀 صعود اليوم: +{signal['daily_return']:.2f}%\n"
        f"📈 الترند: EMA9 > EMA20 > EMA50 ✅\n"
        f"🛡 طويل الأجل: EMA50 > EMA200 ✅\n"
        f"📍 VWAP: ${signal['vwap']:.2f} ✅\n"
        f"📏 بُعد EMA9: {signal['distance_ema9']:.2f}%\n"
        f"📊 RVOL: {signal['volume'] / signal['avg_volume']:.2f}x\n"
        f"💰 Dollar Volume: ${signal['dollar_volume']:,.0f}\n"
        f"⚡ RSI: {signal['rsi']:.1f}\n"
        f"🔥 ADX: {signal['adx']:.1f}\n"
        f"🔳 اختراق 10 شمعات: فوق ${signal['highest_of_last_10']:.2f}\n\n"
        f"⚠️ تحقق من الشارت قبل الدخول."
    )

def safe_run_async(coro):
    try:
        asyncio.run(coro)
    except Exception as e:
        logging.error(f"Async error: {e}")

def send_alert(symbol: str, signal: dict) -> None:
    safe_run_async(send_alert_async(symbol, signal))

async def send_alert_async(symbol: str, signal: dict) -> None:
    await _get_bot().send_message(chat_id=TELEGRAM_CHAT_ID, text=format_signal_message(symbol, signal))

def send_startup_message(watchlist_size, timeframe):
    async def _send():
        text = (
            f"🤖 Sniper Bot شغال بنجاح!\n"
            f"يراقب {watchlist_size} سهم على الفريم {timeframe}m.\n"
            f"جاهز لإرسال الإشارات ✅"
        )
        await _get_bot().send_message(chat_id=TELEGRAM_CHAT_ID, text=text)
    try:
        safe_run_async(_send())
    except Exception as exc:
        logging.error(f"Startup message failed: {exc}")

def send_heartbeat() -> None:
    async def _send():
        await _get_bot().send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=f"✅ Sniper Bot شغال ويراقب {len(TICKERS)} سهم.",
        )
    try:
        safe_run_async(_send())
    except Exception as exc:
        logging.error(f"Heartbeat failed: {exc}")

def send_error_message(text: str) -> None:
    async def _send():
        await _get_bot().send_message(chat_id=TELEGRAM_CHAT_ID, text=f"⚠️ خطأ: {text}")
    try:
        safe_run_async(_send())
    except Exception:
        pass

# =============================================================================
# Alert State Tracking
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
# Health Check Server
# =============================================================================

_health_app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

@_health_app.route("/")
def _alive():
    return "I am alive"

def start_health_server() -> None:
    thread = threading.Thread(
        target=lambda: _health_app.run(host="0.0.0.0", port=PORT),
        daemon=True
    )
    thread.start()

# =============================================================================
# Market Hours & Scan Loop
# =============================================================================

def is_market_open() -> bool:
    now_utc = datetime.now(timezone.utc)
    if now_utc.weekday() >= 5:
        return False
    market_start = now_utc.replace(hour=13, minute=30, second=0, microsecond=0)
    market_end = now_utc.replace(hour=20, minute=0, second=0, microsecond=0)
    return market_start <= now_utc <= market_end

def check_credentials_soft() -> bool:
    missing = [
        name for name, value in [
            ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
            ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
        ] if not value
    ]
    if missing:
        logging.error(f"Missing ENVs: {', '.join(missing)}")
        return False
    return True

def scan_once(state: dict) -> None:
    t_total_start = time.monotonic()

    t_download_start = time.monotonic()
    all_dfs, missing_tickers = fetch_all_bars_bulk(TICKERS)
    download_time = time.monotonic() - t_download_start

    t_analysis_start = time.monotonic()
    signals_count = 0
    rejected_count = 0
    reasons_counter = Counter()

    filter_pass_counts = {
        "EMA Trend": 0,
        "VWAP": 0,
        "RSI": 0,
        "ADX": 0,
        "Volume": 0,
        "Breakout": 0
    }

    scanned_count = len(TICKERS)

    for symbol in TICKERS:
        if symbol in missing_tickers or symbol not in all_dfs:
            reasons_counter["Missing Data"] += 1
            rejected_count += 1
            continue

        df = all_dfs[symbol]
        bar_time_iso = df.index[-1].isoformat()

        if already_alerted(state, symbol, bar_time_iso):
            reasons_counter["Already Alerted"] += 1
            rejected_count += 1
            continue

        signal, reject_reason, f_stats = evaluate_signal(df)

        for key, val in f_stats.items():
            if val:
                filter_pass_counts[key] += 1

        if signal is None:
            reasons_counter[reject_reason] += 1
            rejected_count += 1
            if DEBUG:
                logging.info(f"{symbol} ❌ {reject_reason}")
            continue

        if DEBUG:
            logging.info(f"{symbol} ✅ PASS @ ${signal['close']:.2f}")

        send_alert(symbol, signal)
        mark_alerted(state, symbol, bar_time_iso)
        save_alerted_bars(state)
        signals_count += 1

    analysis_time = time.monotonic() - t_analysis_start
    total_time = time.monotonic() - t_total_start

    report = []
    report.append("\n========== Scan Summary ==========")
    report.append(f"Scanned  : {scanned_count}")
    report.append(f"Signals  : {signals_count}")
    report.append(f"Rejected : {rejected_count}\n")
    report.append("Top Reject Reasons\n")
    for idx, (reason, count) in enumerate(reasons_counter.most_common(), start=1):
        pct = (count / scanned_count) * 100 if scanned_count > 0 else 0
        report.append(f"{idx}- {reason:<22} : {count:<2} ({pct:.0f}%)")
    report.append("\n============================\n")
    report.append("Filter Statistics\n")
    for f_name in ["EMA Trend", "VWAP", "RSI", "ADX", "Volume", "Breakout"]:
        report.append(f"{f_name:<16} : {filter_pass_counts[f_name]} PASS")
    report.append("\n============================\n")
    report.append(f"Download : {download_time:.2f}s")
    report.append(f"Analysis : {analysis_time:.2f}s")
    report.append(f"Total    : {total_time:.2f}s\n")

    logging.info("\n".join(report))

def main() -> None:
    start_health_server()
    if not check_credentials_soft():
        while True:
            time.sleep(3600)

    send_startup_message(len(TICKERS), TIMEFRAME_MINUTES)

    state = load_alerted_bars()
    last_heartbeat = 0.0
    last_scan_minute = -1

    while True:
        try:
            now = time.monotonic()
            if now - last_heartbeat >= HEARTBEAT_SECONDS:
                send_heartbeat()
                last_heartbeat = now

            current_time = datetime.now(timezone.utc)
            current_minute = current_time.minute

            # ✅ تعديل 5: الفحص كل 30 دقيقة بدل 15
            if current_minute % 30 == 0 and current_minute != last_scan_minute:
                if is_market_open():
                    logging.info(f"⏰ [{current_time.strftime('%H:%M')}] بدء الفحص...")
                    time.sleep(10)  # انتظر 10 ثواني للتأكد من إغلاق الشمعة
                    scan_once(state)
                else:
                    logging.info(f"💤 السوق مغلق...")
                last_scan_minute = current_minute

            time.sleep(10)

        except Exception as exc:
            logging.exception(f"Main loop error: {exc}")
            send_error_message(str(exc))
            time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()