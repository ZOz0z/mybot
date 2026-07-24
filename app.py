"""
Sniper Bot — 30-minute long-setup scanner with Telegram alerts.

إصلاحات هذه النسخة:
  1. ✅ استبعاد الشمعة غير المكتملة (الباق الذي كان يمنع كل الإشارات)
  2. ✅ اللوق على stdout بدل stderr (يمنع اللون الأحمر في Railway)
  3. ✅ إضافة القوة النسبية مقابل QQQ (بدون طلب إضافي لياهو)
  4. ✅ حساب وقف الخسارة والهدف تلقائياً بناءً على ATR
"""

import asyncio
from collections import Counter
import json
import logging
import os
import sys
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

# ✅ إصلاح 2: توجيه اللوق إلى stdout بدل stderr
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    stream=sys.stdout,
)

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

# مؤشر السوق العام — يُحمّل ضمن نفس الطلب، بدون طلب إضافي لياهو
BENCHMARK = "QQQ"

TIMEFRAME_MINUTES = 30

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
MAX_EMA9_DISTANCE = 0.04
MAX_CANDLE_RANGE = 0.06
MIN_ATR_PCT = 0.006
DISTANCE_FROM_HIGH = 0.97
MAX_DAILY_RETURN = 0.08

# ✅ إصلاح 4: إدارة المخاطر
ATR_STOP_MULT = 1.5      # وقف الخسارة = السعر - 1.5 × ATR
RISK_REWARD = 2.0        # الهدف = ضعف المخاطرة
MAX_LOSS_PCT = 0.02      # لا تسمح بوقف خسارة أبعد من 2%

POLL_SECONDS = 60
HEARTBEAT_SECONDS = 14400
STATE_FILE = os.path.join(os.path.dirname(__file__), ".alert_state.json")
PORT = int(os.environ.get("PORT", 8080))

# =============================================================================
# Bulk Data Fetching via yfinance
# =============================================================================

def fetch_all_bars_bulk(tickers_list: list) -> tuple[dict[str, pd.DataFrame], list[str]]:
    """
    تحميل جماعي بطلب واحد. يستبعد الشمعة الأخيرة غير المكتملة.
    """
    try:
        # نضيف مؤشر السوق داخل نفس الطلب — لا طلب إضافي
        download_list = list(dict.fromkeys(tickers_list + [BENCHMARK]))
        tickers_str = " ".join(download_list)

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

        req_cols = ["open", "high", "low", "close", "volume"]

        def _clean(df_in: pd.DataFrame) -> pd.DataFrame | None:
            df_t = df_in.copy()
            df_t.dropna(subset=["Close"], inplace=True)
            if df_t.empty:
                return None
            df_t.columns = [str(c).lower() for c in df_t.columns]
            if not all(c in df_t.columns for c in req_cols):
                return None
            df_t = df_t[req_cols].sort_index()

            # ✅ إصلاح 1: استبعاد الشمعة الأخيرة (غير مكتملة)
            if len(df_t) > 1:
                df_t = df_t.iloc[:-1]

            if len(df_t) < 5:
                return None
            return df_t

        if isinstance(data.columns, pd.MultiIndex):
            available = set(data.columns.levels[0])
            for ticker in download_list:
                if ticker in available:
                    cleaned = _clean(data[ticker])
                    if cleaned is not None:
                        all_dfs[ticker] = cleaned
                    elif ticker != BENCHMARK:
                        missing_tickers.append(ticker)
                elif ticker != BENCHMARK:
                    missing_tickers.append(ticker)
        else:
            cleaned = _clean(data)
            if cleaned is not None:
                all_dfs[download_list[0]] = cleaned
            else:
                missing_tickers.append(download_list[0])

        return all_dfs, missing_tickers

    except Exception as e:
        logging.error(f"خطأ أثناء التحميل الجماعي: {e}")
        return {}, tickers_list


def compute_benchmark_return(all_dfs: dict) -> float | None:
    """أداء مؤشر السوق اليوم بالنسبة المئوية."""
    df = all_dfs.get(BENCHMARK)
    if df is None or df.empty:
        return None
    try:
        day = df.index[-1].date()
        day_bars = df[df.index.date == day]
        if day_bars.empty:
            return None
        open_p = day_bars["open"].iloc[0]
        close_p = day_bars["close"].iloc[-1]
        return float((close_p - open_p) / open_p * 100)
    except Exception:
        return None

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

# =============================================================================
# Signal evaluation
# =============================================================================

def evaluate_signal(df: pd.DataFrame, bench_return: float | None = None) -> tuple[dict | None, str | None, dict]:
    df = compute_indicators(df)
    needed_cols = [f"ema{EMA_FAST}", f"ema{EMA_MID}", f"ema{EMA_SLOW}",
                   f"ema{EMA_LONG}", "rsi", "vwap", "avg_volume", "adx", "atr"]

    filter_stats = {
        "EMA Trend": False, "VWAP": False, "RSI": False,
        "ADX": False, "Volume": False, "Breakout": False
    }

    last_row = df[needed_cols].iloc[-1]
    missing = last_row[last_row.isna()]
    if not missing.empty:
        return None, "Missing Indicators", filter_stats

    last = df.iloc[-1]
    close, open_price = last["close"], last["open"]
    high, low = last["high"], last["low"]
    volume, avg_volume = last["volume"], last["avg_volume"]
    rsi, vwap, adx, atr = last["rsi"], last["vwap"], last["adx"], last["atr"]

    ema_fast = last[f"ema{EMA_FAST}"]
    ema_mid = last[f"ema{EMA_MID}"]
    ema_slow = last[f"ema{EMA_SLOW}"]
    ema_long = last[f"ema{EMA_LONG}"]

    previous_10_bars = df.iloc[-11:-1] if len(df) >= 12 else df.iloc[:-1]
    highest_of_last_10 = previous_10_bars["high"].max() if not previous_10_bars.empty else high
    target_breakout_price = highest_of_last_10 * 1.002

    dollar_volume = close * volume
    rvol = volume / avg_volume if avg_volume > 0 else 0

    # إحصائيات الفلاتر
    if (ema_slow > ema_long) and (ema_fast > ema_mid > ema_slow) and (close > ema_fast):
        filter_stats["EMA Trend"] = True
    if close > vwap:
        filter_stats["VWAP"] = True
    if RSI_MIN <= rsi <= RSI_MAX:
        filter_stats["RSI"] = True
    if ADX_MIN <= adx <= ADX_MAX:
        filter_stats["ADX"] = True
    if (rvol > VOLUME_MULTIPLIER) and (volume > avg_volume) and (dollar_volume >= MIN_DOLLAR_VOLUME):
        filter_stats["Volume"] = True
    if close > target_breakout_price:
        filter_stats["Breakout"] = True

    # ---------------- شروط الرفض ----------------
    candle_range = high - low
    if candle_range == 0:
        return None, "No Range Candle", filter_stats

    if close <= open_price:
        return None, "Bearish Candle", filter_stats

    candle_size_pct = candle_range / close
    if candle_size_pct > MAX_CANDLE_RANGE:
        return None, "Large Candle (>6%)", filter_stats

    if len(df) >= 2:
        last_2 = df.iloc[-2:]
        r1 = last_2["high"].iloc[0] - last_2["low"].iloc[0]
        r2 = last_2["high"].iloc[1] - last_2["low"].iloc[1]
        if r1 > 0 and r2 > 0:
            c1_bull = last_2["close"].iloc[0] > last_2["open"].iloc[0]
            c2_bull = last_2["close"].iloc[1] > last_2["open"].iloc[1]
            b1 = (last_2["close"].iloc[0] - last_2["open"].iloc[0]) / r1
            b2 = (last_2["close"].iloc[1] - last_2["open"].iloc[1]) / r2
            if c1_bull and c2_bull and b1 > 0.8 and b2 > 0.8:
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
        return None, "Far From EMA9 (>4%)", filter_stats

    if not (rvol > VOLUME_MULTIPLIER and volume > avg_volume):
        return None, "Low Volume", filter_stats
    if not (RSI_MIN <= rsi <= RSI_MAX):
        return None, "RSI Out of Range", filter_stats
    if not (ADX_MIN <= adx <= ADX_MAX):
        return None, "ADX Out of Range", filter_stats

    # ✅ إصلاح 4: حساب وقف الخسارة والهدف
    stop_loss = close - (ATR_STOP_MULT * atr)
    max_allowed_stop = close * (1 - MAX_LOSS_PCT)
    stop_loss = max(stop_loss, max_allowed_stop)   # لا نسمح بخسارة أكبر من 2%
    risk = close - stop_loss
    take_profit = close + (risk * RISK_REWARD)

    # ✅ إصلاح 3: القوة النسبية مقابل السوق
    daily_return_pct = daily_return * 100
    if bench_return is None:
        rel_strength = None
    else:
        rel_strength = daily_return_pct - bench_return

    return {
        "bar_time": df.index[-1],
        "close": float(close),
        "vwap": float(vwap),
        "volume": float(volume),
        "avg_volume": float(avg_volume),
        "rsi": float(rsi),
        "adx": float(adx),
        "atr": float(atr),
        "daily_return": float(daily_return_pct),
        "bench_return": bench_return,
        "rel_strength": rel_strength,
        "highest_of_last_10": float(highest_of_last_10),
        "dollar_volume": float(dollar_volume),
        "distance_ema9": float(distance_from_ema9 * 100),
        "stop_loss": float(stop_loss),
        "take_profit": float(take_profit),
        "risk_pct": float(risk / close * 100),
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


def format_signal_message(symbol: str, s: dict) -> str:
    bar_time = s["bar_time"]
    bar_time_str = bar_time.strftime("%Y-%m-%d %H:%M") if isinstance(bar_time, datetime) else str(bar_time)

    if s["rel_strength"] is None:
        rel_line = "📊 القوة النسبية: غير متاحة\n"
    elif s["rel_strength"] > 0:
        rel_line = f"💪 أقوى من السوق بـ {s['rel_strength']:+.2f}% (QQQ {s['bench_return']:+.2f}%)\n"
    else:
        rel_line = f"⚠️ أضعف من السوق بـ {s['rel_strength']:.2f}% (QQQ {s['bench_return']:+.2f}%)\n"

    return (
        f"🎯 صيدة — {symbol}\n"
        f"الوقت: {bar_time_str}\n\n"
        f"💵 سعر الدخول : ${s['close']:.2f}\n"
        f"🛑 وقف الخسارة : ${s['stop_loss']:.2f}  (-{s['risk_pct']:.2f}%)\n"
        f"🎯 الهدف       : ${s['take_profit']:.2f}  (+{s['risk_pct']*RISK_REWARD:.2f}%)\n\n"
        f"🚀 صعود اليوم: +{s['daily_return']:.2f}%\n"
        f"{rel_line}"
        f"📍 VWAP: ${s['vwap']:.2f} ✅\n"
        f"📊 RVOL: {s['volume']/s['avg_volume']:.2f}x\n"
        f"⚡ RSI: {s['rsi']:.1f}   🔥 ADX: {s['adx']:.1f}\n"
        f"🔳 اختراق فوق ${s['highest_of_last_10']:.2f}\n\n"
        f"⚠️ ضع وقف الخسارة فور الشراء. اخرج إن مشى عرضياً 30 دقيقة."
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
            f"✅ تم إصلاح قراءة الشمعة المكتملة\n"
            f"✅ وقف الخسارة والهدف مضافين"
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
# Alert State
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
    t_total = time.monotonic()

    t_dl = time.monotonic()
    all_dfs, missing_tickers = fetch_all_bars_bulk(TICKERS)
    download_time = time.monotonic() - t_dl

    bench_return = compute_benchmark_return(all_dfs)

    t_an = time.monotonic()
    signals_count = 0
    rejected_count = 0
    reasons = Counter()
    filter_pass = {"EMA Trend": 0, "VWAP": 0, "RSI": 0, "ADX": 0, "Volume": 0, "Breakout": 0}

    for symbol in TICKERS:
        if symbol in missing_tickers or symbol not in all_dfs:
            reasons["Missing Data"] += 1
            rejected_count += 1
            continue

        df = all_dfs[symbol]
        bar_time_iso = df.index[-1].isoformat()

        if already_alerted(state, symbol, bar_time_iso):
            reasons["Already Alerted"] += 1
            rejected_count += 1
            continue

        signal, reject_reason, f_stats = evaluate_signal(df, bench_return)

        for k, v in f_stats.items():
            if v:
                filter_pass[k] += 1

        if signal is None:
            reasons[reject_reason] += 1
            rejected_count += 1
            if DEBUG:
                logging.info(f"{symbol} ❌ {reject_reason}")
            continue

        logging.info(f"✅ SIGNAL {symbol} @ ${signal['close']:.2f} | SL ${signal['stop_loss']:.2f} | TP ${signal['take_profit']:.2f}")
        send_alert(symbol, signal)
        mark_alerted(state, symbol, bar_time_iso)
        save_alerted_bars(state)
        signals_count += 1

    analysis_time = time.monotonic() - t_an
    total_time = time.monotonic() - t_total

    bench_txt = f"{bench_return:+.2f}%" if bench_return is not None else "N/A"

    report = ["\n========== Scan Summary =========="]
    report.append(f"Market (QQQ) : {bench_txt}")
    report.append(f"Scanned  : {len(TICKERS)}")
    report.append(f"Signals  : {signals_count}")
    report.append(f"Rejected : {rejected_count}\n")
    report.append("Top Reject Reasons")
    for i, (reason, count) in enumerate(reasons.most_common(6), start=1):
        pct = count / len(TICKERS) * 100
        report.append(f"{i}- {reason:<22}: {count:<3} ({pct:.0f}%)")
    report.append("\nFilter Statistics")
    for f in ["EMA Trend", "VWAP", "RSI", "ADX", "Volume", "Breakout"]:
        report.append(f"{f:<16}: {filter_pass[f]} PASS")
    report.append(f"\nDownload {download_time:.2f}s | Analysis {analysis_time:.2f}s | Total {total_time:.2f}s")
    report.append("==================================\n")

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

            if current_minute % 30 == 0 and current_minute != last_scan_minute:
                if is_market_open():
                    logging.info(f"⏰ [{current_time.strftime('%H:%M')}] بدء الفحص...")
                    time.sleep(20)   # مهلة لضمان توفر الشمعة المغلقة لدى Yahoo
                    scan_once(state)
                else:
                    logging.info("💤 السوق مغلق...")
                last_scan_minute = current_minute

            time.sleep(10)

        except Exception as exc:
            logging.exception(f"Main loop error: {exc}")
            send_error_message(str(exc))
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()