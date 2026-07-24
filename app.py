"""
Sniper Bot — DAILY swing scanner with Telegram alerts.

الفريم: يومي (Daily)
الفحص: مرة واحدة يومياً بعد إغلاق السوق
الهدف: صفقات تُحتفظ بها من يومين إلى أسبوعين

تغييرات جوهرية عن نسخة النص ساعة:
  • حُذف VWAP (لا معنى له على الفريم اليومي)
  • الاختراق أصبح فوق أعلى قمة 20 يوم بدل 10 شمعات
  • وقف الخسارة 2×ATR بحد أقصى 8%  |  الهدف = ضعف المخاطرة
  • القوة النسبية مقابل QQQ على 5 أيام
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
# Configuration
# =============================================================================

DEBUG = False

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    stream=sys.stdout,
)

# ---- القائمة الأصلية (79 سهم) ----
_original = [
    "RIVN", "NIO", "PLUG", "SOUN", "XPEV", "RIOT", "AMD", "INTC", "OPEN", "PATH",
    "TOST", "RBLX", "FRSH", "CPRT", "CELH", "TTD", "NKE", "ABT", "KGC", "GRWG",
    "HIVE", "BE", "FCX", "SLB", "AA", "SMR", "HIMS", "AUR", "BTE", "AMPX",
    "CRDO", "ALAB", "KSCP", "BLNK", "GLW", "SNDK", "ON", "RZLV", "LAES", "GFI",
    "U", "FIG", "IOVA", "ERIC", "CMPS", "RLMD", "ALTO", "HELP", "JLHL", "NN",
    "CCRN", "SONO", "PESI", "SSRM", "PEGA", "SDGR", "TEM", "NBIS", "RKLB", "LUNR",
    "OUST", "AEHR", "ACLS", "CAMT", "PDFS", "FORM", "AMKR", "VECO", "VIAV", "S",
    "DOCN", "ENPH", "SEDG", "MRVL", "MTSI", "ALGM", "COHR", "AAOI", "CARG"
]

# ---- الإضافات الجديدة المعتمدة شرعياً (39 سهم) ----
_added = [
    "HIMX", "SIMO", "QRVO", "MGNI", "PUBM", "ZETA", "DV", "YEXT", "TDC", "BOX",
    "OPRA", "BTU", "ARLP", "HL", "CDE", "EXK", "AG", "EQX", "BTG", "IAG",
    "FSM", "DRD", "HMY", "SANA", "ALKS", "ADMA", "RDW", "SFIX", "TDUP", "MQ",
    "MXL", "COHU", "PLAB", "UCTT", "ICHR", "AOSL", "ACMR", "LITE", "NVTS"
]

TICKERS = sorted(set(_original + _added))

BENCHMARK = "QQQ"

# ---- إعدادات المؤشرات (يومية) ----
VOLUME_MULTIPLIER = 1.3      # حجم اليوم مقابل متوسط 20 يوم
VOLUME_AVG_PERIOD = 20
RSI_PERIOD = 14
RSI_MIN = 50
RSI_MAX = 72

EMA_FAST = 10
EMA_MID = 20
EMA_SLOW = 50
EMA_LONG = 200

ADX_MIN = 20
ADX_MAX = 55

BREAKOUT_LOOKBACK = 20        # اختراق أعلى قمة 20 يوم
BREAKOUT_BUFFER = 1.002       # هامش أمان 0.2%

MIN_DOLLAR_VOLUME = 5_000_000.0   # سيولة يومية لا تقل عن 5 مليون دولار
MIN_ATR_PCT = 0.02                # حركة يومية لا تقل عن 2%
MAX_CANDLE_RANGE = 0.15           # تجنّب أيام الجنون (>15%)
MAX_EXTENSION = 0.12              # لا يبعد أكثر من 12% عن EMA20
MAX_DAILY_RETURN = 0.10           # لا يكون قافز أكثر من 10% اليوم

# ---- إدارة المخاطر ----
ATR_STOP_MULT = 2.0
RISK_REWARD = 2.0
MAX_LOSS_PCT = 0.08               # وقف الخسارة لا يتجاوز 8%

# ---- توقيت الفحص ----
SCAN_HOUR_UTC = 21                # 21:00 UTC = بعد إغلاق السوق بساعة
SCAN_MINUTE_UTC = 0

HEARTBEAT_SECONDS = 43200         # كل 12 ساعة
STATE_FILE = os.path.join(os.path.dirname(__file__), ".alert_state_daily.json")
PORT = int(os.environ.get("PORT", 8080))

# =============================================================================
# Data
# =============================================================================

def fetch_all_bars_bulk(tickers_list: list) -> tuple[dict[str, pd.DataFrame], list[str]]:
    """تحميل جماعي لشموع يومية بطلب واحد. آخر شمعة يومية مغلقة بالفعل بعد الإغلاق."""
    try:
        download_list = list(dict.fromkeys(tickers_list + [BENCHMARK]))
        data = yf.download(
            tickers=" ".join(download_list),
            period="2y",          # سنتان تكفيان لحساب EMA200 بدقة
            interval="1d",
            group_by="ticker",
            progress=False,
            auto_adjust=False,
        )

        all_dfs, missing = {}, []
        if data.empty:
            return {}, tickers_list

        req = ["open", "high", "low", "close", "volume"]

        def _clean(df_in):
            d = df_in.copy()
            d.dropna(subset=["Close"], inplace=True)
            if d.empty:
                return None
            d.columns = [str(c).lower() for c in d.columns]
            if not all(c in d.columns for c in req):
                return None
            d = d[req].sort_index()
            return d if len(d) >= 210 else None   # لا بد من بيانات كافية لـ EMA200

        if isinstance(data.columns, pd.MultiIndex):
            available = set(data.columns.levels[0])
            for t in download_list:
                if t in available:
                    c = _clean(data[t])
                    if c is not None:
                        all_dfs[t] = c
                    elif t != BENCHMARK:
                        missing.append(t)
                elif t != BENCHMARK:
                    missing.append(t)
        else:
            c = _clean(data)
            if c is not None:
                all_dfs[download_list[0]] = c
            else:
                missing.append(download_list[0])

        return all_dfs, missing

    except Exception as e:
        logging.error(f"خطأ في التحميل الجماعي: {e}")
        return {}, tickers_list


def benchmark_5d_return(all_dfs: dict) -> float | None:
    """أداء مؤشر السوق خلال آخر 5 أيام."""
    df = all_dfs.get(BENCHMARK)
    if df is None or len(df) < 6:
        return None
    try:
        return float((df["close"].iloc[-1] / df["close"].iloc[-6] - 1) * 100)
    except Exception:
        return None

# =============================================================================
# Indicators
# =============================================================================

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df[f"ema{EMA_FAST}"] = ta.ema(df["close"], length=EMA_FAST)
    df[f"ema{EMA_MID}"]  = ta.ema(df["close"], length=EMA_MID)
    df[f"ema{EMA_SLOW}"] = ta.ema(df["close"], length=EMA_SLOW)
    df[f"ema{EMA_LONG}"] = ta.ema(df["close"], length=EMA_LONG)
    df["rsi"] = ta.rsi(df["close"], length=RSI_PERIOD)
    df["avg_volume"] = df["volume"].rolling(VOLUME_AVG_PERIOD).mean()
    df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=14)
    df["adx"] = adx_df["ADX_14"] if (adx_df is not None and "ADX_14" in adx_df.columns) else 0
    return df

# =============================================================================
# Signal evaluation
# =============================================================================

def evaluate_signal(df: pd.DataFrame, bench_5d: float | None) -> tuple[dict | None, str | None, dict]:
    df = compute_indicators(df)

    stats = {"EMA Trend": False, "RSI": False,
             "ADX": False, "Volume": False, "Breakout": False}

    needed = [f"ema{EMA_FAST}", f"ema{EMA_MID}", f"ema{EMA_SLOW}",
              f"ema{EMA_LONG}", "rsi", "avg_volume", "adx", "atr"]
    last_row = df[needed].iloc[-1]
    if last_row.isna().any():
        return None, "Missing Indicators", stats

    last = df.iloc[-1]
    close, open_p = last["close"], last["open"]
    high, low = last["high"], last["low"]
    volume, avg_volume = last["volume"], last["avg_volume"]
    rsi, adx, atr = last["rsi"], last["adx"], last["atr"]

    ema_f = last[f"ema{EMA_FAST}"]
    ema_m = last[f"ema{EMA_MID}"]
    ema_s = last[f"ema{EMA_SLOW}"]
    ema_l = last[f"ema{EMA_LONG}"]

    prev_bars = df.iloc[-(BREAKOUT_LOOKBACK + 1):-1]
    highest = prev_bars["high"].max()
    breakout_price = highest * BREAKOUT_BUFFER

    dollar_volume = close * volume
    rvol = volume / avg_volume if avg_volume > 0 else 0

    # --- إحصائيات ---
    if ema_f > ema_m > ema_s > ema_l:
        stats["EMA Trend"] = True
    if RSI_MIN <= rsi <= RSI_MAX:
        stats["RSI"] = True
    if ADX_MIN <= adx <= ADX_MAX:
        stats["ADX"] = True
    if rvol > VOLUME_MULTIPLIER and dollar_volume >= MIN_DOLLAR_VOLUME:
        stats["Volume"] = True
    if close > breakout_price:
        stats["Breakout"] = True

    # --- الشروط ---
    if close <= open_p:
        return None, "Bearish Day", stats

    candle_range = high - low
    if candle_range == 0:
        return None, "No Range", stats
    if candle_range / close > MAX_CANDLE_RANGE:
        return None, "Crazy Range Day", stats

    if close <= breakout_price:
        return None, f"No 20D Breakout", stats

    if dollar_volume < MIN_DOLLAR_VOLUME:
        return None, "Low Dollar Volume", stats

    if atr / close < MIN_ATR_PCT:
        return None, "Too Quiet (ATR)", stats

    daily_return = (close - df["close"].iloc[-2]) / df["close"].iloc[-2]
    if daily_return > MAX_DAILY_RETURN:
        return None, "Gapped Too Much", stats

    # الترند الكامل: 10 > 20 > 50 > 200
    if not (ema_f > ema_m > ema_s > ema_l):
        return None, "EMA Stack Wrong", stats

    if close < ema_f:
        return None, "Below EMA10", stats

    extension = (close - ema_m) / ema_m
    if extension > MAX_EXTENSION:
        return None, f"Overextended ({extension*100:.1f}%)", stats

    if rvol <= VOLUME_MULTIPLIER:
        return None, f"Low Volume ({rvol:.2f}x)", stats

    if not (RSI_MIN <= rsi <= RSI_MAX):
        return None, f"RSI Out ({rsi:.0f})", stats

    if not (ADX_MIN <= adx <= ADX_MAX):
        return None, f"ADX Out ({adx:.0f})", stats

    # --- إدارة المخاطر ---
    stop = close - ATR_STOP_MULT * atr
    stop = max(stop, close * (1 - MAX_LOSS_PCT))
    risk = close - stop
    target = close + risk * RISK_REWARD

    # --- القوة النسبية على 5 أيام ---
    stock_5d = float((close / df["close"].iloc[-6] - 1) * 100) if len(df) >= 6 else None
    rel = None if (bench_5d is None or stock_5d is None) else stock_5d - bench_5d

    return {
        "bar_date": df.index[-1],
        "close": float(close),
        "rsi": float(rsi),
        "adx": float(adx),
        "atr": float(atr),
        "rvol": float(rvol),
        "dollar_volume": float(dollar_volume),
        "daily_return": float(daily_return * 100),
        "stock_5d": stock_5d,
        "bench_5d": bench_5d,
        "rel_strength": rel,
        "breakout_level": float(highest),
        "extension": float(extension * 100),
        "stop_loss": float(stop),
        "take_profit": float(target),
        "risk_pct": float(risk / close * 100),
    }, None, stats

# =============================================================================
# Telegram
# =============================================================================

_bot: Bot | None = None

def _get_bot() -> Bot:
    global _bot
    if _bot is None:
        _bot = Bot(token=TELEGRAM_BOT_TOKEN)
    return _bot


def format_signal_message(symbol: str, s: dict) -> str:
    d = s["bar_date"]
    date_str = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)

    if s["rel_strength"] is None:
        rel = "📊 القوة النسبية: غير متاحة\n"
    elif s["rel_strength"] > 0:
        rel = f"💪 أقوى من السوق بـ {s['rel_strength']:+.1f}% خلال 5 أيام\n"
    else:
        rel = f"⚠️ أضعف من السوق بـ {s['rel_strength']:.1f}% خلال 5 أيام\n"

    return (
        f"🎯 صيدة يومية — {symbol}\n"
        f"شمعة {date_str}\n\n"
        f"💵 الدخول      : ${s['close']:.2f}\n"
        f"🛑 وقف الخسارة : ${s['stop_loss']:.2f}   (-{s['risk_pct']:.1f}%)\n"
        f"🎯 الهدف       : ${s['take_profit']:.2f}   (+{s['risk_pct']*RISK_REWARD:.1f}%)\n\n"
        f"🔳 اخترق قمة 20 يوم عند ${s['breakout_level']:.2f}\n"
        f"📈 اليوم: {s['daily_return']:+.1f}%   |   بُعده عن EMA20: {s['extension']:.1f}%\n"
        f"{rel}"
        f"📊 RVOL: {s['rvol']:.2f}x   |   سيولة: ${s['dollar_volume']:,.0f}\n"
        f"⚡ RSI: {s['rsi']:.0f}   |   🔥 ADX: {s['adx']:.0f}\n\n"
        f"📋 الخطة: ادخل بأمر شراء عند الفتح، وضع وقف الخسارة فوراً.\n"
        f"⚠️ لا تخاطر بأكثر من 25% من رأس مالك في صفقة واحدة."
    )


def safe_run(coro):
    try:
        asyncio.run(coro)
    except Exception as e:
        logging.error(f"Async error: {e}")


def send_alert(symbol, signal):
    safe_run(_get_bot().send_message(chat_id=TELEGRAM_CHAT_ID,
                                     text=format_signal_message(symbol, signal)))


def send_msg(text: str):
    safe_run(_get_bot().send_message(chat_id=TELEGRAM_CHAT_ID, text=text))

# =============================================================================
# State
# =============================================================================

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}

def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

# =============================================================================
# Health server
# =============================================================================

_app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

@_app.route("/")
def _alive():
    return "I am alive"

def start_health_server():
    threading.Thread(target=lambda: _app.run(host="0.0.0.0", port=PORT), daemon=True).start()

# =============================================================================
# Scan
# =============================================================================

def scan_once(state: dict):
    t0 = time.monotonic()
    logging.info(f"🔍 بدء الفحص اليومي لـ {len(TICKERS)} سهم...")

    all_dfs, missing = fetch_all_bars_bulk(TICKERS)
    bench_5d = benchmark_5d_return(all_dfs)

    signals = 0
    rejected = 0
    reasons = Counter()
    passes = {"EMA Trend": 0, "RSI": 0, "ADX": 0, "Volume": 0, "Breakout": 0}

    for symbol in TICKERS:
        if symbol in missing or symbol not in all_dfs:
            reasons["Missing Data"] += 1
            rejected += 1
            continue

        df = all_dfs[symbol]
        bar_key = str(df.index[-1].date())

        if state.get(symbol) == bar_key:
            reasons["Already Alerted"] += 1
            rejected += 1
            continue

        signal, reason, stats = evaluate_signal(df, bench_5d)
        for k, v in stats.items():
            if v:
                passes[k] += 1

        if signal is None:
            reasons[reason] += 1
            rejected += 1
            if DEBUG:
                logging.info(f"{symbol} ❌ {reason}")
            continue

        logging.info(f"✅ SIGNAL {symbol} @ ${signal['close']:.2f} | SL ${signal['stop_loss']:.2f} | TP ${signal['take_profit']:.2f}")
        send_alert(symbol, signal)
        state[symbol] = bar_key
        save_state(state)
        signals += 1

    elapsed = time.monotonic() - t0
    bench_txt = f"{bench_5d:+.2f}%" if bench_5d is not None else "N/A"

    rep = ["\n========== Daily Scan =========="]
    rep.append(f"QQQ (5 أيام) : {bench_txt}")
    rep.append(f"Scanned  : {len(TICKERS)}")
    rep.append(f"Signals  : {signals}")
    rep.append(f"Rejected : {rejected}\n")
    rep.append("Top Reject Reasons")
    for i, (r, c) in enumerate(reasons.most_common(8), 1):
        rep.append(f"{i}- {r:<26}: {c:<3} ({c/len(TICKERS)*100:.0f}%)")
    rep.append("\nFilter Statistics")
    for k in ["EMA Trend", "RSI", "ADX", "Volume", "Breakout"]:
        rep.append(f"{k:<14}: {passes[k]} PASS")
    rep.append(f"\nTotal {elapsed:.1f}s")
    rep.append("================================\n")
    logging.info("\n".join(rep))

    if signals == 0:
        send_msg(f"📭 فحص اليوم انتهى — لا توجد صيدة.\nالسوق (QQQ 5 أيام): {bench_txt}")

# =============================================================================
# Main
# =============================================================================

def main():
    start_health_server()

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("❌ متغيرات تيليجرام مفقودة")
        while True:
            time.sleep(3600)

    send_msg(
        f"🤖 Sniper Bot — النسخة اليومية\n"
        f"يراقب {len(TICKERS)} سهم على الفريم اليومي.\n"
        f"فحص واحد يومياً بعد إغلاق السوق ✅"
    )

    state = load_state()
    last_scan_date = None
    last_hb = 0.0

    while True:
        try:
            now = datetime.now(timezone.utc)

            if time.monotonic() - last_hb >= HEARTBEAT_SECONDS:
                logging.info("💓 heartbeat")
                last_hb = time.monotonic()

            is_weekday = now.weekday() < 5
            is_scan_time = (now.hour == SCAN_HOUR_UTC and now.minute >= SCAN_MINUTE_UTC)
            today = now.date()

            if is_weekday and is_scan_time and last_scan_date != today:
                scan_once(state)
                last_scan_date = today

            time.sleep(60)

        except Exception as exc:
            logging.exception(f"Loop error: {exc}")
            time.sleep(300)


if __name__ == "__main__":
    main()