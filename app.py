""" Sniper Bot — Yahoo Finance 15-minute long-setup scanner with Telegram alerts.
Fully customized with Abdulaziz's ultra-strict, institutional-grade breakout rules.
Optimized Settings: Powered by Yahoo Finance for real-time full SIP market data.
Watchlist: Fully merged with newly added tickers, dynamically de-duplicated.
"""

import asyncio
import json
import logging
import os
import threading
import time
import traceback
from datetime import datetime, timezone, timedelta
import pandas as pd
import pandas_ta as ta
import yfinance as yf
from flask import Flask
from telegram import Bot
from telegram.constants import ParseMode

# إعداد السجلات (Logging) لمراقبة عمل البوت على منصة Railway
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# =============================================================================
# Configuration & Environment Variables
# =============================================================================

# جلب بيانات التليجرام مباشرة من المتغيرات البيئية في ريل واي
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# قائمة الأسهم الخاصة بك كاملة ومحمية من التكرار
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

TIMEFRAME = "15m"
PERIOD_LOOKBACK = "30d"  # يمنحنا حوالي 700 شمعة وهي كافية ودقيقة جداً للمؤشرات

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

MIN_DOLLAR_VOLUME = 1500000.0  # تعمل الآن بكفاءة لأن سيولة ياهو تمثل 100% من السوق
MAX_EMA9_DISTANCE = 0.035      # تم ضبطها على 3.5% لمنع خنق اختراقات الزخم القوية
MAX_CANDLE_RANGE = 0.06

# الفحص كل 15 دقيقة (900 ثانية) مع إغلاق كل شمعة
POLL_SECONDS = 900 
PORT = int(os.environ.get("PORT", 8080))

# تهيئة بوت التليجرام
telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None

# =============================================================================
# Flask Server Setup
# =============================================================================
app = Flask(__name__)

@app.route('/')
def health_check():
    return {"status": "healthy", "bot": "Sniper Scanner Running with Yahoo Finance Data"}, 200

# =============================================================================
# Yahoo Finance Bulk Data Access
# =============================================================================

def fetch_all_bars_yf(tickers_list: list) -> dict[str, pd.DataFrame]:
    """ جلب بيانات السوق كاملة لـ 79 سهماً بطلب واحد ذكي لمنع الحظر وبسرعة البرق """
    try:
        logging.info(f"🔄 جلب بيانات {len(tickers_list)} سهماً بشكل جماعي من Yahoo Finance...")
        tickers_str = " ".join(tickers_list)
        
        data = yf.download(tickers=tickers_str, period=PERIOD_LOOKBACK, interval=TIMEFRAME, group_by='ticker', progress=False)
        
        all_dfs = {}
        for ticker in tickers_list:
            if ticker in data.columns.levels:
                df_ticker = data[ticker].copy()
                df_ticker.dropna(subset=["Close"], inplace=True)
                
                if df_ticker.empty:
                    continue
                
                # توحيد الحروف الصغيرة لتتوافق مع دوالك الفنية
                df_ticker.columns = [col.lower() for col in df_ticker.columns]
                all_dfs[ticker] = df_ticker.sort_index()
                
        return all_dfs
    except Exception as e:
        logging.error(f"❌ خطأ أثناء جلب البيانات الجماعية: {e}")
        return {}

# =============================================================================
# Technical Indicators Computation
# =============================================================================

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df[f"ema{EMA_FAST}"] = ta.ema(df["close"], length=EMA_FAST)
    df[f"ema{EMA_MID}"] = ta.ema(df["close"], length=EMA_MID)
    df[f"ema{EMA_SLOW}"] = ta.ema(df["close"], length=EMA_SLOW)
    df[f"ema{EMA_LONG}"] = ta.ema(df["close"], length=EMA_LONG)
    df["rsi"] = ta.rsi(df["close"], length=RSI_PERIOD)
    
    # معادلة الـ VWAP التراكمية الدقيقة المتوافقة مع ياهو فاينانس
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    df["vwap"] = (typical_price * df["volume"]).cumsum() / df["volume"].cumsum()
    
    df["avg_volume"] = df["volume"].rolling(window=VOLUME_AVG_PERIOD).mean()
    df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
    
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=14)
    df["adx"] = adx_df["ADX_14"] if adx_df is not None else 0
    return df

# =============================================================================
# Signal Evaluation (Abdulaziz's Strict Rules)
# =============================================================================

def evaluate_signal(df: pd.DataFrame) -> tuple[dict | None, str | None]:
    if len(df) < 50:
        return None, "بيانات غير كافية لحساب المؤشرات الفنية"
        
    df = compute_indicators(df)
    
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
    ema_long = last[f"ema{EMA_LONG}"]

    # --- 1) شمعة صاعدة قوية ---
    candle_range = high - low
    if candle_range == 0: 
        return None, "شمعة بلا مدى سعري"
    if (close - open_price) <= candle_range * 0.5: 
        return None, "❌ الشمعة ليست قوية"

    # --- 2) فلتر الشمعة العملاقة ---
    candle_size_pct = candle_range / close
    if candle_size_pct > MAX_CANDLE_RANGE: 
        return None, f"❌ شمعة متفجرة بشكل مفرط وعملاقة ({candle_size_pct*100:.1f}%)"

    # --- 3) فلتر الشمعتين الاستنزافيتين ---
    if len(df) >= 2:
        c1_row = df.iloc[-2]
        c2_row = df.iloc[-1]
        
        c1_range = c1_row["high"] - c1_row["low"]
        c2_range = c2_row["high"] - c2_row["low"]
        
        if c1_range > 0 and c2_range > 0:
            c1_bullish = c1_row["close"] > c1_row["open"]
            c2_bullish = c2_row["close"] > c2_row["open"]
            
            c1_body_pct = (c1_row["close"] - c1_row["open"]) / c1_range
            c2_body_pct = (c2_row["close"] - c2_row["open"]) / c2_range
            
            if c1_bullish and c2_bullish and c1_body_pct > 0.8 and c2_body_pct > 0.8:
                return None, "❌ تتابع صعود عمودي استنزافي حاد"

    # --- 4) فلاتر الزخم والاتجاه المؤسساتي ---
    if not (rsi_min <= rsi <= rsi_max): 
        return None, f"❌ RSI خارج النطاق المطلوب ({rsi:.1f})"
        
    if not (ema_fast > ema_mid > ema_slow > ema_long): 
        return None, "❌ المتوسطات الفنية ليست مرتبة تصاعدياً"
        
    if close < vwap: 
        return None, "❌ السعر يتداول تحت خط الـ VWAP"
        
    if volume < (avg_volume * VOLUME_MULTIPLIER): 
        return None, f"❌ حجم السيولة الحالي أقل من المتوسط المطلق"
        
    if not (adx_min <= adx <= adx_max): 
        return None, f"❌ مؤشر قوة الاتجاه ADX غير مثالي ({adx:.1f})"
    
    # فلتر السيولة النقدية بالشمعة بالدولار
    dollar_volume = volume * close
    if dollar_volume < MIN_DOLLAR_VOLUME: 
        return None, f"❌ سيولة الشمعة النقدية ضعيفة"
        
    # فلتر الابتعاد عن متوسط الـ 9 أيام
    ema_dist = (close - ema_fast) / ema_fast
    if ema_dist > MAX_EMA9_DISTANCE: 
        return None, f"❌ السعر ممتد ومبتعد جداً عن خط الـ EMA9"

    # نجاح الإشارة وتخطي كافة الفلاتر بنجاح
    return {
        "close": close,
        "rsi": rsi,
        "adx": adx,
        "volume_mult": volume / avg_volume,
        "time": df.index[-1].strftime("%Y-%m-%d %H:%M")
    }, None

# =============================================================================
# Telegram Alerts Sender
# =============================================================================

def send_telegram_alert(symbol: str, metrics: dict):
    if not telegram_bot or not TELEGRAM_CHAT_ID:
        logging.warning("⚠️ إعدادات التليجرام غير مكتملة في الـ Variables")
        return

    message = (
        f"🎯 **إشارة دخول قوية مكتشفة! — Sniper Bot** 🎯\n\n"
        f"▪️ **السهم:** `{symbol}`\n"
        f"▪️ **سعر الإغلاق:** `${metrics['close']:.2f}`\n"
        f"▪️ **مؤشر الـ RSI:** `{metrics['rsi']:.1f}`\n"
        f"▪️ **مؤشر الـ ADX:** `{metrics['adx']:.1f}`\n"
        f"▪️ **مضاعف الحجم (Vol Mult):** `{metrics['volume_mult']:.2f}x`\n"
        f"▪️ **توقيت الشمعة (EST):** `{metrics['time']}`\n\n"
        f"🚀 *ينطبق عليها بالكامل شروط عبد العزيز الصارمة للاختراقات.*"
    )
    try:
        telegram_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode=ParseMode.MARKDOWN)
        logging.info(f"🚀 تم إرسال تنبيه السهم {symbol} بنجاح.")
    except Exception as e:
        logging.error(f"❌ فشل إرسال التنبيه إلى تليجرام لـ {symbol}: {e}")

def verify_telegram_connection():
    """ دالة فحص وتأكيد عند بداية تشغيل السيرفر للتأكد من وصول رسائل التليجرام """
    if telegram_bot and TELEGRAM_CHAT_ID:
        try:
            telegram_bot.send_message(
                chat_id=TELEGRAM_CHAT_ID, 
