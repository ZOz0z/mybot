"""
Sniper Bot — DAILY swing scanner with Telegram alerts.

الفريم: يومي (Daily)
الفحص: مرة واحدة يومياً بعد إغلاق السوق
الهدف: صفقات تُحتفظ بها من يومين إلى أسبوعين

=============================================================================
تعديلات v3 — تخفيف الاختناقات (بناءً على تقرير القمع الفعلي)
=============================================================================
تقرير القمع أظهر أن أضيق ثلاثة فلاتر هي:
    RVOL Today  15%  ← الأضيق
    Breakout    15%  ← جوهر الاستراتيجية، لا يُمسّ
    Tight Base  37%

والمشكلة ليست في فلتر منفرد بل في تقاطعها: الاختراق يتطلب حجماً منفجراً،
والقاعدة الضيقة تتطلب هدوءاً — وهما متعاكسان زمنياً بطبيعتهما.

لذلك خُفّف اثنان فقط، مع إبقاء جوهر الاستراتيجية سليماً:
  1) RVOL_TODAY_MIN          1.5  → 1.2
  2) MAX_CONSOLIDATION_RANGE 0.14 → 0.20
  3) MIN_SCORE                 45 → 35   (النقاط تُرشد لا تحجب)

لم يُمسّ إطلاقاً: EMA Stack، Breakout، وقف الخسارة، نسبة الربح للمخاطرة.
=============================================================================
"""

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

import requests
from flask import Flask

# =============================================================================
# Configuration
# =============================================================================

DEBUG = False

# فحص فوري عند الإقلاع (للاختبار فقط). أبقِه False في التشغيل العادي.
RUN_ON_START = False

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

# ---- الدفعة الثالثة المعتمدة شرعياً (35 سهم) ----
_added2 = [
    "MRAM", "LASR", "KLIC", "ADEA", "VSH", "TTMI", "KN", "CTS", "AXTI", "DAKT",
    "YELP", "VIPS", "SUPN", "HRMY", "OMCL", "NEO", "CDNA", "TXG", "SHLS", "FLR",
    "HUBG", "RXO", "SNDR", "MRTN", "CVLG", "UUUU", "PAAS", "SVM", "OR", "SHOO",
    "LOVE", "HVT", "BBW", "WRBY", "FIGS",
]

# ---- الدفعة الرابعة المعتمدة شرعياً (45 سهم) ----
_added3 = [
    "POWI", "DIOD", "LSCC", "RMBS", "SLAB", "SYNA", "SANM", "BHE", "OSIS", "VICR",
    "ROG", "ONTO", "CALX", "EXTR", "NTGR", "DGII", "ADTN", "HLIT", "CSGS", "HALO",
    "CORT", "VCEL", "ARDX", "AXSM", "FOLD", "USPH", "MYRG", "PRIM", "ROAD", "TPC",
    "GVA", "PTEN", "WHD", "RES", "LBRT", "MUR", "EGO", "AGI", "BOOT", "SCVL",
    "ZUMZ", "URBN", "VRA", "LEVI", "MOV",
]

# ---- الدفعة الخامسة: شركات كبرى (85 سهم) ----
_added4 = [
    "NTAP", "WDC", "STX", "CIEN", "AKAM", "ZBRA", "KEYS", "TER", "ENTG", "MKSI",
    "NXPI", "SWKS", "MCHP", "DOX", "EPAM", "CTSH", "IT", "SCSC", "CACI", "JCI",
    "EMR", "ROK", "AME", "AOS", "DOV", "PNR", "IEX", "GGG", "NDSN", "CSL",
    "MAS", "MLM", "VMC", "SSD", "AWI", "CHRW", "EXPD", "LSTR", "ODFL", "SAIA",
    "XPO", "KEX", "MATX", "BKR", "NOV", "FTI", "TDW", "STLD", "NUE", "RS",
    "CRS", "CENX", "LII", "WSO", "POOL", "SITE", "FAST", "UFPI", "TREX", "DKS",
    "ASO", "WSM", "LZB", "ETD", "DECK", "SKX", "BKE", "ANF", "INGR", "CALM",
    "SMPL", "INCY", "EXEL", "NBIX", "NVDA", "GOOGL", "GOOG", "AMZN", "ADBE", "KLAC",
    "CVX", "QCOM", "ORCL", "CSCO", "NEM",
]

# ---- الدفعة السادسة: منتقاة من قائمة الأسهم الشرعية (101 سهم) ----
_added5 = [
    "ENSG", "MASI", "PODD", "DXCM", "EW", "MDT", "GILD", "BMRN", "LNTH", "IRTC",
    "GMED", "MMSI", "RDNT", "ADUS", "HQY", "APH", "ALLE", "NVT", "ZS", "QLYS",
    "CVLT", "OKTA", "TEAM", "ESTC", "SMTC", "CRUS", "AMBA", "TJX", "TPR", "CROX",
    "OLLI", "DG", "DLTR", "TGT", "BJ", "SBUX", "MNST", "CAVA", "KO", "PEP",
    "MDLZ", "KMB", "CHD", "CL", "PG", "CTVA", "CF", "ALB", "SQM", "AEM",
    "WPM", "CCJ", "TECK", "RIO", "XOM", "COP", "DINO", "WFRD", "STNG", "VAL",
    "EXE", "CNR", "HCC", "KNX", "ARCB", "TFII", "UBER", "DAL", "TRNO", "DHI",
    "LEN", "TOL", "PHM", "BCC", "LPX", "WMS", "DCI", "TTC", "IR", "XYL",
    "VLTO", "OTIS", "CARR", "ALC", "ATKR", "MLI", "FSS", "BMI", "CGNX", "PRLB",
    "FTDR", "CWST", "WCN", "ECG", "TRI", "EFX", "SPSC", "WK", "NYT", "UL",
    "SU",
]

TICKERS = sorted(set(_original + _added + _added2 + _added3 + _added4 + _added5))

# المؤشر المرجعي لحساب القوة النسبية.
BENCHMARK = "SPY"

# مؤشر إضافي يُعرض للعلم فقط ولا يدخل في أي حساب.
SECONDARY_INDEX = "QQQ"

# ---- إعدادات المؤشرات (يومية) ----
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

# ---- الفلاتر النوعية ----

# 1) التجميع الضيق: يشترط أن يسبق الاختراقَ انضغاطٌ في السعر.
#
#    ⬇ v3: خُفّف من 0.14 إلى 0.20
#    السبب: تقرير القمع أظهر أن 37% فقط تجتاز هذا الشرط، وهو ثالث أضيق فلتر.
#    والأهم أنه يتعارض بطبيعته مع شرط الاختراق: الاختراق حركة عنيفة،
#    والقاعدة الضيقة سكون. اتساع 20% لا يزال يستبعد الفوضى الحقيقية.
CONSOLIDATION_DAYS = 8          # عدد الأيام السابقة للاختراق التي نقيس ضيقها
MAX_CONSOLIDATION_RANGE = 0.20  # أقصى اتساع مسموح لتلك الفترة (20%)

# 2) الدخول المتأخر: يمنع الشراء بعد أن يكون السهم قد ابتعد عن نقطة الاختراق.
#    لم يُمسّ — يمر 93% أصلاً وهو ليس اختناقاً.
MAX_ABOVE_BREAKOUT = 0.04       # أقصى ارتفاع فوق مستوى الاختراق (4%)

# 3) الحجم النسبي — الحساب المصحَّح في v2 (حجم اليوم ÷ متوسط 20 يوماً سابقة).
#
#    ⬇ v3: RVOL_TODAY_MIN خُفّف من 1.5 إلى 1.2
#    السبب: كان الفلتر الأضيق على الإطلاق (15% فقط يجتازونه).
#    1.2 لا يزال يعني حجماً أعلى من المعتاد بـ20%، وهو تأكيد كافٍ للاختراق.
RVOL_TODAY_MIN = 1.2
RVOL_5D_MIN = 1.0
RVOL_DAYS = 5

# شرط اليوم الأخضر (إغلاق فوق الفتح).
REQUIRE_GREEN_DAY = True

# ---- فلتر الأرباح ----
EARNINGS_BLACKOUT_DAYS = 7    # تجاهل السهم إذا كانت أرباحه خلال هذا العدد من الأيام
EARNINGS_CACHE_HOURS = 24     # تحديث تواريخ الأرباح مرة كل 24 ساعة

# ---- نظام تقييم قوة الإشارة (مجموعه 100) ----
W_REL_STRENGTH = 30   # التفوق على السوق
W_RVOL         = 25   # قوة الحجم
W_ADX          = 25   # وضوح الاتجاه
W_PROXIMITY    = 20   # قرب السعر من EMA20 (دخول نظيف)

# ---- عتبة النقاط ----
# الباكتست على 226 صفقة أظهر أن عتبة 45 تضاعف العائد (0.115 → 0.247 R)،
# لكنها تركت 31 إشارة فقط من 226 — أي رفض 86% مما اجتاز كل الفلاتر الفنية.
#
# ⬇ v3: خُفّضت إلى 35 لفتح الإشارات مع إبقاء الترتيب عاملاً.
#    سهم متوسط في كل العوامل الأربعة يحصل على ~50 نقطة، فـ35 ترفض الضعيف فقط.
#    إن جاءت إشارات كثيرة ضعيفة الأداء، أعدها تدريجياً إلى 40 ثم 45.
MIN_SCORE = 35.0

# ---- مدة الاحتفاظ ----
# الباكتست: 113 صفقة من 226 كانت تموت بلا حسم خلال 10 أيام،
# وتمديدها إلى 20 يوماً ضاعف العائد (0.056 → 0.115 R).
HOLD_DAYS = 20

# ---- إدارة المخاطر (لا تُمسّ) ----
ATR_STOP_MULT = 2.0
RISK_REWARD = 2.0
MAX_LOSS_PCT = 0.08               # وقف الخسارة لا يتجاوز 8%

# ---- توقيت الفحص ----
# 22:00 UTC = الساعة 1:00 بعد منتصف الليل بتوقيت السعودية، طوال السنة.
SCAN_HOUR_UTC = 22
SCAN_MINUTE_UTC = 0

HEARTBEAT_SECONDS = 43200         # كل 12 ساعة

# ---- تخزين الحالة ----
# على Railway نظام الملفات مؤقت: أي نشر جديد يمسح الملف، فتتكرر التنبيهات.
# الحل: أنشئ Volume في Railway واربطه بمسار /data ثم اضبط STATE_DIR=/data
_default_state_dir = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.environ.get("STATE_DIR", _default_state_dir)
if not os.path.isdir(STATE_DIR):
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
    except OSError:
        STATE_DIR = _default_state_dir
STATE_FILE = os.path.join(STATE_DIR, ".alert_state_daily.json")

PORT = int(os.environ.get("PORT", 8080))

# حد تيليجرام 4096 حرفاً — نبقى دونه بهامش أمان.
TELEGRAM_MAX_CHARS = 3600

# =============================================================================
# Data
# =============================================================================

def fetch_all_bars_bulk(tickers_list: list) -> tuple[dict[str, pd.DataFrame], list[str]]:
    """تحميل جماعي لشموع يومية بطلب واحد. آخر شمعة يومية مغلقة بالفعل بعد الإغلاق."""
    try:
        download_list = list(dict.fromkeys(tickers_list + [BENCHMARK, SECONDARY_INDEX]))
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
                    elif t not in (BENCHMARK, SECONDARY_INDEX):
                        missing.append(t)
                elif t not in (BENCHMARK, SECONDARY_INDEX):
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


_earnings_cache: dict[str, object] = {}
_earnings_cache_time: float = 0.0


def get_next_earnings_date(symbol: str):
    """
    يُرجع تاريخ أقرب إعلان أرباح قادم، أو None إن لم يتوفر.
    النتائج تُخزّن في الذاكرة 24 ساعة لتقليل الطلبات على ياهو.
    """
    global _earnings_cache, _earnings_cache_time

    if time.time() - _earnings_cache_time > EARNINGS_CACHE_HOURS * 3600:
        _earnings_cache = {}
        _earnings_cache_time = time.time()

    if symbol in _earnings_cache:
        return _earnings_cache[symbol]

    result = None
    try:
        cal = yf.Ticker(symbol).calendar
        dates = None
        if isinstance(cal, dict):
            dates = cal.get("Earnings Date")
        elif isinstance(cal, pd.DataFrame) and "Earnings Date" in cal.index:
            dates = cal.loc["Earnings Date"].tolist()

        if dates:
            if not isinstance(dates, (list, tuple)):
                dates = [dates]
            parsed = []
            for d in dates:
                try:
                    parsed.append(pd.Timestamp(d).date())
                except Exception:
                    continue
            today = datetime.now(timezone.utc).date()
            upcoming = sorted([d for d in parsed if d >= today])
            if upcoming:
                result = upcoming[0]
    except Exception:
        result = None

    _earnings_cache[symbol] = result
    return result


def has_earnings_soon(symbol: str) -> tuple[bool, object]:
    """True إذا كانت الأرباح خلال نافذة الحظر."""
    d = get_next_earnings_date(symbol)
    if d is None:
        return False, None
    days = (d - datetime.now(timezone.utc).date()).days
    return (0 <= days <= EARNINGS_BLACKOUT_DAYS), d


def index_5d_return(all_dfs: dict, symbol: str) -> float | None:
    """أداء أي مؤشر خلال آخر 5 أيام."""
    df = all_dfs.get(symbol)
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
    df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=14)
    df["adx"] = adx_df["ADX_14"] if (adx_df is not None and "ADX_14" in adx_df.columns) else 0
    return df

# =============================================================================
# Signal evaluation
# =============================================================================

# ترتيب الشروط في التقرير وفي اختيار سبب الرفض المعروض.
CHECK_ORDER = [
    "Liquidity",     # سيولة دولارية
    "EMA Stack",     # 10 > 20 > 50 > 200
    "Above EMA10",
    "RSI",
    "ADX",
    "ATR",
    "Extension",     # قرب EMA20
    "Green Day",
    "Sane Range",
    "No Gap",
    "Breakout",      # اختراق قمة 20 يوم
    "Not Late",      # لم يبتعد فوق نقطة الاختراق
    "Tight Base",    # تجميع ضيق قبل الاختراق
    "RVOL Today",    # انفجار حجم يوم الاختراق
    "RVOL 5d",
]


def _scale(value, lo, hi, weight) -> float:
    """يحوّل قيمة إلى نقاط ضمن النطاق [lo, hi] مضروبة في وزنها."""
    if value is None:
        return weight * 0.5          # قيمة محايدة عند غياب البيانات
    if hi == lo:
        return weight * 0.5
    ratio = (value - lo) / (hi - lo)
    ratio = max(0.0, min(1.0, ratio))
    return weight * ratio


def score_signal(sig: dict) -> float:
    """
    يحسب قوة الإشارة من 100.
    كلما ارتفع الرقم كانت الإشارة أنقى وأجدر بالدخول.
    """
    # 1) القوة النسبية: من -5% (ضعيف) إلى +15% (متفوق بقوة)
    pts_rel = _scale(sig.get("rel_strength"), -5.0, 15.0, W_REL_STRENGTH)

    # 2) الحجم: من الحد الأدنى المقبول إلى 5x (انفجار حجم)
    pts_vol = _scale(sig.get("rvol_today"), RVOL_TODAY_MIN, 5.0, W_RVOL)

    # 3) الاتجاه: من 20 (بداية اتجاه) إلى 45 (اتجاه قوي جداً)
    pts_adx = _scale(sig.get("adx"), 20.0, 45.0, W_ADX)

    # 4) القرب من EMA20: كلما قلّ البعد زادت النقاط (معكوس)
    ext = sig.get("extension")
    pts_prox = W_PROXIMITY - _scale(ext, 0.0, 12.0, W_PROXIMITY) if ext is not None else W_PROXIMITY * 0.5

    return round(pts_rel + pts_vol + pts_adx + pts_prox, 1)


def evaluate_signal(df: pd.DataFrame, bench_5d: float | None) -> tuple[dict | None, str | None, dict]:
    """
    يعيد (إشارة أو None، سبب الرفض، قاموس نتائج كل الشروط).
    كل الشروط تُحسب أولاً ثم يُقرّر الرفض، فيظهر القمع كاملاً في التقرير.
    """
    df = compute_indicators(df)

    needed = [f"ema{EMA_FAST}", f"ema{EMA_MID}", f"ema{EMA_SLOW}",
              f"ema{EMA_LONG}", "rsi", "adx", "atr"]
    if len(df) < max(BREAKOUT_LOOKBACK, VOLUME_AVG_PERIOD, CONSOLIDATION_DAYS) + 3:
        return None, "Not Enough Bars", {}
    if df[needed].iloc[-1].isna().any():
        return None, "Missing Indicators", {}

    last = df.iloc[-1]
    close, open_p = float(last["close"]), float(last["open"])
    high, low = float(last["high"]), float(last["low"])
    volume = float(last["volume"])
    rsi, adx, atr = float(last["rsi"]), float(last["adx"]), float(last["atr"])

    ema_f = float(last[f"ema{EMA_FAST}"])
    ema_m = float(last[f"ema{EMA_MID}"])
    ema_s = float(last[f"ema{EMA_SLOW}"])
    ema_l = float(last[f"ema{EMA_LONG}"])

    # ---- الاختراق ----
    prev_bars = df.iloc[-(BREAKOUT_LOOKBACK + 1):-1]
    highest = float(prev_bars["high"].max())
    breakout_price = highest * BREAKOUT_BUFFER
    above_breakout = (close - highest) / highest if highest > 0 else 0.0

    # ---- الحجم ----
    # القاعدة: متوسط 20 يوماً *سابقة* — لا تشمل اليوم الحالي إطلاقاً.
    vol_base = float(df["volume"].iloc[-(VOLUME_AVG_PERIOD + 1):-1].mean())
    rvol_today = volume / vol_base if vol_base > 0 else 0.0
    rvol_5d = float(df["volume"].iloc[-RVOL_DAYS:].mean()) / vol_base if vol_base > 0 else 0.0
    dollar_volume = close * volume

    # ---- التجميع الضيق ----
    cons = df.iloc[-(CONSOLIDATION_DAYS + 1):-1]
    c_high, c_low = float(cons["high"].max()), float(cons["low"].min())
    cons_range = (c_high - c_low) / c_low if c_low > 0 else 1.0

    # ---- بقية القياسات ----
    candle_range = high - low
    range_pct = candle_range / close if close > 0 else 0.0
    prev_close = float(df["close"].iloc[-2])
    daily_return = (close - prev_close) / prev_close if prev_close > 0 else 0.0
    extension = (close - ema_m) / ema_m if ema_m > 0 else 0.0

    # ---- كل الشروط، مستقلة ----
    checks = {
        "Liquidity":  dollar_volume >= MIN_DOLLAR_VOLUME,
        "EMA Stack":  ema_f > ema_m > ema_s > ema_l,
        "Above EMA10": close >= ema_f,
        "RSI":        RSI_MIN <= rsi <= RSI_MAX,
        "ADX":        ADX_MIN <= adx <= ADX_MAX,
        "ATR":        (atr / close) >= MIN_ATR_PCT if close > 0 else False,
        "Extension":  extension <= MAX_EXTENSION,
        "Green Day":  (close > open_p) if REQUIRE_GREEN_DAY else True,
        "Sane Range": 0 < range_pct <= MAX_CANDLE_RANGE,
        "No Gap":     daily_return <= MAX_DAILY_RETURN,
        "Breakout":   close > breakout_price,
        "Not Late":   above_breakout <= MAX_ABOVE_BREAKOUT,
        "Tight Base": cons_range <= MAX_CONSOLIDATION_RANGE,
        "RVOL Today": rvol_today >= RVOL_TODAY_MIN,
        "RVOL 5d":    rvol_5d >= RVOL_5D_MIN,
    }

    failed = [k for k in CHECK_ORDER if not checks.get(k, True)]
    if failed:
        # سبب مقروء لأول شرط ساقط، مع رقمه للتشخيص
        detail = {
            "RSI": f"RSI Out ({rsi:.0f})",
            "ADX": f"ADX Out ({adx:.0f})",
            "Extension": f"Overextended ({extension*100:.1f}%)",
            "Not Late": f"Late Entry ({above_breakout*100:.1f}%)",
            "Tight Base": f"Wide Base ({cons_range*100:.0f}%)",
            "RVOL Today": f"Low RVOL ({rvol_today:.2f}x)",
            "RVOL 5d": f"Low RVOL 5d ({rvol_5d:.2f}x)",
        }.get(failed[0], failed[0])
        return None, detail, checks

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
        "close": close,
        "rsi": rsi,
        "adx": adx,
        "atr": atr,
        "rvol_today": rvol_today,
        "rvol_5d": rvol_5d,
        "above_breakout": above_breakout * 100,
        "cons_range": cons_range * 100,
        "dollar_volume": dollar_volume,
        "daily_return": daily_return * 100,
        "stock_5d": stock_5d,
        "bench_5d": bench_5d,
        "rel_strength": rel,
        "breakout_level": highest,
        "extension": extension * 100,
        "stop_loss": stop,
        "take_profit": target,
        "risk_pct": risk / close * 100,
    }, None, checks

# =============================================================================
# Telegram
# =============================================================================

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def send_msg(text: str) -> bool:
    """
    إرسال مباشر عبر HTTP — بلا asyncio، فلا مشكلة event loop إطلاقاً.
    يعيد المحاولة 3 مرات قبل الاستسلام.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("بيانات تيليجرام ناقصة")
        return False

    url = TELEGRAM_API.format(token=TELEGRAM_BOT_TOKEN)
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}

    for attempt in range(1, 4):
        try:
            r = requests.post(url, json=payload, timeout=20)
            if r.status_code == 200:
                return True
            logging.error(f"تيليجرام رد {r.status_code}: {r.text[:200]}")
        except Exception as e:
            logging.error(f"محاولة إرسال {attempt} فشلت: {e}")
        time.sleep(2 * attempt)

    return False


def send_msgs(parts: list[str]) -> bool:
    """إرسال عدة رسائل متتابعة مع فاصل بسيط لتفادي حد المعدل."""
    ok = True
    for i, p in enumerate(parts):
        if not send_msg(p):
            ok = False
        if i < len(parts) - 1:
            time.sleep(1)
    return ok


def _grade(score: float) -> str:
    if score >= 75:
        return "🥇 ممتازة"
    if score >= 60:
        return "🥈 قوية"
    if score >= 45:
        return "🥉 مقبولة"
    return "⚪ ضعيفة"


def _signal_block(rank: int, symbol: str, sg: dict) -> list[str]:
    if sg["rel_strength"] is None:
        rel = "القوة النسبية غير متاحة"
    elif sg["rel_strength"] > 0:
        rel = f"أقوى من السوق بـ {sg['rel_strength']:+.1f}%"
    else:
        rel = f"أضعف من السوق بـ {sg['rel_strength']:.1f}%"

    ed = sg.get("earnings_date")
    earn = f"🗓 أرباح: {ed}" if ed else "🗓 لا أرباح قريبة"

    return [
        "━━━━━━━━━━━━━━━━━━",
        f"{rank}) {symbol}   {sg['score']}/100  {_grade(sg['score'])}",
        "",
        f"💵 الدخول      : ${sg['close']:.2f}",
        f"🛑 وقف الخسارة : ${sg['stop_loss']:.2f}   (-{sg['risk_pct']:.1f}%)",
        f"🎯 الهدف       : ${sg['take_profit']:.2f}   (+{sg['risk_pct']*RISK_REWARD:.1f}%)",
        "",
        f"🔳 اخترق قمة 20 يوم عند ${sg['breakout_level']:.2f}",
        f"💪 {rel}",
        f"📊 RVOL اليوم {sg['rvol_today']:.2f}x  (5 أيام {sg['rvol_5d']:.2f}x)",
        f"⚡ RSI {sg['rsi']:.0f}  |  🔥 ADX {sg['adx']:.0f}",
        f"📏 بُعده عن EMA20: {sg['extension']:.1f}%  |  فوق الاختراق: {sg['above_breakout']:.1f}%",
        f"🤏 قاعدة: {sg['cons_range']:.0f}% خلال {CONSOLIDATION_DAYS} أيام",
        earn,
    ]


def format_batch_messages(signals: list, bench_5d, qqq_5d=None) -> list[str]:
    """
    يبني رسائل تيليجرام مرتبة من الأقوى للأضعف، ويقسّمها تلقائياً
    قبل حد 4096 حرفاً بحيث لا تُقطع أي صيدة في المنتصف.
    """
    signals = sorted(signals, key=lambda x: x[1]["score"], reverse=True)

    d = signals[0][1]["bar_date"]
    date_str = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
    bench_txt = f"{bench_5d:+.1f}%" if bench_5d is not None else "غير متاح"
    qqq_txt   = f"{qqq_5d:+.1f}%" if qqq_5d is not None else "غير متاح"

    header = [
        f"🎯 صيدات اليوم — {len(signals)}",
        f"شمعة {date_str}",
        f"📉 السوق 5 أيام — SPY {bench_txt}  |  QQQ {qqq_txt}",
        "",
        "مرتبة من الأقوى للأضعف 👇",
    ]

    footer = [
        "━━━━━━━━━━━━━━━━━━",
        "",
        "📋 الخطة:",
        "• انتظر 15 دقيقة بعد الفتح ثم ادخل بأمر محدد",
        "• إن فتح أعلى من سعر الدخول بأكثر من 2% → تجاهله",
        "• ضع وقف الخسارة فوراً بعد التنفيذ",
        f"• اخرج إن لم يتحرك خلال {HOLD_DAYS} يوم تداول",
        "",
        "⚠️ برأس مال صغير اكتفِ بالصيدة الأولى فقط.",
    ]

    messages: list[str] = []
    current = list(header)

    for rank, (symbol, sg) in enumerate(signals, start=1):
        block = _signal_block(rank, symbol, sg)
        if len("\n".join(current + block)) > TELEGRAM_MAX_CHARS and len(current) > len(header):
            messages.append("\n".join(current))
            current = [f"…تكملة صيدات اليوم ({len(messages)+1})"]
        current += block

    current += footer
    messages.append("\n".join(current))
    return messages


def format_funnel_report(check_counter: Counter, evaluated: int) -> str:
    """رسالة تشخيصية: كم سهماً اجتاز كل شرط على حدة، الأضيق أولاً."""
    if evaluated == 0:
        return "📭 لم يُقيَّم أي سهم اليوم."
    rows = sorted(CHECK_ORDER, key=lambda k: check_counter.get(k, 0))
    lines = [f"🔬 قمع الفلاتر — من {evaluated} سهم مُقيَّم", ""]
    for k in rows:
        c = check_counter.get(k, 0)
        lines.append(f"{c:>4} ({c/evaluated*100:>3.0f}%)  {k}")
    lines += ["", "الأعلى في القائمة = الاختناق الأضيق."]
    return "\n".join(lines)


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
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except OSError as e:
        logging.error(f"تعذّر حفظ الحالة: {e}")

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
    bench_5d = index_5d_return(all_dfs, BENCHMARK)          # SPY — يدخل في الحساب
    qqq_5d   = index_5d_return(all_dfs, SECONDARY_INDEX)    # QQQ — للعرض فقط

    signals = 0
    rejected = 0
    evaluated = 0
    found = []
    weak = []           # اجتازت كل الشروط لكن نقاطها دون العتبة
    reasons = Counter()
    check_counter = Counter()

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

        signal, reason, checks = evaluate_signal(df, bench_5d)

        if checks:
            evaluated += 1
            for k, v in checks.items():
                if v:
                    check_counter[k] += 1

        if signal is None:
            reasons[reason] += 1
            rejected += 1
            if DEBUG:
                logging.info(f"{symbol} ❌ {reason}")
            continue

        # ── عتبة النقاط ──
        # تُحسب قبل فلتر الأرباح لأنها مجانية، بينما فلتر الأرباح يكلّف
        # طلباً إضافياً لياهو لكل سهم.
        score = score_signal(signal)
        if score < MIN_SCORE:
            reasons[f"Score < {MIN_SCORE:.0f}"] += 1
            rejected += 1
            weak.append((symbol, score))
            continue

        # فلتر الأرباح — يُفحص أخيراً لأنه يتطلب طلباً إضافياً لياهو
        soon, edate = has_earnings_soon(symbol)
        if soon:
            reasons["Earnings Soon"] += 1
            rejected += 1
            logging.info(f"{symbol} ⛔ تم تجاهله — أرباح بتاريخ {edate}")
            continue

        signal["earnings_date"] = edate
        signal["score"] = score

        logging.info(
            f"✅ SIGNAL {symbol} | {signal['score']}/100 | ${signal['close']:.2f} "
            f"| SL ${signal['stop_loss']:.2f} | TP ${signal['take_profit']:.2f}"
        )
        found.append((symbol, signal))
        state[symbol] = bar_key
        signals += 1

    # ── إرسال الصيدات (مقسّمة إن لزم) ──
    if found:
        save_state(state)
        if not send_msgs(format_batch_messages(found, bench_5d, qqq_5d)):
            logging.error("⚠️ فشل إرسال رسالة الصيدات إلى تيليجرام")
        top = max(found, key=lambda x: x[1]["score"])
        logging.info(f"🏆 الأقوى اليوم: {top[0]} بـ {top[1]['score']}/100")

    elapsed = time.monotonic() - t0
    bench_txt = f"{bench_5d:+.2f}%" if bench_5d is not None else "N/A"
    qqq_txt   = f"{qqq_5d:+.2f}%" if qqq_5d is not None else "N/A"

    funnel_txt = format_funnel_report(check_counter, evaluated)

    rep = ["\n========== Daily Scan =========="]
    rep.append(f"SPY (5 أيام) : {bench_txt}   ← مرجع القوة النسبية")
    rep.append(f"QQQ (5 أيام) : {qqq_txt}   ← للعلم فقط")
    rep.append(f"Scanned   : {len(TICKERS)}")
    rep.append(f"Evaluated : {evaluated}")
    rep.append(f"Signals   : {signals}")
    rep.append(f"Rejected  : {rejected}\n")
    rep.append("Top Reject Reasons")
    for i, (r, c) in enumerate(reasons.most_common(8), 1):
        rep.append(f"{i}- {str(r):<26}: {c:<3} ({c/len(TICKERS)*100:.0f}%)")
    rep.append("\n" + funnel_txt)
    rep.append(f"\nTotal {elapsed:.1f}s")
    rep.append("================================\n")
    logging.info("\n".join(rep))

    if weak:
        weak_sorted = sorted(weak, key=lambda x: x[1], reverse=True)
        logging.info("إشارات دون العتبة: " +
                     " | ".join(f"{s} {sc:.0f}" for s, sc in weak_sorted[:10]))

    if signals == 0:
        msg = ("📭 فحص اليوم انتهى — لا توجد صيدة.\n"
               f"السوق 5 أيام — SPY {bench_txt}  |  QQQ {qqq_txt}\n")
        if weak:
            ws = sorted(weak, key=lambda x: x[1], reverse=True)[:5]
            msg += (f"\n⚪ اجتازت الشروط لكن نقاطها < {MIN_SCORE:.0f}:\n"
                    + "\n".join(f"   {s} — {sc:.0f}/100" for s, sc in ws) + "\n")
        send_msg(msg + "\n" + funnel_txt)

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
        f"🤖 Sniper Bot — النسخة اليومية v3\n"
        f"يراقب {len(TICKERS)} سهم على الفريم اليومي.\n"
        f"تم تخفيف: RVOL {RVOL_TODAY_MIN}x | قاعدة {MAX_CONSOLIDATION_RANGE*100:.0f}% | نقاط {MIN_SCORE:.0f}\n"
        f"فحص واحد يومياً بعد إغلاق السوق ✅"
    )

    state = load_state()
    last_scan_date = None
    last_hb = 0.0

    if RUN_ON_START:
        logging.info("⚡ فحص فوري عند الإقلاع...")
        scan_once(state)
        last_scan_date = datetime.now(timezone.utc).date()

    while True:
        try:
            now = datetime.now(timezone.utc)

            if time.monotonic() - last_hb >= HEARTBEAT_SECONDS:
                logging.info("💓 heartbeat")
                last_hb = time.monotonic()

            is_weekday = now.weekday() < 5
            # النافذة ممتدة: أي لحظة من 22:00 حتى 23:59 تصلح للفحص،
            # فإعادة تشغيل Railway داخل هذه الفترة لا تُضيّع فحص اليوم.
            after_scan_time = (
                now.hour > SCAN_HOUR_UTC
                or (now.hour == SCAN_HOUR_UTC and now.minute >= SCAN_MINUTE_UTC)
            )
            today = now.date()

            if is_weekday and after_scan_time and last_scan_date != today:
                scan_once(state)
                last_scan_date = today

            time.sleep(60)

        except Exception as exc:
            logging.exception(f"Loop error: {exc}")
            time.sleep(300)


if __name__ == "__main__":
    main()
