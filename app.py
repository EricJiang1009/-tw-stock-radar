
import os
import asyncio
import time
import json
from datetime import datetime
from statistics import mean
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

BASE = "https://api.fugle.tw/marketdata/v1.0/stock"
API_KEY = os.getenv("FUGLE_API_KEY", "").strip()
FINMIND_TOKEN = os.getenv("FINMIND_TOKEN", "").strip()
FINMIND_BASE = "https://api.finmindtrade.com/api/v4/data"
FINMIND_ENABLED = bool(FINMIND_TOKEN)
FINMIND_STATUS = "disabled" if not FINMIND_ENABLED else "starting"
FINMIND_LAST_ATTEMPT_AT = 0.0
FINMIND_LAST_SUCCESS_AT = 0.0
FINMIND_LAST_ERROR = ""
FINMIND_LOOP_LAST_AT = 0.0
FINMIND_INTERVAL_MARKET = 300
FINMIND_INTERVAL_AFTER = 900
FINMIND_INTERVAL_OFF = 1800
LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() == "true" and bool(API_KEY)
TZ = ZoneInfo("Asia/Taipei")

app = FastAPI(title="台股波段雷達 Buffered Live v3")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

WATCHLIST = [
    "2330","2303","2454","3034","2379","2317","2382","3231","2356","2324",
    "2376","2313","4958","8046","3037","2368","2408","2344","2337","2327",
    "2492","2308","1605","1717","6488","6182","5425","3374","6217","3481",
    "2409","6770","3706","3017","3324","1303","2383","2883","2882","2881"
]

DEFAULT_HOLDINGS = {
    "1303","2303","2313","2317","2356","2376","2382","2408","2492",
    "4958","3374","5425","6217"
}

quote_cache: dict[str, dict] = {}
history_cache: dict[str, dict] = {}
analysis_cache: dict[str, dict] = {}
holding_symbols = set(DEFAULT_HOLDINGS)

working_today: list[dict] = []
working_after: list[dict] = []

published_today = {"updatedAt": 0, "rows": []}
published_after = {"updatedAt": 0, "rows": []}
published_holdings = {"updatedAt": 0, "rows": []}

collector_status = "starting"
last_error = ""
last_collect_at = 0.0
scan_lock = asyncio.Lock()
rotation_index = 0

HISTORY_TTL = 3600
QUOTE_STALE = 90
COLLECT_INTERVAL_MARKET = 60
COLLECT_INTERVAL_AFTER = 300
COLLECT_INTERVAL_OFF = 900
CHUNK_SIZE = 18

# V7 three-layer radar
MARGIN_CAUTION = 20.0
MARGIN_EXCLUDE = 30.0
ENRICH_TTL = 21600
fundamental_cache: dict[str, dict] = {}
chip_cache: dict[str, dict] = {}
enrich_updated_at = 0.0
UNIVERSE_REFRESH_TTL = 21600
UNIVERSE_URLS = [
    "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
    "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
]
universe_symbols: list[str] = []
universe_updated_at = 0.0
universe_source = "fallback"


def tw_now():
    return datetime.now(TZ)


def in_market():
    n = tw_now()
    return n.weekday() < 5 and (9, 0) <= (n.hour, n.minute) <= (13, 30)


def is_afterhours():
    n = tw_now()
    return n.weekday() < 5 and (13, 31) <= (n.hour, n.minute) <= (18, 0)



def valid_symbol(v: str) -> bool:
    v = str(v or "").strip()
    return v.isdigit() and len(v) == 4


def extract_symbol(row: dict) -> str | None:
    for k in ("公司代號", "SecuritiesCompanyCode", "Code", "股票代號", "證券代號"):
        v = row.get(k)
        if valid_symbol(v):
            return str(v).strip()
    return None


async def refresh_universe(force: bool = False):
    """Build a dynamic TWSE + TPEx common-stock universe from official OpenAPI.
    Falls back to WATCHLIST if the official list is temporarily unavailable.
    """
    global universe_symbols, universe_updated_at, universe_source, last_error
    if universe_symbols and not force and time.time() - universe_updated_at < UNIVERSE_REFRESH_TTL:
        return universe_symbols

    found = set()
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        for url in UNIVERSE_URLS:
            try:
                r = await client.get(url)
                r.raise_for_status()
                data = r.json()
                if isinstance(data, list):
                    for row in data:
                        if isinstance(row, dict):
                            s = extract_symbol(row)
                            if s:
                                found.add(s)
            except Exception as e:
                last_error = f"universe: {str(e)}"

    if found:
        # Four-digit common-stock universe. High-priced names are filtered later by score/price.
        universe_symbols = sorted(found)
        universe_updated_at = time.time()
        universe_source = "TWSE+TPEx OpenAPI"
    elif not universe_symbols:
        universe_symbols = list(WATCHLIST)
        universe_updated_at = time.time()
        universe_source = "fallback WATCHLIST"

    return universe_symbols


def current_universe():
    return universe_symbols if universe_symbols else WATCHLIST

async def fugle_get(path: str, params: dict | None = None):
    if not LIVE_MODE:
        raise RuntimeError("LIVE mode disabled")

    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(
            f"{BASE}{path}",
            headers={"X-API-KEY": API_KEY},
            params=params or {},
        )
        r.raise_for_status()
        return r.json()


def normalize_quote(symbol: str, q: dict) -> dict:
    raw_price = (
        q.get("lastPrice")
        or q.get("closePrice")
        or q.get("tradePrice")
        or q.get("price")
    )
    prev = (
        q.get("previousClose")
        or q.get("referencePrice")
        or q.get("previousClosePrice")
    )
    total = q.get("total") or {}
    volume = (
        total.get("tradeVolume")
        or q.get("tradeVolume")
        or q.get("volume")
        or 0
    )

    price = float(raw_price) if raw_price not in (None, "") else None
    previous = float(prev) if prev not in (None, "") else None
    change = q.get("changePercent")

    if change is None and price and previous:
        change = (price / previous - 1) * 100

    return {
        "symbol": str(q.get("symbol") or symbol),
        "name": q.get("name") or symbol,
        "price": price,
        "lastPrice": price,
        "closePrice": float(q["closePrice"]) if q.get("closePrice") not in (None, "") else None,
        "previousClose": previous,
        "changePercent": round(float(change or 0), 2),
        "volume": float(volume or 0),
        "updatedAt": time.time(),
    }


def history_rows(payload: Any) -> list[dict]:
    if isinstance(payload, dict):
        for key in ("data", "candles"):
            if isinstance(payload.get(key), list):
                return payload[key]
    return payload if isinstance(payload, list) else []


async def get_history(symbol: str):
    cached = history_cache.get(symbol)
    if cached and time.time() - cached["ts"] < HISTORY_TTL:
        return cached["data"]

    try:
        data = await fugle_get(
            f"/historical/candles/{symbol}",
            {"timeframe": "D", "fields": "open,high,low,close,volume"},
        )
        history_cache[symbol] = {"ts": time.time(), "data": data}
        return data
    except Exception:
        return cached["data"] if cached else []


def calc_technical(price: float, payload: Any) -> dict:
    rows = history_rows(payload)
    try:
        rows = sorted(rows, key=lambda x: str(x.get("date") or x.get("time") or ""))
    except Exception:
        pass
    rows = rows[-30:]

    closes, highs, lows, vols = [], [], [], []
    for x in rows:
        try:
            c = float(x.get("close", 0) or 0)
            h = float(x.get("high", 0) or 0)
            l = float(x.get("low", 0) or 0)
            v = float(x.get("volume", 0) or 0)
            if c > 0: closes.append(c)
            if h > 0: highs.append(h)
            if l > 0: lows.append(l)
            if v > 0: vols.append(v)
        except Exception:
            pass

    if not closes:
        return {
            "ma5": price * .98,
            "ma10": price * .96,
            "high20": price,
            "support": price * .95,
            "avg5vol": 0,
            "avg20vol": 0,
            "atrLike": price * .02,
            "ma20": price * .94,
            "goldenCross": False,
            "bullishMA": False,
            "bias5": 0,
            "bias20": 0,
        }

    ma5 = mean(closes[-5:]) if len(closes) >= 5 else closes[-1]
    ma10 = mean(closes[-10:]) if len(closes) >= 10 else ma5
    high20 = max(highs[-20:] or [price])
    support = min(lows[-10:] or [price * .95])
    ranges = [h - l for h, l in zip(highs[-10:], lows[-10:]) if h >= l]
    atr_like = mean(ranges) if ranges else price * .02
    avg5 = mean(vols[-5:]) if len(vols) >= 5 else (mean(vols) if vols else 0)
    avg20 = mean(vols[-20:]) if len(vols) >= 20 else (mean(vols) if vols else 0)

    ma20 = mean(closes[-20:]) if len(closes) >= 20 else ma10
    prev5 = mean(closes[-6:-1]) if len(closes) >= 6 else ma5
    prev10 = mean(closes[-11:-1]) if len(closes) >= 11 else ma10
    golden_cross = (ma5 > ma10 and prev5 <= prev10)
    bullish_ma = ma5 > ma10 > ma20
    bias5 = (price / ma5 - 1) * 100 if ma5 else 0
    bias20 = (price / ma20 - 1) * 100 if ma20 else 0

    return {
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "high20": high20,
        "support": support,
        "avg5vol": avg5,
        "avg20vol": avg20,
        "atrLike": atr_like,
        "goldenCross": golden_cross,
        "bullishMA": bullish_ma,
        "bias5": bias5,
        "bias20": bias20,
    }



def score_technical(change, vol_ratio, dist, tech):
    s = 0.0
    s += min(max(vol_ratio, 0) / 3, 1) * 25
    s += 20 if dist <= .5 else 16 if dist <= 1.5 else 8 if dist <= 3 else 0
    s += 14 if 1 <= change <= 6 else 9 if 6 < change <= 9 else 3 if change > 9 else 0
    if tech.get("goldenCross"): s += 16
    elif tech.get("bullishMA"): s += 10
    b = abs(float(tech.get("bias5", 0) or 0))
    if b <= 3: s += 15
    elif b <= 6: s += 10
    elif b <= 10: s += 4
    else: s -= 8
    if float(tech.get("bias20", 0) or 0) > 15: s -= 8
    return round(max(0, min(100, s)), 1)


def score_fundamental(f):
    if not f or not f.get("available"):
        return 50.0
    s = 50.0
    eps = f.get("eps")
    pe = f.get("pe")
    yoy = f.get("revenueYoY")
    profitable = f.get("profitable")
    if profitable is True: s += 15
    elif profitable is False: s -= 25
    if eps is not None:
        s += 10 if eps > 0 else -15
    if yoy is not None:
        s += 12 if yoy >= 10 else 6 if yoy > 0 else -10
    if pe is not None and pe > 0:
        s += 10 if 8 <= pe <= 25 else 5 if pe <= 40 else -10
    return round(max(0, min(100, s)), 1)


def score_chip(c):
    if not c or not c.get("available"):
        return 50.0
    s = 55.0
    mr = c.get("marginRatio")
    inst = c.get("institutionNet")
    if mr is not None:
        s += 15 if mr < 10 else 5 if mr < 20 else -15 if mr < 30 else -40
    if inst is not None:
        s += 20 if inst > 0 else -15 if inst < 0 else 0
    return round(max(0, min(100, s)), 1)


def enriched(symbol):
    return fundamental_cache.get(symbol, {}), chip_cache.get(symbol, {})

def build_analysis(q: dict, tech: dict) -> dict | None:
    price = q.get("price")
    if not price:
        return None

    vol = q.get("volume", 0)
    avg5 = tech.get("avg5vol", 0)

    if avg5:
        comparable = vol * 1000 if vol < avg5 / 100 else vol
        vol_ratio = comparable / avg5
    else:
        vol_ratio = 0

    high20 = tech.get("high20", price)
    dist = max((high20 - price) / price * 100, 0)
    change = q.get("changePercent", 0)

    if vol_ratio >= 2.2 and 1 <= change <= 7 and dist <= 1.5:
        stage = "首日放量"
    elif vol_ratio >= 2.8 and change > 7:
        stage = "巨量突破"
    elif dist <= 0.5 and change > 0:
        stage = "準備突破"
    elif vol_ratio >= 1.5 and change > 0:
        stage = "量價轉強"
    else:
        stage = "觀察"

    fundamental, chip = enriched(q["symbol"])
    technical_score = score_technical(change, vol_ratio, dist, tech)
    fundamental_score = score_fundamental(fundamental)
    chip_score = score_chip(chip)

    # 技術 50%、基本面 25%、籌碼 25%。資料不足採中性 50，不偽造數字。
    score = round(technical_score * .50 + fundamental_score * .25 + chip_score * .25, 1)

    # 融資使用率 >=30% 原則上不進 Top20；在資料列保留旗標供過濾與顯示。
    margin_ratio = chip.get("marginRatio")
    margin_excluded = margin_ratio is not None and margin_ratio >= MARGIN_EXCLUDE

    ar = max(tech.get("atrLike", price * .02), price * .012)
    pullback = max(tech.get("ma5", price * .98), price - ar * .65)
    breakout = max(high20, price * 1.002)
    no_chase = breakout + max(ar * .7, price * .018)
    radar_stop = max(pullback - ar * 1.0, price * .94)
    risk = max(price - radar_stop, price * .015)
    t1 = price + risk * 1.6
    t2 = price + risk * 2.4

    return {
        "symbol": q["symbol"],
        "name": q["name"],
        "price": round(price, 2),
        "changePercent": round(change, 2),
        "volume": vol,
        "volRatio": round(vol_ratio, 2),
        "breakoutDistance": round(dist, 2),
        "stage": stage,
        "score": round(score, 1),
        "pullback": round(pullback, 2),
        "breakout": round(breakout, 2),
        "noChase": round(no_chase, 2),
        "stop": round(radar_stop, 2),
        "target1": round(t1, 2),
        "target2": round(t2, 2),
        "ma5": round(tech.get("ma5", 0), 2),
        "ma10": round(tech.get("ma10", 0), 2),
        "ma20": round(tech.get("ma20", 0), 2),
        "support": round(tech.get("support", 0), 2),
        "goldenCross": bool(tech.get("goldenCross")),
        "bullishMA": bool(tech.get("bullishMA")),
        "bias5": round(float(tech.get("bias5", 0) or 0), 2),
        "bias20": round(float(tech.get("bias20", 0) or 0), 2),
        "technicalScore": technical_score,
        "fundamentalScore": fundamental_score,
        "chipScore": chip_score,
        "eps": fundamental.get("eps"),
        "pe": fundamental.get("pe"),
        "revenueYoY": fundamental.get("revenueYoY"),
        "profitable": fundamental.get("profitable"),
        "marginRatio": margin_ratio,
        "institutionNet": chip.get("institutionNet"),
        "fundamentalAvailable": bool(fundamental.get("available")),
        "chipAvailable": bool(chip.get("available")),
        "marginExcluded": margin_excluded,
        "updatedAt": q.get("updatedAt", time.time()),
    }


async def collect_symbol_full(symbol: str):
    global last_error
    try:
        raw = await fugle_get(f"/intraday/quote/{symbol}")
        q = normalize_quote(symbol, raw)
        quote_cache[symbol] = q

        hist = await get_history(symbol)
        tech = calc_technical(q["price"], hist) if q["price"] else {}
        a = build_analysis(q, tech) if q["price"] else None

        if a:
            analysis_cache[symbol] = a

        return a
    except Exception as e:
        last_error = f"{symbol}: {str(e)}"
        print("collect error", symbol, str(e))
        return analysis_cache.get(symbol)


def rebuild_working():
    global working_today, working_after
    rows = [
        x for x in analysis_cache.values()
        if x and not x.get("marginExcluded") and (x["price"] < 1000 or x["score"] >= 93)
    ]
    rows.sort(key=lambda x: x["score"], reverse=True)
    working_today = rows[:20]
    working_after = rows[:20]


async def collect_holdings_full():
    """
    v3 修正重點：
    所有持股都會做完整分析，不只補 quote。
    這樣量比 / 5日線 / 技術支撐不會長時間維持 '-'
    """
    if not holding_symbols:
        return

    sem = asyncio.Semaphore(2)

    async def guarded(s):
        async with sem:
            await collect_symbol_full(s)
            await asyncio.sleep(.25)

    await asyncio.gather(*[guarded(s) for s in list(holding_symbols)])


async def collect_cycle():
    global rotation_index, collector_status, last_collect_at

    if not LIVE_MODE:
        collector_status = "demo"
        return

    if scan_lock.locked():
        return

    async with scan_lock:
        collector_status = "collecting"
        await refresh_enrichment()

        # 1) 持股優先完整更新
        await collect_holdings_full()

        # 2) 動態全市場候選池：上市＋上櫃官方清單，分批輪掃。
        # 基本 Fugle 方案無法一次 snapshot 全市場，因此用背景輪替避免 429。
        await refresh_universe()
        symbols = current_universe()
        if symbols:
            start = rotation_index % len(symbols)
            chunk = [
                symbols[(start + i) % len(symbols)]
                for i in range(min(CHUNK_SIZE, len(symbols)))
            ]
            rotation_index = (start + len(chunk)) % len(symbols)

            sem = asyncio.Semaphore(3)

            async def guarded(s):
                async with sem:
                    r = await collect_symbol_full(s)
                    await asyncio.sleep(.25)
                    return r

            await asyncio.gather(*[guarded(s) for s in chunk])

        rebuild_working()
        last_collect_at = time.time()
        collector_status = "ready"




async def finmind_get(dataset: str, data_id: str | None = None, start_date: str | None = None, end_date: str | None = None):
    global FINMIND_STATUS, FINMIND_LAST_ATTEMPT_AT, FINMIND_LAST_SUCCESS_AT, FINMIND_LAST_ERROR

    if not FINMIND_ENABLED:
        FINMIND_STATUS = "disabled"
        return []

    FINMIND_STATUS = "updating"
    FINMIND_LAST_ATTEMPT_AT = time.time()

    params={"dataset":dataset,"token":FINMIND_TOKEN}
    if data_id: params["data_id"]=data_id
    if start_date: params["start_date"]=start_date
    if end_date: params["end_date"]=end_date

    try:
        async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
            r=await client.get(FINMIND_BASE, params=params)

            if r.status_code == 429:
                FINMIND_STATUS = "rate_limited"
                FINMIND_LAST_ERROR = "FinMind API rate limit (429)"
                return []

            if r.status_code == 402:
                FINMIND_STATUS = "error"
                FINMIND_LAST_ERROR = "FinMind plan/permission rejected (402)"
                return []

            r.raise_for_status()
            payload=r.json()
            rows = payload.get("data",[]) if isinstance(payload,dict) else []

            FINMIND_STATUS = "ready"
            FINMIND_LAST_SUCCESS_AT = time.time()
            FINMIND_LAST_ERROR = ""
            return rows

    except Exception as e:
        FINMIND_STATUS = "error"
        FINMIND_LAST_ERROR = str(e)[:180]
        raise


def fm_float(v):
    try:
        if v is None or v=="": return None
        return float(str(v).replace(",","").replace("%",""))
    except Exception:
        return None


async def enrich_finmind_symbol(symbol: str):
    """Low-frequency FinMind enrichment for candidates/holdings.
    Fugle remains the live-price source; FinMind supplies deeper daily/fundamental/chip data.
    """
    import datetime
    today=datetime.date.today()
    start=(today-datetime.timedelta(days=420)).isoformat()

    f=fundamental_cache.setdefault(symbol,{})
    c=chip_cache.setdefault(symbol,{})

    try:
        per=await finmind_get("TaiwanStockPER",symbol,(today-datetime.timedelta(days=45)).isoformat())
        if per:
            row=per[-1]
            pe=fm_float(row.get("PER"))
            if pe is not None: f["pe"]=pe
    except Exception:
        pass

    try:
        rev=await finmind_get("TaiwanStockMonthRevenue",symbol,start)
        if len(rev)>=13:
            cur=rev[-1]
            current=fm_float(cur.get("revenue"))
            # Find same month previous year where possible
            target_month=cur.get("revenue_month")
            target_year=(cur.get("revenue_year") or 0)-1
            prev=next((x for x in reversed(rev[:-1]) if x.get("revenue_month")==target_month and x.get("revenue_year")==target_year),None)
            previous=fm_float(prev.get("revenue")) if prev else None
            if current is not None and previous not in (None,0):
                f["revenueYoY"]=round((current/previous-1)*100,2)
    except Exception:
        pass

    try:
        fs=await finmind_get("TaiwanStockFinancialStatements",symbol,start)
        if fs:
            # FinMind financial statements are account rows; use latest BasicEPS / net income-like rows.
            latest={}
            for row in fs:
                typ=str(row.get("type",""))
                val=fm_float(row.get("value"))
                if val is not None:
                    latest[typ]=val
            eps=None
            for k,v in latest.items():
                if "EPS" in k.upper() or "EarningsPerShare" in k:
                    eps=v
            if eps is not None: f["eps"]=eps
            profit_vals=[v for k,v in latest.items() if any(t in k.lower() for t in ("netincome","profitloss","incomeaftertax"))]
            if profit_vals: f["profitable"]=profit_vals[-1] > 0
    except Exception:
        pass

    try:
        margin=await finmind_get("TaiwanStockMarginPurchaseShortSale",symbol,(today-datetime.timedelta(days=45)).isoformat())
        if margin:
            row=margin[-1]
            # Keep raw balance when ratio denominator isn't supplied; don't fabricate a percentage.
            bal=None
            for k in ("MarginPurchaseTodayBalance","MarginPurchaseBalance","MarginPurchaseStockTodayBalance"):
                if k in row:
                    bal=fm_float(row.get(k)); break
            if bal is not None: c["marginBalance"]=bal
    except Exception:
        pass

    try:
        inst=await finmind_get("TaiwanStockInstitutionalInvestorsBuySell",symbol,(today-datetime.timedelta(days=20)).isoformat())
        if inst:
            latest_date=inst[-1].get("date")
            rows=[x for x in inst if x.get("date")==latest_date]
            net=0.0; got=False
            for row in rows:
                buy=fm_float(row.get("buy")); sell=fm_float(row.get("sell"))
                if buy is not None and sell is not None:
                    net += buy-sell; got=True
            if got: c["institutionNet"]=net
    except Exception:
        pass

    if any(k in f for k in ("eps","pe","revenueYoY","profitable")): f["available"]=True
    if any(k in c for k in ("marginRatio","marginBalance","institutionNet")): c["available"]=True


async def refresh_finmind_priority():
    global FINMIND_STATUS
    if not FINMIND_ENABLED:
        FINMIND_STATUS = "disabled"
        return
    FINMIND_STATUS = "updating"
    # Enrich holdings first, then strongest currently-known candidates.
    symbols=[]
    for h in holdings:
        s=str(h.get("symbol",""))
        if s and s not in symbols: symbols.append(s)
    ranked=sorted(analysis_cache.values(), key=lambda x:x.get("score",0), reverse=True)
    for x in ranked[:20]:
        s=str(x.get("symbol",""))
        if s and s not in symbols: symbols.append(s)
    # Rate-limit friendly: only a few symbols per collector pass.
    for s in symbols[:4]:
        await enrich_finmind_symbol(s)

    if FINMIND_STATUS == "updating":
        FINMIND_STATUS = "ready"


async def refresh_enrichment():
    """Best-effort official public-data enrichment.
    Missing/changed upstream fields remain unavailable rather than being fabricated.
    """
    global enrich_updated_at, fundamental_cache, chip_cache
    if enrich_updated_at and time.time() - enrich_updated_at < ENRICH_TTL:
        return

    urls = [
        ("https://openapi.twse.com.tw/v1/opendata/t187ap14_L", "fund"),
        ("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O", "fund"),
        ("https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX", "valuation"),
    ]
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        for url, kind in urls:
            try:
                r = await client.get(url)
                if not r.is_success: continue
                data = r.json()
                if not isinstance(data, list): continue
                for row in data:
                    if not isinstance(row, dict): continue
                    s = extract_symbol(row)
                    if not s: continue
                    f = fundamental_cache.setdefault(s, {})
                    # Flexible field-name matching because official schemas differ by market.
                    for k,v in row.items():
                        key=str(k)
                        try:
                            vv=float(str(v).replace(",","").replace("%",""))
                        except Exception:
                            continue
                        if "每股盈餘" in key or key.upper()=="EPS": f["eps"]=vv
                        if "本益比" in key or key.upper()=="PE": f["pe"]=vv
                        if ("增減" in key or "年增" in key) and "%" in key: f["revenueYoY"]=vv
                        if "淨利" in key or "淨損" in key:
                            f["profitable"]=vv > 0
                    if any(k in f for k in ("eps","pe","revenueYoY","profitable")):
                        f["available"]=True
            except Exception:
                pass

    # Chip/margin data schemas vary substantially across TWSE/TPEx.
    # Leave unavailable until a verified value is obtained; UI explicitly shows 資料不足.
    enrich_updated_at=time.time()


async def finmind_loop():
    global FINMIND_STATUS, FINMIND_LOOP_LAST_AT, FINMIND_LAST_ERROR

    if not FINMIND_ENABLED:
        FINMIND_STATUS = "disabled"
        return

    await asyncio.sleep(5)

    while True:
        try:
            FINMIND_STATUS = "updating"
            await refresh_finmind_priority()
            FINMIND_LOOP_LAST_AT = time.time()

            if FINMIND_STATUS == "updating":
                FINMIND_STATUS = "ready"

        except Exception as e:
            FINMIND_STATUS = "error"
            FINMIND_LAST_ERROR = str(e)[:180]
            print("finmind loop", e)

        if in_market():
            delay = FINMIND_INTERVAL_MARKET
        elif is_afterhours():
            delay = FINMIND_INTERVAL_AFTER
        else:
            delay = FINMIND_INTERVAL_OFF

        await asyncio.sleep(delay)

async def collector_loop():
    await asyncio.sleep(2)

    while True:
        try:
            await collect_cycle()
        except Exception as e:
            print("collector loop", e)

        if in_market():
            delay = COLLECT_INTERVAL_MARKET
        elif is_afterhours():
            delay = COLLECT_INTERVAL_AFTER
        else:
            delay = COLLECT_INTERVAL_OFF

        await asyncio.sleep(delay)


@app.on_event("startup")
async def startup_event():
    try:
        await refresh_universe(force=True)
    except Exception as e:
        print("universe startup", e)

    asyncio.create_task(collector_loop())
    asyncio.create_task(finmind_loop())


@app.get("/")
async def root():
    return FileResponse("index.html")


@app.get("/api/status")
async def status():
    return {
        "live": LIVE_MODE,
        "provider": "Fugle" if LIVE_MODE else "DEMO",
        "collector": collector_status,
        "workingTodayCount": len(working_today),
        "workingAfterCount": len(working_after),
        "lastCollectAt": last_collect_at,
        "finmind": FINMIND_ENABLED,
        "finmindStatus": FINMIND_STATUS,
        "finmindLastAttemptAt": FINMIND_LAST_ATTEMPT_AT,
        "finmindLastSuccessAt": FINMIND_LAST_SUCCESS_AT,
        "finmindLastError": FINMIND_LAST_ERROR,
        "finmindLoopLastAt": FINMIND_LOOP_LAST_AT,
        "lastError": last_error,
        "universeCount": len(current_universe()),
        "universeSource": universe_source,
        "universeUpdatedAt": universe_updated_at,
    }


@app.post("/api/holdings/register")
async def register_holdings(symbols: str = Query(...)):
    for s in [x.strip() for x in symbols.split(",") if x.strip()]:
        holding_symbols.add(s)
    return {"ok": True, "count": len(holding_symbols)}


def published_payload(obj):
    return {
        "live": LIVE_MODE,
        "updatedAt": obj["updatedAt"],
        "rows": obj["rows"],
        "collector": collector_status,
    }


@app.get("/api/radar/intraday")
async def radar_intraday():
    return published_payload(published_today)


@app.get("/api/radar/afterhours")
async def radar_afterhours():
    return published_payload(published_after)


@app.post("/api/radar/publish/{mode}")
async def publish_radar(mode: str):
    global published_today, published_after

    if mode == "after":
        published_after = {
            "updatedAt": time.time(),
            "rows": [dict(x) for x in working_after]
        }
        return published_payload(published_after)

    published_today = {
        "updatedAt": time.time(),
        "rows": [dict(x) for x in working_today]
    }
    return published_payload(published_today)


def holding_row(symbol: str):
    q = quote_cache.get(symbol)
    a = analysis_cache.get(symbol)

    if not q:
        return {
            "symbol": symbol,
            "error": True,
            "note": "背景尚未取得行情"
        }

    return {
        "symbol": symbol,
        "name": q.get("name", symbol),
        "price": q.get("price"),
        "lastPrice": q.get("lastPrice"),
        "closePrice": q.get("closePrice"),
        "previousClose": q.get("previousClose"),
        "changePercent": q.get("changePercent"),
        "volume": q.get("volume"),
        "volRatio": a.get("volRatio") if a else None,
        "ma5": a.get("ma5") if a else None,
        "ma10": a.get("ma10") if a else None,
        "support": a.get("support") if a else None,
        "stage": a.get("stage") if a else None,
        "score": a.get("score") if a else None,
        "updatedAt": q.get("updatedAt"),
        "analysisReady": bool(a),
    }


@app.get("/api/holdings/published")
async def holdings_published():
    return {
        "live": LIVE_MODE,
        "updatedAt": published_holdings["updatedAt"],
        "rows": published_holdings["rows"],
    }


@app.post("/api/holdings/publish")
async def publish_holdings(symbols: str = Query(...)):
    global published_holdings

    syms = [x.strip() for x in symbols.split(",") if x.strip()]
    for s in syms:
        holding_symbols.add(s)

    rows = [holding_row(s) for s in syms]

    published_holdings = {
        "updatedAt": time.time(),
        "rows": rows
    }

    return {
        "live": LIVE_MODE,
        **published_holdings
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "live": LIVE_MODE,
        "collector": collector_status,
        "lastCollectAt": last_collect_at,
    }
