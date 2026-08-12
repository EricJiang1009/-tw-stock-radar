
import os
import asyncio
import time
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
LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() == "true" and bool(API_KEY)
TZ = ZoneInfo("Asia/Taipei")

app = FastAPI(title="台股波段雷達 Buffered Live")
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
scan_lock = asyncio.Lock()
rotation_index = 0

HISTORY_TTL = 3600
COLLECT_INTERVAL = 60
CHUNK_SIZE = 20

def tw_now():
    return datetime.now(TZ)

def in_market():
    n = tw_now()
    return n.weekday() < 5 and (9, 0) <= (n.hour, n.minute) <= (13, 30)

def is_afterhours():
    n = tw_now()
    return n.weekday() < 5 and (13, 31) <= (n.hour, n.minute) <= (18, 0)

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
            c=float(x.get("close",0) or 0); h=float(x.get("high",0) or 0)
            l=float(x.get("low",0) or 0); v=float(x.get("volume",0) or 0)
            if c>0: closes.append(c)
            if h>0: highs.append(h)
            if l>0: lows.append(l)
            if v>0: vols.append(v)
        except Exception:
            pass

    if not closes:
        return {
            "ma5": price*.98, "ma10": price*.96, "high20": price,
            "support": price*.95, "avg5vol": 0, "avg20vol": 0, "atrLike": price*.02
        }

    ma5 = mean(closes[-5:]) if len(closes)>=5 else closes[-1]
    ma10 = mean(closes[-10:]) if len(closes)>=10 else ma5
    high20 = max(highs[-20:] or [price])
    support = min(lows[-10:] or [price*.95])
    ranges=[h-l for h,l in zip(highs[-10:],lows[-10:]) if h>=l]
    atr_like=mean(ranges) if ranges else price*.02
    avg5=mean(vols[-5:]) if len(vols)>=5 else (mean(vols) if vols else 0)
    avg20=mean(vols[-20:]) if len(vols)>=20 else (mean(vols) if vols else 0)
    return {
        "ma5":ma5,"ma10":ma10,"high20":high20,"support":support,
        "avg5vol":avg5,"avg20vol":avg20,"atrLike":atr_like
    }

def build_analysis(q: dict, tech: dict) -> dict | None:
    price=q.get("price")
    if not price:
        return None

    vol=q.get("volume",0)
    avg5=tech.get("avg5vol",0)
    if avg5:
        comparable=vol*1000 if vol < avg5/100 else vol
        vol_ratio=comparable/avg5
    else:
        vol_ratio=0

    high20=tech.get("high20",price)
    dist=max((high20-price)/price*100,0)
    change=q.get("changePercent",0)

    if vol_ratio>=2.2 and 1<=change<=7 and dist<=1.5:
        stage="首日放量"
    elif vol_ratio>=2.8 and change>7:
        stage="巨量突破"
    elif dist<=0.5 and change>0:
        stage="準備突破"
    elif vol_ratio>=1.5 and change>0:
        stage="量價轉強"
    else:
        stage="觀察"

    score=min(100,
        min(vol_ratio/3,1)*35
        + (25 if dist<=.5 else 20 if dist<=1.5 else 10 if dist<=3 else 0)
        + (20 if 1<=change<=6 else 13 if 6<change<=9 else 6 if change>9 else 0)
        + (8 if price<1000 else 0)
    )

    ar=max(tech.get("atrLike",price*.02), price*.012)
    pullback=max(tech.get("ma5",price*.98), price-ar*.65)
    breakout=max(high20, price*1.002)
    no_chase=breakout+max(ar*.7,price*.018)

    # 雷達本身的技術停損不使用持股的固定 -5%，避免混淆。
    radar_stop=max(pullback-ar*1.0, price*.94)
    risk=max(price-radar_stop,price*.015)
    t1=price+risk*1.6
    t2=price+risk*2.4

    return {
        "symbol":q["symbol"],"name":q["name"],"price":round(price,2),
        "changePercent":round(change,2),"volume":vol,"volRatio":round(vol_ratio,2),
        "breakoutDistance":round(dist,2),"stage":stage,"score":round(score,1),
        "pullback":round(pullback,2),"breakout":round(breakout,2),
        "noChase":round(no_chase,2),"stop":round(radar_stop,2),
        "target1":round(t1,2),"target2":round(t2,2),
        "ma5":round(tech.get("ma5",0),2),"ma10":round(tech.get("ma10",0),2),
        "support":round(tech.get("support",0),2),"updatedAt":q.get("updatedAt",time.time())
    }

async def collect_symbol(symbol: str):
    try:
        raw=await fugle_get(f"/intraday/quote/{symbol}")
        q=normalize_quote(symbol,raw)
        quote_cache[symbol]=q
        hist=await get_history(symbol)
        tech=calc_technical(q["price"],hist) if q["price"] else {}
        a=build_analysis(q,tech) if q["price"] else None
        if a:
            analysis_cache[symbol]=a
        return a
    except Exception as e:
        print("collect error",symbol,str(e))
        return analysis_cache.get(symbol)

def rebuild_working():
    global working_today, working_after
    rows=[x for x in analysis_cache.values() if x and (x["price"]<1000 or x["score"]>=93)]
    rows.sort(key=lambda x:x["score"],reverse=True)
    working_today=rows[:20]
    # 盤後暫以同一分析池，收盤後報價會自然成為收盤資料。
    working_after=rows[:20]

async def collect_cycle():
    global rotation_index, collector_status
    if not LIVE_MODE:
        collector_status="demo"
        return

    async with scan_lock:
        collector_status="collecting"
        symbols=WATCHLIST
        if not symbols:
            return
        start=rotation_index % len(symbols)
        chunk=[symbols[(start+i)%len(symbols)] for i in range(min(CHUNK_SIZE,len(symbols)))]
        rotation_index=(start+len(chunk))%len(symbols)

        sem=asyncio.Semaphore(3)
        async def guarded(s):
            async with sem:
                r=await collect_symbol(s)
                await asyncio.sleep(.25)
                return r
        await asyncio.gather(*[guarded(s) for s in chunk])

        # 持股只抓 quote，不強制每次重抓歷史。
        for s in list(holding_symbols):
            if s not in quote_cache or time.time()-quote_cache[s].get("updatedAt",0)>90:
                try:
                    raw=await fugle_get(f"/intraday/quote/{s}")
                    quote_cache[s]=normalize_quote(s,raw)
                    await asyncio.sleep(.18)
                except Exception:
                    pass

        rebuild_working()
        collector_status="ready"

async def collector_loop():
    await asyncio.sleep(2)
    while True:
        try:
            await collect_cycle()
        except Exception as e:
            print("collector loop",e)
        # 盤中每分鐘、盤後5分鐘、其他15分鐘。
        if in_market():
            delay=60
        elif is_afterhours():
            delay=300
        else:
            delay=900
        await asyncio.sleep(delay)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(collector_loop())

@app.get("/")
async def root():
    return FileResponse("index.html")

@app.get("/api/status")
async def status():
    return {
        "live":LIVE_MODE,
        "provider":"Fugle" if LIVE_MODE else "DEMO",
        "collector":collector_status,
        "workingTodayCount":len(working_today),
        "workingAfterCount":len(working_after),
        "lastCollectAt":max([x.get("updatedAt",0) for x in analysis_cache.values()] or [0]),
    }

@app.post("/api/holdings/register")
async def register_holdings(symbols: str = Query(...)):
    for s in [x.strip() for x in symbols.split(",") if x.strip()]:
        holding_symbols.add(s)
    return {"ok":True,"count":len(holding_symbols)}

def published_payload(obj):
    return {
        "live":LIVE_MODE,
        "updatedAt":obj["updatedAt"],
        "rows":obj["rows"],
        "collector":collector_status,
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
    if mode=="after":
        published_after={"updatedAt":time.time(),"rows":[dict(x) for x in working_after]}
        return published_payload(published_after)
    published_today={"updatedAt":time.time(),"rows":[dict(x) for x in working_today]}
    return published_payload(published_today)

def holding_row(symbol: str):
    q=quote_cache.get(symbol)
    a=analysis_cache.get(symbol)
    if not q:
        return {"symbol":symbol,"error":True,"note":"背景尚未取得行情"}
    return {
        "symbol":symbol,
        "name":q.get("name",symbol),
        "price":q.get("price"),
        "lastPrice":q.get("lastPrice"),
        "closePrice":q.get("closePrice"),
        "previousClose":q.get("previousClose"),
        "changePercent":q.get("changePercent"),
        "volume":q.get("volume"),
        "volRatio":a.get("volRatio") if a else None,
        "ma5":a.get("ma5") if a else None,
        "ma10":a.get("ma10") if a else None,
        "support":a.get("support") if a else None,
        "stage":a.get("stage") if a else None,
        "score":a.get("score") if a else None,
        "updatedAt":q.get("updatedAt"),
    }

@app.get("/api/holdings/published")
async def holdings_published():
    return {
        "live":LIVE_MODE,
        "updatedAt":published_holdings["updatedAt"],
        "rows":published_holdings["rows"],
    }

@app.post("/api/holdings/publish")
async def publish_holdings(symbols: str = Query(...)):
    global published_holdings
    syms=[x.strip() for x in symbols.split(",") if x.strip()]
    for s in syms:
        holding_symbols.add(s)
    rows=[holding_row(s) for s in syms]
    published_holdings={"updatedAt":time.time(),"rows":rows}
    return {"live":LIVE_MODE,**published_holdings}

@app.get("/health")
async def health():
    return {"ok":True,"live":LIVE_MODE,"collector":collector_status}
