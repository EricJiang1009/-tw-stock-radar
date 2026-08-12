
import os, math, asyncio
from statistics import mean
from typing import Any
import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import FileResponse

from fastapi.middleware.cors import CORSMiddleware

BASE = "https://api.fugle.tw/marketdata/v1.0/stock"
API_KEY = os.getenv("FUGLE_API_KEY","").strip()
LIVE_MODE = os.getenv("LIVE_MODE","false").lower() == "true" and bool(API_KEY)

app = FastAPI(title="台股波段雷達 Live")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


DEMO = [
 {"symbol":"1717","name":"長興","price":78.6,"changePercent":9.32,"volume":63148,"volRatio":3.1,"breakoutDistance":0.3,"stage":"首日急放量","score":94},
 {"symbol":"2356","name":"英業達","price":69.0,"changePercent":5.99,"volume":79797,"volRatio":2.4,"breakoutDistance":0.8,"stage":"首日放量","score":92},
 {"symbol":"1605","name":"華新","price":40.0,"changePercent":4.44,"volume":145231,"volRatio":2.0,"breakoutDistance":1.0,"stage":"資金轉強","score":88},
 {"symbol":"2324","name":"仁寶","price":39.9,"changePercent":9.92,"volume":171700,"volRatio":3.8,"breakoutDistance":0.0,"stage":"強勢突破","score":86},
 {"symbol":"6770","name":"力積電","price":73.7,"changePercent":10.0,"volume":420000,"volRatio":4.2,"breakoutDistance":0.0,"stage":"巨量點火","score":84},
]

def headers():
    return {"X-API-KEY": API_KEY}

async def fugle_get(path:str, params:dict|None=None):
    if not LIVE_MODE:
        raise RuntimeError("live disabled")
    async with httpx.AsyncClient(timeout=18.0) as client:
        r = await client.get(f"{BASE}{path}", headers=headers(), params=params or {})
        if r.status_code == 429:
            raise HTTPException(429, "Fugle API rate limit reached")
        r.raise_for_status()
        return r.json()

def rows_from_historical(payload:Any):
    if isinstance(payload, dict):
        for k in ("data","candles"):
            if isinstance(payload.get(k), list):
                return payload[k]
    return payload if isinstance(payload, list) else []

def calc_levels(price, history):
    candles = rows_from_historical(history)[-30:]
    closes=[float(x.get("close",x.get("closePrice",0)) or 0) for x in candles]
    highs=[float(x.get("high",x.get("highPrice",0)) or 0) for x in candles]
    lows=[float(x.get("low",x.get("lowPrice",0)) or 0) for x in candles]
    vols=[float(x.get("volume",0) or 0) for x in candles]
    closes=[x for x in closes if x>0]; highs=[x for x in highs if x>0]; lows=[x for x in lows if x>0]
    if not closes:
        return {"breakout":price*1.01,"pullback":price*0.98,"noChase":price*1.035,"stop":price*0.95,"target1":price*1.06,"target2":price*1.10,
                "avg5vol":0,"avg20vol":0,"high20":price}
    high20=max(highs[-20:] or [price])
    low10=min(lows[-10:] or [price])
    ranges=[h-l for h,l in zip(highs[-10:], lows[-10:]) if h>=l]
    ar=mean(ranges) if ranges else price*0.02
    pullback=max(price-ar*0.7, mean(closes[-5:]) if len(closes)>=5 else price*0.98)
    breakout=max(high20, price*1.002)
    no_chase=breakout + max(ar*0.8, price*0.02)
    stop=min(pullback-ar*1.1, low10*0.995)
    risk=max(price-stop, ar*0.8, price*0.015)
    t1=price+risk*1.6
    t2=price+risk*2.4
    avg5=mean(vols[-5:]) if len(vols)>=5 else 0
    avg20=mean(vols[-20:]) if len(vols)>=20 else (mean(vols) if vols else 0)
    return {"breakout":breakout,"pullback":pullback,"noChase":no_chase,"stop":stop,"target1":t1,"target2":t2,"avg5vol":avg5,"avg20vol":avg20,"high20":high20}

def classify_stage(change, vol_ratio, breakout_distance):
    if vol_ratio >= 2.2 and 1 <= change <= 7 and breakout_distance <= 1.5: return "首日放量"
    if vol_ratio >= 2.8 and change > 7: return "巨量突破"
    if breakout_distance <= 0.5 and change > 0: return "準備突破"
    if vol_ratio >= 1.5 and change > 0: return "量價轉強"
    return "觀察"

def score_item(change, vol_ratio, breakout_distance, price):
    s=0
    s += min(vol_ratio/3,1)*35
    if breakout_distance <= .5:s+=25
    elif breakout_distance <=1.5:s+=20
    elif breakout_distance <=3:s+=10
    if 1<=change<=6:s+=20
    elif 6<change<=9:s+=13
    elif change>9:s+=6
    if price<1000:s+=8
    return round(min(s,100),1)

async def analyze_active(x):
    symbol=str(x.get("symbol",""))
    name=x.get("name","")
    price=float(x.get("tradePrice",x.get("lastPrice",x.get("closePrice",0))) or 0)
    volume=float(x.get("tradeVolume",x.get("volume",0)) or 0)
    change=float(x.get("changePercent",0) or 0)
    if not symbol or price<=0: return None
    try:
        hist = await fugle_get(f"/historical/candles/{symbol}")
        lv = calc_levels(price,hist)
    except Exception:
        lv = calc_levels(price,[])
    vol_ratio=(volume/lv["avg5vol"]) if lv["avg5vol"] else 0
    # During intraday, compare to full-day avg conservatively; actual projected volume can be added later.
    breakout_distance=max((lv["high20"]-price)/price*100,0) if price else 99
    stage=classify_stage(change,vol_ratio,breakout_distance)
    score=score_item(change,vol_ratio,breakout_distance,price)
    return {
      "symbol":symbol,"name":name,"price":round(price,2),"changePercent":round(change,2),"volume":volume,
      "volRatio":round(vol_ratio,2),"breakoutDistance":round(breakout_distance,2),"stage":stage,"score":score,
      "pullback":round(lv["pullback"],2),"breakout":round(lv["breakout"],2),"noChase":round(lv["noChase"],2),
      "stop":round(lv["stop"],2),"target1":round(lv["target1"],2),"target2":round(lv["target2"],2)
    }

async def scan_market():
    # Fugle Snapshot Actives is available on Developer/Advanced plans.
    pools=[]
    for market in ("TSE","OTC"):
        try:
            data=await fugle_get(f"/snapshot/actives/{market}", {"trade":"volume","type":"COMMONSTOCK"})
            pools.extend((data.get("data") or [])[:80])
        except Exception:
            pass
    if not pools: return []
    sem=asyncio.Semaphore(8)
    async def guarded(x):
        async with sem: return await analyze_active(x)
    analyzed=await asyncio.gather(*[guarded(x) for x in pools])
    rows=[x for x in analyzed if x]
    # hard rules: avoid disposition through ticker list upstream where possible; high-priced names require exceptional score
    rows=[x for x in rows if x["price"]<1000 or x["score"]>=93]
    rows.sort(key=lambda z:z["score"], reverse=True)
    return rows[:20]

@app.get("/")
async def root():
    return FileResponse("index.html")

@app.get("/api/status")
async def status():
    return {"live":LIVE_MODE,"provider":"Fugle" if LIVE_MODE else "DEMO"}

@app.get("/api/quote/{symbol}")
async def quote(symbol:str):
    if not LIVE_MODE:
        for x in DEMO:
            if x["symbol"]==symbol: return x
        return {"symbol":symbol,"name":symbol,"closePrice":None,"lastPrice":None}
    return await fugle_get(f"/intraday/quote/{symbol}")

@app.get("/api/quotes")
async def quotes(symbols:str=Query(...,description="comma separated")):
    syms=[x.strip() for x in symbols.split(",") if x.strip()][:50]
    if not LIVE_MODE:
        by={x["symbol"]:x for x in DEMO}
        return {"rows":[by.get(s,{"symbol":s,"name":s,"price":None}) for s in syms]}
    sem=asyncio.Semaphore(8)
    async def one(s):
        async with sem:
            try:return await fugle_get(f"/intraday/quote/{s}")
            except Exception:return {"symbol":s,"error":True}
    return {"rows":await asyncio.gather(*[one(s) for s in syms])}

@app.get("/api/radar/intraday")
async def radar_intraday():
    if not LIVE_MODE:
        rows=[]
        for x in DEMO:
            p=x["price"]
            rows.append({**x,"pullback":round(p*.98,2),"breakout":round(p*1.012,2),"noChase":round(p*1.035,2),"stop":round(p*.94,2),"target1":round(p*1.07,2),"target2":round(p*1.12,2)})
        return {"live":False,"rows":rows}
    return {"live":True,"rows":await scan_market()}

@app.get("/api/radar/afterhours")
async def radar_afterhours():
    # Same engine after market close; final full-day volume makes volRatio more reliable.
    if not LIVE_MODE:
        return await radar_intraday()
    return {"live":True,"rows":await scan_market()}

@app.get("/api/holdings/quotes")
async def holdings_quotes(symbols:str):
    return await quotes(symbols)
