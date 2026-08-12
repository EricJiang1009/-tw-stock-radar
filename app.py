import os, asyncio, time
from statistics import mean
from typing import Any
import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

BASE='https://api.fugle.tw/marketdata/v1.0/stock'
API_KEY=os.getenv('FUGLE_API_KEY','').strip()
LIVE_MODE=os.getenv('LIVE_MODE','false').lower()=='true' and bool(API_KEY)
app=FastAPI(title='台股波段雷達 Live')
app.add_middleware(CORSMiddleware,allow_origins=['*'],allow_methods=['*'],allow_headers=['*'])

WATCHLIST=['2330','2303','2454','3034','2379','3443','3661','2317','2382','3231','2356','2324','2376','2313','4958','8046','3037','2383','2368','2408','2344','2337','2327','2492','2308','1605','1717','6488','6182','5425','6510','6223','6515','3374','6217','3481','2409','6770','3706','3017','3324']
DEMO=[]
_hist_cache={}
CACHE_TTL=60*30

def headers(): return {'X-API-KEY':API_KEY}
async def fugle_get(path,params=None):
    if not LIVE_MODE: raise RuntimeError('LIVE mode disabled')
    async with httpx.AsyncClient(timeout=15.0) as client:
        r=await client.get(f'{BASE}{path}',headers=headers(),params=params or {})
        if r.status_code==429: raise HTTPException(429,'Fugle API rate limit reached')
        r.raise_for_status(); return r.json()

def hist_rows(p:Any):
    if isinstance(p,dict):
        for k in ('data','candles'):
            if isinstance(p.get(k),list): return p[k]
    return p if isinstance(p,list) else []

async def history(symbol):
    now=time.time(); c=_hist_cache.get(symbol)
    if c and now-c[0]<CACHE_TTL: return c[1]
    x=await fugle_get(f'/historical/candles/{symbol}',{'timeframe':'D','fields':'open,high,low,close,volume'})
    _hist_cache[symbol]=(now,x); return x

def qnum(q,*keys):
    for k in keys:
        v=q.get(k)
        if v not in (None,''):
            try:return float(v)
            except:pass
    return 0.0

def normalize_quote(symbol,q):
    price=qnum(q,'lastPrice','closePrice','price')
    prev=qnum(q,'previousClose','referencePrice','previousClosePrice')
    ch=qnum(q,'changePercent')
    if not ch and price and prev: ch=(price/prev-1)*100
    return {'symbol':str(q.get('symbol') or symbol),'name':q.get('name') or symbol,'price':round(price,2) if price else None,'lastPrice':round(price,2) if price else None,'closePrice':round(price,2) if price else None,'previousClose':round(prev,2) if prev else None,'changePercent':round(ch,2),'total':q.get('total') or {},'raw':q}

def tech(price,h):
    cs=list(reversed(hist_rows(h))) if hist_rows(h) else []
    cs=cs[-30:]; closes=[]; highs=[]; lows=[]; vols=[]
    for x in cs:
        for arr,key in ((closes,'close'),(highs,'high'),(lows,'low'),(vols,'volume')):
            try:
                v=float(x.get(key,0) or 0)
                if v>0: arr.append(v)
            except: pass
    if not closes:
        return {'pullback':price*.98,'breakout':price*1.01,'noChase':price*1.035,'high20':price,'avg5vol':0,'support':price*.95,'trendStrong':False}
    high20=max(highs[-20:] or [price]); low10=min(lows[-10:] or [price]); ma5=mean(closes[-5:]) if len(closes)>=5 else closes[-1]
    ma20=mean(closes[-20:]) if len(closes)>=20 else mean(closes)
    ranges=[a-b for a,b in zip(highs[-10:],lows[-10:]) if a>=b]; ar=mean(ranges) if ranges else price*.02
    pull=max(price-ar*.7,ma5); breakout=max(high20,price*1.002); nochase=breakout+max(ar*.8,price*.02)
    support=max(low10,ma20) if ma20 < price else low10
    avg5=mean(vols[-5:]) if len(vols)>=5 else (mean(vols) if vols else 0)
    trend=price>=ma5>=ma20
    return {'pullback':pull,'breakout':breakout,'noChase':nochase,'high20':high20,'avg5vol':avg5,'support':support,'trendStrong':trend}

def stage(change,vr,dist):
    if vr>=2.8 and change>7:return '巨量突破'
    if vr>=2.2 and 1<=change<=7 and dist<=1.5:return '首日放量'
    if dist<=.5 and change>0:return '準備突破'
    if vr>=1.5 and change>0:return '量價轉強'
    return '觀察'

def score(change,vr,dist,price):
    s=min(vr/3,1)*35
    s+=25 if dist<=.5 else 20 if dist<=1.5 else 10 if dist<=3 else 0
    s+=20 if 1<=change<=6 else 13 if 6<change<=9 else 6 if change>9 else 0
    if price<1000:s+=8
    return round(min(s,100),1)

async def analyze(symbol):
    try:
        q=normalize_quote(symbol,await fugle_get(f'/intraday/quote/{symbol}')); p=q['price']
        if not p:return None
        h=await history(symbol); lv=tech(p,h); total=q.get('total') or {}; vol=float(total.get('tradeVolume',q['raw'].get('tradeVolume',0)) or 0); avg=lv['avg5vol']
        comp=vol*1000 if avg and vol<avg/100 else vol; vr=comp/avg if avg else 0
        dist=max((lv['high20']-p)/p*100,0); st=stage(q['changePercent'],vr,dist); sc=score(q['changePercent'],vr,dist,p)
        # Radar stop is capped near 5%; holdings use exact cost-based 5% separately.
        stop=max(p*.95, min(lv['pullback']*.985, p*.985))
        risk=max(p-stop,p*.015); t1=p+risk*1.8; t2=p+risk*2.8
        return {'symbol':symbol,'name':q['name'],'price':p,'changePercent':q['changePercent'],'volume':vol,'volRatio':round(vr,2),'breakoutDistance':round(dist,2),'stage':st,'score':sc,'pullback':round(lv['pullback'],2),'breakout':round(lv['breakout'],2),'noChase':round(lv['noChase'],2),'stop':round(stop,2),'target1':round(t1,2),'target2':round(t2,2)}
    except Exception as e:
        print('analyze error',symbol,str(e)); return None

async def scan_market():
    rows=[]
    # Scan in small batches; continue through watchlist until 20 valid names are collected.
    for i in range(0,len(WATCHLIST),8):
        batch=WATCHLIST[i:i+8]; sem=asyncio.Semaphore(3)
        async def one(s):
            async with sem:
                x=await analyze(s); await asyncio.sleep(.18); return x
        got=await asyncio.gather(*(one(s) for s in batch))
        rows.extend(x for x in got if x and (x['price']<1000 or x['score']>=93))
        # Don't stop too early: gather enough candidates to rank a real Top 20.
        if len(rows)>=24: break
    rows.sort(key=lambda x:x['score'],reverse=True)
    return rows[:20]

@app.get('/')
async def root(): return FileResponse('index.html')
@app.get('/api/status')
async def status(): return {'live':LIVE_MODE,'provider':'Fugle' if LIVE_MODE else 'DEMO','watchlist':len(WATCHLIST)}
@app.get('/api/radar/intraday')
async def intraday(): return {'live':LIVE_MODE,'rows':await scan_market() if LIVE_MODE else DEMO}
@app.get('/api/radar/afterhours')
async def afterhours(): return {'live':LIVE_MODE,'rows':await scan_market() if LIVE_MODE else DEMO}

@app.get('/api/holdings/quotes')
async def holdings_quotes(symbols:str=Query(...)):
    syms=[x.strip() for x in symbols.split(',') if x.strip()][:30]
    if not LIVE_MODE:return {'live':False,'rows':[]}
    sem=asyncio.Semaphore(3)
    async def one(s):
        async with sem:
            try:
                q=normalize_quote(s,await fugle_get(f'/intraday/quote/{s}'))
                p=q['price']
                if not p:return {'symbol':s,'error':True}
                h=await history(s); lv=tech(p,h)
                total=q.get('total') or {}; vol=float(total.get('tradeVolume',q['raw'].get('tradeVolume',0)) or 0); avg=lv['avg5vol']; comp=vol*1000 if avg and vol<avg/100 else vol; vr=comp/avg if avg else 0
                q.pop('raw',None); q['volRatio']=round(vr,2); q['support']=round(lv['support'],2); q['trendStrong']=bool(lv['trendStrong']); q['breakout']=round(lv['breakout'],2)
                return q
            except Exception as e:
                print('holding error',s,str(e)); return {'symbol':s,'error':True}
    return {'live':True,'rows':await asyncio.gather(*(one(s) for s in syms))}

@app.get('/health')
async def health(): return {'ok':True,'live':LIVE_MODE}
