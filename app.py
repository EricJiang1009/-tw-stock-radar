import os
import asyncio
from statistics import mean
from typing import Any

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware


# =========================================================
# Fugle
# =========================================================

BASE = "https://api.fugle.tw/marketdata/v1.0/stock"

API_KEY = os.getenv("FUGLE_API_KEY", "").strip()

LIVE_MODE = (
    os.getenv("LIVE_MODE", "false").lower() == "true"
    and bool(API_KEY)
)


# =========================================================
# FastAPI
# =========================================================

app = FastAPI(title="台股波段雷達 Live")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# 免費版掃描池
#
# Fugle 基本用戶不能使用 snapshot/actives，
# 因此改成逐檔掃描候選股票。
# 之後可以繼續擴充。
# =========================================================

WATCHLIST = [

    # 半導體 / AI
    "2330",  # 台積電
    "2303",  # 聯電
    "2454",  # 聯發科
    "3034",  # 聯詠
    "2379",  # 瑞昱
    "3443",  # 創意
    "3661",  # 世芯-KY

    # AI Server / ODM
    "2317",  # 鴻海
    "2382",  # 廣達
    "3231",  # 緯創
    "2356",  # 英業達
    "2324",  # 仁寶
    "2376",  # 技嘉

    # PCB
    "2313",  # 華通
    "4958",  # 臻鼎-KY
    "8046",  # 南電
    "3037",  # 欣興
    "2383",  # 台光電
    "2368",  # 金像電

    # 記憶體
    "2408",  # 南亞科
    "2344",  # 華邦電
    "2337",  # 旺宏

    # 被動元件
    "2327",  # 國巨
    "2492",  # 華新科

    # 電源 / 能源
    "2308",  # 台達電
    "1605",  # 華新
    "1717",  # 長興

    # 半導體周邊
    "6488",  # 環球晶
    "6182",  # 合晶
    "5425",  # 台半
    "6510",  # 精測
    "6223",  # 旺矽
    "6515",  # 穎崴
    "3374",  # 精材
    "6217",  # 中探針

    # 面板 / 電子
    "3481",  # 群創
    "2409",  # 友達

    # 其他熱門
    "6770",  # 力積電
    "3706",  # 神達
    "3017",  # 奇鋐
    "3324",  # 雙鴻
]


# =========================================================
# DEMO
# =========================================================

DEMO = [
    {
        "symbol": "1717",
        "name": "長興",
        "price": 78.6,
        "changePercent": 9.32,
        "volume": 63148,
        "volRatio": 3.1,
        "breakoutDistance": 0.3,
        "stage": "首日急放量",
        "score": 94,
    },
    {
        "symbol": "2356",
        "name": "英業達",
        "price": 69.0,
        "changePercent": 5.99,
        "volume": 79797,
        "volRatio": 2.4,
        "breakoutDistance": 0.8,
        "stage": "首日放量",
        "score": 92,
    },
]


# =========================================================
# Fugle request
# =========================================================

def headers():
    return {
        "X-API-KEY": API_KEY
    }


async def fugle_get(path: str, params: dict | None = None):

    if not LIVE_MODE:
        raise RuntimeError("LIVE mode disabled")

    async with httpx.AsyncClient(timeout=15.0) as client:

        r = await client.get(
            f"{BASE}{path}",
            headers=headers(),
            params=params or {},
        )

        if r.status_code == 429:
            raise HTTPException(
                status_code=429,
                detail="Fugle API rate limit reached",
            )

        r.raise_for_status()

        return r.json()


# =========================================================
# Historical data
# =========================================================

def rows_from_historical(payload: Any):

    if isinstance(payload, dict):

        data = payload.get("data")

        if isinstance(data, list):
            return data

        candles = payload.get("candles")

        if isinstance(candles, list):
            return candles

    if isinstance(payload, list):
        return payload

    return []


# =========================================================
# 技術價位
# =========================================================

def calc_levels(price, history):

    candles = rows_from_historical(history)

    # Fugle historical 預設可能是 desc
    # 反轉成舊 -> 新
    if candles:
        candles = list(reversed(candles))

    candles = candles[-30:]

    closes = []
    highs = []
    lows = []
    vols = []

    for x in candles:

        close = float(x.get("close", 0) or 0)
        high = float(x.get("high", 0) or 0)
        low = float(x.get("low", 0) or 0)
        volume = float(x.get("volume", 0) or 0)

        if close > 0:
            closes.append(close)

        if high > 0:
            highs.append(high)

        if low > 0:
            lows.append(low)

        if volume > 0:
            vols.append(volume)

    if not closes:

        return {
            "breakout": price * 1.01,
            "pullback": price * 0.98,
            "noChase": price * 1.035,
            "stop": price * 0.95,
            "target1": price * 1.06,
            "target2": price * 1.10,
            "avg5vol": 0,
            "avg20vol": 0,
            "high20": price,
        }

    high20 = max(highs[-20:] or [price])

    low10 = min(lows[-10:] or [price])

    ranges = []

    for h, l in zip(highs[-10:], lows[-10:]):

        if h >= l:
            ranges.append(h - l)

    ar = mean(ranges) if ranges else price * 0.02

    ma5 = (
        mean(closes[-5:])
        if len(closes) >= 5
        else price * 0.98
    )

    pullback = max(
        price - ar * 0.7,
        ma5,
    )

    breakout = max(
        high20,
        price * 1.002,
    )

    no_chase = (
        breakout
        + max(ar * 0.8, price * 0.02)
    )

    stop = min(
        pullback - ar * 1.1,
        low10 * 0.995,
    )

    risk = max(
        price - stop,
        ar * 0.8,
        price * 0.015,
    )

    target1 = price + risk * 1.6

    target2 = price + risk * 2.4

    avg5 = (
        mean(vols[-5:])
        if len(vols) >= 5
        else 0
    )

    avg20 = (
        mean(vols[-20:])
        if len(vols) >= 20
        else mean(vols)
        if vols
        else 0
    )

    return {
        "breakout": breakout,
        "pullback": pullback,
        "noChase": no_chase,
        "stop": stop,
        "target1": target1,
        "target2": target2,
        "avg5vol": avg5,
        "avg20vol": avg20,
        "high20": high20,
    }


# =========================================================
# 分類
# =========================================================

def classify_stage(
    change,
    vol_ratio,
    breakout_distance,
):

    if (
        vol_ratio >= 2.2
        and 1 <= change <= 7
        and breakout_distance <= 1.5
    ):
        return "首日放量"

    if (
        vol_ratio >= 2.8
        and change > 7
    ):
        return "巨量突破"

    if (
        breakout_distance <= 0.5
        and change > 0
    ):
        return "準備突破"

    if (
        vol_ratio >= 1.5
        and change > 0
    ):
        return "量價轉強"

    return "觀察"


# =========================================================
# 評分
# =========================================================

def score_item(
    change,
    vol_ratio,
    breakout_distance,
    price,
):

    score = 0

    score += min(
        vol_ratio / 3,
        1,
    ) * 35

    if breakout_distance <= 0.5:
        score += 25

    elif breakout_distance <= 1.5:
        score += 20

    elif breakout_distance <= 3:
        score += 10

    if 1 <= change <= 6:
        score += 20

    elif 6 < change <= 9:
        score += 13

    elif change > 9:
        score += 6

    if price < 1000:
        score += 8

    return round(
        min(score, 100),
        1,
    )


# =========================================================
# 分析單一股票
# =========================================================

async def analyze_symbol(symbol):

    try:

        # 即時行情
        quote = await fugle_get(
            f"/intraday/quote/{symbol}"
        )

        price = float(
            quote.get(
                "lastPrice",
                quote.get("closePrice", 0),
            )
            or 0
        )

        if price <= 0:
            return None

        name = quote.get(
            "name",
            symbol,
        )

        change = float(
            quote.get(
                "changePercent",
                0,
            )
            or 0
        )

        total = quote.get("total") or {}

        volume = float(
            total.get(
                "tradeVolume",
                quote.get("tradeVolume", 0),
            )
            or 0
        )

        # 歷史日K
        history = await fugle_get(
            f"/historical/candles/{symbol}",
            {
                "timeframe": "D",
                "fields": "open,high,low,close,volume",
            },
        )

        lv = calc_levels(
            price,
            history,
        )

        # 注意 historical volume 為股
        # 即時 quote tradeVolume 通常需依 API 格式處理
        #
        # 為避免單位差異造成離譜量比，
        # 先自動偵測比例。

        hist_avg = lv["avg5vol"]

        if hist_avg:

            if volume < hist_avg / 100:
                comparable_volume = volume * 1000
            else:
                comparable_volume = volume

            vol_ratio = (
                comparable_volume
                / hist_avg
            )

        else:
            vol_ratio = 0

        high20 = lv["high20"]

        breakout_distance = max(
            (
                (high20 - price)
                / price
                * 100
            ),
            0,
        )

        stage = classify_stage(
            change,
            vol_ratio,
            breakout_distance,
        )

        score = score_item(
            change,
            vol_ratio,
            breakout_distance,
            price,
        )

        return {

            "symbol": symbol,

            "name": name,

            "price": round(
                price,
                2,
            ),

            "changePercent": round(
                change,
                2,
            ),

            "volume": volume,

            "volRatio": round(
                vol_ratio,
                2,
            ),

            "breakoutDistance": round(
                breakout_distance,
                2,
            ),

            "stage": stage,

            "score": score,

            "pullback": round(
                lv["pullback"],
                2,
            ),

            "breakout": round(
                lv["breakout"],
                2,
            ),

            "noChase": round(
                lv["noChase"],
                2,
            ),

            "stop": round(
                lv["stop"],
                2,
            ),

            "target1": round(
                lv["target1"],
                2,
            ),

            "target2": round(
                lv["target2"],
                2,
            ),
        }

    except Exception as e:

        print(
            "analyze error",
            symbol,
            str(e),
        )

        return None


# =========================================================
# 免費版掃描
# =========================================================

async def scan_market():

    rows = []

    #
    # 基本方案 60 requests/min。
    #
    # 每檔目前會使用：
    #
    # 1 次 quote
    # 1 次 historical
    #
    # 因此不能一次 40 檔全部掃。
    #
    # 每次先掃 25 檔。
    #

    scan_symbols = WATCHLIST[:25]

    sem = asyncio.Semaphore(4)

    async def guarded(symbol):

        async with sem:

            result = await analyze_symbol(
                symbol
            )

            # 稍微錯開 request
            await asyncio.sleep(0.15)

            return result

    analyzed = await asyncio.gather(
        *[
            guarded(symbol)
            for symbol in scan_symbols
        ]
    )

    for x in analyzed:

        if x:
            rows.append(x)

    # 千金股原則排除
    rows = [
        x
        for x in rows
        if x["price"] < 1000
        or x["score"] >= 93
    ]

    rows.sort(
        key=lambda x: x["score"],
        reverse=True,
    )

    return rows[:20]


# =========================================================
# 首頁
# =========================================================

@app.get("/")
async def root():

    # 你目前 index.html 在根目錄
    return FileResponse(
        "index.html"
    )


# =========================================================
# Status
# =========================================================

@app.get("/api/status")
async def status():

    return {
        "live": LIVE_MODE,
        "provider": (
            "Fugle"
            if LIVE_MODE
            else "DEMO"
        ),
        "watchlist": len(WATCHLIST),
    }


# =========================================================
# 單一股票報價
# =========================================================

@app.get("/api/quote/{symbol}")
async def quote(symbol: str):

    if not LIVE_MODE:

        return {
            "symbol": symbol,
            "live": False,
        }

    return await fugle_get(
        f"/intraday/quote/{symbol}"
    )


# =========================================================
# 多股票報價
# =========================================================

@app.get("/api/quotes")
async def quotes(
    symbols: str = Query(...)
):

    syms = [
        x.strip()
        for x in symbols.split(",")
        if x.strip()
    ][:20]

    if not LIVE_MODE:

        return {
            "live": False,
            "rows": [],
        }

    sem = asyncio.Semaphore(4)

    async def one(symbol):

        async with sem:

            try:

                return await fugle_get(
                    f"/intraday/quote/{symbol}"
                )

            except Exception:

                return {
                    "symbol": symbol,
                    "error": True,
                }

    rows = await asyncio.gather(
        *[
            one(symbol)
            for symbol in syms
        ]
    )

    return {
        "live": True,
        "rows": rows,
    }


# =========================================================
# 盤中 Radar
# =========================================================

@app.get("/api/radar/intraday")
async def radar_intraday():

    if not LIVE_MODE:

        return {
            "live": False,
            "rows": DEMO,
        }

    rows = await scan_market()

    return {
        "live": True,
        "rows": rows,
    }


# =========================================================
# 盤後 Radar
# =========================================================

@app.get("/api/radar/afterhours")
async def radar_afterhours():

    if not LIVE_MODE:

        return {
            "live": False,
            "rows": DEMO,
        }

    rows = await scan_market()

    return {
        "live": True,
        "rows": rows,
    }


# =========================================================
# 持股即時報價
# =========================================================

@app.get("/api/holdings/quotes")
async def holdings_quotes(
    symbols: str
):

    return await quotes(
        symbols
    )


# =========================================================
# Health check
# =========================================================

@app.get("/health")
async def health():

    return {
        "ok": True,
        "live": LIVE_MODE,
    }
