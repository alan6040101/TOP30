"""
台股成交值 TOP30 後端 API Server
使用 FastAPI + 爬取 TWSE/Yahoo Finance 資料

依賴：
  pip install fastapi uvicorn httpx python-dotenv

啟動：
  uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import asyncio
import json
from datetime import datetime
from typing import Optional
from pathlib import Path

app = FastAPI(title="台股TOP30 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_cache: dict = {}
_cache_time: Optional[datetime] = None
_revenue_cache: dict = {}
_revenue_cache_time: Optional[datetime] = None
CACHE_SECONDS = 30
REVENUE_CACHE_SECONDS = 3600  # 月營收一小時快取一次即可

HISTORY_DIR = Path("./history")
HISTORY_DIR.mkdir(exist_ok=True)


def is_market_open() -> bool:
    tw_now = datetime.now()
    weekday = tw_now.weekday()
    if weekday >= 5:
        return False
    h, m = tw_now.hour, tw_now.minute
    return (h == 9 and m >= 0) or (9 < h < 13) or (h == 13 and m <= 30)


async def fetch_twse_top30() -> list[dict]:
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)",
        "Referer": "https://www.twse.com.tw/",
    }
    params = {"response": "json", "type": "ALL", "_": int(datetime.now().timestamp() * 1000)}

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params, headers=headers)
        r.raise_for_status()
        raw = r.json()

    rows = raw.get("data9", [])
    stocks = []

    for row in rows:
        try:
            code = row[0].strip()
            name = row[1].strip()
            amount_str = row[4].replace(",", "").strip()
            price_str = row[8].replace(",", "").strip()
            sign = row[9]
            diff_str = row[10].replace(",", "").strip()
            volume_str = row[2].replace(",", "").strip()

            if not amount_str or amount_str == "--":
                continue

            amount = int(amount_str) // 10000
            price = float(price_str) if price_str not in ("--", "") else 0
            diff = float(diff_str) if diff_str not in ("--", "") else 0
            volume = int(volume_str) // 1000 if volume_str not in ("--", "") else 0

            if "color:red" in sign or "+" in sign:
                diff = abs(diff)
            elif "color:green" in sign or "-" in sign:
                diff = -abs(diff)
            else:
                diff = 0

            prev_price = price - diff
            change_pct = round((diff / prev_price * 100), 2) if prev_price else 0

            stocks.append({
                "code": code,
                "name": name,
                "price": price,
                "change": diff,
                "changePct": change_pct,
                "volume": volume,
                "amount": amount,
            })
        except (ValueError, IndexError):
            continue

    stocks.sort(key=lambda x: x["amount"], reverse=True)
    return stocks[:30]


async def fetch_monthly_revenue() -> dict:
    """
    從 TWSE OpenAPI 抓取上市公司月營收資料
    API: https://openapi.twse.com.tw/v1/opendata/t187ap05_L
    每支股票回傳最新公布的那個月（不管是當月還是上月），直接顯示數字+月份。
    備註欄位包含「歷史新高」時標記 revenueIsHigh=True。
    """
    global _revenue_cache, _revenue_cache_time

    if _revenue_cache and _revenue_cache_time:
        elapsed = (datetime.now() - _revenue_cache_time).total_seconds()
        if elapsed < REVENUE_CACHE_SECONDS:
            return _revenue_cache

    url = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)",
        "Accept": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            rows = r.json()

        result = {}
        for row in rows:
            try:
                code = str(row.get("公司代號", "")).strip()
                if not code:
                    continue

                # 年增率：欄位名為「營業收入_去年同月增減」
                yoy_str = str(row.get("營業收入_去年同月增減", "")).strip()

                # 資料年月格式為 "11404" = 民國114年4月
                period_str = str(row.get("資料年月", "")).strip()

                # 備註欄位，包含「歷史新高」或「本月營收創歷史新高」等字樣
                note = str(row.get("備註", "")).strip()

                month_label = ""
                if len(period_str) >= 5:
                    try:
                        month_num = int(period_str[-2:])
                        month_label = f"{month_num}月"
                    except ValueError:
                        pass

                yoy = None
                if yoy_str and yoy_str not in ("--", "", "nan", "N/A", "不適用"):
                    try:
                        yoy = round(float(yoy_str), 2)
                    except ValueError:
                        yoy = None

                # 判斷是否創歷史新高
                is_high = "歷史新高" in note or "歷史高" in note

                result[code] = {
                    "revenueYoY": yoy,
                    "revenueMonth": month_label,
                    "revenueIsHigh": is_high,
                    "period": period_str,
                }
            except Exception:
                continue

        _revenue_cache = result
        _revenue_cache_time = datetime.now()
        return result

    except Exception:
        return _revenue_cache or {}


async def fetch_taiex() -> dict:
    url = "https://mis.twse.com.tw/stock/data/mis_ohlc_TSE.txt"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, headers=headers)
            parts = r.text.strip().split("|")
            if len(parts) >= 6:
                price = float(parts[2].replace(",", "")) if parts[2] else 0
                diff = float(parts[3].replace(",", "")) if parts[3] else 0
                prev = price - diff
                pct = round(diff / prev * 100, 2) if prev else 0
                total_amount = float(parts[5]) if parts[5] else 0
                return {
                    "price": round(price, 2),
                    "change": diff,
                    "changePct": pct,
                    "totalMarketAmount": round(total_amount * 100, 0)
                }
    except Exception:
        pass
    return {"price": 0, "change": 0, "changePct": 0, "totalMarketAmount": 0}


def save_history(date_str: str, stocks: list):
    path = HISTORY_DIR / f"{date_str}.json"
    data = {
        "date": date_str,
        "savedAt": datetime.now().isoformat(),
        "stocks": stocks,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_history(days: int = 30) -> list:
    files = sorted(HISTORY_DIR.glob("*.json"), reverse=True)[:days]
    result = []
    for f in files:
        try:
            with open(f, encoding="utf-8") as fp:
                result.append(json.load(fp))
        except Exception:
            pass
    return result


# ====== API Routes ======

@app.get("/api/top30")
async def get_top30():
    global _cache, _cache_time

    if _cache and _cache_time:
        elapsed = (datetime.now() - _cache_time).total_seconds()
        if elapsed < CACHE_SECONDS:
            return JSONResponse(_cache)

    try:
        stocks, taiex_data, revenue_map = await asyncio.gather(
            fetch_twse_top30(),
            fetch_taiex(),
            fetch_monthly_revenue(),
        )

        # 將月營收資料 join 進每支股票
        for s in stocks:
            rev = revenue_map.get(s["code"])
            if rev:
                s["revenueYoY"] = rev["revenueYoY"]
                s["revenueMonth"] = rev["revenueMonth"]
                s["revenueIsHigh"] = rev["revenueIsHigh"]
            else:
                s["revenueYoY"] = None
                s["revenueMonth"] = ""
                s["revenueIsHigh"] = False

        tw_now = datetime.now()
        date_str = tw_now.strftime("%Y-%m-%d")
        time_str = tw_now.strftime("%H:%M:%S")

        if not is_market_open():
            save_history(date_str, stocks)

        response = {
            "success": True,
            "source": "TWSE MI_INDEX",
            "updateTime": time_str,
            "isMarketOpen": is_market_open(),
            "stocks": stocks,
            "taiex": {
                "price": taiex_data["price"],
                "changePct": taiex_data["changePct"],
            },
            "totalMarketAmount": taiex_data.get("totalMarketAmount", 0),
        }

        _cache = response
        _cache_time = datetime.now()
        return JSONResponse(response)

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


@app.get("/api/history")
async def get_history(days: int = 30):
    data = load_history(days)
    return JSONResponse({"success": True, "count": len(data), "history": data})


@app.get("/api/history/{date_str}")
async def get_history_date(date_str: str):
    path = HISTORY_DIR / f"{date_str}.json"
    if not path.exists():
        return JSONResponse({"success": False, "error": "查無此日期"}, status_code=404)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return JSONResponse({"success": True, **data})


@app.get("/api/analysis")
async def get_analysis(days: int = 30):
    history = load_history(days)
    if len(history) < 2:
        return JSONResponse({"success": False, "error": "資料不足"})

    freq = {}
    for h in history:
        seen = set()
        for i, s in enumerate(h["stocks"]):
            code = s["code"]
            if code not in seen:
                seen.add(code)
                if code not in freq:
                    freq[code] = {"code": code, "name": s["name"], "days": 0, "top10Days": 0}
                freq[code]["days"] += 1
                if i < 10:
                    freq[code]["top10Days"] += 1

    freq_list = sorted(freq.values(), key=lambda x: x["days"], reverse=True)[:20]
    total_days = len(history)
    for f in freq_list:
        f["appearRate"] = round(f["days"] / total_days * 100, 1)

    return JSONResponse({
        "success": True,
        "totalDays": total_days,
        "dateRange": {
            "from": history[-1]["date"],
            "to": history[0]["date"],
        },
        "frequentStocks": freq_list,
    })


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
