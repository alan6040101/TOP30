"""
台股成交值 TOP30 後端 API Server
使用 FastAPI + 爬取 TWSE/Yahoo Finance 資料

依賴：
  pip install fastapi uvicorn httpx python-dotenv apscheduler

啟動：
  uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import asyncio
import json
import os
import re
from datetime import datetime, date
from typing import Optional
from pathlib import Path

app = FastAPI(title="台股TOP30 API")

# ====== CORS（允許前端 domain 呼叫）======
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 正式環境請改為你的前端網域
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ====== 快取 ======
_cache: dict = {}
_cache_time: Optional[datetime] = None
CACHE_SECONDS = 30  # 30秒快取

# ====== 歷史紀錄存放路徑 ======
HISTORY_DIR = Path("./history")
HISTORY_DIR.mkdir(exist_ok=True)


def is_market_open() -> bool:
    """判斷目前是否為台股交易時間（台灣時間 09:00–13:30 平日）"""
    tw_now = datetime.now()  # 請確保伺服器時區為 Asia/Taipei，或用 pytz
    weekday = tw_now.weekday()  # 0=Mon, 6=Sun
    if weekday >= 5:
        return False
    h, m = tw_now.hour, tw_now.minute
    return (h == 9 and m >= 0) or (9 < h < 13) or (h == 13 and m <= 30)


async def fetch_twse_top30() -> list[dict]:
    """
    從 TWSE MI_INDEX 取得所有上市股票當日成交資料，排序取 TOP30。
    
    TWSE MI_INDEX API：
    https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&type=ALL
    
    欄位 data9：上市個股
      [0] 證券代號, [1] 名稱, [2] 成交股數, [3] 成交筆數,
      [4] 成交金額, [5] 開盤, [6] 最高, [7] 最低,
      [8] 收盤(現價), [9] 漲跌符號, [10] 漲跌價差, [14] 本益比
    """
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
            amount_str = row[4].replace(",", "").strip()  # 成交金額（元）
            price_str = row[8].replace(",", "").strip()   # 收盤價
            sign = row[9]  # 漲跌符號 html tag
            diff_str = row[10].replace(",", "").strip()   # 漲跌價差
            volume_str = row[2].replace(",", "").strip()  # 成交股數

            if not amount_str or amount_str == "--":
                continue

            amount = int(amount_str) // 10000  # 轉換為萬元
            price = float(price_str) if price_str not in ("--", "") else 0
            diff = float(diff_str) if diff_str not in ("--", "") else 0
            volume = int(volume_str) // 1000 if volume_str not in ("--", "") else 0  # 張

            # 漲跌方向
            if "color:red" in sign or "+" in sign:
                diff = abs(diff)
            elif "color:green" in sign or "-" in sign:
                diff = -abs(diff)
            else:
                diff = 0

            # 昨收
            prev_price = price - diff
            change_pct = round((diff / prev_price * 100), 2) if prev_price else 0

            stocks.append({
                "code": code,
                "name": name,
                "price": price,
                "change": diff,
                "changePct": change_pct,
                "volume": volume,
                "amount": amount,  # 萬元
            })
        except (ValueError, IndexError):
            continue

    # 依成交金額降序排列，取 TOP30
    stocks.sort(key=lambda x: x["amount"], reverse=True)
    return stocks[:30]


async def fetch_taiex() -> dict:
    """取得加權指數即時資料"""
    url = "https://mis.twse.com.tw/stock/data/mis_ohlc_TSE.txt"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, headers=headers)
            # 格式: 日期|時間|加權指數|漲跌|漲跌幅|成交金額(億)|...
            parts = r.text.strip().split("|")
            if len(parts) >= 6:
                price = float(parts[2].replace(",", "")) if parts[2] else 0
                diff = float(parts[3].replace(",", "")) if parts[3] else 0
                prev = price - diff
                pct = round(diff / prev * 100, 2) if prev else 0
                total_amount = float(parts[5]) if parts[5] else 0  # 億
                return {
                    "price": round(price, 2),
                    "change": diff,
                    "changePct": pct,
                    "totalMarketAmount": round(total_amount * 100, 0)  # 億→萬
                }
    except Exception:
        pass
    return {"price": 0, "change": 0, "changePct": 0, "totalMarketAmount": 0}


def save_history(date_str: str, stocks: list):
    """儲存當日排行至 JSON 檔"""
    path = HISTORY_DIR / f"{date_str}.json"
    data = {
        "date": date_str,
        "savedAt": datetime.now().isoformat(),
        "stocks": stocks,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_history(days: int = 30) -> list:
    """讀取最近 N 天的歷史紀錄"""
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
    """取得即時 TOP30 成交值排行"""
    global _cache, _cache_time

    # 快取檢查
    if _cache and _cache_time:
        elapsed = (datetime.now() - _cache_time).total_seconds()
        if elapsed < CACHE_SECONDS:
            return JSONResponse(_cache)

    try:
        stocks, taiex_data = await asyncio.gather(
            fetch_twse_top30(),
            fetch_taiex(),
        )

        tw_now = datetime.now()
        date_str = tw_now.strftime("%Y-%m-%d")
        time_str = tw_now.strftime("%H:%M:%S")

        # 每日收盤後儲存一次
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
    """取得歷史排行紀錄"""
    data = load_history(days)
    return JSONResponse({"success": True, "count": len(data), "history": data})


@app.get("/api/history/{date_str}")
async def get_history_date(date_str: str):
    """取得特定日期排行"""
    path = HISTORY_DIR / f"{date_str}.json"
    if not path.exists():
        return JSONResponse({"success": False, "error": "查無此日期"}, status_code=404)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return JSONResponse({"success": True, **data})


@app.get("/api/analysis")
async def get_analysis(days: int = 30):
    """分析最近 N 天的常駐股票"""
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
