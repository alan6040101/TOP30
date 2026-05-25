"""
台股成交值 TOP30 後端 API Server

資料來源：
  - 即時成交值排行：全市場掃描 mis.twse.com.tw/stock/api/getStockInfo.jsp
    欄位 v=累積成交量(股), z=現價, y=昨收, n=簡稱, c=代號
    成交值(萬) = int(v) * float(z) / 10000
  - 月營收：openapi.twse.com.tw/v1/opendata/t187ap05_L (JSON，永遠最新)
  - 加權指數：mis.twse.com.tw/stock/data/mis_ohlc_TSE.txt
  - 上市股票清單：openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
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

# ── 快取 ──────────────────────────────────────────────
_cache: dict = {}
_cache_time: Optional[datetime] = None
_revenue_cache: dict = {}
_revenue_cache_time: Optional[datetime] = None
_codelist_cache: list = []
_codelist_cache_time: Optional[datetime] = None

CACHE_SECONDS        = 30
REVENUE_CACHE_SECS   = 3600   # 月營收每小時更新一次
CODELIST_CACHE_SECS  = 86400  # 股票清單每天更新一次

HISTORY_DIR = Path("./history")
HISTORY_DIR.mkdir(exist_ok=True)

HEADERS_MIS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://mis.twse.com.tw/",
}
HEADERS_API = {
    "User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)",
    "Accept": "application/json",
}


def is_market_open() -> bool:
    tw = datetime.now()
    if tw.weekday() >= 5:
        return False
    h, m = tw.hour, tw.minute
    return (h == 9) or (9 < h < 13) or (h == 13 and m <= 30)


# ── 取上市股票代號清單 ───────────────────────────────
async def fetch_stock_codes() -> list[str]:
    """
    從 TWSE OpenAPI 取所有上市股票代號（排除 ETF/ETN：代號不為純4位數字）
    """
    global _codelist_cache, _codelist_cache_time
    if _codelist_cache and _codelist_cache_time:
        if (datetime.now() - _codelist_cache_time).total_seconds() < CODELIST_CACHE_SECS:
            return _codelist_cache

    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(url, headers=HEADERS_API)
        r.raise_for_status()
        rows = r.json()

    codes = []
    for row in rows:
        code = str(row.get("Code", "")).strip()
        # 只保留 4 位純數字（普通股）
        if code.isdigit() and len(code) == 4:
            codes.append(code)

    _codelist_cache = codes
    _codelist_cache_time = datetime.now()
    return codes


# ── 即時全市場掃描（成交值排行）────────────────────────
async def fetch_intraday_top30() -> list[dict]:
    """
    分批呼叫 getStockInfo.jsp，計算每支股票累積成交值，取 TOP30。
    欄位：c=代號, n=簡稱, z=現價, y=昨收, v=累積成交量(股)
    成交值(萬) = int(v) * float(z) / 10000
    """
    codes = await fetch_stock_codes()

    BATCH = 100
    results = []
    async with httpx.AsyncClient(timeout=20) as client:
        tasks = []
        for i in range(0, len(codes), BATCH):
            batch = codes[i:i+BATCH]
            ex_ch = "|".join(f"tse_{c}.tw" for c in batch)
            url = (f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
                   f"?ex_ch={ex_ch}&json=1&delay=0"
                   f"&_={int(datetime.now().timestamp()*1000)}")
            tasks.append(client.get(url, headers=HEADERS_MIS))

        responses = await asyncio.gather(*tasks, return_exceptions=True)

    for resp in responses:
        if isinstance(resp, Exception):
            continue
        try:
            data = resp.json()
        except Exception:
            continue
        for item in data.get("msgArray", []):
            try:
                code = item.get("c", "").strip()
                name = item.get("n", "").strip()
                z    = item.get("z", "-")   # 現價
                y    = item.get("y", "-")   # 昨收
                v    = item.get("v", "-")   # 累積成交量（股）

                if z in ("-", "--", "") or v in ("-", "--", ""):
                    continue

                price      = float(z)
                yesterday  = float(y) if y not in ("-", "--", "") else price
                volume_lots = int(float(v))  # 張（1張=1000股）
                # 成交值(萬) = 張數 × 每股價格 × 1000股 ÷ 10000 = 張數 × 價格 ÷ 10
                amount_wan = int(volume_lots * price / 10)

                if amount_wan <= 0:
                    continue

                change     = round(price - yesterday, 2)
                change_pct = round(change / yesterday * 100, 2) if yesterday else 0

                results.append({
                    "code":      code,
                    "name":      name,
                    "price":     price,
                    "change":    change,
                    "changePct": change_pct,
                    "amount":    amount_wan,
                })
            except (ValueError, ZeroDivisionError):
                continue

    results.sort(key=lambda x: x["amount"], reverse=True)
    return results[:30]


# ── 月營收（TWSE OpenAPI JSON，永遠最新）──────────────
async def fetch_monthly_revenue() -> dict:
    """
    欄位（t187ap05_L JSON 版）：
      公司代號, 資料年月(11502→2月), 營業收入_去年同月增減, 備註
    注意：JSON 欄位用底線，不同於 CSV 的減號。
    先試 JSON；若失敗改抓 mopsfin CSV。
    """
    global _revenue_cache, _revenue_cache_time
    if _revenue_cache and _revenue_cache_time:
        if (datetime.now() - _revenue_cache_time).total_seconds() < REVENUE_CACHE_SECS:
            return _revenue_cache

    result = await _fetch_revenue_json()
    if not result:
        result = await _fetch_revenue_csv()

    if result:
        _revenue_cache = result
        _revenue_cache_time = datetime.now()
    return result or _revenue_cache or {}


async def _fetch_revenue_json() -> dict:
    url = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(url, headers=HEADERS_API)
            r.raise_for_status()
            rows = r.json()

        result = {}
        for row in rows:
            code       = str(row.get("公司代號", "")).strip()
            period_str = str(row.get("資料年月", "")).strip()
            note       = str(row.get("備註", "")).strip()

            # JSON 欄位名：底線版本（從 Swagger 確認）
            yoy_str = str(row.get("營業收入_去年同月增減", "")).strip()
            # 備用欄位名（防止更新）
            if not yoy_str or yoy_str in ("-", "--", ""):
                yoy_str = str(row.get("去年同月增減(%)", "")).strip()

            if not code:
                continue

            month_label = _parse_month(period_str)
            yoy = _parse_float(yoy_str)
            is_high = "歷史新高" in note or "歷史高" in note

            result[code] = {
                "revenueYoY":   yoy,
                "revenueMonth": month_label,
                "revenueIsHigh": is_high,
                "period":       period_str,
            }
        return result
    except Exception:
        return {}


async def _fetch_revenue_csv() -> dict:
    """備用：從 mopsfin CSV 抓取"""
    url = "https://mopsfin.twse.com.tw/opendata/t187ap05_L.csv"
    headers = {**HEADERS_API, "Accept": "text/csv,*/*",
               "Referer": "https://mopsfin.twse.com.tw/"}
    try:
        import csv, io
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.get(url, headers=headers)
            r.raise_for_status()
            text = r.text

        reader = csv.DictReader(io.StringIO(text))
        result = {}
        for row in reader:
            def clean(s): return s.strip().strip('"').strip() if s else ""
            code       = clean(row.get("公司代號", ""))
            period_str = clean(row.get("資料年月", ""))
            note       = clean(row.get("備註", ""))
            yoy_str    = clean(row.get("營業收入-去年同月增減(%)", ""))

            if not code:
                continue

            result[code] = {
                "revenueYoY":    _parse_float(yoy_str),
                "revenueMonth":  _parse_month(period_str),
                "revenueIsHigh": "歷史新高" in note or "歷史高" in note,
                "period":        period_str,
            }
        return result
    except Exception:
        return {}


def _parse_month(period_str: str) -> str:
    """'11502' → '2月'，'11412' → '12月'"""
    s = period_str.strip()
    if len(s) >= 2:
        try:
            month_num = int(s[-2:])
            if 1 <= month_num <= 12:
                return f"{month_num}月"
        except ValueError:
            pass
    return ""


def _parse_float(s: str) -> Optional[float]:
    if not s or s in ("-", "--", "N/A", "不適用", ""):
        return None
    try:
        return round(float(s), 2)
    except ValueError:
        return None


# ── 加權指數 ─────────────────────────────────────────
async def fetch_taiex() -> dict:
    url = "https://mis.twse.com.tw/stock/data/mis_ohlc_TSE.txt"
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(url, headers=HEADERS_MIS)
            parts = r.text.strip().split("|")
            if len(parts) >= 6:
                price = float(parts[2].replace(",", "")) if parts[2] else 0
                diff  = float(parts[3].replace(",", "")) if parts[3] else 0
                prev  = price - diff
                pct   = round(diff / prev * 100, 2) if prev else 0
                total = float(parts[5]) if parts[5] else 0  # 億
                return {"price": round(price, 2), "changePct": pct,
                        "totalMarketAmount": round(total * 100, 0)}
    except Exception:
        pass
    return {"price": 0, "changePct": 0, "totalMarketAmount": 0}


# ── 歷史存檔 ─────────────────────────────────────────
def save_history(date_str: str, stocks: list):
    path = HISTORY_DIR / f"{date_str}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"date": date_str, "savedAt": datetime.now().isoformat(),
                   "stocks": stocks}, f, ensure_ascii=False, indent=2)


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


# ── API Routes ────────────────────────────────────────
@app.get("/api/top30")
async def get_top30():
    global _cache, _cache_time
    if _cache and _cache_time:
        if (datetime.now() - _cache_time).total_seconds() < CACHE_SECONDS:
            return JSONResponse(_cache)

    try:
        stocks, taiex_data, revenue_map = await asyncio.gather(
            fetch_intraday_top30(),
            fetch_taiex(),
            fetch_monthly_revenue(),
        )

        for s in stocks:
            rev = revenue_map.get(s["code"])
            if rev:
                s["revenueYoY"]    = rev["revenueYoY"]
                s["revenueMonth"]  = rev["revenueMonth"]
                s["revenueIsHigh"] = rev["revenueIsHigh"]
            else:
                s["revenueYoY"]    = None
                s["revenueMonth"]  = ""
                s["revenueIsHigh"] = False

        tw_now   = datetime.now()
        date_str = tw_now.strftime("%Y-%m-%d")
        time_str = tw_now.strftime("%H:%M:%S")

        if not is_market_open():
            save_history(date_str, [
                {"code": s["code"], "name": s["name"],
                 "amount": s["amount"], "changePct": s["changePct"]}
                for s in stocks
            ])

        response = {
            "success":           True,
            "source":            "mis.twse.com.tw getStockInfo",
            "updateTime":        time_str,
            "isMarketOpen":      is_market_open(),
            "stocks":            stocks,
            "taiex":             {"price": taiex_data["price"],
                                  "changePct": taiex_data["changePct"]},
            "totalMarketAmount": taiex_data.get("totalMarketAmount", 0),
        }

        _cache      = response
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
    return JSONResponse({"success": True, "totalDays": total_days,
                         "dateRange": {"from": history[-1]["date"], "to": history[0]["date"]},
                         "frequentStocks": freq_list})


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
