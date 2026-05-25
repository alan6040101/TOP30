"""
台股成交值 TOP30 後端 API Server

架構：
  Railway (後端) → Cloudflare Worker Proxy → mis.twse.com.tw
  
  Railway 的 IP 被 TWSE 封鎖，所以透過 Cloudflare Worker 當中繼。
  Cloudflare 免費方案每日 10 萬次請求，足夠使用。

資料來源：
  - 即時成交值排行：mis.twse.com.tw/stock/api/getStockInfo.jsp
    透過 Cloudflare Worker 代理
    v = 累積成交量（張），z = 現價，y = 昨收
    成交值(萬) = v(張) × z(元) × 1000股/張 ÷ 10000 = v × z ÷ 10
  - 月營收：openapi.twse.com.tw/v1/opendata/t187ap05_L（直接呼叫，無封鎖問題）
  - 加權指數：mis.twse.com.tw/stock/data/mis_ohlc_TSE.txt（透過 Proxy）
  - 上市股票清單：openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL（直接呼叫）
"""

import os
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

# ── 設定 ──────────────────────────────────────────────
# Cloudflare Worker URL，部署後填入（可用環境變數覆蓋）
MIS_PROXY = os.environ.get(
    "MIS_PROXY",
    "https://twse-proxy.YOUR_ACCOUNT.workers.dev"  # ← 換成你的 Worker 網址
)

OPENAPI_BASE = "https://openapi.twse.com.tw/v1"

# ── 快取 ──────────────────────────────────────────────
_cache: dict = {}
_cache_time: Optional[datetime] = None
_revenue_cache: dict = {}
_revenue_cache_time: Optional[datetime] = None
_codelist_cache: list = []
_codelist_cache_time: Optional[datetime] = None

CACHE_SECONDS       = 30
REVENUE_CACHE_SECS  = 3600
CODELIST_CACHE_SECS = 86400

HISTORY_DIR = Path("./history")
HISTORY_DIR.mkdir(exist_ok=True)

HEADERS_PROXY = {
    "User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)",
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


def _parse_month(period_str: str) -> str:
    s = period_str.strip()
    if len(s) >= 2:
        try:
            n = int(s[-2:])
            if 1 <= n <= 12:
                return f"{n}月"
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


# ── 取上市股票代號清單 ───────────────────────────────
async def fetch_stock_codes() -> list[str]:
    global _codelist_cache, _codelist_cache_time
    if _codelist_cache and _codelist_cache_time:
        if (datetime.now() - _codelist_cache_time).total_seconds() < CODELIST_CACHE_SECS:
            return _codelist_cache

    url = f"{OPENAPI_BASE}/exchangeReport/STOCK_DAY_ALL"
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(url, headers=HEADERS_API)
        r.raise_for_status()
        rows = r.json()

    # 只保留 4 位純數字普通股
    codes = [
        str(row.get("Code", "")).strip()
        for row in rows
        if str(row.get("Code", "")).strip().isdigit()
        and len(str(row.get("Code", "")).strip()) == 4
    ]
    _codelist_cache = codes
    _codelist_cache_time = datetime.now()
    return codes


# ── 透過 Cloudflare Worker Proxy 呼叫 TWSE ──────────
async def proxy_get(path_and_query: str) -> Optional[str]:
    """
    透過 Cloudflare Worker Proxy 呼叫 mis.twse.com.tw
    path_and_query 例如：/stock/api/getStockInfo.jsp?ex_ch=tse_2330.tw&json=1&delay=0
    """
    url = MIS_PROXY.rstrip("/") + path_and_query
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(url, headers=HEADERS_PROXY)
        if r.status_code == 200:
            return r.text
    return None


# ── 即時全市場掃描（成交值排行）────────────────────────
async def fetch_intraday_top30() -> list[dict]:
    """
    分批呼叫 getStockInfo.jsp（透過 Cloudflare Worker），
    計算 v(張) × z(元) ÷ 10 = 成交值(萬)，取 TOP30。
    """
    codes = await fetch_stock_codes()

    BATCH = 100
    tasks = []
    batch_list = []
    for i in range(0, len(codes), BATCH):
        batch = codes[i:i + BATCH]
        batch_list.append(batch)
        ex_ch = "|".join(f"tse_{c}.tw" for c in batch)
        ts = int(datetime.now().timestamp() * 1000)
        path = f"/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={ts}"
        tasks.append(proxy_get(path))

    responses = await asyncio.gather(*tasks, return_exceptions=True)

    results = []
    for resp in responses:
        if isinstance(resp, Exception) or resp is None:
            continue
        try:
            data = json.loads(resp)
        except Exception:
            continue

        for item in data.get("msgArray", []):
            try:
                code = item.get("c", "").strip()
                name = item.get("n", "").strip()
                z    = item.get("z", "-")   # 現價
                y    = item.get("y", "-")   # 昨收
                v    = item.get("v", "-")   # 累積成交量（張）

                if z in ("-", "--", "") or v in ("-", "--", ""):
                    continue

                price     = float(z)
                yesterday = float(y) if y not in ("-", "--", "") else price
                vol_lots  = int(float(v))   # 張

                # 成交值(萬) = 張 × 元/股 × 1000股/張 ÷ 10000 = v × z ÷ 10
                amount_wan = int(vol_lots * price / 10)

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


# ── 月營收 ────────────────────────────────────────────
async def fetch_monthly_revenue() -> dict:
    global _revenue_cache, _revenue_cache_time
    if _revenue_cache and _revenue_cache_time:
        if (datetime.now() - _revenue_cache_time).total_seconds() < REVENUE_CACHE_SECS:
            return _revenue_cache

    # 先試 TWSE OpenAPI JSON
    result = await _fetch_revenue_json()
    # Fallback: mopsfin CSV
    if not result:
        result = await _fetch_revenue_csv()

    if result:
        _revenue_cache = result
        _revenue_cache_time = datetime.now()
    return result or _revenue_cache or {}


async def _fetch_revenue_json() -> dict:
    url = f"{OPENAPI_BASE}/opendata/t187ap05_L"
    try:
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.get(url, headers=HEADERS_API)
            r.raise_for_status()
            rows = r.json()

        result = {}
        for row in rows:
            code       = str(row.get("公司代號", "")).strip()
            period_str = str(row.get("資料年月", "")).strip()
            note       = str(row.get("備註", "")).strip()
            # JSON 欄位嘗試多種可能名稱
            yoy_str = (str(row.get("營業收入_去年同月增減", "")).strip()
                       or str(row.get("去年同月增減(%)", "")).strip())
            if not code:
                continue
            result[code] = {
                "revenueYoY":    _parse_float(yoy_str),
                "revenueMonth":  _parse_month(period_str),
                "revenueIsHigh": "歷史新高" in note,
                "period":        period_str,
            }
        return result
    except Exception:
        return {}


async def _fetch_revenue_csv() -> dict:
    url = "https://mopsfin.twse.com.tw/opendata/t187ap05_L.csv"
    headers = {**HEADERS_API, "Accept": "text/csv,*/*",
               "Referer": "https://mopsfin.twse.com.tw/"}
    try:
        import csv, io
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(url, headers=headers)
            r.raise_for_status()

        reader = csv.DictReader(io.StringIO(r.text))
        result = {}
        for row in reader:
            def cl(s): return s.strip().strip('"').strip() if s else ""
            code       = cl(row.get("公司代號", ""))
            period_str = cl(row.get("資料年月", ""))
            note       = cl(row.get("備註", ""))
            yoy_str    = cl(row.get("營業收入-去年同月增減(%)", ""))
            if not code:
                continue
            result[code] = {
                "revenueYoY":    _parse_float(yoy_str),
                "revenueMonth":  _parse_month(period_str),
                "revenueIsHigh": "歷史新高" in note,
                "period":        period_str,
            }
        return result
    except Exception:
        return {}


# ── 加權指數（透過 Proxy）───────────────────────────
async def fetch_taiex() -> dict:
    text = await proxy_get("/stock/data/mis_ohlc_TSE.txt")
    if text:
        try:
            parts = text.strip().split("|")
            if len(parts) >= 6:
                price = float(parts[2].replace(",", "")) if parts[2] else 0
                diff  = float(parts[3].replace(",", "")) if parts[3] else 0
                prev  = price - diff
                pct   = round(diff / prev * 100, 2) if prev else 0
                total = float(parts[5]) if parts[5] else 0
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
            s["revenueYoY"]    = rev["revenueYoY"]    if rev else None
            s["revenueMonth"]  = rev["revenueMonth"]  if rev else ""
            s["revenueIsHigh"] = rev["revenueIsHigh"] if rev else False

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
            "source":            "mis.twse.com.tw via Cloudflare Worker",
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
    return JSONResponse({"success": True, "count": len(load_history(days)),
                         "history": load_history(days)})


@app.get("/api/history/{date_str}")
async def get_history_date(date_str: str):
    path = HISTORY_DIR / f"{date_str}.json"
    if not path.exists():
        return JSONResponse({"success": False, "error": "查無此日期"}, status_code=404)
    with open(path, encoding="utf-8") as f:
        return JSONResponse({"success": True, **json.load(f)})


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
    proxy_status = "unknown"
    try:
        result = await proxy_get("/stock/data/mis_ohlc_TSE.txt")
        proxy_status = "ok" if result else "error - empty response"
    except Exception as e:
        proxy_status = f"error - {str(e)[:100]}"
    return {
        "status": "ok",
        "proxy": proxy_status,
        "proxy_url": MIS_PROXY,
        "time": datetime.now().isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
