"""
台股成交值 TOP30 後端 API Server

資料來源策略：
  【收盤後】openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
    - 有今日最終正確成交金額 TradeValue（元）
    - Railway 可直接呼叫，無封鎖問題

  【盤中】mis.twse.com.tw/stock/api/getStockInfo.jsp（透過 Cloudflare Worker）
    - v = 累積成交量（張），z = 現價
    - 成交值(萬) = v × z ÷ 10

  【月營收】openapi.twse.com.tw/v1/opendata/t187ap05_L
    - 正確欄位名（JSON版）：「營業收入-去年同月增減(%)」（含減號）
    - 資料年月格式：「11504」= 115年4月

  【加權指數】mis.twse.com.tw/stock/data/mis_ohlc_TSE.txt（透過 Cloudflare Worker）
"""

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Optional
from pathlib import Path

app = FastAPI(title="台股TOP30 API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

MIS_PROXY    = os.environ.get("MIS_PROXY", "https://twse-proxy.YOUR_ACCOUNT.workers.dev")
OPENAPI_BASE = "https://openapi.twse.com.tw/v1"

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

HEADERS_PROXY = {"User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)"}
HEADERS_API   = {"User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)", "Accept": "application/json"}


TW_TZ = timezone(timedelta(hours=8))

def tw_now() -> datetime:
    """取得台灣時間（UTC+8），不依賴伺服器時區"""
    return datetime.now(timezone.utc).astimezone(TW_TZ)


def is_market_open() -> bool:
    tw = tw_now()
    if tw.weekday() >= 5:
        return False
    h, m = tw.hour, tw.minute
    return (h == 9) or (9 < h < 13) or (h == 13 and m <= 30)


def _parse_month(period_str: str) -> str:
    """'11504' → '4月'，'11411' → '11月'"""
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


async def proxy_get(path_and_query: str) -> Optional[str]:
    """透過 Cloudflare Worker 呼叫 mis.twse.com.tw"""
    url = MIS_PROXY.rstrip("/") + path_and_query
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(url, headers=HEADERS_PROXY)
            if r.status_code == 200:
                return r.text
    except Exception:
        pass
    return None


# ── 收盤後：STOCK_DAY_ALL（最準確）────────────────────
def _is_today_tw(date_str: str) -> bool:
    """
    確認 STOCK_DAY_ALL 的 Date 欄位是否是台灣今日。
    格式：'1150525' = 民國115年5月25日
    """
    today = tw_now()
    # 民國年 = 西元年 - 1911
    roc_year = today.year - 1911
    expected = f"{roc_year}{today.month:02d}{today.day:02d}"
    return date_str.strip() == expected


async def fetch_closing_top30() -> list[dict]:
    """
    從 STOCK_DAY_ALL 取今日最終成交金額，排行取 TOP30。
    TWSE 在每個交易日收盤後約 14:30~16:00 更新，
    若資料不是今日（例如剛收盤 API 還沒更新），
    fallback 到 getStockInfo 盤後快照（v 值雖然不是最終，但至少是今日）。
    """
    url = f"{OPENAPI_BASE}/exchangeReport/STOCK_DAY_ALL"
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(url, headers=HEADERS_API)
        r.raise_for_status()
        rows = r.json()

    # 檢查資料日期
    sample_date = rows[0].get("Date", "") if rows else ""
    if not _is_today_tw(sample_date):
        # 資料不是今日，改用 getStockInfo（盤後快照）
        return await fetch_intraday_top30()

    results = []
    for row in rows:
        code = str(row.get("Code", "")).strip()
        if not (code.isdigit() and len(code) == 4):
            continue
        try:
            trade_value = int(row.get("TradeValue", "0").replace(",", ""))
            amount_wan  = trade_value // 10000   # 元 → 萬元

            close_str  = row.get("ClosingPrice", "0").replace(",", "")
            change_str = row.get("Change", "0").replace(",", "").replace("+", "")
            price  = float(close_str) if close_str not in ("--", "") else 0
            change = float(change_str) if change_str not in ("--", "", "X") else 0
            prev   = price - change
            pct    = round(change / prev * 100, 2) if prev else 0

            if amount_wan <= 0:
                continue

            results.append({
                "code": code, "name": row.get("Name", ""),
                "price": price, "change": change, "changePct": pct,
                "amount": amount_wan,
            })
        except (ValueError, ZeroDivisionError):
            continue

    results.sort(key=lambda x: x["amount"], reverse=True)
    return results[:30]


# ── 盤中：getStockInfo 全市場掃描────────────────────
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
    codes = [
        str(row.get("Code", "")).strip()
        for row in rows
        if str(row.get("Code", "")).strip().isdigit()
        and len(str(row.get("Code", "")).strip()) == 4
    ]
    _codelist_cache = codes
    _codelist_cache_time = datetime.now()
    return codes


async def fetch_intraday_top30() -> list[dict]:
    """
    盤中透過 Cloudflare Worker 呼叫 getStockInfo.jsp
    v = 累積成交量（張，即千股）
    成交值(萬) = v(張) × z(元/股) × 1000 ÷ 10000 = v × z ÷ 10
    """
    codes = await fetch_stock_codes()
    BATCH = 100
    tasks = []
    for i in range(0, len(codes), BATCH):
        batch = codes[i:i+BATCH]
        ex_ch = "|".join(f"tse_{c}.tw" for c in batch)
        ts = int(datetime.now().timestamp() * 1000)
        tasks.append(proxy_get(
            f"/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={ts}"
        ))

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
                z    = item.get("z", "-")   # 現價（元）
                y    = item.get("y", "-")   # 昨收（元）
                v    = item.get("v", "-")   # 累積成交量（張）
                if z in ("-", "--", "") or v in ("-", "--", ""):
                    continue
                price     = float(z)
                yesterday = float(y) if y not in ("-", "--", "") else price
                vol_lots  = int(float(v))            # 張（千股）
                amount_wan = int(vol_lots * price / 10)  # 萬元
                if amount_wan <= 0:
                    continue
                change     = round(price - yesterday, 2)
                change_pct = round(change / yesterday * 100, 2) if yesterday else 0
                results.append({
                    "code": code, "name": name,
                    "price": price, "change": change, "changePct": change_pct,
                    "amount": amount_wan,
                })
            except (ValueError, ZeroDivisionError):
                continue

    results.sort(key=lambda x: x["amount"], reverse=True)
    return results[:30]


# ── 月營收（欄位名確認版）──────────────────────────────
async def fetch_monthly_revenue() -> dict:
    global _revenue_cache, _revenue_cache_time
    if _revenue_cache and _revenue_cache_time:
        if (datetime.now() - _revenue_cache_time).total_seconds() < REVENUE_CACHE_SECS:
            return _revenue_cache

    result = await _fetch_revenue_json()
    if not result:
        result = await _fetch_revenue_csv()

    # 另外抓「本月營收創新高」清單，補強 is_high 判斷
    high_codes = await _fetch_revenue_high_set()
    for code, data in result.items():
        if code in high_codes:
            data["revenueIsHigh"] = True

    if result:
        _revenue_cache = result
        _revenue_cache_time = datetime.now()
    return result or _revenue_cache or {}


async def _fetch_revenue_high_set() -> set:
    """
    取本月營收創歷史新高的股票代號集合。
    先試 TWSE OpenAPI JSON（t187ap26_L），再 fallback CSV。
    """
    # 方法1：OpenAPI JSON
    try:
        url = f"{OPENAPI_BASE}/opendata/t187ap26_L"
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(url, headers=HEADERS_API)
            if r.status_code == 200:
                rows = r.json()
                codes = set()
                for row in rows:
                    # 嘗試各種可能欄位名
                    code = (str(row.get("公司代號","") or row.get("Code","") or "")).strip()
                    if code:
                        codes.add(code)
                if codes:
                    return codes
    except Exception:
        pass

    # 方法2：mopsfin CSV（有 BOM，需特別處理）
    try:
        import csv, io
        url = "https://mopsfin.twse.com.tw/opendata/t187ap26_L.csv"
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(url, headers={**HEADERS_API, "Accept": "text/csv,*/*",
                                          "Referer": "https://mopsfin.twse.com.tw/"})
            r.raise_for_status()

        # 移除 BOM，並處理引號
        text = r.text.lstrip("\ufeff")
        reader = csv.reader(io.StringIO(text))
        codes = set()
        header = None
        for row in reader:
            if header is None:
                # 找到「公司代號」欄位的 index
                cleaned = [c.strip().strip('"') for c in row]
                header = cleaned
                try:
                    code_idx = header.index("公司代號")
                except ValueError:
                    code_idx = 0  # 通常第一欄是代號
                continue
            if row and len(row) > code_idx:
                code = row[code_idx].strip().strip('"')
                if code and code.isdigit():
                    codes.add(code)
        return codes
    except Exception:
        return set()


async def _fetch_revenue_json() -> dict:
    url = f"{OPENAPI_BASE}/opendata/t187ap05_L"
    try:
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.get(url, headers=HEADERS_API)
            r.raise_for_status()
            rows = r.json()
        if not rows:
            return {}

        # 從第一筆確認年增率欄位名（可能有底線或減號版本）
        first_keys = list(rows[0].keys())
        yoy_key = ""
        for candidate in [
            "營業收入-去年同月增減(%)",   # JSON版確認欄位（減號）
            "營業收入_去年同月增減",       # 底線版備用
            "去年同月增減(%)",
        ]:
            if candidate in first_keys:
                yoy_key = candidate
                break

        result = {}
        for row in rows:
            code       = str(row.get("公司代號", "")).strip()
            period_str = str(row.get("資料年月", "")).strip()
            note       = str(row.get("備註", "")).strip()
            yoy_str    = str(row.get(yoy_key, "")).strip() if yoy_key else ""
            if not code:
                continue
            yoy = _parse_float(yoy_str)
            # 歷史新高判斷：備註含關鍵字，或累計YoY超過門檻（備用）
            is_high = ("歷史新高" in note or "歷史" in note)
            result[code] = {
                "revenueYoY":    yoy,
                "revenueMonth":  _parse_month(period_str),
                "revenueIsHigh": is_high,
                "period":        period_str,
            }
        return result
    except Exception:
        return {}


async def _fetch_revenue_csv() -> dict:
    """備用：mopsfin CSV"""
    url = "https://mopsfin.twse.com.tw/opendata/t187ap05_L.csv"
    try:
        import csv, io
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(url, headers={**HEADERS_API, "Accept": "text/csv,*/*",
                                          "Referer": "https://mopsfin.twse.com.tw/"})
            r.raise_for_status()
        reader = csv.DictReader(io.StringIO(r.text))
        result = {}
        for row in reader:
            def cl(s): return s.strip().strip('"').strip() if s else ""
            code = cl(row.get("公司代號", ""))
            if not code:
                continue
            result[code] = {
                "revenueYoY":    _parse_float(cl(row.get("營業收入-去年同月增減(%)", ""))),
                "revenueMonth":  _parse_month(cl(row.get("資料年月", ""))),
                "revenueIsHigh": "歷史新高" in cl(row.get("備註", "")),
                "period":        cl(row.get("資料年月", "")),
            }
        return result
    except Exception:
        return {}


# ── 加權指數 ─────────────────────────────────────────
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
                total = float(parts[5]) if parts[5] else 0  # 億
                return {"price": round(price, 2), "changePct": pct,
                        "totalMarketAmount": round(total * 100, 0)}  # 億→萬
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
        # 盤中用 getStockInfo，收盤後用 STOCK_DAY_ALL
        if is_market_open():
            stocks_task = fetch_intraday_top30()
        else:
            stocks_task = fetch_closing_top30()

        stocks, taiex_data, revenue_map = await asyncio.gather(
            stocks_task, fetch_taiex(), fetch_monthly_revenue(),
        )

        for s in stocks:
            rev = revenue_map.get(s["code"])
            s["revenueYoY"]    = rev["revenueYoY"]    if rev else None
            s["revenueMonth"]  = rev["revenueMonth"]  if rev else ""
            s["revenueIsHigh"] = rev["revenueIsHigh"] if rev else False

        _now     = tw_now()
        date_str = _now.strftime("%Y-%m-%d")
        time_str = _now.strftime("%H:%M:%S")

        if not is_market_open():
            save_history(date_str, [
                {"code": s["code"], "name": s["name"],
                 "amount": s["amount"], "changePct": s["changePct"]}
                for s in stocks
            ])

        source = "mis.twse.com.tw (盤中)" if is_market_open() else "STOCK_DAY_ALL (收盤)"
        response = {
            "success": True, "source": source,
            "updateTime": time_str, "isMarketOpen": is_market_open(),
            "stocks": stocks,
            "taiex": {"price": taiex_data["price"], "changePct": taiex_data["changePct"]},
            "totalMarketAmount": taiex_data.get("totalMarketAmount", 0),
        }
        _cache = response
        _cache_time = datetime.now()
        return JSONResponse(response)
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


@app.get("/api/debug")
async def debug():
    """原始 API 資料檢查"""
    # 1. getStockInfo 幾支股票
    codes = ["2330","2303","2327","2344","2408"]
    ex_ch = "|".join(f"tse_{c}.tw" for c in codes)
    ts = int(datetime.now().timestamp() * 1000)
    raw = await proxy_get(f"/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={ts}")
    stock_debug = []
    if raw:
        try:
            data = json.loads(raw)
            for item in data.get("msgArray", []):
                z = item.get("z","-"); y = item.get("y","-"); v = item.get("v","-")
                amt = "N/A"
                if v not in ("-","--","") and z not in ("-","--",""):
                    amt = f"{int(float(v)*float(z)/10):,}萬"
                stock_debug.append({
                    "code": item.get("c",""), "name": item.get("n",""),
                    "z": z, "y": y, "v": v, "成交值(v×z÷10)": amt,
                })
        except Exception as e:
            stock_debug = [{"error": str(e)}]

    # 2. 月營收欄位名 + 創新高清單
    rev_debug = {}
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{OPENAPI_BASE}/opendata/t187ap05_L", headers=HEADERS_API)
            rows = r.json()
        rev_debug["欄位名"] = list(rows[0].keys()) if rows else []
        for row in rows:
            if row.get("公司代號","") == "2330":
                rev_debug["台積電"] = row
                break
    except Exception as e:
        rev_debug["error"] = str(e)

    # 2b. 創新高清單
    high_debug = {}
    try:
        import csv, io
        # 先試 OpenAPI JSON
        async with httpx.AsyncClient(timeout=15) as c:
            r_json = await c.get(f"{OPENAPI_BASE}/opendata/t187ap26_L", headers=HEADERS_API)
        high_debug["json_status"] = r_json.status_code
        if r_json.status_code == 200:
            rows_h = r_json.json()
            high_debug["json_count"] = len(rows_h)
            high_debug["json_fields"] = list(rows_h[0].keys()) if rows_h else []
            high_debug["json_sample"] = rows_h[:3]
        # 也試 CSV
        async with httpx.AsyncClient(timeout=15) as c:
            r_csv = await c.get("https://mopsfin.twse.com.tw/opendata/t187ap26_L.csv",
                               headers={**HEADERS_API, "Accept":"text/csv,*/*",
                                        "Referer":"https://mopsfin.twse.com.tw/"})
        high_debug["csv_status"] = r_csv.status_code
        text_h = r_csv.text.lstrip("\ufeff")
        high_debug["csv_first200"] = text_h[:200]
        reader_h = csv.reader(io.StringIO(text_h))
        rows_csv = list(reader_h)
        high_debug["csv_header"] = rows_csv[0] if rows_csv else []
        high_debug["csv_row2"] = rows_csv[1] if len(rows_csv) > 1 else []
        high_debug["csv_total_rows"] = len(rows_csv)
    except Exception as e:
        high_debug["error"] = str(e)

    # 3. STOCK_DAY_ALL 前5名 + 日期驗證
    day_all_debug = []
    stock_day_date = ""
    is_today = False
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{OPENAPI_BASE}/exchangeReport/STOCK_DAY_ALL", headers=HEADERS_API)
            rows = r.json()
        stock_day_date = rows[0].get("Date","") if rows else ""
        is_today = _is_today_tw(stock_day_date)
        stocks = [
            {"code": row["Code"], "name": row["Name"],
             "amount_wan": int(row["TradeValue"])//10000,
             "close": row["ClosingPrice"], "change": row["Change"],
             "date": row["Date"]}
            for row in rows
            if row.get("Code","").isdigit() and len(row.get("Code",""))==4
            and int(row.get("TradeValue","0")) > 0
        ]
        stocks.sort(key=lambda x: x["amount_wan"], reverse=True)
        day_all_debug = stocks[:10]
    except Exception as e:
        day_all_debug = [{"error": str(e)}]

    # 4. 加權指數
    taiex_raw = await proxy_get("/stock/data/mis_ohlc_TSE.txt")

    return JSONResponse({
        "time": datetime.now().isoformat(),
        "tw_time": tw_now().isoformat(),
        "is_market_open": is_market_open(),
        "getStockInfo": stock_debug,
        "revenue_fields": rev_debug,
        "revenue_high_t187ap26": high_debug,
        "STOCK_DAY_ALL_date": stock_day_date,
        "STOCK_DAY_ALL_is_today": is_today,
        "STOCK_DAY_ALL_top10": day_all_debug,
        "taiex_raw": taiex_raw,
    })


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
    for f in freq_list:
        f["appearRate"] = round(f["days"] / len(history) * 100, 1)
    return JSONResponse({"success": True, "totalDays": len(history),
                         "dateRange": {"from": history[-1]["date"], "to": history[0]["date"]},
                         "frequentStocks": freq_list})


@app.get("/health")
async def health():
    proxy_status = "unknown"
    try:
        result = await proxy_get("/stock/data/mis_ohlc_TSE.txt")
        proxy_status = "ok" if result else "error"
    except Exception as e:
        proxy_status = f"error: {str(e)[:60]}"
    return {"status": "ok", "proxy": proxy_status,
            "proxy_url": MIS_PROXY, "time": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
