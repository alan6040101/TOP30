# 台股成交值 TOP30 即時榜 — 部署指南

## 整體架構

```
前端 (index.html)          後端 (main.py)
 Vercel / GitHub Pages  →   Railway / Render / Fly.io
        ↕                           ↕
  localStorage 歷史存檔       TWSE MI_INDEX API
```

---

## 資料來源

### 主要 API（免費、無需申請 key）

| 用途 | API 端點 |
|------|---------|
| 所有上市個股當日成交資料 | `https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&type=ALL` |
| 加權指數即時資料 | `https://mis.twse.com.tw/stock/data/mis_ohlc_TSE.txt` |
| 個股即時報價（需組合代號） | `https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_2330.tw` |

> 注意：TWSE 有 rate limit，每 5 秒不超過 3 次請求。本專案後端已加入 30 秒快取。

---

## 部署步驟

### Step 1：部署後端（選一）

#### 選項 A：Railway（推薦，有免費額度）
1. 前往 https://railway.app 登入 GitHub
2. New Project → Deploy from GitHub Repo
3. 選擇你的 repo 的 `backend/` 資料夾
4. 自動偵測 Dockerfile 並部署
5. 記下給你的 domain，例如 `https://twstock-api.up.railway.app`

#### 選項 B：Render（免費但冷啟動慢）
1. 前往 https://render.com → New Web Service
2. 連接 GitHub，選 `backend/` 目錄
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `uvicorn main:app --host 0.0.0.0 --port $PORT`

#### 選項 C：本地測試
```bash
cd backend
pip install -r requirements.txt
python main.py
# 啟動在 http://localhost:8000
```

---

### Step 2：修改前端設定

打開 `frontend/index.html`，找到這行：

```javascript
const PROXY = 'https://YOUR_BACKEND_URL';
```

改為你的後端 URL：

```javascript
const PROXY = 'https://twstock-api.up.railway.app';
```

---

### Step 3：部署前端（選一）

#### 選項 A：Vercel（最簡單）
1. 前往 https://vercel.com → New Project
2. 上傳或連接 GitHub repo
3. 選 `frontend/` 為 root directory
4. Deploy → 取得 `https://twstock.vercel.app`

#### 選項 B：GitHub Pages（免費）
1. 把 `frontend/index.html` 放入 `docs/` 或 `gh-pages` branch
2. GitHub Settings → Pages → 選 branch
3. 網址：`https://你的帳號.github.io/repo名稱/`

#### 選項 C：Cloudflare Pages（免費、快）
1. 前往 https://pages.cloudflare.com
2. 連接 GitHub，選 `frontend/` 資料夾
3. 自動部署

---

## 功能說明

### 即時榜單
- 每 30 秒自動向後端拉取最新資料
- 對比前一次資料，標示「NEW」新上榜股票
- 顯示名次變化（▲/▼）
- 點擊任何股票跳轉至 Yahoo 股市

### 歷史紀錄
- 每次更新後自動存入瀏覽器 localStorage
- 最多保存 60 天
- 可點擊日期查看當日完整排行

### 統計分析
- 常駐榜排行（哪些股票最常進 TOP30）
- 最常新上榜股票
- 每日換榜趨勢圖

---

## CORS 問題說明

TWSE API 不支援跨來源請求（CORS），所以：
- **不能**直接從瀏覽器呼叫 TWSE
- **必須**透過自己的後端 proxy 才能取得資料
- 本專案的後端就是作為 proxy 使用

---

## 擴充方向

- [ ] 加入上櫃（OTC/TPEx）股票
- [ ] 加入 WebSocket 即時推播（盤中）
- [ ] 加入個股歷史成交值趨勢圖
- [ ] 加入三大法人買賣超整合
- [ ] PostgreSQL 持久化歷史資料（取代 JSON 檔）
- [ ] LINE Notify 每日收盤推播排名
