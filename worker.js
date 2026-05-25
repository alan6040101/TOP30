/**
 * Cloudflare Worker — TWSE mis.twse.com.tw Proxy
 *
 * 部署方式：
 * 1. 前往 https://workers.cloudflare.com/ 登入（免費帳號即可）
 * 2. 點「Create Worker」
 * 3. 把此檔案內容貼上，點「Save and Deploy」
 * 4. 取得 Worker 網址，例如 https://twse-proxy.你的帳號.workers.dev
 * 5. 把這個網址填入 main.py 的 MIS_PROXY 變數
 *
 * 免費方案：每日 100,000 次請求，完全夠用
 */

const ALLOWED_PATHS = [
  '/stock/api/getStockInfo.jsp',
  '/stock/data/mis_ohlc_TSE.txt',
];

const TWSE_ORIGIN = 'https://mis.twse.com.tw';

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, OPTIONS',
          'Access-Control-Allow-Headers': '*',
        },
      });
    }

    // 只允許特定路徑
    const allowed = ALLOWED_PATHS.some(p => url.pathname.startsWith(p));
    if (!allowed) {
      return new Response('Not allowed', { status: 403 });
    }

    // 轉發請求到 TWSE
    const targetUrl = TWSE_ORIGIN + url.pathname + url.search;
    const proxyReq = new Request(targetUrl, {
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://mis.twse.com.tw/',
        'Accept': '*/*',
      },
    });

    const resp = await fetch(proxyReq);
    const body = await resp.text();

    return new Response(body, {
      status: resp.status,
      headers: {
        'Content-Type': resp.headers.get('Content-Type') || 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-cache',
      },
    });
  },
};
