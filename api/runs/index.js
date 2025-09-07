// Proxy to Python GET /api/runs
const fetchFn = (typeof fetch !== "undefined") ? fetch : (...args) => import("node-fetch").then(m => m.default(...args));

module.exports = async function (context, req) {
  try {
    const base = (process.env.PY_API_BASE || "http://127.0.0.1:7071").replace(/\/+$/, "");
    const url  = `${base}/api/runs`;
    const r = await fetchFn(url);
    const text = await r.text();
    context.res = { status: r.status, headers: { "Content-Type": r.headers.get("content-type") || "application/json" }, body: text };
  } catch (e) {
    context.log.error("runs proxy error", e);
    context.res = { status: 502, headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "runs_proxy_exception", detail: String(e?.message || e) }) };
  }
};
