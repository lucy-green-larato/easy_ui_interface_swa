// /api/campaign-fetch/index.js — proxy GET /api/campaign/fetch to Python (7071)
const fetchFn = (typeof fetch !== "undefined")
  ? fetch
  : (...args) => import("node-fetch").then(m => m.default(...args));

module.exports = async function (_context, req) {
  try {
    const file  = (req.query && req.query.file) || "campaign";
    const runId = (req.query && req.query.runId) || "";
    const base  = (process.env.PY_API_BASE || "http://127.0.0.1:7071").replace(/\/+$/, "");

    const qs = new URLSearchParams({ file });
    if (runId) qs.set("runId", runId);

    const url = `${base}/api/campaign/fetch?${qs.toString()}`;
    const r   = await fetchFn(url);
    const body = await r.text();
    const contentType = r.headers.get("content-type") || "application/json";

    // Return via $return binding
    return { status: r.status, headers: { "Content-Type": contentType }, body };
  } catch (e) {
    // Still never crash the host
    return {
      status: 502,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "fetch_proxy_exception", detail: String(e?.message || e) })
    };
  }
};