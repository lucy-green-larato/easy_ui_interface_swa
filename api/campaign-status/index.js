// Proxy to Python status endpoint
const fetchFn = (typeof fetch !== "undefined")
  ? fetch
  : (...args) => import("node-fetch").then(m => m.default(...args));

module.exports = async function (context, req) {
  try {
    const runId = req.query && req.query.runId;
    if (!runId) {
      context.res = { status: 400, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: "Missing runId" }) };
      return;
    }
    const base = (process.env.PY_API_BASE || "http://127.0.0.1:7071").replace(/\/+$/, "");
    const url  = `${base}/api/campaign/status?runId=${encodeURIComponent(runId)}`;

    const r = await fetchFn(url);
    const text = await r.text();
    context.res = {
      status: r.status,
      headers: { "Content-Type": r.headers.get("content-type") || "application/json" },
      body: text
    };
  } catch (e) {
    context.log.error("campaign-status error", e);
    context.res = { status: 500, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: "status proxy exception", detail: String(e && e.message || e) }) };
  }
};
