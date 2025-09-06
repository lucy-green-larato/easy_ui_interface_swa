// Proxy to Python fetch endpoint
const fetchFn = (typeof fetch !== "undefined")
  ? fetch
  : (...args) => import("node-fetch").then(m => m.default(...args));

module.exports = async function (context, req) {
  try {
    const file  = (req.query && req.query.file) || "campaign";
    const runId = (req.query && req.query.runId) || "";
    const base  = (process.env.PY_API_BASE || "http://127.0.0.1:7071").replace(/\/+$/, "");
    const qs = new URLSearchParams({ file });
    if (runId) qs.set("runId", runId);

    const url = `${base}/api/campaign/fetch?${qs.toString()}`;
    const r   = await fetchFn(url);
    const text = await r.text();
    context.res = {
      status: r.status,
      headers: { "Content-Type": r.headers.get("content-type") || "application/json" },
      body: text
    };
  } catch (e) {
    context.log.error("campaign-fetch error", e);
    context.res = { status: 502, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: "fetch proxy exception", detail: String(e && e.message || e) }) };
  }
};
