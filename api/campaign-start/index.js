// Proxy to Python Durable starter
const fetchFn = (typeof fetch !== "undefined")
  ? fetch
  : (...args) => import("node-fetch").then(m => m.default(...args));

module.exports = async function (context, req) {
  try {
    const base = (process.env.PY_API_BASE || "http://127.0.0.1:7071").replace(/\/+$/, "");
    const url  = `${base}/api/orchestrators/CampaignOrchestration`;
    const payload = (req.body && typeof req.body === "object") ? req.body : {};

    const r = await fetchFn(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const text = await r.text();
    context.res = {
      status: r.status,
      headers: { "Content-Type": r.headers.get("content-type") || "application/json" },
      body: text
    };
  } catch (e) {
    context.log.error("campaign-start error", e);
    context.res = { status: 502, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: "upstream_unreachable", detail: String(e && e.message || e) }) };
  }
};
