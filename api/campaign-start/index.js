// Use fetch if available, otherwise lazy-load node-fetch (Node 16/18 in Functions)
const fetchFn = (typeof fetch !== "undefined")
  ? fetch
  : (...args) => import("node-fetch").then(m => m.default(...args));

module.exports = async function (context, req) {
  try {
    const base = (process.env.PY_API_BASE || "http://127.0.0.1:7071").replace(/\/+$/, "");
    const url = `${base}/api/orchestrators/CampaignOrchestration`; // NOTE: includes /api
    const payload = req.body && typeof req.body === "object" ? req.body : {};

    const r = await fetchFn(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const text = await r.text();
    let data = {};
    try { data = JSON.parse(text); } catch {} // tolerate non-JSON (unlikely)

    if (!r.ok) {
      context.log.warn("Upstream start failed", r.status, text);
      context.res = {
        status: r.status,
        jsonBody: { error: "upstream start failed", detail: data || text || null }
      };
      return;
    }

    const runId = data?.id || extractIdFromStatusUrl(data?.statusQueryGetUri) || null;
    context.res = { status: 200, jsonBody: { ok: true, runId, raw: data } };
  } catch (err) {
    context.log.error("campaign-start error", err);
    context.res = { status: 500, jsonBody: { error: "start proxy exception", detail: String(err) } };
  }
};

function extractIdFromStatusUrl(u) {
  if (!u || typeof u !== "string") return null;
  const m = u.match(/instances\/([^?\/]+)/i);
  return (m && m[1]) || null;
}
