// api/campaign-status/index.js
const fetchFn = (typeof fetch !== "undefined")
  ? fetch
  : (...args) => import("node-fetch").then(m => m.default(...args));

module.exports = async function (context, req) {
  try {
    const runId = req.query?.runId;
    if (!runId) {
      context.res = { status: 400, jsonBody: { error: "Missing runId" } };
      return;
    }

    const base = (process.env.PY_API_BASE || "http://127.0.0.1:7071").replace(/\/+$/, "");
    const url = `${base}/api/campaign/status?runId=${encodeURIComponent(runId)}`;

    const resp = await fetchFn(url);
    const text = await resp.text();
    const ct = resp.headers.get("content-type") || "application/json";

    // Pass-through exactly what Python returned
    context.res = {
      status: resp.status,
      headers: { "Content-Type": ct },
      body: text
    };
  } catch (e) {
    context.log.error("campaign-status error", e);
    context.res = {
      status: 500,
      jsonBody: { error: "status proxy exception", detail: String(e.message || e) }
    };
  }
};
