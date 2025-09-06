// api/campaign-fetch/index.js
const fetchFn = (typeof fetch !== "undefined")
  ? fetch
  : (...args) => import("node-fetch").then(m => m.default(...args));

module.exports = async function (context, req) {
  try {
    const file = req.query?.file || "campaign";
    const runId = req.query?.runId || "";

    const base = (process.env.PY_API_BASE || "http://127.0.0.1:7071").replace(/\/+$/, "");
    const qs = new URLSearchParams({ file });
    if (runId) qs.set("runId", runId);

    const url = `${base}/api/campaign/fetch?${qs.toString()}`;

    const resp = await fetchFn(url);
    const text = await resp.text();
    const ct = resp.headers.get("content-type") || "application/json";

    // Pass through exactly what Python returned
    context.res = {
      status: resp.status,
      headers: { "Content-Type": ct },
      body: text
    };
  } catch (e) {
    context.log.error("campaign-fetch error", e);
    context.res = {
      status: 500,
      jsonBody: { error: "fetch proxy exception", detail: String(e.message || e) }
    };
  }
};
