// /api/campaign-fetch/handler.cjs — proxy GET /api/campaign/fetch to Python (7071)
const fetchFn = (typeof fetch !== "undefined")
  ? fetch
  : (...args) => import("node-fetch").then(m => m.default(...args));

module.exports = async function (_context, req) {
  try {
    const file  = req.query?.file || "campaign";
    const runId = req.query?.runId || "";
    const base  = (process.env.PY_API_BASE || "http://127.0.0.1:7071").replace(/\/+$/, "");

    const qs = new URLSearchParams({ file });
    if (runId) qs.set("runId", runId);

    const url  = `${base}/api/campaign/fetch?${qs.toString()}`;
    const resp = await fetchFn(url);

    const contentType = resp.headers.get("content-type") || "application/json";
    // Safer body handling: binary when needed, text otherwise
    const isBinary = /^(application\/(octet-stream|vnd\.|pdf)|image\/|audio\/|video\/)/i.test(contentType);
    const body = isBinary ? Buffer.from(await resp.arrayBuffer()) : await resp.text();

    return {
      status: resp.status,
      headers: { "Content-Type": contentType },
      body
    };
  } catch (e) {
    return {
      status: 502,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "fetch_proxy_exception", detail: String(e?.message || e) })
    };
  }
};
