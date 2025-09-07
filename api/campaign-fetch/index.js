// Proxy GET /api/campaign/fetch → Python 7071
const fetchFn = (typeof fetch !== "undefined") ? fetch : (...a)=>import("node-fetch").then(m=>m.default(...a));
module.exports = async function (_ctx, req) {
  try {
    const file  = req.query?.file || "campaign";
    const runId = req.query?.runId || "";
    const base  = (process.env.PY_API_BASE || "http://127.0.0.1:7071").replace(/\/+$/, "");
    const qs    = new URLSearchParams({ file });
    if (runId) qs.set("runId", runId);

    const r   = await fetchFn(`${base}/api/campaign/fetch?${qs.toString()}`);
    const ct  = r.headers.get("content-type") || "application/json";
    const bin = /^(application\/(octet-stream|vnd\.|pdf)|image\/|audio\/|video\/)/i.test(ct);
    const body = bin ? Buffer.from(await r.arrayBuffer()) : await r.text();

    return { status: r.status, headers: { "Content-Type": ct }, body };
  } catch (e) {
    return { status:502, headers:{ "Content-Type":"application/json" },
             body: JSON.stringify({ error:"fetch_proxy_exception", detail:String(e?.message||e) }) };
  }
};
