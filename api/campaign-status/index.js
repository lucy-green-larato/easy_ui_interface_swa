const fetchFn = (typeof fetch !== "undefined")
  ? fetch
  : (...args) => import("node-fetch").then(m => m.default(...args));

module.exports = async function (context, req) {
  const runId = (req.query && req.query.runId) || "";
  if (!runId) return { status: 400, body: "Missing runId" };

  const base = process.env.PY_API_BASE || "http://127.0.0.1:7071";
  const url = `${base}/api/campaign/status?runId=${encodeURIComponent(runId)}`;

  try {
    const resp = await fetchFn(url);
    const text = await resp.text();
    const ct = resp.headers.get("content-type") || "application/json";
    const body = /json/.test(ct) ? (text ? JSON.parse(text) : {}) : text;
    return { status: resp.status, headers: { "Content-Type": ct }, body };
  } catch (e) {
    return {
      status: 500, headers: { "Content-Type": "application/json" },
      body: { error: "status error", detail: e.message }
    };
  }
};
