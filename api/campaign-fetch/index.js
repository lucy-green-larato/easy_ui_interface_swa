module.exports = async function (context, req) {
  const file = (req.query && req.query.file) || "campaign";
  const runId = (req.query && req.query.runId) || "";
  const base = process.env.PY_API_BASE || "http://127.0.0.1:7071";

  const qs = new URLSearchParams({ file });
  if (runId) qs.set("runId", runId);
  const url = `${base}/api/campaign/fetch?${qs.toString()}`;

  try {
    const resp = await fetch(url);
    const text = await resp.text();
    const ct = resp.headers.get("content-type") || "application/json";
    return { status: resp.status, body: /json/.test(ct) ? JSON.parse(text || "{}") : text, headers: { "Content-Type": ct } };
  } catch (e) {
    return { status: 500, body: `fetch error: ${e.message}` };
  }
};
