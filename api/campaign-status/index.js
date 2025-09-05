module.exports = async function (context, req) {
  const runId = (req.query && req.query.runId) || "";
  if (!runId) return { status: 400, body: "Missing runId" };

  const base = process.env.PY_API_BASE || "http://127.0.0.1:7071";
  const url = `${base}/api/campaign/status?runId=${encodeURIComponent(runId)}`;

  try {
    const resp = await fetch(url);
    const text = await resp.text();
    const ct = resp.headers.get("content-type") || "application/json";
    return { status: resp.status, body: /json/.test(ct) ? JSON.parse(text || "{}") : text, headers: { "Content-Type": ct } };
  } catch (e) {
    return { status: 500, body: `status error: ${e.message}` };
  }
};
