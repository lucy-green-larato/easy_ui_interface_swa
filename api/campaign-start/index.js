module.exports = async function (context, req) {
  const base = process.env.PY_API_BASE || "http://127.0.0.1:7071";
  const url = `${base}/api/orchestrators/CampaignOrchestration`;

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req.body || {})
    });
    const text = await resp.text();
    let runId = "";

    try {
      const js = JSON.parse(text || "{}");
      runId = js.runId || js.id || "";
      if (!runId && js.statusQueryGetUri) {
        const m = String(js.statusQueryGetUri).match(/instances\/([^\/\?]+)/);
        if (m) runId = m[1];
      }
    } catch (_) { /* non-JSON */ }

    if (!resp.ok && !runId) {
      return { status: resp.status, body: text || "", headers: { "Content-Type": "text/plain" } };
    }
    if (!runId) {
      return { status: 502, body: "Could not determine runId from starter response." };
    }
    return { status: 200, body: { runId }, headers: { "Content-Type": "application/json" } };
  } catch (e) {
    return { status: 500, body: `start error: ${e.message}` };
  }
};
